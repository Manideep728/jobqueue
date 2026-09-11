# QUEUE

A small distributed job queue written in Go, backed by PostgreSQL.

Producers submit jobs over a REST API. A pool of worker processes claims those
jobs, executes them, and reports back. Jobs that fail are retried with
exponential backoff; jobs whose worker dies mid-execution are detected through
expired leases and returned to the queue.

It is deliberately small — about 3,000 lines of Go plus 1,700 lines of tests —
but the mechanics are the real ones: atomic claiming under concurrency, leases,
heartbeats, retry budgets, idempotent completion, and graceful shutdown.

---

## Table of contents

- [Why this exists](#why-this-exists)
- [Architecture](#architecture)
- [Quick start](#quick-start)
- [CLI](#cli)
- [API reference](#api-reference)
- [Job lifecycle](#job-lifecycle)
- [Concurrency model](#concurrency-model-how-duplicate-claims-are-prevented)
- [Retries and backoff](#retries-and-backoff)
- [Leases, heartbeats and failure recovery](#leases-heartbeats-and-failure-recovery)
- [Database schema](#database-schema)
- [Configuration](#configuration)
- [Testing](#testing)
- [Security](#security)
- [Design tradeoffs](#design-tradeoffs)
- [Limitations](#limitations)
- [Future improvements](#future-improvements)

---

## Why this exists

Almost every backend eventually needs to run work outside a request: send the
email, transcode the video, rebuild the report. The usual answer is to reach for
Celery, Sidekiq, or SQS. Those are excellent, and they are also opaque — it is
entirely possible to use one for years without being able to explain how it
guarantees a job is not executed twice.

This project implements those guarantees from scratch, on top of nothing but
PostgreSQL and the Go standard library, so that every mechanism is visible:

- **Atomic claiming** — what actually stops two workers from taking the same job
  (`FOR UPDATE SKIP LOCKED`), demonstrated by a test that fails when the locking
  is removed.
- **Leases** — how a queue notices that a worker died without being told.
- **Retry budgets and backoff** — how a failing job is retried without becoming
  an infinite hot loop.
- **Idempotency** — why a duplicated completion request must not be an error.

The scope is intentionally bounded. This is not a Kafka replacement; it is a
correct, understandable queue.

---

## Architecture

```
    ┌──────────────┐                          ┌──────────────┐
    │  queue CLI   │                          │  your app    │
    │  (submit,    │                          │  (any HTTP   │
    │   list, get) │                          │   client)    │
    └──────┬───────┘                          └──────┬───────┘
           │                                         │
           │            HTTP / JSON                  │
           └────────────────────┬────────────────────┘
                                │
                                ▼
                   ┌─────────────────────────┐
                   │      queue-server       │
                   │                         │
                   │  • REST API             │
                   │  • claim (atomic)       │
                   │  • state transitions    │
                   │  • lease reaper  ───────┼──┐  background sweep,
                   │  • structured logs      │  │  every 5s
                   └───────────┬─────────────┘  │
                               │                │
                               │  SQL           │
                               ▼                │
                   ┌─────────────────────────┐  │
                   │      PostgreSQL         │◀─┘
                   │                         │
                   │  jobs     (state)       │   the single source of truth:
                   │  workers  (liveness)    │   the server keeps no queue
                   └─────────────────────────┘   state in memory
                               ▲
                               │  (workers never talk to the database)
                               │
           ┌───────────────────┴───────────────────┐
           │              HTTP / JSON              │
    ┌──────┴───────┐   ┌──────────────┐   ┌────────┴─────┐
    │ queue-worker │   │ queue-worker │   │ queue-worker │
    │              │   │              │   │              │
    │ register     │   │  claim →     │   │  renew lease │
    │ heartbeat    │   │  execute →   │   │  report      │
    └──────────────┘   └──────────────┘   └──────────────┘
```

Three binaries, one database:

| Component | Path | Responsibility |
|---|---|---|
| `queue-server` | `cmd/server` | REST API, state transitions, lease reaper |
| `queue-worker` | `cmd/worker` | Claims jobs, executes them, reports results |
| `queue` | `cmd/queue` | CLI client for humans |

```
.
├── cmd/
│   ├── server/          queue-server entrypoint (flags, wiring, shutdown)
│   ├── worker/          queue-worker entrypoint
│   └── queue/           CLI client
├── internal/
│   ├── api/             HTTP handlers, middleware, error mapping, reaper
│   ├── database/        connection pool and embedded migrations
│   ├── jobs/            job model, validation, backoff, and all SQL
│   └── worker/          API client, shell executor, claim/execute/report loop
├── migrations/          001_init.sql, embedded into the binary
├── tests/               integration tests against a real PostgreSQL
├── Dockerfile
└── docker-compose.yml
```

**Workers never touch the database.** Every state change goes through the
server, so the rules about what a worker is allowed to do live in exactly one
place. It also means a worker needs no database credentials.

---

## Quick start

### With Docker (recommended)

```bash
docker compose up -d
```

That starts PostgreSQL, the queue server on `127.0.0.1:8080`, and two workers.

```bash
# Build the CLI (needs Go 1.24+), or use the one inside the image
go build -o queue ./cmd/queue

./queue submit --type shell --payload "echo hello" --wait
```

```
ID:        1
Status:    COMPLETED
Type:      shell
Payload:   echo hello
Attempts:  1/3
Worker:    bd648814fbe3-2673f0
Duration:  10ms
Exit code: 0
--- stdout ---
hello
```

Scale the pool and watch work spread out:

```bash
docker compose up -d --scale worker=5
for i in $(seq 1 20); do ./queue submit --type shell --payload "echo job-$i; sleep 0.2"; done
./queue workers
```

```
ID                     ALIVE   PROCESSED  CURRENT JOB  LAST HEARTBEAT
bd648814fbe3-2673f0    true    11         -            4s ago
ad99eae9b507-77a28b    true    10         -            4s ago
```

> **Port conflicts.** The compose file publishes Postgres on host port **5433**
> (not 5432, which a locally installed Postgres usually occupies) and the server
> on **8080**. Override either if they clash:
> `QUEUE_PORT=8090 QUEUE_PG_PORT=55432 docker compose up -d`

### Without Docker

```bash
# 1. A PostgreSQL to talk to
docker run -d --name queue-pg -e POSTGRES_USER=queue -e POSTGRES_PASSWORD=queue \
  -e POSTGRES_DB=queue -p 5433:5432 postgres:16-alpine

export DATABASE_URL='postgres://queue:queue@localhost:5433/queue?sslmode=disable'

# 2. The server (migrations run automatically on startup)
go run ./cmd/server

# 3. A worker, in another terminal
go run ./cmd/worker --server http://localhost:8080

# 4. Submit
go run ./cmd/queue submit --type shell --payload "echo hello" --wait
```

---

## CLI

```
queue submit --type shell --payload "echo hello" [--max-attempts 3] [--wait]
queue get <id>
queue list [--status pending] [--limit 20]
queue cancel <id>
queue workers
queue stats
```

Every command accepts `--server URL` (or `$QUEUE_SERVER`) and `--json` for raw
output that pipes into `jq`.

```bash
$ queue submit --type shell --payload "echo hello"
Job submitted successfully.
ID: 42
Status: PENDING

$ queue list
ID     STATUS     ATTEMPTS  WORKER                 PAYLOAD
42     COMPLETED  1/3       worker-a3f1            echo hello
41     FAILED     2/2       worker-b7c2            exit 7

$ queue stats
Jobs:
  PENDING:   0
  RUNNING:   1
  COMPLETED: 21
  FAILED:    1
  CANCELED:  0
Workers: 2 alive / 2 total
```

`--wait` blocks until the job reaches a terminal state and exits non-zero if it
did not succeed, so it composes with shell scripts:

```bash
queue submit --type shell --payload "./deploy.sh" --wait && echo "deployed"
```

---

## API reference

All requests and responses are JSON. Errors always have the shape
`{"error": "human readable", "code": "machine_readable"}`.

### Jobs

#### `POST /jobs` — create a job

```bash
curl -X POST localhost:8080/jobs \
  -d '{"type":"shell","payload":"echo hello","max_attempts":3}'
```

`max_attempts` defaults to 3. Returns **201** with the created job, or **400**
with an explanation.

#### `GET /jobs` — list jobs

Query parameters: `status` (case-insensitive), `limit` (default 100, max 500),
`offset`.

```bash
curl 'localhost:8080/jobs?status=pending&limit=10'
```

#### `GET /jobs/{id}` — one job, including its captured result

**200**, or **404** if it does not exist, or **400** if the id is not a positive
integer.

#### `POST /jobs/{id}/cancel` — cancel a PENDING job

**200** on success (and on a repeat cancel — it is idempotent), **409** if the
job is already RUNNING or terminal, **404** if unknown.

A RUNNING job cannot be canceled: the queue has no way to reach into another
process and stop a command it already started, so claiming otherwise would be a
lie. See [Limitations](#limitations).

### Worker protocol

#### `POST /workers/register`

```json
{"worker_id": "worker-a3f1", "hostname": "box-01"}
```

**201.** Registration is an upsert, so a restarted worker keeps its identity.
Claiming requires a registered worker.

#### `POST /workers/{id}/heartbeat`

**200**, or **404** if the server does not know this worker — which tells the
worker to register again rather than heartbeat into the void.

#### `POST /jobs/claim` — atomically claim one job

```json
{"worker_id": "worker-a3f1", "lease_seconds": 30}
```

**200** with the claimed job (now `RUNNING`, `attempts` incremented), or **404**
with code `no_jobs_available` when the queue is empty — the normal idle case, so
workers can distinguish it from a real error. `lease_seconds` is clamped by the
server to `[MinLease, MaxLease]`.

#### `POST /jobs/{id}/lease` — renew a lease

```json
{"worker_id": "worker-a3f1", "lease_seconds": 30}
```

**200**, or **409** if this worker no longer owns the job — which is the signal
to stop executing it immediately.

#### `POST /jobs/{id}/complete`

```json
{"worker_id": "worker-a3f1", "stdout": "hello\n", "stderr": "",
 "exit_code": 0, "duration_ms": 12}
```

**200.** Repeating a completion you already made returns **200** again (an
idempotent replay — the realistic cause is a retried request whose first
response was lost). **409** if you are not the current owner.

#### `POST /jobs/{id}/fail`

Same body plus `"error": "why it failed"`. The server decides what happens next:
back to `PENDING` with a backoff delay if attempts remain, otherwise `FAILED`.

### Operations

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness — actually queries the database, so it fails when the database is unreachable |
| `GET /stats` | Job counts by status, worker counts |
| `GET /workers` | All workers with liveness, jobs processed, current job |

### Status codes

| Code | Meaning |
|---|---|
| 400 | Malformed JSON, unknown field, invalid id, failed validation |
| 401 | Missing or wrong bearer token (only when `QUEUE_AUTH_TOKEN` is set) |
| 404 | No such job or worker; also `no_jobs_available` on claim |
| 405 | Wrong method for a known path |
| 409 | The job's state contradicts the request (not the owner, not cancelable) |
| 500 | Unexpected server-side error (details are logged, never returned) |
| 503 | Database unreachable (health check) |

---

## Job lifecycle

```
                      POST /jobs
                          │
                          ▼
                     ┌─────────┐
        cancel ◀─────│ PENDING │◀──────────────────────┐
           │         └────┬────┘                       │
           ▼              │ POST /jobs/claim           │ attempts < max_attempts
    ┌──────────┐          │ (atomic; attempts += 1;    │ available_at = now + backoff
    │ CANCELED │          │  lease_until = now + 30s)  │
    └──────────┘          ▼                            │
                     ┌─────────┐                       │
                     │ RUNNING │───────────────────────┤
                     └────┬────┘   POST /jobs/{id}/fail│
                          │        or lease expired    │
        POST /jobs/{id}/  │                            │
             complete     │                            │ attempts >= max_attempts
                          ▼                            ▼
                   ┌───────────┐                  ┌────────┐
                   │ COMPLETED │                  │ FAILED │
                   └───────────┘                  └────────┘
```

Terminal states are `COMPLETED`, `FAILED` and `CANCELED`. A job in any of them
is never claimed again.

Two details worth noting:

**`attempts` is incremented at claim time, not at failure time.** If it were
incremented on failure, a worker that crashes without reporting would never burn
an attempt, and a job that reliably kills its worker would be retried forever.

**`CANCELED` is a separate state from `FAILED`.** "A human stopped this" and
"this exhausted its retries" are different operational events; collapsing them
would make the job list lie about what happened.

---

## Concurrency model: how duplicate claims are prevented

This is the heart of the project. The entire claim is one SQL statement:

```sql
UPDATE jobs SET
    status      = 'RUNNING',
    worker_id   = $1,
    lease_until = now() + make_interval(secs => $2),
    attempts    = attempts + 1,
    updated_at  = now()
WHERE id = (
    SELECT id FROM jobs
    WHERE status = 'PENDING' AND available_at <= now()
    ORDER BY available_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;
```

- **`FOR UPDATE`** takes a row lock on the candidate. A second transaction
  cannot modify that row until the first commits.
- **`SKIP LOCKED`** tells Postgres not to *wait* for a locked row but to move
  past it to the next candidate. This is what turns correctness into throughput:
  ten workers claiming simultaneously walk to ten different rows instead of
  queueing behind one.
- Because it is a single statement, it is atomic on its own. (The implementation
  still wraps it in a transaction, in order to stamp the worker's `current_job_id`
  in the same commit.)

The naive version — `SELECT` a pending job, then `UPDATE` it in a second
statement — looks equivalent and is not. Between the two statements, every other
worker can read the same row.

**This is tested, and the test is verified to work.** `TestConcurrentClaimSingleJob`
fires 50 goroutines at one job and asserts exactly one wins;
`TestConcurrentClaimManyJobs` races 10 workers over 50 jobs and asserts every job
was claimed exactly once. Removing `FOR UPDATE SKIP LOCKED` from the query makes
them fail immediately:

```
concurrency_test.go:124: job 24 was claimed 4 times by [w0 w1 w5 w3]; want exactly once
concurrency_test.go:124: job  4 was claimed 5 times by [w1 w0 w9 w8 w5]; want exactly once
```

Everything else that could race is guarded the same way. Every terminal update
is scoped `WHERE id = $1 AND status = 'RUNNING' AND worker_id = $2`, so a worker
whose lease expired cannot overwrite the result of the worker that took over.
The reaper also uses `SKIP LOCKED`, so several server instances can sweep
concurrently without double-requeuing anything.

---

## Retries and backoff

A job carries a retry budget (`max_attempts`, default 3). When a worker reports
failure:

- attempts remain → back to `PENDING`, with `available_at = now() + backoff`
- budget exhausted → `FAILED`, permanently

Backoff is exponential — `base * 2^(attempt-1)`, capped:

| Attempt | Delay before the next try |
|---|---|
| 1 | 1s |
| 2 | 2s |
| 3 | 4s |
| 4 | 8s |
| … | doubling, capped at 5 minutes |

Tunable with `--retry-base-seconds` and `--retry-max-seconds`.

Exponential rather than fixed backoff matters for two reasons: a permanently
broken job stops burning a worker in a hot loop, and a struggling downstream
dependency is not hit by a synchronized retry stampede. Workers additionally
apply ±25% jitter to their *polling* interval, so a pool started at the same
moment does not fall into lockstep against the database.

The delay is computed in Go, not in SQL, so the failure path and the
lease-expiry path share one implementation and cannot drift apart.

---

## Leases, heartbeats and failure recovery

A worker can die in ways it cannot report: `kill -9`, a power loss, a network
partition. There is no callback for that. The only usable evidence is the
absence of a signal.

**Leases.** Claiming a job sets `lease_until = now() + 30s`. While executing,
the worker renews the lease every ⅓ of its duration (so two consecutive renewal
failures are survivable before the lease actually lapses). The server's reaper
sweeps every 5 seconds:

```sql
SELECT id, attempts, max_attempts FROM jobs
WHERE status = 'RUNNING' AND lease_until < now()
FOR UPDATE SKIP LOCKED LIMIT 100;
```

Anything it finds goes back to `PENDING` (or to `FAILED` if the retry budget is
spent), with the reason recorded in `last_error`.

Demonstrated end to end:

```console
$ queue submit --type shell --payload "sleep 60"     # claimed by a worker
$ docker kill -s KILL $(docker compose ps -q worker | head -1)
$ sleep 40 && queue get 22

ID:        22
Status:    PENDING
Attempts:  1/3
Worker:    -
Retry at:  2026-09-10T13:31:13-05:00
Error:     lease expired (worker b69e9df81693-452059 stopped reporting)
```

```json
{"level":"WARN","msg":"lease_expired","job_id":22,
 "worker_id":"b69e9df81693-452059","attempts":1,
 "new_status":"PENDING","event":"requeued_for_retry"}
```

**Losing a lease stops execution.** If a renewal comes back **409**, the worker
knows the job was reassigned and cancels the running command immediately.
Continuing would produce exactly the double execution leases exist to prevent.

**Heartbeats** are separate from leases and answer a different question. A lease
says "this *job* is still being worked on"; a heartbeat says "this *worker* is
still alive". Workers beat every 10 seconds; a worker silent for longer than
`--heartbeat-timeout` (60s) is reported as not alive by `GET /workers`.
Heartbeats are observability, not correctness — job recovery depends only on
leases, so a heartbeat storm or outage can never cause a job to be lost or
duplicated.

### Every failure case, and what happens

| Failure | Behavior |
|---|---|
| Job exits non-zero | Recorded with exit code, stdout and stderr; retried with backoff |
| Job hangs | Killed after `--job-timeout-seconds` (300s); the whole process group is killed, so children do not survive |
| Worker crashes (`kill -9`) | Lease expires; the reaper requeues the job |
| Worker loses its lease | Next renewal returns 409; the worker cancels the running command and abandons the job |
| Worker shuts down gracefully | Running command is canceled and the failure reported at once, so the job is retried immediately instead of waiting out the lease |
| Server restarts | No state is lost — everything lives in Postgres. In-flight jobs are recovered by lease expiry |
| Server unreachable | Workers keep retrying with backoff instead of exiting; a failed report is retried 4 times, then left to lease expiry |
| Database unreachable | `/healthz` returns 503; the reaper logs and retries on the next tick rather than dying |
| Retries exhausted | `FAILED`, with `last_error` explaining why |
| Duplicate completion | Idempotent 200 — the worker's counter is not double-incremented |
| Stale worker reports late | 409; the job stays with its current owner |
| Unknown / malformed job id | 404 / 400, never a 500 |
| Malformed JSON, unknown field | 400 with a message naming the problem |
| Oversized payload or output | Payloads capped at 64KB (400); captured output truncated at 32KB with a marker |
| Panic in a handler | Recovered, logged, returned as 500; the process stays up |

---

## Database schema

Two tables. `jobs` is the queue; `workers` is liveness bookkeeping.

```sql
CREATE TABLE jobs (
    id               BIGSERIAL PRIMARY KEY,
    type             TEXT        NOT NULL,
    payload          TEXT        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'PENDING'
                       CHECK (status IN ('PENDING','RUNNING','COMPLETED','FAILED','CANCELED')),
    attempts         INTEGER     NOT NULL DEFAULT 0,
    max_attempts     INTEGER     NOT NULL DEFAULT 3,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    available_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    worker_id        TEXT,
    lease_until      TIMESTAMPTZ,
    last_error       TEXT,
    result_stdout    TEXT,
    result_stderr    TEXT,
    result_exit_code INTEGER,
    duration_ms      BIGINT
);
```

### Indexes

```sql
CREATE INDEX idx_jobs_claimable      ON jobs (available_at, id) WHERE status = 'PENDING';
CREATE INDEX idx_jobs_expired_leases ON jobs (lease_until)      WHERE status = 'RUNNING';
CREATE INDEX idx_jobs_status_id      ON jobs (status, id DESC);
CREATE INDEX idx_jobs_worker         ON jobs (worker_id) WHERE worker_id IS NOT NULL;
```

The first two are **partial indexes**, matching the exact `WHERE` clauses of the
claim query and the reaper sweep. This is the single most important performance
decision in the schema: a completed job leaves the claimable index entirely, so
the queue does not get slower as millions of finished rows accumulate. A plain
index on `(status, available_at)` would keep growing forever.

`available_at` doing double duty — scheduling *and* retry backoff — means
"delayed job" and "retry later" need no separate machinery.

Migrations are embedded into the binary with `go:embed` and applied on startup,
tracked in a `schema_migrations` table, each inside its own transaction. Running
them twice is a no-op, which is why `docker compose up` works with nothing else
installed.

---

## Configuration

Every flag has an environment-variable equivalent; flags win.

### Server

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--addr` | `QUEUE_ADDR` | `127.0.0.1:8080` | Listen address |
| `--database-url` | `DATABASE_URL` | `postgres://queue:queue@localhost:5432/queue?sslmode=disable` | Postgres DSN |
| `--lease-seconds` | `QUEUE_LEASE_SECONDS` | `30` | Default lease granted on claim |
| `--reap-seconds` | `QUEUE_REAP_SECONDS` | `5` | Lease sweep interval |
| `--heartbeat-timeout` | `QUEUE_HEARTBEAT_TIMEOUT` | `60` | Silence before a worker is "not alive" |
| `--retry-base-seconds` | `QUEUE_RETRY_BASE_SECONDS` | `1` | Backoff base |
| `--retry-max-seconds` | `QUEUE_RETRY_MAX_SECONDS` | `300` | Backoff cap |
| `--log-level` / `--log-format` | `QUEUE_LOG_LEVEL` / `QUEUE_LOG_FORMAT` | `info` / `text` | `debug…error`, `text` or `json` |
| — | `QUEUE_AUTH_TOKEN` | unset | Require `Authorization: Bearer …` |

`QUEUE_AUTH_TOKEN` is environment-only on purpose: a flag would put the secret
in the process list, readable by any other user on the machine.

### Worker

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--server` | `QUEUE_SERVER` | `http://localhost:8080` | Queue server URL |
| `--id` | `QUEUE_WORKER_ID` | `hostname-<random>` | Worker identity |
| `--lease-seconds` | `QUEUE_LEASE_SECONDS` | `30` | Lease to request |
| `--poll-seconds` | `QUEUE_POLL_SECONDS` | `1` | Idle poll interval (±25% jitter) |
| `--heartbeat-seconds` | `QUEUE_HEARTBEAT_SECONDS` | `10` | Heartbeat interval |
| `--job-timeout-seconds` | `QUEUE_JOB_TIMEOUT_SECONDS` | `300` | Kill a job that runs longer |

### Observability

Structured logging via `log/slog`, `text` for humans and `json` for machines.
Every event carries a timestamp, the component, and the relevant job and worker
ids:

```json
{"time":"2026-09-10T18:31:12Z","level":"INFO","msg":"job_claimed","component":"server",
 "job_id":22,"worker_id":"worker-a3f1","attempt":1,"lease_seconds":30}
```

Events: `job_created`, `job_claimed`, `job_started`, `job_completed`,
`job_failed`, `job_retrying`, `job_canceled`, `lease_renewed`, `lease_lost`,
`lease_expired`, `worker_registered`, `worker_started`, `worker_stopped`,
`reaper_started`, `http_request`.

Claim polls against an empty queue log at `debug`, so an idle pool does not
drown the log in noise.

---

## Testing

```bash
# Unit tests only — no database needed
go test -race ./internal/...

# Everything, including integration tests
docker compose up -d postgres
TEST_DATABASE_URL='postgres://queue:queue@localhost:5433/queue?sslmode=disable' \
  go test -race ./...
```

The integration package skips itself entirely when `TEST_DATABASE_URL` is unset,
so `go test ./...` still works on a machine with no database.

The suite runs under `-race` because the interesting code is concurrent: the
worker pool, the lease-renewal goroutine, and the shared output buffers that
`exec` writes to from multiple goroutines.

**Unit tests** (`internal/jobs`, `internal/worker`) cover backoff arithmetic
including integer-overflow edges, input validation, output truncation, and the
shell executor: exit codes, stderr capture, timeouts, parent cancellation,
unbounded output, and commands that cannot start.

**Integration tests** (`tests/`) run against real PostgreSQL through the real
HTTP API — deliberately, since the correctness that matters lives in SQL. A test
against a mocked store would pass while the actual queue handed one job to two
workers.

| Test | What it would catch |
|---|---|
| `TestConcurrentClaimSingleJob` | Two workers claiming the same job |
| `TestConcurrentClaimManyJobs` | Duplicate or lost jobs under load; work not being distributed |
| `TestConcurrentCompletionOfSameJob` | Racing duplicate reports double-counting a worker's total |
| `TestConcurrentReapersDoNotDoubleRequeue` | Two servers both requeuing one expired lease |
| `TestStaleWorkerCannotOverwriteReassignedJob` | A late worker overwriting the new owner's result |
| `TestLeaseExpiryRequeuesJob` | A crashed worker stranding its job forever |
| `TestLeaseExpiryRespectsMaxAttempts` | A crash-looping job being retried forever |
| `TestReaperLeavesHealthyLeasesAlone` | The reaper stealing jobs that are progressing fine |
| `TestRetryUntilMaxAttempts` | Retries not happening, or happening past the budget |
| `TestBackoffDelaysRetry` | A failing job becoming a hot retry loop |
| `TestDuplicateCompletionIsIdempotent` | A retried request erroring or double-counting |
| `TestWorkerAbandonsJobWhenLeaseIsLost` | A worker executing a job it no longer owns |
| `TestWorkerPoolEndToEnd` | Any break across claim → execute → report with a live pool |
| `TestClaimRequiresRegistration` | Untracked workers; a rejected claim burning an attempt |
| `TestCancelJob` | Canceling a RUNNING job; a canceled job being claimed |
| API validation tests | Malformed JSON, bad ids, bad filters returning 500 instead of 4xx |

Each test's comment names the specific bug it exists to catch. The concurrency
tests were verified by mutation: deleting `FOR UPDATE SKIP LOCKED` from the claim
query makes them fail loudly, which is what proves they are not passing
vacuously.

---

## Security

**This queue executes arbitrary shell commands submitted through its API. It is
a remote code execution service by design.**

The `shell` job type means exactly what it says: a payload is passed to `sh -c`.
There is no sandbox and no attempt at one — an escape-proof sandbox is a far
larger project than this queue, and a half-hearted one is worse than none
because it invites false confidence.

The protections are operational:

- **Localhost by default.** The server binds `127.0.0.1`, and the compose file
  publishes to `127.0.0.1` only.
- **Optional bearer token.** Set `QUEUE_AUTH_TOKEN` to require
  `Authorization: Bearer …` on every endpoint except `/healthz`. Compared in
  constant time, so the check cannot be defeated by timing one character at a
  time.
- **Unprivileged container user.** The image runs as uid 10001, so a submitted
  job is not root.
- **Bounded inputs.** Request bodies capped at 256KB, payloads at 64KB, stored
  output at 32KB — one client cannot exhaust memory or disk.
- **Parameterized SQL everywhere.** No string concatenation, so job payloads
  cannot become SQL.
- **Errors do not leak internals.** Database errors are logged in full and
  returned as a generic 500; driver messages otherwise expose schema details.
- **HTTP timeouts** on read, write and idle, so a client that connects and goes
  silent cannot pin a goroutine forever.

If you ever run this somewhere real: put it behind a private network, set a
token, run workers in throwaway containers with dropped capabilities, and treat
the job payload as untrusted input — because it is.

---

## Design tradeoffs

**PostgreSQL as the queue, not a message broker.** A real broker pushes to
consumers and holds far higher throughput. Polling a database ceilings out
around a few thousand jobs/second and adds latency equal to half the poll
interval. In exchange: job state is queryable with SQL, transactional with the
rest of your data, durable by default, and operable with tools every backend
developer already has. Below a few thousand jobs a second — which is most
systems — this is the better trade, and it is why the pattern is so common in
production.

**Polling instead of `LISTEN/NOTIFY`.** Postgres can push notifications, which
would cut idle latency to near zero. Polling was chosen because it is trivially
understandable, degrades gracefully (a missed notification is invisible; the
next poll picks the job up), and needs no reconnection logic. The cost is up to
one poll interval of latency and a steady trickle of empty queries — jittered
and hitting a partial index, so it is cheap.

**Workers speak HTTP, not SQL.** A direct database connection would remove a
network hop. Going through the API means the state machine lives in one place,
workers hold no credentials, and a worker can run anywhere it can reach the
server. Worth one hop.

**Server-side state machine.** Workers report facts ("it exited 7"); only the
server decides consequences ("retry in 4 seconds" vs "give up"). Retry policy
therefore cannot drift between worker versions.

**`attempts` incremented at claim time.** Costs an attempt when a worker dies
mid-job, which is slightly pessimistic. The alternative lets a job that reliably
crashes its worker retry forever — a much worse failure mode.

**Idempotent duplicate completion.** Returning 409 for a repeat would be more
literal. But the realistic cause is a retried request whose first response was
lost, and the intent already succeeded; erroring would make a correct worker
look broken.

**At-least-once, not exactly-once.** A worker can finish a job and die before
reporting; the job is then retried and the work happens twice. Exactly-once
across a process boundary is not achievable without cooperation from the job
itself — so this queue makes the honest guarantee and asks that jobs be
idempotent. See [Limitations](#limitations).

**No job priorities or dependencies.** Both are genuinely useful and both add
scheduling complexity that would obscure the mechanisms this project exists to
show.

---

## Limitations

Known and deliberate:

- **At-least-once delivery.** A job may execute more than once if a worker dies
  between finishing and reporting. Write idempotent jobs.
- **No exactly-once semantics**, for the reason above.
- **A RUNNING job cannot be canceled.** The queue cannot stop a command another
  process already started. Cancel only applies to PENDING jobs.
- **One job type (`shell`).** Adding a type means extending one switch in the
  worker; the queue itself is type-agnostic.
- **No sandbox.** See [Security](#security).
- **No priorities, no scheduled/cron jobs, no job dependencies or workflows.**
- **No dead-letter queue.** Failed jobs stay in the table with their error; there
  is no separate retry-later store.
- **Polling latency.** Up to one poll interval (1s by default) before an idle
  worker notices new work.
- **Single-database throughput ceiling.** Fine for thousands of jobs/second, not
  for millions.
- **No automatic cleanup.** Completed jobs accumulate; there is no retention
  policy yet (the partial indexes mean they do not slow the queue down, but they
  do use disk).
- **`GET /jobs` uses offset pagination**, which drifts if rows are inserted while
  you page through.

---

## Future improvements

Roughly in order of value per unit of complexity:

1. **Retention / archival** — move terminal jobs older than N days to an archive
   table or delete them. The only real operational gap today.
2. **`LISTEN/NOTIFY` wakeups**, with polling kept as the fallback: near-zero idle
   latency without giving up the graceful-degradation property.
3. **Prometheus metrics** — queue depth by status, claim latency, execution
   duration histograms, retry and lease-expiry rates. The structured logs already
   carry the data; this makes it graphable.
4. **Job priorities** — one `priority` column and one `ORDER BY` change, plus a
   plan for starvation.
5. **Scheduled jobs** — `available_at` already supports "run later"; this is
   mostly an API and a CLI flag.
6. **More job types** — HTTP callback, container run — behind the same executor
   interface.
7. **Dead-letter queue** with a `queue retry <id>` command for manual recovery.
8. **Cursor pagination** on `GET /jobs`, replacing offsets.
9. **Per-worker concurrency** — one worker process running N jobs at once, rather
   than one at a time.
10. **A small web dashboard** over the existing API.
