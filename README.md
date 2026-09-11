# jobqueue

A distributed job queue built on PostgreSQL row-level locking. It exposes a REST
API for job submission, hands each job to exactly one worker using
`SELECT ... FOR UPDATE SKIP LOCKED`, executes it as a subprocess, and recovers
jobs from crashed workers through expiring leases. Delivery is at-least-once.
Retries use exponential backoff with a per-job attempt budget.

No message broker and no queue library. Job state lives entirely in Postgres, so
the queue is durable across restarts and horizontally scalable by adding worker
processes.

## Stack

| Layer | Choice |
|---|---|
| Language | Go 1.24+ (stdlib only, except the Postgres driver) |
| Database | PostgreSQL 16 |
| Driver | `lib/pq` over `database/sql`, pooled at 25 connections |
| HTTP | `net/http` with Go 1.22 method/wildcard routing patterns |
| Concurrency | `FOR UPDATE SKIP LOCKED` claiming, lease-based fencing |
| Execution | `os/exec` with process-group kill and bounded output capture |
| Logging | `log/slog`, text or JSON |
| Migrations | `go:embed`, applied on startup in a transaction |
| Container | Multi-stage build, `CGO_ENABLED=0`, non-root uid 10001 |

One external dependency: `github.com/lib/pq`.

## Components

| Binary | Role |
|---|---|
| `queue-server` | REST API, state machine, lease reaper |
| `queue-worker` | Claim → execute → report loop, lease renewal, heartbeats |
| `queue` | CLI client |

Workers reach the database only through the server, so they hold no credentials
and the state machine has a single implementation.

```
cmd/{server,worker,queue}   entrypoints, flag/env config
internal/api                handlers, middleware, error mapping, reaper
internal/jobs               job model, validation, backoff, all SQL
internal/worker             API client, shell executor, run loop
internal/database           connection pool, embedded migrations
migrations                  001_init.sql
tests                       integration tests against real Postgres
```

## Requirements

- Docker and Docker Compose (Postgres, server, workers)
- Go 1.24+ to build the CLI or run the test suite

## Quick start

```bash
docker compose up -d                 # Postgres, server, two workers
go build -o queue ./cmd/queue
./queue submit --type shell --payload "echo hello" --wait
```

```
ID:        1
Status:    COMPLETED
Type:      shell
Payload:   echo hello
Attempts:  1/3
Worker:    fe49b0e927b7-386994
Created:   2026-09-11T18:00:01-05:00
Duration:  4ms
Exit code: 0
--- stdout ---
hello
```

Scale the pool with `docker compose up -d --scale worker=5`.

The server publishes on `127.0.0.1:8080` and Postgres on `127.0.0.1:5433`. Both
host ports are overridable:

```bash
QUEUE_PORT=8090 QUEUE_PG_PORT=55432 docker compose up -d
./queue stats --server http://localhost:8090
```

Without Docker: start Postgres, export `DATABASE_URL`, then run
`go run ./cmd/server` and `go run ./cmd/worker`. The server applies its
migrations on boot.

## CLI

```
queue submit --type shell --payload "<cmd>" [--max-attempts 3] [--wait]
queue get <id>
queue list [--status pending] [--limit 20]
queue cancel <id>
queue workers
queue stats
```

All commands accept `--server URL` (or `$QUEUE_SERVER`) and `--json`.
`submit --wait` polls to a terminal state and exits non-zero on failure.

## Claiming

Workers poll concurrently and each job must be dispatched exactly once. The
claim is a single statement, so there is no read-then-write window:

```sql
UPDATE jobs SET
    status      = 'RUNNING',
    worker_id   = $1,
    lease_until = now() + make_interval(secs => $2),
    attempts    = attempts + 1
WHERE id = (
    SELECT id FROM jobs
    WHERE status = 'PENDING' AND available_at <= now()
    ORDER BY available_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;
```

- `FOR UPDATE` takes a row-level write lock on the selected row for the duration
  of the transaction.
- `SKIP LOCKED` makes concurrent transactions step past locked rows to the next
  candidate instead of blocking, which keeps claim throughput linear in worker
  count rather than serializing on the queue head.
- The subquery and update commit atomically, so no interleaved transaction can
  observe the row as `PENDING` after it is selected.

Two supporting indexes are partial, matching the claim and reaper predicates:

```sql
CREATE INDEX IF NOT EXISTS idx_jobs_claimable
    ON jobs (available_at, id) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_jobs_expired_leases
    ON jobs (lease_until) WHERE status = 'RUNNING';
```

Terminal transitions are fenced by `WHERE status = 'RUNNING' AND worker_id = $2`,
so a worker whose lease lapsed cannot overwrite the result of its replacement.

The concurrency tests were validated by mutation: deleting `FOR UPDATE SKIP
LOCKED` makes them fail immediately.

```
job 24 was claimed 4 times by [w0 w1 w5 w3]; want exactly once
job  4 was claimed 5 times by [w1 w0 w9 w8 w5]; want exactly once
```

Leases, heartbeats, retry scheduling, the state machine and the full API are in
[docs/DESIGN.md](docs/DESIGN.md).

## Benchmarks

The harness that produced these numbers is in the repo
([cmd/queue-bench](cmd/queue-bench/main.go)) -- it drives the real stack over
HTTP and prints the table below:

```bash
docker compose up -d --scale worker=5
make bench          # go run ./cmd/queue-bench --jobs 300 --concurrency 16
```

Measured on a MacBook Air (M1, 8 cores) with Docker Desktop allotted 4 CPUs and
2 GB: Postgres, the server and 5 workers all in containers, 300 no-op jobs
(`true`). Each figure is the range across 4 consecutive runs.

| Metric | Result |
|---|---|
| Submit throughput (16 connections) | 1,320-1,570 jobs/s |
| End-to-end throughput (submit, claim, execute, report) | 315-350 jobs/s |
| Job execution time | <1 ms (a no-op payload; process spawn dominates) |
| Single-job latency, idle pool, n=20 | 19-37 ms fastest, 158-211 ms median, 841 ms slowest |

Read them with the caveats:

- **Run-to-run variance is real.** The ranges above are four runs on an
  otherwise idle machine; an earlier set taken while the machine was busy
  measured submit throughput as low as 910 jobs/s. Everything competes for the
  same 4 CPUs, so this measures this laptop, not Postgres.
- **End-to-end is measured from the first submit**, so it includes submission,
  and it is bounded by 5 single-slot workers: each worker runs one job at a time,
  which is why per-worker concurrency is on the improvement list.
- **End-to-end throughput needs a large enough `--jobs`.** The first claim can
  wait out a poll interval, and that fixed cost is amortized over the run: at
  `--jobs 50` the same stack measures about 100 jobs/s.
- **The latency spread is the polling interval, not execution cost.** An idle
  worker sleeps 1s +/- 25% jitter between claim attempts, so a job arriving just
  after a poll waits out the remainder. Under sustained load workers never idle
  and the throughput figure applies instead. `LISTEN/NOTIFY` would remove the
  wait.
- **Throughput is bounded by Postgres round trips and the 25-connection pool**,
  not by the claim query.

## Tests

```bash
go test -race ./internal/... ./cmd/...      # no database required

docker compose up -d postgres      # Postgres only -- see the note below
TEST_DATABASE_URL='postgres://queue:queue@localhost:5433/queue?sslmode=disable' \
  go test -race ./...
```

> Run the integration tests against Postgres **without** a worker pool attached
> to the same database (`docker compose stop worker` if the full stack is up).
> Live workers claim the tests' jobs, which fails any test asserting on queue
> depth or attempt counts.

57 tests, ~1,950 lines. The suite in `tests/` exercises the real HTTP API
against real Postgres: 50 goroutines racing for one job, 10 workers over 50
jobs, lease expiry and reclaim, stale-worker fencing, retry exhaustion,
idempotent completion, concurrent reapers, and a live three-worker pool. Without
`TEST_DATABASE_URL` the integration package skips. `make test` runs everything.

## Security

`shell` jobs execute arbitrary commands via `sh -c`. The API is remote code
execution by design, with no sandbox.

- Default listen address is loopback; Compose maps host ports to `127.0.0.1`.
- `QUEUE_AUTH_TOKEN` enables bearer auth on every route except `/healthz`,
  compared with `crypto/subtle.ConstantTimeCompare`.
- Container runs as uid 10001.
- Bounded inputs: 256 KB request bodies, 64 KB payloads, 32 KB captured output.
- Parameterized queries throughout; driver errors are logged, never returned.
- `ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout` and `IdleTimeout` are set.

Run it on localhost or a private network.

## Limitations

- At-least-once delivery. A worker that finishes and dies before reporting will
  have its job re-executed, so jobs must be idempotent.
- Cancellation applies to `PENDING` jobs only; the server cannot signal a
  process it did not spawn.
- Up to ~1s dispatch latency on an idle pool (polling, not `LISTEN/NOTIFY`).
- Single job type (`shell`); no priorities, scheduling, or dependencies.
- No retention policy — terminal jobs accumulate, though the partial indexes
  exclude them.
- Throughput ceiling is one Postgres instance: thousands of jobs/s, not millions.
