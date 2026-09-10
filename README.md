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
