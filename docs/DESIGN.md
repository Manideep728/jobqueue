# Design notes

Reference material that doesn't belong in the README.

## Job lifecycle

```
            POST /jobs
                |
                v
  cancel <-- PENDING <-----------------------------+
    |           |                                  | fail or lease expired,
    v           | claim (attempts + 1, lease set)  | attempts left
 CANCELED       v                                  |
             RUNNING ------------------------------+
                |                  |
        complete|                  | fail or lease expired,
                v                  | no attempts left
            COMPLETED              v
                                 FAILED
```

`COMPLETED`, `FAILED` and `CANCELED` are final. A job in one of those states is
never claimed again.

The attempt counter goes up when a job is claimed, not when it fails. So a
worker that crashes without reporting still uses up one attempt.

## Retries

Each job has `max_attempts` (default 3). When a worker reports a failure:

- If attempts are left, the job goes back to `PENDING` with
  `available_at = now() + delay`.
- If none are left, it becomes `FAILED`.

The delay is `base * 2^(attempt - 1)`: 1s, 2s, 4s, 8s, and so on, up to 5
minutes. Both values can be changed with `--retry-base-seconds` and
`--retry-max-seconds`. The same function sets the delay when a lease expires.

Workers also add ±25% random jitter to their poll interval.

## Leases and recovery

A claim sets `lease_until` 30 seconds ahead. While a job runs, the worker
renews the lease every 10 seconds. A background loop on the server checks every
5 seconds for expired leases:

```sql
SELECT id, attempts, max_attempts FROM jobs
WHERE status = 'RUNNING' AND lease_until < now()
FOR UPDATE SKIP LOCKED LIMIT 100;
```

Each expired job goes back to `PENDING`, or to `FAILED` if it has no attempts
left. `last_error` records which worker stopped responding.

If a renewal comes back 409, the job has been given to another worker. The
original worker kills its command and moves on.

`complete` and `fail` only update the row
`WHERE status = 'RUNNING' AND worker_id = <caller>`. A worker whose lease already
expired gets a 409 and can't overwrite the new owner's result.

Heartbeats are separate from leases. Workers send one every 10 seconds, and
`GET /workers` marks a worker dead after 60 seconds without one. They're only
for monitoring. Job recovery uses leases alone.

## Failure handling

| What happens | Result |
|---|---|
| Command exits non-zero | Exit code, stdout and stderr stored; retried |
| Command runs too long | Killed after 300s, including child processes |
| Worker killed | Lease expires, job requeued |
| Worker stopped with SIGTERM | Command killed, failure reported, job retried right away |
| Server restarts | Nothing lost; running jobs recovered by lease expiry |
| Server unreachable | Workers keep retrying; failed reports retried 4 times |
| Same completion sent twice | Second one returns 200 and changes nothing |
| Late report from old worker | 409 |
| Payload over 64KB | 400 |
| Output over 32KB | Truncated, with a marker |
| Handler panics | 500, server keeps running |

## Schema

Two tables. `jobs` holds all queue state. `workers` tracks registration and
heartbeats. Migrations live in `migrations/`, are compiled into the server
binary, and run on startup. Each migration runs once, inside a transaction.

Indexes:

```sql
CREATE INDEX idx_jobs_claimable      ON jobs (available_at, id) WHERE status = 'PENDING';
CREATE INDEX idx_jobs_expired_leases ON jobs (lease_until)      WHERE status = 'RUNNING';
CREATE INDEX idx_jobs_status_id      ON jobs (status, id DESC);
CREATE INDEX idx_jobs_worker         ON jobs (worker_id) WHERE worker_id IS NOT NULL;
```

The first two are partial indexes. They match the claim query and the reaper
query, and they only hold rows those queries can return. Completed jobs aren't
in them, so they stay small as the table grows.

`available_at` handles both delayed jobs and retry backoff.

## API

Requests and responses are JSON. Errors look like
`{"error": "...", "code": "..."}`.

| Method | Path | Notes |
|---|---|---|
| POST | `/jobs` | `{"type":"shell","payload":"...","max_attempts":3}` returns 201 |
| GET | `/jobs` | `?status=`, `?limit=` (max 500), `?offset=` |
| GET | `/jobs/{id}` | Includes stdout, stderr, exit code, duration |
| POST | `/jobs/{id}/cancel` | `PENDING` only; 409 otherwise |
| POST | `/jobs/claim` | `{"worker_id":"...","lease_seconds":30}`; 404 `no_jobs_available` when empty |
| POST | `/jobs/{id}/lease` | Renew; 409 if the caller no longer owns the job |
| POST | `/jobs/{id}/complete` | `worker_id`, `stdout`, `stderr`, `exit_code`, `duration_ms` |
| POST | `/jobs/{id}/fail` | Same fields plus `error` |
| POST | `/workers/register` | `{"worker_id":"...","hostname":"..."}`; required before claiming |
| POST | `/workers/{id}/heartbeat` | 404 means register again |
| GET | `/workers` | Liveness, jobs processed, current job |
| GET | `/stats` | Job counts by status, worker counts |
| GET | `/healthz` | Queries the database; 503 if it can't |

Status codes: 400 bad input, 401 bad token, 404 not found, 405 wrong method,
409 state conflict, 500 unexpected error (details go to the log only), 503
database down.

## Configuration

Every flag has an environment variable. If both are set, the flag wins.

Server:

| Flag | Env | Default |
|---|---|---|
| `--addr` | `QUEUE_ADDR` | `127.0.0.1:8080` |
| `--database-url` | `DATABASE_URL` | `postgres://queue:queue@localhost:5432/queue?sslmode=disable` |
| `--lease-seconds` | `QUEUE_LEASE_SECONDS` | `30` |
| `--reap-seconds` | `QUEUE_REAP_SECONDS` | `5` |
| `--heartbeat-timeout` | `QUEUE_HEARTBEAT_TIMEOUT` | `60` |
| `--retry-base-seconds` | `QUEUE_RETRY_BASE_SECONDS` | `1` |
| `--retry-max-seconds` | `QUEUE_RETRY_MAX_SECONDS` | `300` |
| `--log-level`, `--log-format` | `QUEUE_LOG_LEVEL`, `QUEUE_LOG_FORMAT` | `info`, `text` |
| | `QUEUE_AUTH_TOKEN` | unset (env only) |

Worker:

| Flag | Env | Default |
|---|---|---|
| `--server` | `QUEUE_SERVER` | `http://localhost:8080` |
| `--id` | `QUEUE_WORKER_ID` | `hostname-<random>` |
| `--lease-seconds` | `QUEUE_LEASE_SECONDS` | `30` |
| `--poll-seconds` | `QUEUE_POLL_SECONDS` | `1` |
| `--heartbeat-seconds` | `QUEUE_HEARTBEAT_SECONDS` | `10` |
| `--job-timeout-seconds` | `QUEUE_JOB_TIMEOUT_SECONDS` | `300` |

## Logging

Logs use `log/slog`. Pass `--log-format json` for JSON output. Each line has a
timestamp, the component, and the job and worker IDs where they apply. Event
names include `job_claimed`, `job_completed`, `job_failed`, `job_retrying`,
`lease_expired`, `lease_lost` and `worker_registered`. Empty claim polls are
logged at debug level.
