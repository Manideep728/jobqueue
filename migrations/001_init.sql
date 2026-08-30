-- 001_init.sql -- core schema for the queue.
--
-- Design notes:
--   * Job state lives entirely in this table. There is no in-memory queue, so a
--     server restart loses nothing and multiple server instances stay consistent.
--   * `available_at` is how both scheduling and retry backoff are expressed: a job
--     is claimable when status = 'PENDING' AND available_at <= now().
--   * `lease_until` is the crash-recovery mechanism. A RUNNING job whose lease has
--     expired is assumed orphaned and is returned to the pool by the reaper.

CREATE TABLE IF NOT EXISTS jobs (
    id               BIGSERIAL PRIMARY KEY,
    type             TEXT        NOT NULL,
    payload          TEXT        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'PENDING'
                       CHECK (status IN ('PENDING','RUNNING','COMPLETED','FAILED','CANCELED')),
    attempts         INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts     INTEGER     NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
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

-- The claim query's exact predicate. A partial index keeps the index small: it
-- only contains rows that are actually claimable candidates, so the queue does
-- not slow down as millions of COMPLETED rows pile up.
CREATE INDEX IF NOT EXISTS idx_jobs_claimable
    ON jobs (available_at, id) WHERE status = 'PENDING';

-- The lease reaper's predicate, same reasoning.
CREATE INDEX IF NOT EXISTS idx_jobs_expired_leases
    ON jobs (lease_until) WHERE status = 'RUNNING';

-- Supports GET /jobs and GET /jobs?status=...
CREATE INDEX IF NOT EXISTS idx_jobs_status_id ON jobs (status, id DESC);

-- Supports "what has this worker been doing".
CREATE INDEX IF NOT EXISTS idx_jobs_worker ON jobs (worker_id) WHERE worker_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS workers (
    id             TEXT        PRIMARY KEY,
    hostname       TEXT        NOT NULL DEFAULT '',
    registered_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
    jobs_processed BIGINT      NOT NULL DEFAULT 0,
    current_job_id BIGINT      REFERENCES jobs(id) ON DELETE SET NULL
);

-- Supports "which workers are still alive".
CREATE INDEX IF NOT EXISTS idx_workers_heartbeat ON workers (last_heartbeat);
