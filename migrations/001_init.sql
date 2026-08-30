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
