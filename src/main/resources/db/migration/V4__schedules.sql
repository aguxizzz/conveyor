-- Recurring jobs. Each due schedule enqueues an ordinary job, so retries, leases and metrics are the
-- same as for any other job; the scheduler only decides *when* to enqueue.
CREATE TABLE schedules (
    name         TEXT        PRIMARY KEY,
    cron         TEXT        NOT NULL,
    -- Cron fields are read in this zone, so "0 0 3 * * *" in America/Argentina/Buenos_Aires means 3 AM
    -- there, through DST changes.
    time_zone    TEXT        NOT NULL DEFAULT 'UTC',
    queue        TEXT        NOT NULL DEFAULT 'default',
    type         TEXT        NOT NULL,
    payload      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    priority     INT         NOT NULL DEFAULT 0,
    max_attempts INT         NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    enabled      BOOLEAN     NOT NULL DEFAULT true,
    next_run_at  TIMESTAMPTZ NOT NULL,
    last_run_at  TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX schedules_due_idx ON schedules (next_run_at) WHERE enabled;

-- One row per leader role. Whoever holds an unexpired lease is the leader; term goes up on every change
-- of holder and fences off a previous leader that doesn't know it was replaced. Rows are never deleted:
-- starting over at term 1 would make an old leader's token valid again.
CREATE TABLE leader_leases (
    name       TEXT        PRIMARY KEY,
    holder     TEXT        NOT NULL,
    term       BIGINT      NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
