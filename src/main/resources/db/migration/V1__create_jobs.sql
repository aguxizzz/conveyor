CREATE TABLE jobs (
    id              BIGSERIAL PRIMARY KEY,
    queue           TEXT        NOT NULL DEFAULT 'default',
    type            TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT        NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'DEAD')),
    priority        INT         NOT NULL DEFAULT 0,
    attempts        INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    last_error      TEXT,
    idempotency_key TEXT UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

-- Partial indexes keep the claim query cheap no matter how many finished jobs pile up.
CREATE INDEX jobs_ready_idx   ON jobs (queue, priority DESC, run_at) WHERE status = 'PENDING';
CREATE INDEX jobs_leased_idx  ON jobs (queue, locked_until)          WHERE status = 'RUNNING';
CREATE INDEX jobs_status_idx  ON jobs (status, id DESC);
