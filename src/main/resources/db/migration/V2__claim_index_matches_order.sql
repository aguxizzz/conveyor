-- Same column order as the claim's ORDER BY (including the id tie-breaker), so the claim is an index
-- scan that stops after LIMIT rows instead of sorting every pending job. Jobs enqueued in one burst
-- share run_at, and without id here Postgres had to sort all of them to find the first few.
DROP INDEX jobs_ready_idx;
CREATE INDEX jobs_ready_idx ON jobs (queue, priority DESC, run_at, id) WHERE status = 'PENDING';
