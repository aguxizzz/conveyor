-- Lets retention find the oldest finished jobs of a status with an index range scan, instead of reading
-- the whole table on every batch. Partial: pending and running jobs (finished_at IS NULL) get no entry.
--
-- CONCURRENTLY so that upgrading a deployment with millions of jobs doesn't block enqueues and claims
-- while the index builds. It can't run inside a transaction; Flyway sees that and runs this script
-- without one, which is why it's the only statement here.
CREATE INDEX CONCURRENTLY jobs_finished_idx ON jobs (status, finished_at) WHERE finished_at IS NOT NULL;
