-- Wakes idle workers as soon as a job becomes runnable, instead of waiting for their next poll.
-- The payload is the queue name, so each worker only reacts to its own queues.
--
-- A trigger rather than a call in the application's enqueue: it also fires for revived jobs and for
-- rows inserted by any other producer (SQL scripts, other services). Notifications are delivered only
-- when the transaction commits, so a woken worker can always see the row; and identical notifications
-- within one transaction are collapsed, so a bulk insert of 10,000 jobs wakes each listener once.
--
-- Delayed jobs and retries with backoff (run_at in the future) aren't announced: nothing could run
-- them yet. Workers still poll on an interval, which picks those up and covers any missed notification.
CREATE FUNCTION conveyor_notify_ready() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('conveyor_jobs', NEW.queue);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER jobs_notify_ready
    AFTER INSERT OR UPDATE OF status ON jobs
    FOR EACH ROW
    WHEN (NEW.status = 'PENDING' AND NEW.run_at <= now())
    EXECUTE FUNCTION conveyor_notify_ready();
