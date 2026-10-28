package dev.aguxis.conveyor.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * All timestamps are computed by Postgres ({@code now()}), never by the JVM, so workers on hosts with
 * skewed clocks still agree on when a job is due or a lease has expired.
 */
@Repository
public class JobRepository {

	private static final RowMapper<Job> JOB_MAPPER = JobRepository::mapJob;

	private final JdbcClient jdbc;

	public JobRepository(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	/**
	 * Inserts a job. If {@code idempotencyKey} is already taken, the existing job is returned instead,
	 * so clients can safely retry the enqueue call itself.
	 */
	@Transactional
	public Job enqueue(NewJob job) {
		Optional<Job> inserted = jdbc.sql("""
				INSERT INTO jobs (queue, type, payload, priority, max_attempts, run_at, idempotency_key)
				VALUES (:queue, :type, CAST(:payload AS jsonb), :priority, :maxAttempts,
				        now() + make_interval(secs => :delaySeconds), :idempotencyKey)
				ON CONFLICT (idempotency_key) DO NOTHING
				RETURNING *
				""")
				.param("queue", job.queue())
				.param("type", job.type())
				.param("payload", job.payload() == null ? "{}" : job.payload())
				.param("priority", job.priority())
				.param("maxAttempts", job.maxAttempts())
				.param("delaySeconds", seconds(job.delay()))
				.param("idempotencyKey", job.idempotencyKey())
				.query(JOB_MAPPER)
				.optional();
		return inserted.orElseGet(() -> findByIdempotencyKey(job.idempotencyKey()).orElseThrow());
	}

	/**
	 * Atomically leases up to {@code limit} runnable jobs for {@code workerId}.
	 * <p>
	 * {@code FOR UPDATE SKIP LOCKED} lets many workers poll the same queue concurrently without
	 * blocking on, or double-claiming, each other's rows. Jobs whose lease expired (worker crashed or
	 * stalled) are first put back to {@code PENDING}, so they're runnable again in their normal order.
	 * <p>
	 * Expired leases are handled in their own statement on purpose: with an {@code OR} of both cases in
	 * one query, Postgres can't read {@code jobs_ready_idx} in order and stop at {@code limit}; it scans
	 * and sorts the whole backlog on every claim, which made claims slower the further behind the queue was.
	 */
	@Transactional
	public List<Job> claim(String queue, String workerId, int limit, Duration lease) {
		jdbc.sql("""
				UPDATE jobs
				   SET status = 'PENDING', locked_by = NULL, locked_until = NULL, updated_at = now()
				 WHERE id IN (
				       SELECT id FROM jobs
				        WHERE queue = :queue AND status = 'RUNNING' AND locked_until < now()
				        FOR UPDATE SKIP LOCKED)
				""")
				.param("queue", queue)
				.update();
		return jdbc.sql("""
				UPDATE jobs
				   SET status = 'RUNNING',
				       locked_by = :workerId,
				       locked_until = now() + make_interval(secs => :leaseSeconds),
				       attempts = attempts + 1,
				       updated_at = now()
				 WHERE id IN (
				       SELECT id FROM jobs
				        WHERE queue = :queue AND status = 'PENDING' AND run_at <= now()
				        ORDER BY priority DESC, run_at, id
				        LIMIT :limit
				        FOR UPDATE SKIP LOCKED)
				RETURNING *
				""")
				.param("queue", queue)
				.param("workerId", workerId)
				.param("leaseSeconds", seconds(lease))
				.param("limit", limit)
				.query(JOB_MAPPER)
				.list();
	}

	/** Extends the lease of jobs still held by {@code workerId}. Returns how many were extended. */
	public int heartbeat(String workerId, Collection<Long> ids, Duration lease) {
		if (ids.isEmpty()) {
			return 0;
		}
		return jdbc.sql("""
				UPDATE jobs
				   SET locked_until = now() + make_interval(secs => :leaseSeconds), updated_at = now()
				 WHERE id IN (:ids) AND status = 'RUNNING' AND locked_by = :workerId
				""")
				.param("leaseSeconds", seconds(lease))
				.param("ids", ids)
				.param("workerId", workerId)
				.update();
	}

	/** Returns false if the lease was lost (the job was re-claimed by another attempt). */
	public boolean complete(Job job) {
		return jdbc.sql("""
				UPDATE jobs
				   SET status = 'SUCCEEDED', locked_by = NULL, locked_until = NULL,
				       finished_at = now(), updated_at = now()
				 WHERE id = :id AND status = 'RUNNING' AND locked_by = :workerId AND attempts = :attempt
				""")
				.params(fence(job))
				.update() == 1;
	}

	public boolean reschedule(Job job, String error, Duration delay) {
		return jdbc.sql("""
				UPDATE jobs
				   SET status = 'PENDING', locked_by = NULL, locked_until = NULL, last_error = :error,
				       run_at = now() + make_interval(secs => :delaySeconds), updated_at = now()
				 WHERE id = :id AND status = 'RUNNING' AND locked_by = :workerId AND attempts = :attempt
				""")
				.params(fence(job))
				.param("error", error)
				.param("delaySeconds", seconds(delay))
				.update() == 1;
	}

	public boolean markDead(Job job, String error) {
		return jdbc.sql("""
				UPDATE jobs
				   SET status = 'DEAD', locked_by = NULL, locked_until = NULL, last_error = :error,
				       finished_at = now(), updated_at = now()
				 WHERE id = :id AND status = 'RUNNING' AND locked_by = :workerId AND attempts = :attempt
				""")
				.params(fence(job))
				.param("error", error)
				.update() == 1;
	}

	/** Manually re-queues a dead job with a fresh attempt budget. */
	public Optional<Job> revive(long id) {
		return jdbc.sql("""
				UPDATE jobs
				   SET status = 'PENDING', attempts = 0, run_at = now(), finished_at = NULL, updated_at = now()
				 WHERE id = :id AND status = 'DEAD'
				RETURNING *
				""")
				.param("id", id)
				.query(JOB_MAPPER)
				.optional();
	}

	public Optional<Job> findById(long id) {
		return jdbc.sql("SELECT * FROM jobs WHERE id = :id").param("id", id).query(JOB_MAPPER).optional();
	}

	public Optional<Job> findByIdempotencyKey(String key) {
		return jdbc.sql("SELECT * FROM jobs WHERE idempotency_key = :key")
				.param("key", key)
				.query(JOB_MAPPER)
				.optional();
	}

	public List<Job> list(JobStatus status, String queue, int limit) {
		return jdbc.sql("""
				SELECT * FROM jobs
				 WHERE (CAST(:status AS text) IS NULL OR status = :status)
				   AND (CAST(:queue AS text) IS NULL OR queue = :queue)
				 ORDER BY id DESC
				 LIMIT :limit
				""")
				.param("status", status == null ? null : status.name())
				.param("queue", queue)
				.param("limit", limit)
				.query(JOB_MAPPER)
				.list();
	}

	public List<QueueStats> stats() {
		return jdbc.sql("""
				SELECT queue,
				       count(*) FILTER (WHERE status = 'PENDING')   AS pending,
				       count(*) FILTER (WHERE status = 'RUNNING')   AS running,
				       count(*) FILTER (WHERE status = 'SUCCEEDED') AS succeeded,
				       count(*) FILTER (WHERE status = 'DEAD')      AS dead,
				       coalesce(extract(epoch FROM now() - min(run_at)
				                FILTER (WHERE status = 'PENDING' AND run_at <= now())), 0) AS oldest_ready_seconds
				  FROM jobs
				 GROUP BY queue
				 ORDER BY queue
				""")
				.query((rs, n) -> new QueueStats(rs.getString("queue"), rs.getLong("pending"),
						rs.getLong("running"), rs.getLong("succeeded"), rs.getLong("dead"),
						rs.getDouble("oldest_ready_seconds")))
				.list();
	}

	private static Map<String, Object> fence(Job job) {
		return Map.of("id", job.id(), "workerId", job.lockedBy(), "attempt", job.attempts());
	}

	private static double seconds(Duration duration) {
		return duration == null ? 0 : duration.toMillis() / 1000.0;
	}

	private static Job mapJob(ResultSet rs, int rowNum) throws SQLException {
		return new Job(
				rs.getLong("id"),
				rs.getString("queue"),
				rs.getString("type"),
				rs.getString("payload"),
				JobStatus.valueOf(rs.getString("status")),
				rs.getInt("priority"),
				rs.getInt("attempts"),
				rs.getInt("max_attempts"),
				instant(rs, "run_at"),
				rs.getString("locked_by"),
				instant(rs, "locked_until"),
				rs.getString("last_error"),
				rs.getString("idempotency_key"),
				instant(rs, "created_at"),
				instant(rs, "updated_at"),
				instant(rs, "finished_at"));
	}

	private static Instant instant(ResultSet rs, String column) throws SQLException {
		OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
		return value == null ? null : value.toInstant();
	}
}
