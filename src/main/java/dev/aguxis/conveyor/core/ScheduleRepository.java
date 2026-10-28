package dev.aguxis.conveyor.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class ScheduleRepository {

	private static final RowMapper<Schedule> SCHEDULE_MAPPER = ScheduleRepository::mapSchedule;

	private final JdbcClient jdbc;

	public ScheduleRepository(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	/**
	 * Creates or replaces a schedule. Its next run is recomputed from the database clock, so changing
	 * the cron expression takes effect from now on.
	 *
	 * @throws IllegalArgumentException if the cron expression or time zone is invalid, or never fires
	 */
	@Transactional
	public Schedule save(NewSchedule schedule) {
		Instant nextRunAt = Schedule.nextRunAfter(schedule.cron(), schedule.timeZone(), now())
				.orElseThrow(() -> new IllegalArgumentException("Cron expression never fires: " + schedule.cron()));
		return jdbc.sql("""
				INSERT INTO schedules (name, cron, time_zone, queue, type, payload, priority, max_attempts, enabled,
				                       next_run_at)
				VALUES (:name, :cron, :timeZone, :queue, :type, CAST(:payload AS jsonb), :priority, :maxAttempts,
				        :enabled, :nextRunAt)
				ON CONFLICT (name) DO UPDATE
				   SET cron = EXCLUDED.cron, time_zone = EXCLUDED.time_zone, queue = EXCLUDED.queue,
				       type = EXCLUDED.type, payload = EXCLUDED.payload, priority = EXCLUDED.priority,
				       max_attempts = EXCLUDED.max_attempts, enabled = EXCLUDED.enabled,
				       next_run_at = EXCLUDED.next_run_at, updated_at = now()
				RETURNING *
				""")
				.param("name", schedule.name())
				.param("cron", schedule.cron())
				.param("timeZone", schedule.timeZone())
				.param("queue", schedule.queue())
				.param("type", schedule.type())
				.param("payload", schedule.payload() == null ? "{}" : schedule.payload())
				.param("priority", schedule.priority())
				.param("maxAttempts", schedule.maxAttempts())
				.param("enabled", schedule.enabled())
				.param("nextRunAt", OffsetDateTime.ofInstant(nextRunAt, ZoneOffset.UTC))
				.query(SCHEDULE_MAPPER)
				.single();
	}

	/**
	 * Locks up to {@code limit} schedules that are due. Must run inside a transaction; the locks keep
	 * anyone else from firing them until it commits.
	 */
	List<Schedule> lockDue(int limit) {
		return jdbc.sql("""
				SELECT * FROM schedules
				 WHERE enabled AND next_run_at <= now()
				 ORDER BY next_run_at
				 LIMIT :limit
				 FOR UPDATE SKIP LOCKED
				""")
				.param("limit", limit)
				.query(SCHEDULE_MAPPER)
				.list();
	}

	/** Records that the firing due at {@code schedule.nextRunAt()} happened and moves on to {@code next}. */
	void advance(Schedule schedule, Instant next) {
		jdbc.sql("""
				UPDATE schedules
				   SET last_run_at = next_run_at, next_run_at = :next, updated_at = now()
				 WHERE name = :name
				""")
				.param("name", schedule.name())
				.param("next", OffsetDateTime.ofInstant(next, ZoneOffset.UTC))
				.update();
	}

	/** For expressions that will never fire again (e.g. a fixed date in the past). */
	void disable(Schedule schedule) {
		jdbc.sql("UPDATE schedules SET enabled = false, last_run_at = next_run_at, updated_at = now() WHERE name = :name")
				.param("name", schedule.name())
				.update();
	}

	public Optional<Schedule> findByName(String name) {
		return jdbc.sql("SELECT * FROM schedules WHERE name = :name").param("name", name).query(SCHEDULE_MAPPER)
				.optional();
	}

	public List<Schedule> findAll() {
		return jdbc.sql("SELECT * FROM schedules ORDER BY name").query(SCHEDULE_MAPPER).list();
	}

	public boolean delete(String name) {
		return jdbc.sql("DELETE FROM schedules WHERE name = :name").param("name", name).update() == 1;
	}

	/** The transaction's start time in Postgres: the clock every scheduling decision is based on. */
	Instant now() {
		return jdbc.sql("SELECT now()").query(OffsetDateTime.class).single().toInstant();
	}

	private static Schedule mapSchedule(ResultSet rs, int rowNum) throws SQLException {
		return new Schedule(
				rs.getString("name"),
				rs.getString("cron"),
				rs.getString("time_zone"),
				rs.getString("queue"),
				rs.getString("type"),
				rs.getString("payload"),
				rs.getInt("priority"),
				rs.getInt("max_attempts"),
				rs.getBoolean("enabled"),
				instant(rs, "next_run_at"),
				instant(rs, "last_run_at"),
				instant(rs, "created_at"),
				instant(rs, "updated_at"));
	}

	private static Instant instant(ResultSet rs, String column) throws SQLException {
		OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
		return value == null ? null : value.toInstant();
	}
}
