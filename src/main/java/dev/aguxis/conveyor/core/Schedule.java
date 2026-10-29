package dev.aguxis.conveyor.core;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

import org.springframework.scheduling.support.CronExpression;

/**
 * Snapshot of a row in the {@code schedules} table: a cron expression and the job to enqueue each time
 * it fires.
 * <p>
 * Cron expressions use Spring's six-field format, seconds first ({@code "0 *&#47;5 * * * *"} is every five
 * minutes), and the macros {@code @hourly}, {@code @daily}, {@code @weekly}, {@code @monthly}.
 */
public record Schedule(
		String name,
		String cron,
		String timeZone,
		String queue,
		String type,
		String payload,
		int priority,
		int maxAttempts,
		boolean enabled,
		Instant nextRunAt,
		Instant lastRunAt,
		Instant createdAt,
		Instant updatedAt) {

	/** The first firing strictly after {@code instant}; empty if the expression never fires again. */
	public Optional<Instant> nextRunAfter(Instant instant) {
		return nextRunAfter(cron, timeZone, instant);
	}

	/** @throws IllegalArgumentException if the expression or the zone is invalid */
	public static Optional<Instant> nextRunAfter(String cron, String timeZone, Instant instant) {
		ZonedDateTime next = CronExpression.parse(cron).next(instant.atZone(zone(timeZone)));
		return Optional.ofNullable(next).map(ZonedDateTime::toInstant);
	}

	/**
	 * Identifies one firing. The job it enqueues uses this as its idempotency key, so even if two
	 * schedulers somehow fired the same occurrence, only one job would exist.
	 */
	String firingKey() {
		return "schedule:" + name + ":" + nextRunAt;
	}

	NewJob jobForFiring() {
		return new NewJob(queue, type, payload, priority, maxAttempts, null, firingKey());
	}

	private static ZoneId zone(String timeZone) {
		try {
			return ZoneId.of(timeZone);
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Unknown time zone '" + timeZone + "'", e);
		}
	}
}
