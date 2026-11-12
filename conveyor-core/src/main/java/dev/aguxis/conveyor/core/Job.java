package dev.aguxis.conveyor.core;

import java.time.Duration;
import java.time.Instant;

/**
 * Snapshot of a row in the {@code jobs} table.
 * <p>
 * {@code attempts} doubles as a fencing token: it is incremented on every claim, so a worker whose
 * lease expired (and whose job was re-claimed by someone else) can no longer complete or fail it.
 * <p>
 * {@code timeout} is the maximum run time of one attempt, or null for no limit.
 */
public record Job(
		long id,
		String queue,
		String type,
		String payload,
		JobStatus status,
		int priority,
		int attempts,
		int maxAttempts,
		Duration timeout,
		Instant runAt,
		String lockedBy,
		Instant lockedUntil,
		String lastError,
		String idempotencyKey,
		Instant createdAt,
		Instant updatedAt,
		Instant finishedAt) {

	/** True once this attempt was the last one allowed; a failure now sends the job to DEAD. */
	public boolean attemptsExhausted() {
		return attempts >= maxAttempts;
	}
}
