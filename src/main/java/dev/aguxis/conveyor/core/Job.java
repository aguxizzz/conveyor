package dev.aguxis.conveyor.core;

import java.time.Instant;

/**
 * Snapshot of a row in the {@code jobs} table.
 * <p>
 * {@code attempts} doubles as a fencing token: it is incremented on every claim, so a worker whose
 * lease expired (and whose job was re-claimed by someone else) can no longer complete or fail it.
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
		Instant runAt,
		String lockedBy,
		Instant lockedUntil,
		String lastError,
		String idempotencyKey,
		Instant createdAt,
		Instant updatedAt,
		Instant finishedAt) {

	public boolean attemptsExhausted() {
		return attempts >= maxAttempts;
	}
}
