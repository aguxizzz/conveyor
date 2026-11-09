package dev.aguxis.conveyor.core;

import java.time.Duration;

/**
 * A job about to be enqueued, before it has an id or a row. {@link #of} gives the same defaults as the
 * REST API, and the {@code with*} methods tweak one field at a time.
 */
public record NewJob(
		String queue,
		String type,
		String payload,
		int priority,
		int maxAttempts,
		Duration delay,
		String idempotencyKey, // null means no deduplication
		Duration timeout) { // null means no limit per attempt

	public static NewJob of(String type, String payload) {
		return new NewJob("default", type, payload, 0, 5, Duration.ZERO, null, null);
	}

	public NewJob withQueue(String queue) {
		return new NewJob(queue, type, payload, priority, maxAttempts, delay, idempotencyKey, timeout);
	}

	public NewJob withMaxAttempts(int maxAttempts) {
		return new NewJob(queue, type, payload, priority, maxAttempts, delay, idempotencyKey, timeout);
	}

	public NewJob withIdempotencyKey(String idempotencyKey) {
		return new NewJob(queue, type, payload, priority, maxAttempts, delay, idempotencyKey, timeout);
	}

	public NewJob withTimeout(Duration timeout) {
		return new NewJob(queue, type, payload, priority, maxAttempts, delay, idempotencyKey, timeout);
	}
}
