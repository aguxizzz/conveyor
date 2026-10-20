package dev.aguxis.conveyor.core;

import java.time.Duration;

public record NewJob(
		String queue,
		String type,
		String payload,
		int priority,
		int maxAttempts,
		Duration delay,
		String idempotencyKey) {

	public static NewJob of(String type, String payload) {
		return new NewJob("default", type, payload, 0, 5, Duration.ZERO, null);
	}

	public NewJob withQueue(String queue) {
		return new NewJob(queue, type, payload, priority, maxAttempts, delay, idempotencyKey);
	}

	public NewJob withMaxAttempts(int maxAttempts) {
		return new NewJob(queue, type, payload, priority, maxAttempts, delay, idempotencyKey);
	}

	public NewJob withIdempotencyKey(String idempotencyKey) {
		return new NewJob(queue, type, payload, priority, maxAttempts, delay, idempotencyKey);
	}
}
