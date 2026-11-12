package dev.aguxis.conveyor.core;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with "full jitter": delay = random(0, min(max, base * 2^(attempt-1))).
 * Jitter spreads retries out so a downstream outage doesn't come back to a thundering herd.
 */
public record RetryPolicy(Duration base, Duration max) {

	public Duration delayFor(int attempt) {
		long baseMs = base.toMillis();
		int exponent = Math.min(Math.max(attempt - 1, 0), 30);
		long ceiling = Math.min(max.toMillis(), baseMs * (1L << exponent));
		return Duration.ofMillis(ThreadLocalRandom.current().nextLong(ceiling + 1));
	}
}
