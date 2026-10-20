package dev.aguxis.conveyor.core;

/**
 * Executes jobs of a single {@link #type()}. Throwing marks the attempt as failed and the job is
 * retried with backoff until {@code maxAttempts} is reached.
 * <p>
 * Delivery is at-least-once, so implementations should be idempotent.
 */
public interface JobHandler {

	String type();

	void handle(Job job) throws Exception;
}
