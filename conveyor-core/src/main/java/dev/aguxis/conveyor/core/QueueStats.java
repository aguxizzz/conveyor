package dev.aguxis.conveyor.core;

/**
 * Job counts per status for one queue. {@code oldestReadySeconds} is how long the oldest runnable
 * PENDING job has been waiting (0 if none): a backlog that grows here means workers can't keep up.
 */
public record QueueStats(String queue, long pending, long running, long succeeded, long dead,
		double oldestReadySeconds) {
}
