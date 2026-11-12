package dev.aguxis.conveyor.core;

/**
 * Lifecycle of a job: PENDING (waiting, maybe for a retry delay), RUNNING (leased by a worker), then
 * SUCCEEDED or DEAD (out of attempts). DEAD jobs can be sent back to PENDING by hand.
 */
public enum JobStatus {
	PENDING, RUNNING, SUCCEEDED, DEAD
}
