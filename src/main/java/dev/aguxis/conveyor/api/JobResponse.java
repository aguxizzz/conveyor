package dev.aguxis.conveyor.api;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonRawValue;

import dev.aguxis.conveyor.core.Job;
import dev.aguxis.conveyor.core.JobStatus;

public record JobResponse(
		long id,
		String queue,
		String type,
		@JsonRawValue String payload,
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
		Instant finishedAt) {

	static JobResponse from(Job job) {
		return new JobResponse(job.id(), job.queue(), job.type(), job.payload(), job.status(), job.priority(),
				job.attempts(), job.maxAttempts(), job.runAt(), job.lockedBy(), job.lockedUntil(), job.lastError(),
				job.idempotencyKey(), job.createdAt(), job.finishedAt());
	}
}
