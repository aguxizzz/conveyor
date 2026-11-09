package dev.aguxis.conveyor.api;

import java.time.Duration;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import tools.jackson.databind.JsonNode;

import dev.aguxis.conveyor.core.NewJob;

/**
 * Body of {@code POST /api/jobs}. Everything but {@code type} is optional; {@link #toNewJob()} fills in
 * the defaults, so the core never sees nulls from the HTTP layer.
 */
public record EnqueueRequest(
		@NotBlank @Size(max = 200) String type,
		JsonNode payload,
		@Size(max = 100) String queue,
		Integer priority,
		@Min(1) @Max(100) Integer maxAttempts,
		@Min(0) Long delaySeconds,
		@Size(max = 200) String idempotencyKey,
		@Min(1) Integer timeoutSeconds) {

	NewJob toNewJob() {
		// payload stays raw JSON text all the way down: the core stores it as jsonb and never parses it
		return new NewJob(
				queue == null || queue.isBlank() ? "default" : queue,
				type,
				payload == null ? "{}" : payload.toString(),
				priority == null ? 0 : priority,
				maxAttempts == null ? 5 : maxAttempts,
				Duration.ofSeconds(delaySeconds == null ? 0 : delaySeconds),
				idempotencyKey,
				timeoutSeconds == null ? null : Duration.ofSeconds(timeoutSeconds));
	}
}
