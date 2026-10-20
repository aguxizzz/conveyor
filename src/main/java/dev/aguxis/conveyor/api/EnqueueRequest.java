package dev.aguxis.conveyor.api;

import java.time.Duration;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import tools.jackson.databind.JsonNode;

import dev.aguxis.conveyor.core.NewJob;

public record EnqueueRequest(
		@NotBlank @Size(max = 200) String type,
		JsonNode payload,
		@Size(max = 100) String queue,
		Integer priority,
		@Min(1) @Max(100) Integer maxAttempts,
		@Min(0) Long delaySeconds,
		@Size(max = 200) String idempotencyKey) {

	NewJob toNewJob() {
		return new NewJob(
				queue == null || queue.isBlank() ? "default" : queue,
				type,
				payload == null ? "{}" : payload.toString(),
				priority == null ? 0 : priority,
				maxAttempts == null ? 5 : maxAttempts,
				Duration.ofSeconds(delaySeconds == null ? 0 : delaySeconds),
				idempotencyKey);
	}
}
