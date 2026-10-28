package dev.aguxis.conveyor.api;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import tools.jackson.databind.JsonNode;

import dev.aguxis.conveyor.core.NewSchedule;

public record ScheduleRequest(
		@NotBlank @Size(max = 100) String cron,
		@Size(max = 100) String timeZone,
		@NotBlank @Size(max = 200) String type,
		JsonNode payload,
		@Size(max = 100) String queue,
		Integer priority,
		@Min(1) @Max(100) Integer maxAttempts,
		Boolean enabled) {

	NewSchedule toNewSchedule(String name) {
		return new NewSchedule(
				name,
				cron,
				timeZone == null || timeZone.isBlank() ? "UTC" : timeZone,
				queue == null || queue.isBlank() ? "default" : queue,
				type,
				payload == null ? "{}" : payload.toString(),
				priority == null ? 0 : priority,
				maxAttempts == null ? 5 : maxAttempts,
				enabled == null || enabled);
	}
}
