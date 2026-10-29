package dev.aguxis.conveyor.api;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonRawValue;

import dev.aguxis.conveyor.core.Schedule;

public record ScheduleResponse(
		String name,
		String cron,
		String timeZone,
		String queue,
		String type,
		@JsonRawValue String payload,
		int priority,
		int maxAttempts,
		boolean enabled,
		Instant nextRunAt,
		Instant lastRunAt,
		Instant createdAt,
		Instant updatedAt) {

	static ScheduleResponse from(Schedule s) {
		return new ScheduleResponse(s.name(), s.cron(), s.timeZone(), s.queue(), s.type(), s.payload(), s.priority(),
				s.maxAttempts(), s.enabled(), s.nextRunAt(), s.lastRunAt(), s.createdAt(), s.updatedAt());
	}
}
