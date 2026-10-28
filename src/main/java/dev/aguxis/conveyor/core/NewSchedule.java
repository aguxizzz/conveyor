package dev.aguxis.conveyor.core;

public record NewSchedule(
		String name,
		String cron,
		String timeZone,
		String queue,
		String type,
		String payload,
		int priority,
		int maxAttempts,
		boolean enabled) {

	public static NewSchedule of(String name, String cron, String type) {
		return new NewSchedule(name, cron, "UTC", "default", type, "{}", 0, 5, true);
	}
}
