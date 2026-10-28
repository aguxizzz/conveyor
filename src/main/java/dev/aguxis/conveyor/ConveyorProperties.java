package dev.aguxis.conveyor;

import java.time.Duration;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("conveyor")
public record ConveyorProperties(@DefaultValue Worker worker, @DefaultValue Retry retry) {

	public record Worker(
			@DefaultValue("true") boolean enabled,
			@DefaultValue("default") List<String> queues,
			@DefaultValue("8") int concurrency,
			// Fallback only while listen is on: it bounds the wait for delayed jobs, retries and missed notifications.
			@DefaultValue("2s") Duration pollInterval,
			@DefaultValue("true") boolean listen,
			@DefaultValue("30s") Duration lease,
			@DefaultValue("10s") Duration heartbeatInterval,
			@DefaultValue("20s") Duration shutdownTimeout) {
	}

	public record Retry(
			@DefaultValue("2s") Duration base,
			@DefaultValue("10m") Duration max) {
	}
}
