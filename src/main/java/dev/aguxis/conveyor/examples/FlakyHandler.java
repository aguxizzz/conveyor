package dev.aguxis.conveyor.examples;

import java.util.concurrent.ThreadLocalRandom;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import tools.jackson.databind.json.JsonMapper;

import dev.aguxis.conveyor.core.Job;
import dev.aguxis.conveyor.core.JobHandler;

/** {@code demo.flaky} — fails with probability {@code failRate} (default 0.5) to exercise retries. */
@Component
@ConditionalOnProperty(name = "conveyor.examples.enabled", havingValue = "true", matchIfMissing = true)
class FlakyHandler implements JobHandler {

	private final JsonMapper json;

	FlakyHandler(JsonMapper json) {
		this.json = json;
	}

	@Override
	public String type() {
		return "demo.flaky";
	}

	@Override
	public void handle(Job job) {
		double failRate = json.readTree(job.payload()).path("failRate").asDouble(0.5);
		if (ThreadLocalRandom.current().nextDouble() < failRate) {
			throw new IllegalStateException("Simulated failure on attempt " + job.attempts());
		}
	}
}
