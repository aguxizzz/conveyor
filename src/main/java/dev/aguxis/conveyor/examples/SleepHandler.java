package dev.aguxis.conveyor.examples;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import tools.jackson.databind.json.JsonMapper;

import dev.aguxis.conveyor.core.Job;
import dev.aguxis.conveyor.core.JobHandler;

/** {@code demo.sleep} — simulates work: {@code {"ms": 500}}. */
@Component
@ConditionalOnProperty(name = "conveyor.examples.enabled", havingValue = "true", matchIfMissing = true)
class SleepHandler implements JobHandler {

	private final JsonMapper json;

	SleepHandler(JsonMapper json) {
		this.json = json;
	}

	@Override
	public String type() {
		return "demo.sleep";
	}

	@Override
	public void handle(Job job) throws InterruptedException {
		Thread.sleep(json.readTree(job.payload()).path("ms").asLong(500));
	}
}
