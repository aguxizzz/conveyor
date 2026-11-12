package dev.aguxis.conveyor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

import dev.aguxis.conveyor.core.JobRepository;
import dev.aguxis.conveyor.core.NewJob;

/** The whole app, wired as in production: a real worker pool, scraped through the actuator endpoint. */
@SpringBootTest(properties = { "conveyor.examples.enabled=false", "conveyor.worker.poll-interval=20ms",
		"conveyor.metrics.queue-refresh-interval=100ms" })
@AutoConfigureMockMvc
@AutoConfigureMetrics
@Import(TestcontainersConfiguration.class)
class MetricsEndpointTests {

	@Autowired
	MockMvc mvc;

	@Autowired
	JobRepository jobs;

	@Test
	void exposesWorkerAndQueueMetricsToPrometheus() throws Exception {
		// No handlers are registered, so the job is dead-lettered on its first attempt.
		jobs.enqueue(NewJob.of("no.such.handler", "{}"));

		long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
		String body;
		do {
			Thread.sleep(100);
			body = scrape();
		}
		while (!body.contains("status=\"DEAD\"} 1") && System.nanoTime() < deadline);

		assertThat(body)
				.contains("conveyor_queue_jobs{application=\"conveyor\",queue=\"default\",status=\"DEAD\"} 1")
				.containsPattern(
						"conveyor_job_execution_seconds_count\\{[^}]*outcome=\"dead\"[^}]*type=\"unknown\"[^}]*\\} 1")
				.contains("conveyor_job_pickup_latency_seconds_bucket")
				.contains("conveyor_queue_oldest_ready_age_seconds")
				.contains("conveyor_worker_in_flight");
	}

	private String scrape() throws Exception {
		return mvc.perform(get("/actuator/prometheus")).andReturn().getResponse().getContentAsString();
	}
}
