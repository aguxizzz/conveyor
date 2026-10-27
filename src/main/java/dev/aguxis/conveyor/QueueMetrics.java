package dev.aguxis.conveyor;

import java.util.ArrayList;
import java.util.List;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.MultiGauge.Row;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import dev.aguxis.conveyor.core.JobRepository;
import dev.aguxis.conveyor.core.JobStatus;
import dev.aguxis.conveyor.core.QueueStats;

/**
 * Publishes queue backlog from the database as gauges.
 * <p>
 * These are refreshed on a timer instead of on every scrape so Prometheus can't turn into database
 * load. Every instance reports the same values (it's one table), so dashboards aggregate them with
 * {@code max}, not {@code sum}.
 */
class QueueMetrics {

	private static final Logger log = LoggerFactory.getLogger(QueueMetrics.class);

	private final JobRepository jobs;
	private final MultiGauge jobsByStatus;
	private final MultiGauge oldestReadyAge;

	QueueMetrics(JobRepository jobs, MeterRegistry registry) {
		this.jobs = jobs;
		this.jobsByStatus = MultiGauge.builder("conveyor.queue.jobs")
				.description("Jobs in the table, by queue and status")
				.register(registry);
		this.oldestReadyAge = MultiGauge.builder("conveyor.queue.oldest.ready.age")
				.description("How long the oldest runnable PENDING job has been waiting")
				.baseUnit("seconds")
				.register(registry);
	}

	@Scheduled(fixedDelayString = "${conveyor.metrics.queue-refresh-interval:15s}", initialDelay = 0)
	void refresh() {
		List<QueueStats> stats;
		try {
			stats = jobs.stats();
		}
		catch (Exception e) {
			// Keep the last values; a scrape-time gap is better than reporting a fake empty queue.
			log.warn("Could not refresh queue metrics", e);
			return;
		}
		List<Row<?>> counts = new ArrayList<>();
		List<Row<?>> ages = new ArrayList<>();
		for (QueueStats q : stats) {
			counts.add(Row.of(tags(q, JobStatus.PENDING), q.pending()));
			counts.add(Row.of(tags(q, JobStatus.RUNNING), q.running()));
			counts.add(Row.of(tags(q, JobStatus.SUCCEEDED), q.succeeded()));
			counts.add(Row.of(tags(q, JobStatus.DEAD), q.dead()));
			ages.add(Row.of(Tags.of("queue", q.queue()), q.oldestReadySeconds()));
		}
		// overwrite = true drops series for queues that no longer have any rows.
		jobsByStatus.register(counts, true);
		oldestReadyAge.register(ages, true);
	}

	private static Tags tags(QueueStats q, JobStatus status) {
		return Tags.of("queue", q.queue(), "status", status.name());
	}
}
