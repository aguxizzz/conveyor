package dev.aguxis.conveyor.core;

import java.time.Duration;
import java.util.Map;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Deletes finished jobs once they're past their retention: {@code SUCCEEDED} jobs after a short while,
 * {@code DEAD} ones after longer, since those are the jobs someone may still need to inspect or revive.
 * <p>
 * Every instance sweeps, with no leader. Unlike firing a schedule, deleting is idempotent, and
 * {@code SKIP LOCKED} makes sweeps running at the same time split the rows between them instead of
 * blocking each other, so there's nothing to coordinate. A sweep with nothing to delete is one index
 * lookup per status.
 * <p>
 * Deleting a job also frees its idempotency key, so the retention of {@code SUCCEEDED} jobs is how long
 * a retried enqueue is still recognized as a duplicate.
 */
public class RetentionSweeper {

	private static final Logger log = LoggerFactory.getLogger(RetentionSweeper.class);

	public record Settings(Duration succeeded, Duration dead, int batchSize) {
	}

	private final JobRepository jobs;
	private final Settings settings;
	private final Map<JobStatus, Counter> deleted;

	public RetentionSweeper(JobRepository jobs, Settings settings, MeterRegistry meterRegistry) {
		this.jobs = jobs;
		this.settings = settings;
		this.deleted = Map.of(JobStatus.SUCCEEDED, counter(meterRegistry, JobStatus.SUCCEEDED),
				JobStatus.DEAD, counter(meterRegistry, JobStatus.DEAD));
	}

	@Scheduled(fixedDelayString = "${conveyor.retention.interval:5m}")
	void scheduledSweep() {
		try {
			sweep();
		}
		catch (Exception e) {
			// Nothing is lost by skipping a sweep: the jobs are still due next time.
			log.warn("Retention sweep failed", e);
		}
	}

	/** Deletes every job past its retention, in batches. Returns how many were deleted. */
	int sweep() {
		return purge(JobStatus.SUCCEEDED, settings.succeeded()) + purge(JobStatus.DEAD, settings.dead());
	}

	private int purge(JobStatus status, Duration retention) {
		int total = 0;
		int batch;
		do {
			batch = jobs.deleteFinished(status, retention, settings.batchSize());
			total += batch;
			deleted.get(status).increment(batch);
			// A short batch means we've caught up (or the rest is locked; a later sweep gets it).
		} while (batch == settings.batchSize() && !Thread.currentThread().isInterrupted());
		if (total > 0) {
			log.info("Deleted {} {} jobs older than {}", total, status, retention);
		}
		return total;
	}

	private static Counter counter(MeterRegistry registry, JobStatus status) {
		return Counter.builder("conveyor.retention.deleted")
				.description("Finished jobs deleted by retention on this instance")
				.tag("status", status.name())
				.register(registry);
	}
}
