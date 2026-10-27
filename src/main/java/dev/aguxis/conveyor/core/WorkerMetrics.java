package dev.aguxis.conveyor.core;

import java.time.Duration;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Per-process metrics of one {@link WorkerPool}. Queue-wide numbers (backlog per status) come from
 * the database instead, see {@code QueueMetrics}, since every instance sees the same table.
 */
public class WorkerMetrics {

	/** How an attempt ended, as seen by the worker that ran it. */
	public enum Outcome {
		/** Handler returned normally. */
		SUCCEEDED,
		/** Handler threw; the job goes back to PENDING after a backoff. */
		RETRIED,
		/** Handler threw on its last attempt, or the job can't be run at all. */
		DEAD,
		/** The outcome was not recorded because the lease had been taken over (fenced off). */
		LOST
	}

	private final MeterRegistry registry;
	private final String queue;
	private final Timer pickupLatency;

	WorkerMetrics(MeterRegistry registry, WorkerPool pool, WorkerPool.Settings settings) {
		this.registry = registry;
		this.queue = settings.queue();
		this.pickupLatency = Timer.builder("conveyor.job.pickup.latency")
				.description("Time from a job becoming runnable (run_at) to a worker claiming it")
				.tag("queue", queue)
				.publishPercentileHistogram()
				.minimumExpectedValue(Duration.ofMillis(1))
				.maximumExpectedValue(Duration.ofHours(1))
				.register(registry);
		Gauge.builder("conveyor.worker.in.flight", pool, WorkerPool::inFlightCount)
				.description("Jobs currently executing in this worker")
				.tags("queue", queue, "worker", settings.workerId())
				.register(registry);
		Gauge.builder("conveyor.worker.concurrency", settings, WorkerPool.Settings::concurrency)
				.description("Maximum jobs this worker executes at once")
				.tags("queue", queue, "worker", settings.workerId())
				.register(registry);
	}

	/**
	 * Both timestamps come from Postgres ({@code run_at}, and {@code updated_at} which the claim sets to
	 * {@code now()}), so clock skew between hosts doesn't distort the measurement. Jobs recovered after
	 * a lease expired show up as long waits, which is accurate: that's how late they actually ran.
	 */
	void claimed(Job job) {
		Duration waited = Duration.between(job.runAt(), job.updatedAt());
		pickupLatency.record(waited.isNegative() ? Duration.ZERO : waited);
	}

	/**
	 * {@code type} must come from a registered handler: job types are client input, and tagging with
	 * arbitrary strings would create unbounded time series.
	 */
	void finished(String type, Outcome outcome, long startNanos) {
		Timer.builder("conveyor.job.execution")
				.description("Time spent running a job attempt, by outcome")
				.tags("queue", queue, "type", type, "outcome", outcome.name().toLowerCase())
				.publishPercentileHistogram()
				.minimumExpectedValue(Duration.ofMillis(1))
				.maximumExpectedValue(Duration.ofMinutes(10))
				.register(registry)
				.record(Duration.ofNanos(System.nanoTime() - startNanos));
	}
}
