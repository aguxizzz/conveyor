package dev.aguxis.conveyor.core;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import dev.aguxis.conveyor.core.WorkerMetrics.Outcome;

/**
 * Polls one queue and runs claimed jobs on a bounded thread pool.
 * <p>
 * Only as many jobs are claimed as there are free threads, so a worker never holds leases on work it
 * isn't actually executing. While a job runs, its lease is periodically extended (heartbeat); if
 * this process dies, the lease expires and another worker picks the job up.
 * <p>
 * When the queue is drained the poller sleeps for {@code pollInterval}, unless {@link #wakeUp()} is
 * called first (by {@link JobNotificationListener} when a job is enqueued).
 */
public class WorkerPool implements SmartLifecycle {

	private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);
	private static final int MAX_ERROR_LENGTH = 4000;

	public record Settings(
			String workerId,
			String queue,
			int concurrency,
			Duration pollInterval,
			Duration lease,
			Duration heartbeatInterval,
			Duration shutdownTimeout) {
	}

	private final JobRepository jobs;
	private final JobHandlerRegistry handlers;
	private final RetryPolicy retryPolicy;
	private final Settings settings;
	private final WorkerMetrics metrics;
	private final Map<Long, Job> inFlight = new ConcurrentHashMap<>();
	private final Object capacityChanged = new Object();
	private final Object wakeUp = new Object();
	private boolean wakeUpRequested; // guarded by wakeUp

	private volatile boolean running;
	private Thread poller;
	private ExecutorService executor;
	private ScheduledExecutorService heartbeats;

	public WorkerPool(JobRepository jobs, JobHandlerRegistry handlers, RetryPolicy retryPolicy, Settings settings,
			MeterRegistry meterRegistry) {
		this.jobs = jobs;
		this.handlers = handlers;
		this.retryPolicy = retryPolicy;
		this.settings = settings;
		this.metrics = new WorkerMetrics(meterRegistry, this, settings);
	}

	@Override
	public synchronized void start() {
		if (running) {
			return;
		}
		running = true;
		executor = Executors.newFixedThreadPool(settings.concurrency(), namedThreads("conveyor-exec-"));
		heartbeats = Executors.newSingleThreadScheduledExecutor(namedThreads("conveyor-heartbeat-"));
		long hb = settings.heartbeatInterval().toMillis();
		heartbeats.scheduleWithFixedDelay(this::heartbeat, hb, hb, TimeUnit.MILLISECONDS);
		poller = new Thread(this::pollLoop, "conveyor-poller-" + settings.queue());
		poller.start();
		log.info("Worker {} started on queue '{}' with concurrency {}", settings.workerId(), settings.queue(),
				settings.concurrency());
	}

	@Override
	public synchronized void stop() {
		if (!running) {
			return;
		}
		running = false;
		// Stop claiming first so nothing new is handed to the executor once it starts shutting down.
		poller.interrupt();
		try {
			poller.join(settings.shutdownTimeout().toMillis());
			executor.shutdown();
			if (!executor.awaitTermination(settings.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
				// Unfinished jobs keep their lease until it expires, then another worker retries them.
				log.warn("Worker {} stopped with {} jobs still running", settings.workerId(), inFlight.size());
				executor.shutdownNow();
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		heartbeats.shutdownNow();
		log.info("Worker {} stopped", settings.workerId());
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	public int inFlightCount() {
		return inFlight.size();
	}

	public String queue() {
		return settings.queue();
	}

	/**
	 * Ends the current idle wait so the poller claims right away. Safe to call at any time: a wake-up
	 * that arrives while the poller is busy claiming isn't lost, it skips the next wait instead.
	 */
	public void wakeUp() {
		synchronized (wakeUp) {
			wakeUpRequested = true;
			wakeUp.notifyAll();
		}
	}

	private void pollLoop() {
		while (running) {
			try {
				int free = settings.concurrency() - inFlight.size();
				if (free <= 0) {
					awaitCapacity();
					continue;
				}
				List<Job> claimed = jobs.claim(settings.queue(), settings.workerId(), free, settings.lease());
				for (Job job : claimed) {
					metrics.claimed(job);
					inFlight.put(job.id(), job);
					executor.execute(() -> run(job));
				}
				if (claimed.size() < free) {
					// Queue drained; back off. A full batch means there's probably more, so poll again now.
					awaitWakeUp();
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			catch (Exception e) {
				log.error("Worker {} failed to poll queue '{}'", settings.workerId(), settings.queue(), e);
				sleepQuietly(settings.pollInterval());
			}
		}
	}

	private void run(Job job) {
		try {
			execute(job, System.nanoTime());
		}
		catch (Exception e) {
			// Reporting the outcome failed (e.g. DB down); the lease will expire and the job gets retried.
			log.error("Worker {} could not record outcome of job {}", settings.workerId(), job.id(), e);
		}
		finally {
			inFlight.remove(job.id());
			synchronized (capacityChanged) {
				capacityChanged.notifyAll();
			}
		}
	}

	private void execute(Job job, long startNanos) {
		if (job.attempts() > job.maxAttempts()) {
			// Only reachable when previous attempts died without reporting back (lease expiry).
			boolean recorded = jobs.markDead(job, "Exceeded max attempts after lease expiry");
			metrics.finished(knownType(job), recorded ? Outcome.DEAD : Outcome.LOST, startNanos);
			return;
		}
		var handler = handlers.find(job.type());
		if (handler.isEmpty()) {
			boolean recorded = jobs.markDead(job, "No handler registered for type '" + job.type() + "'");
			metrics.finished(knownType(job), recorded ? Outcome.DEAD : Outcome.LOST, startNanos);
			return;
		}
		Outcome outcome;
		try {
			handler.get().handle(job);
			outcome = jobs.complete(job) ? Outcome.SUCCEEDED : Outcome.LOST;
		}
		catch (Exception e) {
			String error = truncate(e.getClass().getName() + ": " + e.getMessage());
			if (job.attemptsExhausted()) {
				log.warn("Job {} ({}) is dead after {} attempts: {}", job.id(), job.type(), job.attempts(), error);
				outcome = jobs.markDead(job, error) ? Outcome.DEAD : Outcome.LOST;
			}
			else {
				outcome = jobs.reschedule(job, error, retryPolicy.delayFor(job.attempts())) ? Outcome.RETRIED
						: Outcome.LOST;
			}
		}
		if (outcome == Outcome.LOST) {
			log.warn("Job {} finished but its lease was lost; another attempt owns it now", job.id());
		}
		metrics.finished(job.type(), outcome, startNanos);
	}

	/** Job type for metric tags: only types with a handler, so clients can't create arbitrary series. */
	private String knownType(Job job) {
		return handlers.find(job.type()).isPresent() ? job.type() : "unknown";
	}

	private void heartbeat() {
		try {
			jobs.heartbeat(settings.workerId(), inFlight.keySet(), settings.lease());
		}
		catch (Exception e) {
			log.warn("Worker {} heartbeat failed", settings.workerId(), e);
		}
	}

	private void awaitCapacity() throws InterruptedException {
		synchronized (capacityChanged) {
			if (inFlight.size() >= settings.concurrency()) {
				capacityChanged.wait(settings.pollInterval().toMillis());
			}
		}
	}

	private void awaitWakeUp() throws InterruptedException {
		synchronized (wakeUp) {
			if (!wakeUpRequested) {
				wakeUp.wait(settings.pollInterval().toMillis());
			}
			wakeUpRequested = false;
		}
	}

	private static void sleepQuietly(Duration duration) {
		try {
			Thread.sleep(duration.toMillis());
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private static String truncate(String s) {
		return s.length() <= MAX_ERROR_LENGTH ? s : s.substring(0, MAX_ERROR_LENGTH);
	}

	private static ThreadFactory namedThreads(String prefix) {
		var counter = new AtomicInteger();
		return r -> {
			Thread t = new Thread(r, prefix + counter.incrementAndGet());
			t.setDaemon(true);
			return t;
		};
	}
}
