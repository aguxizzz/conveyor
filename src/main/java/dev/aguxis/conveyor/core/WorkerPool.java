package dev.aguxis.conveyor.core;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * A job with a timeout that runs past it is failed (retried or dead, like a thrown exception) and its
 * thread is interrupted. Interrupting is only a request: a handler that ignores it keeps its thread,
 * and its slot, until it returns, but its lease is no longer extended and whatever it reports back
 * afterwards is ignored.
 * <p>
 * When the queue is drained the poller sleeps for {@code pollInterval}, unless {@link #wakeUp()} is
 * called first (by {@link JobNotificationListener} when a job is enqueued).
 */
public class WorkerPool implements SmartLifecycle {

	private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);
	// last_error is only for humans reading it; a full stack-trace-sized message would just bloat the row
	private static final int MAX_ERROR_LENGTH = 4000;

	/** One pool per queue; {@code workerId} identifies this process in {@code locked_by} and in metrics. */
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
	// attempts claimed and not yet finished, by job id: drives the free-thread count, the heartbeat and timeouts
	private final Map<Long, Attempt> inFlight = new ConcurrentHashMap<>();
	// monitors the poller waits on: one for "a thread freed up", the other for "new work was announced"
	private final Object capacityChanged = new Object();
	private final Object wakeUp = new Object();
	private boolean wakeUpRequested; // guarded by wakeUp

	private volatile boolean running;
	private Thread poller;
	private ExecutorService executor;
	private ScheduledExecutorService timers; // heartbeats and timeouts

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
		timers = Executors.newSingleThreadScheduledExecutor(namedThreads("conveyor-timers-"));
		long hb = settings.heartbeatInterval().toMillis();
		timers.scheduleWithFixedDelay(this::heartbeat, hb, hb, TimeUnit.MILLISECONDS);
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
		timers.shutdownNow();
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
					var attempt = new Attempt(job);
					inFlight.put(job.id(), attempt);
					executor.execute(() -> run(attempt));
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

	private void run(Attempt attempt) {
		synchronized (attempt) {
			attempt.thread = Thread.currentThread();
		}
		try {
			execute(attempt, System.nanoTime());
		}
		catch (Exception e) {
			// Reporting the outcome failed (e.g. DB down); the lease will expire and the job gets retried.
			log.error("Worker {} could not record outcome of job {}", settings.workerId(), attempt.job.id(), e);
		}
		finally {
			synchronized (attempt) {
				attempt.done = true; // from here on a timeout can't interrupt this thread, now free for other jobs
			}
			if (attempt.timeout != null) {
				attempt.timeout.cancel(false);
			}
			// Clears an interrupt that arrived after the handler returned, so it doesn't hit the next job.
			Thread.interrupted();
			inFlight.remove(attempt.job.id());
			synchronized (capacityChanged) {
				capacityChanged.notifyAll();
			}
		}
	}

	private void execute(Attempt attempt, long startNanos) {
		Job job = attempt.job;
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
		if (job.timeout() != null) {
			attempt.timeout = timers.schedule(() -> timeOut(attempt, startNanos), job.timeout().toMillis(),
					TimeUnit.MILLISECONDS);
		}
		Outcome outcome;
		try {
			handler.get().handle(job);
			if (!attempt.settle()) {
				log.warn("Job {} returned after timing out; its outcome was already recorded", job.id());
				return;
			}
			outcome = jobs.complete(job) ? Outcome.SUCCEEDED : Outcome.LOST;
		}
		catch (Exception e) {
			if (!attempt.settle()) {
				log.warn("Job {} threw after timing out; its outcome was already recorded", job.id());
				return;
			}
			outcome = fail(job, truncate(e.getClass().getName() + ": " + e.getMessage()));
		}
		record(job, outcome, startNanos);
	}

	/**
	 * Runs on the timers thread once the job has run for its whole timeout. Does nothing if the handler
	 * got to {@link Attempt#settle()} first; otherwise the handler loses that race, so whatever it
	 * reports later is dropped instead of overwriting this failure.
	 */
	private void timeOut(Attempt attempt, long startNanos) {
		if (!attempt.settle()) {
			return;
		}
		Job job = attempt.job;
		// Logged before the interrupt: the woken handler logs right away, and this line should come first.
		log.warn("Job {} ({}) timed out after {}s on attempt {}", job.id(), job.type(), job.timeout().toSeconds(),
				job.attempts());
		synchronized (attempt) {
			if (!attempt.done) {
				attempt.thread.interrupt();
			}
		}
		String error = "Timed out after " + job.timeout().toSeconds() + "s";
		metrics.timedOut(job.type());
		try {
			record(job, fail(job, error), startNanos);
		}
		catch (Exception e) {
			// No longer heartbeated, so the lease expires and another worker retries the job anyway.
			log.error("Worker {} could not record timeout of job {}", settings.workerId(), job.id(), e);
		}
	}

	/** Retries the job after a backoff, or marks it dead if this was its last attempt. */
	private Outcome fail(Job job, String error) {
		if (job.attemptsExhausted()) {
			log.warn("Job {} ({}) is dead after {} attempts: {}", job.id(), job.type(), job.attempts(), error);
			return jobs.markDead(job, error) ? Outcome.DEAD : Outcome.LOST;
		}
		return jobs.reschedule(job, error, retryPolicy.delayFor(job.attempts())) ? Outcome.RETRIED : Outcome.LOST;
	}

	private void record(Job job, Outcome outcome, long startNanos) {
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
			// Timed-out jobs are left out: their outcome is recorded, even if the handler is still running.
			List<Long> ids = inFlight.values().stream().filter(a -> !a.settled.get()).map(a -> a.job.id()).toList();
			jobs.heartbeat(settings.workerId(), ids, settings.lease());
		}
		catch (Exception e) {
			log.warn("Worker {} heartbeat failed", settings.workerId(), e);
		}
	}

	private void awaitCapacity() throws InterruptedException {
		synchronized (capacityChanged) {
			// re-check under the lock: a job may have finished between pollLoop's check and here
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

	/** One claimed job being executed by this worker. */
	private static final class Attempt {

		final Job job;
		/** Set once, by whoever records the outcome first: the handler finishing, or its timeout. */
		final AtomicBoolean settled = new AtomicBoolean();
		/** Touched only by the executor thread running the job. */
		ScheduledFuture<?> timeout;
		/** Guarded by this; lets the timeout interrupt the job's thread only while it's still on this job. */
		Thread thread;
		boolean done;

		Attempt(Job job) {
			this.job = job;
		}

		boolean settle() {
			return settled.compareAndSet(false, true);
		}
	}

	private static ThreadFactory namedThreads(String prefix) {
		var counter = new AtomicInteger();
		return r -> {
			Thread t = new Thread(r, prefix + counter.incrementAndGet());
			t.setDaemon(true); // a stuck handler must not keep the JVM alive after stop() gives up on it
			return t;
		};
	}
}
