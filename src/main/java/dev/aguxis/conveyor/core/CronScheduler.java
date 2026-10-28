package dev.aguxis.conveyor.core;

import java.time.Duration;
import java.time.Instant;
import java.util.OptionalLong;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.transaction.support.TransactionOperations;

/**
 * Enqueues a job each time a {@link Schedule} is due. Every instance runs one, but only the leader
 * (see {@link LeaderLease}) fires; the rest stand by to take over.
 * <p>
 * A single leader keeps the work in one place, but correctness doesn't depend on there being only
 * one, since a paused leader may wake up thinking it still leads. Each firing is one transaction that:
 * <ol>
 * <li>locks the lease row for this leader's term, or does nothing if the term is stale;</li>
 * <li>locks the due schedules ({@code FOR UPDATE SKIP LOCKED});</li>
 * <li>enqueues one job per schedule, keyed by the firing's time, so a repeat would be a no-op;</li>
 * <li>moves each schedule to its next run.</li>
 * </ol>
 * Firings missed while no leader was running (e.g. every minute during a 10-minute outage) are
 * coalesced: the schedule fires once, late, and then continues from the current time.
 */
public class CronScheduler implements SmartLifecycle {

	public static final String LEASE_NAME = "scheduler";

	private static final Logger log = LoggerFactory.getLogger(CronScheduler.class);
	private static final int BATCH_SIZE = 100;

	public record Settings(String instanceId, Duration tick, Duration lease) {
	}

	private final ScheduleRepository schedules;
	private final JobRepository jobs;
	private final LeaderLease leases;
	private final TransactionOperations tx;
	private final Settings settings;
	private final Counter fired;

	private volatile boolean running;
	/** The term this instance believes it leads, or 0. Only a hint: the database has the final say. */
	private volatile long term;
	private long nextLeaseCheck; // System.nanoTime(); only touched by the scheduler thread
	private Thread thread;

	public CronScheduler(ScheduleRepository schedules, JobRepository jobs, LeaderLease leases, TransactionOperations tx,
			Settings settings, MeterRegistry meterRegistry) {
		this.schedules = schedules;
		this.jobs = jobs;
		this.leases = leases;
		this.tx = tx;
		this.settings = settings;
		this.fired = Counter.builder("conveyor.scheduler.fired")
				.description("Jobs enqueued by schedules on this instance")
				.register(meterRegistry);
		Gauge.builder("conveyor.scheduler.leader", this, s -> s.isLeader() ? 1 : 0)
				.description("1 on the instance currently firing schedules")
				.register(meterRegistry);
	}

	@Override
	public synchronized void start() {
		if (running) {
			return;
		}
		running = true;
		thread = new Thread(this::loop, "conveyor-scheduler");
		thread.setDaemon(true);
		thread.start();
	}

	@Override
	public synchronized void stop() {
		if (!running) {
			return;
		}
		running = false;
		thread.interrupt();
		try {
			thread.join(Duration.ofSeconds(10).toMillis());
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		if (term != 0) {
			term = 0;
			try {
				leases.release(LEASE_NAME, settings.instanceId());
				log.info("Scheduler {} released leadership", settings.instanceId());
			}
			catch (Exception e) {
				log.warn("Could not release leadership; a follower takes over when the lease expires", e);
			}
		}
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	public boolean isLeader() {
		return term != 0;
	}

	public double firedCount() {
		return fired.count();
	}

	private void loop() {
		while (running) {
			try {
				if (System.nanoTime() >= nextLeaseCheck) {
					checkLease();
				}
				if (term != 0) {
					fireDue(term);
				}
			}
			catch (Exception e) {
				if (!running) {
					return;
				}
				log.warn("Scheduler {} tick failed", settings.instanceId(), e);
				// Can't tell if the lease was renewed; ask again next tick. Firings stay fenced meanwhile.
				nextLeaseCheck = 0;
			}
			try {
				Thread.sleep(settings.tick().toMillis());
			}
			catch (InterruptedException e) {
				return;
			}
		}
	}

	/**
	 * Followers ask as often as the leader renews, a third of the lease, so a crashed leader is replaced
	 * at most about 1⅓ leases later, and a leader gets two more tries before its lease runs out.
	 */
	private void checkLease() {
		OptionalLong acquired = leases.tryAcquire(LEASE_NAME, settings.instanceId(), settings.lease());
		long previous = term;
		term = acquired.orElse(0);
		nextLeaseCheck = System.nanoTime() + settings.lease().toNanos() / 3;
		if (term != previous) {
			if (term != 0) {
				log.info("Scheduler {} is now the leader (term {})", settings.instanceId(), term);
			}
			else {
				log.info("Scheduler {} lost leadership", settings.instanceId());
			}
		}
	}

	/**
	 * Fires every due schedule as leader of {@code term}. Returns how many jobs were enqueued, or -1 if
	 * {@code term} is no longer current (and then nothing was enqueued).
	 */
	int fireDue(long term) {
		int count = tx.execute(status -> {
			if (!leases.lockIfHeld(LEASE_NAME, settings.instanceId(), term)) {
				return -1;
			}
			Instant now = schedules.now();
			int n = 0;
			for (Schedule schedule : schedules.lockDue(BATCH_SIZE)) {
				jobs.enqueue(schedule.jobForFiring());
				// After now, not after nextRunAt: missed firings collapse into the one just enqueued.
				schedule.nextRunAfter(now).ifPresentOrElse(next -> schedules.advance(schedule, next),
						() -> schedules.disable(schedule));
				n++;
			}
			return n;
		});
		if (count < 0 && this.term == term) {
			log.warn("Scheduler {} was fenced off: term {} is no longer current", settings.instanceId(), term);
			this.term = 0;
		}
		else if (count > 0) {
			fired.increment(count);
		}
		return count;
	}
}
