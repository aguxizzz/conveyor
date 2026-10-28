package dev.aguxis.conveyor.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

import dev.aguxis.conveyor.TestcontainersConfiguration;

@SpringBootTest(properties = { "conveyor.worker.enabled=false", "conveyor.scheduler.enabled=false",
		"conveyor.examples.enabled=false" })
@Import(TestcontainersConfiguration.class)
class CronSchedulerTests {

	private static final String EVERY_SECOND = "* * * * * *";
	private static final Duration TICK = Duration.ofMillis(50);

	@Autowired
	ScheduleRepository schedules;

	@Autowired
	JobRepository jobs;

	@Autowired
	LeaderLease leases;

	@Autowired
	TransactionTemplate tx;

	@Autowired
	JdbcClient jdbc;

	private final List<CronScheduler> schedulers = new ArrayList<>();

	@BeforeEach
	void cleanDatabase() {
		jdbc.sql("TRUNCATE jobs, schedules, leader_leases RESTART IDENTITY").update();
	}

	@AfterEach
	void stopSchedulers() {
		schedulers.forEach(CronScheduler::stop);
	}

	@Test
	void onlyTheLeaderFiresAndEachOccurrenceBecomesOneJob() {
		schedules.save(NewSchedule.of("tick", EVERY_SECOND, "tick"));
		CronScheduler a = start("a", Duration.ofSeconds(3));
		CronScheduler b = start("b", Duration.ofSeconds(3));
		CronScheduler c = start("c", Duration.ofSeconds(3));

		sleep(Duration.ofMillis(3500));

		assertThat(List.of(a, b, c)).filteredOn(CronScheduler::isLeader).hasSize(1);
		assertThat(List.of(a, b, c)).filteredOn(s -> s.firedCount() > 0).hasSize(1);
		List<String> keys = jdbc.sql("SELECT idempotency_key FROM jobs WHERE type = 'tick' ORDER BY id")
				.query(String.class)
				.list();
		// One job per second, give or take the second the test started and stopped in.
		assertThat(keys).hasSizeBetween(2, 5).doesNotHaveDuplicates().allMatch(k -> k.startsWith("schedule:tick:"));
		assertThat(a.firedCount() + b.firedCount() + c.firedCount()).isEqualTo(keys.size());
	}

	@Test
	void followerTakesOverWhenTheLeaderStops() {
		CronScheduler a = start("a", Duration.ofSeconds(3));
		await(a::isLeader, Duration.ofSeconds(2));
		CronScheduler b = start("b", Duration.ofSeconds(3));
		sleep(Duration.ofMillis(200));
		assertThat(b.isLeader()).isFalse();

		a.stop();

		// Released on shutdown, so b doesn't wait out the 3s lease: it takes over on its next check.
		await(b::isLeader, Duration.ofMillis(1500));
		assertThat(leases.current(CronScheduler.LEASE_NAME).orElseThrow().term()).isEqualTo(2);
	}

	@Test
	void crashedLeaderIsReplacedOnceItsLeaseExpires() {
		// A leader that died without releasing: its row is still there, expired.
		jdbc.sql("INSERT INTO leader_leases VALUES ('scheduler', 'ghost', 7, now() - interval '1 second')").update();

		CronScheduler b = start("b", Duration.ofSeconds(3));

		await(b::isLeader, Duration.ofSeconds(2));
		assertThat(leases.current(CronScheduler.LEASE_NAME).orElseThrow())
				.extracting(LeaderLease.Holder::holder, LeaderLease.Holder::term)
				.containsExactly("b", 8L);
	}

	@Test
	void staleLeaderIsFencedOff() {
		schedules.save(NewSchedule.of("tick", EVERY_SECOND, "tick"));
		makeDue("tick");
		CronScheduler a = scheduler("a", Duration.ofSeconds(30));
		CronScheduler b = scheduler("b", Duration.ofSeconds(30));
		long termA = leases.tryAcquire(CronScheduler.LEASE_NAME, "a", Duration.ofSeconds(30)).orElseThrow();
		// a pauses (GC, network) long enough for its lease to run out, and b takes over.
		jdbc.sql("UPDATE leader_leases SET expires_at = now() - interval '1 second'").update();
		long termB = leases.tryAcquire(CronScheduler.LEASE_NAME, "b", Duration.ofSeconds(30)).orElseThrow();
		assertThat(termB).isEqualTo(termA + 1);

		// a wakes up still believing it leads.
		assertThat(a.fireDue(termA)).isEqualTo(-1);
		assertThat(jobCount()).isZero();

		assertThat(b.fireDue(termB)).isEqualTo(1);
		assertThat(jobCount()).isEqualTo(1);
	}

	@Test
	void takeoverWaitsForTheFiringInProgress() throws Exception {
		long termA = leases.tryAcquire(CronScheduler.LEASE_NAME, "a", Duration.ofSeconds(1)).orElseThrow();
		var locked = new CountDownLatch(1);
		// a starts firing just before its lease ends, and is slow to commit.
		CompletableFuture<Instant> aCommitting = CompletableFuture.supplyAsync(() -> tx.execute(status -> {
			assertThat(leases.lockIfHeld(CronScheduler.LEASE_NAME, "a", termA)).isTrue();
			locked.countDown();
			sleep(Duration.ofMillis(1500));
			return Instant.now();
		}));
		assertThat(locked.await(5, TimeUnit.SECONDS)).isTrue();
		sleep(Duration.ofMillis(1100)); // a's lease has expired by now

		long termB = leases.tryAcquire(CronScheduler.LEASE_NAME, "b", Duration.ofSeconds(30)).orElseThrow();
		Instant bAcquired = Instant.now();

		assertThat(termB).isEqualTo(termA + 1);
		assertThat(bAcquired).isAfterOrEqualTo(aCommitting.get(5, TimeUnit.SECONDS));
	}

	@Test
	void missedFiringsAreCoalescedIntoOne() {
		schedules.save(NewSchedule.of("minutely", "0 * * * * *", "report"));
		// The cluster was down for an hour: 60 firings were missed.
		jdbc.sql("UPDATE schedules SET next_run_at = date_trunc('minute', now()) - interval '1 hour'").update();
		Instant missedSince = schedules.findByName("minutely").orElseThrow().nextRunAt();
		CronScheduler a = scheduler("a", Duration.ofSeconds(30));
		long term = leases.tryAcquire(CronScheduler.LEASE_NAME, "a", Duration.ofSeconds(30)).orElseThrow();

		assertThat(a.fireDue(term)).isEqualTo(1);
		assertThat(a.fireDue(term)).isZero();

		Schedule after = schedules.findByName("minutely").orElseThrow();
		assertThat(after.lastRunAt()).isEqualTo(missedSince);
		assertThat(after.nextRunAt()).isAfter(Instant.now()).isBefore(Instant.now().plusSeconds(61));
		assertThat(jobs.findByIdempotencyKey("schedule:minutely:" + missedSince)).isPresent();
	}

	@Test
	void scheduleThatNeverFiresAgainIsDisabled() {
		schedules.save(NewSchedule.of("once", "0 0 0 1 1 *", "report"));
		makeDue("once");
		jdbc.sql("UPDATE schedules SET cron = '0 0 0 30 2 *'").update(); // February 30th: never
		CronScheduler a = scheduler("a", Duration.ofSeconds(30));
		long term = leases.tryAcquire(CronScheduler.LEASE_NAME, "a", Duration.ofSeconds(30)).orElseThrow();

		assertThat(a.fireDue(term)).isEqualTo(1);

		assertThat(schedules.findByName("once").orElseThrow().enabled()).isFalse();
	}

	@Test
	void disabledSchedulesDoNotFire() {
		schedules.save(new NewSchedule("off", EVERY_SECOND, "UTC", "default", "tick", "{}", 0, 5, false));
		makeDue("off");
		CronScheduler a = scheduler("a", Duration.ofSeconds(30));
		long term = leases.tryAcquire(CronScheduler.LEASE_NAME, "a", Duration.ofSeconds(30)).orElseThrow();

		assertThat(a.fireDue(term)).isZero();
	}

	// --- helpers ---------------------------------------------------------------------------------

	private CronScheduler scheduler(String id, Duration lease) {
		return new CronScheduler(schedules, jobs, leases, tx, new CronScheduler.Settings(id, TICK, lease),
				new SimpleMeterRegistry());
	}

	private CronScheduler start(String id, Duration lease) {
		CronScheduler scheduler = scheduler(id, lease);
		schedulers.add(scheduler);
		scheduler.start();
		return scheduler;
	}

	private void makeDue(String name) {
		jdbc.sql("UPDATE schedules SET next_run_at = now() - interval '1 second' WHERE name = :name")
				.param("name", name)
				.update();
	}

	private long jobCount() {
		return jdbc.sql("SELECT count(*) FROM jobs").query(Long.class).single();
	}

	private static void await(BooleanSupplier condition, Duration timeout) {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() > deadline) {
				throw new AssertionError("Condition not met within " + timeout);
			}
			sleep(Duration.ofMillis(25));
		}
	}

	private static void sleep(Duration d) {
		try {
			Thread.sleep(d.toMillis());
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
	}
}
