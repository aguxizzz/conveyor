package dev.aguxis.conveyor.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import dev.aguxis.conveyor.TestcontainersConfiguration;

@SpringBootTest(properties = { "conveyor.worker.enabled=false", "conveyor.examples.enabled=false" })
@Import(TestcontainersConfiguration.class)
class WorkerPoolTests {

	private static final RetryPolicy FAST_RETRY = new RetryPolicy(Duration.ofMillis(10), Duration.ofMillis(50));

	@Autowired
	JobRepository jobs;

	@Autowired
	JdbcClient jdbc;

	private final List<WorkerPool> pools = new ArrayList<>();

	@BeforeEach
	void cleanDatabase() {
		jdbc.sql("TRUNCATE jobs RESTART IDENTITY").update();
	}

	@AfterEach
	void stopPools() {
		pools.forEach(WorkerPool::stop);
	}

	@Test
	void runsJobToSuccess() {
		var ran = new AtomicInteger();
		startPool("w1", 2, handler("ok", job -> ran.incrementAndGet()));

		Job job = jobs.enqueue(NewJob.of("ok", "{\"hello\":\"world\"}"));

		awaitStatus(job.id(), JobStatus.SUCCEEDED);
		assertThat(ran).hasValue(1);
		assertThat(jobs.findById(job.id()).orElseThrow().attempts()).isEqualTo(1);
	}

	@Test
	void retriesFailedAttemptsWithBackoff() {
		startPool("w1", 1, handler("flaky", job -> {
			if (job.attempts() < 3) {
				throw new IllegalStateException("boom " + job.attempts());
			}
		}));

		Job job = jobs.enqueue(NewJob.of("flaky", "{}"));

		Job done = awaitStatus(job.id(), JobStatus.SUCCEEDED);
		assertThat(done.attempts()).isEqualTo(3);
		assertThat(done.lastError()).contains("boom 2");
	}

	@Test
	void marksJobDeadWhenAttemptsAreExhausted() {
		startPool("w1", 1, handler("broken", job -> {
			throw new IllegalArgumentException("always fails");
		}));

		Job job = jobs.enqueue(NewJob.of("broken", "{}").withMaxAttempts(3));

		Job dead = awaitStatus(job.id(), JobStatus.DEAD);
		assertThat(dead.attempts()).isEqualTo(3);
		assertThat(dead.lastError()).contains("always fails");
	}

	@Test
	void marksJobDeadWhenNoHandlerIsRegistered() {
		startPool("w1", 1);

		Job job = jobs.enqueue(NewJob.of("unknown", "{}"));

		assertThat(awaitStatus(job.id(), JobStatus.DEAD).lastError()).contains("No handler");
	}

	@Test
	void enqueueIsIdempotentPerKey() {
		Job first = jobs.enqueue(NewJob.of("ok", "{\"n\":1}").withIdempotencyKey("order-42"));
		Job second = jobs.enqueue(NewJob.of("ok", "{\"n\":2}").withIdempotencyKey("order-42"));

		assertThat(second.id()).isEqualTo(first.id());
		assertThat(jobs.list(null, null, 10)).hasSize(1);
	}

	@Test
	void competingWorkersRunEachJobExactlyOnce() {
		int jobCount = 300;
		Map<Long, AtomicInteger> runs = new ConcurrentHashMap<>();
		JobHandler counting = handler("count", job -> {
			runs.computeIfAbsent(job.id(), id -> new AtomicInteger()).incrementAndGet();
			Thread.sleep(2);
		});
		for (int i = 0; i < 4; i++) {
			startPool("w" + i, 4, counting);
		}

		for (int i = 0; i < jobCount; i++) {
			jobs.enqueue(NewJob.of("count", "{\"i\":" + i + "}"));
		}

		await(() -> countByStatus(JobStatus.SUCCEEDED) == jobCount, Duration.ofSeconds(30));
		assertThat(runs).hasSize(jobCount);
		assertThat(runs.values()).allSatisfy(count -> assertThat(count).hasValue(1));
	}

	@Test
	void expiredLeaseIsReclaimedAndStaleWorkerIsFencedOff() {
		Job job = jobs.enqueue(NewJob.of("ok", "{}"));
		// A worker claims the job and then "crashes": it never heartbeats or reports back.
		Job stale = jobs.claim("default", "crashed-worker", 1, Duration.ofMillis(200)).get(0);

		var ran = new AtomicInteger();
		startPool("rescuer", 1, handler("ok", j -> ran.incrementAndGet()));

		Job done = awaitStatus(job.id(), JobStatus.SUCCEEDED);
		assertThat(done.attempts()).isEqualTo(2);
		assertThat(ran).hasValue(1);
		// The zombie wakes up and tries to report: its attempt number no longer matches.
		assertThat(jobs.complete(stale)).isFalse();
		assertThat(jobs.markDead(stale, "late failure")).isFalse();
	}

	@Test
	void heartbeatKeepsLongRunningJobLeased() {
		var ran = new AtomicInteger();
		JobHandler slow = handler("slow", job -> {
			ran.incrementAndGet();
			Thread.sleep(1_500);
		});
		// Lease is much shorter than the job; only the heartbeat prevents a second worker from stealing it.
		startPool("a", 1, Duration.ofMillis(400), Duration.ofMillis(100), slow);
		Job job = jobs.enqueue(NewJob.of("slow", "{}"));
		await(() -> ran.get() == 1, Duration.ofSeconds(5));
		startPool("b", 1, Duration.ofMillis(400), Duration.ofMillis(100), slow);

		Job done = awaitStatus(job.id(), JobStatus.SUCCEEDED);
		assertThat(done.attempts()).isEqualTo(1);
		assertThat(ran).hasValue(1);
	}

	@Test
	void delayedJobsWaitUntilDue() {
		var ran = new AtomicInteger();
		startPool("w1", 1, handler("ok", job -> ran.incrementAndGet()));

		Job job = jobs.enqueue(new NewJob("default", "ok", "{}", 0, 5, Duration.ofSeconds(1), null));

		sleep(Duration.ofMillis(500));
		assertThat(ran).hasValue(0);
		awaitStatus(job.id(), JobStatus.SUCCEEDED);
	}

	// --- helpers ---------------------------------------------------------------------------------

	@FunctionalInterface
	interface Body {
		void run(Job job) throws Exception;
	}

	private static JobHandler handler(String type, Body body) {
		return new JobHandler() {
			@Override
			public String type() {
				return type;
			}

			@Override
			public void handle(Job job) throws Exception {
				body.run(job);
			}
		};
	}

	private void startPool(String id, int concurrency, JobHandler... handlers) {
		startPool(id, concurrency, Duration.ofSeconds(30), Duration.ofSeconds(10), handlers);
	}

	private void startPool(String id, int concurrency, Duration lease, Duration heartbeat, JobHandler... handlers) {
		var settings = new WorkerPool.Settings(id, "default", concurrency, Duration.ofMillis(20), lease, heartbeat,
				Duration.ofSeconds(5));
		var pool = new WorkerPool(jobs, new JobHandlerRegistry(List.of(handlers)), FAST_RETRY, settings);
		pools.add(pool);
		pool.start();
	}

	private Job awaitStatus(long id, JobStatus status) {
		await(() -> jobs.findById(id).orElseThrow().status() == status, Duration.ofSeconds(10));
		return jobs.findById(id).orElseThrow();
	}

	private long countByStatus(JobStatus status) {
		return jdbc.sql("SELECT count(*) FROM jobs WHERE status = :s").param("s", status.name()).query(Long.class)
				.single();
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
