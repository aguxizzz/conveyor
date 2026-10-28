package dev.aguxis.conveyor.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

import dev.aguxis.conveyor.TestcontainersConfiguration;

/**
 * Retention here is an hour for SUCCEEDED and two for DEAD, and test jobs are at most a few hours old,
 * so the sweeper of another cached test context (default retention: days) never touches them.
 */
@SpringBootTest(properties = { "conveyor.worker.enabled=false", "conveyor.scheduler.enabled=false",
		"conveyor.retention.enabled=false", "conveyor.examples.enabled=false" })
@Import(TestcontainersConfiguration.class)
class RetentionSweeperTests {

	@Autowired
	JobRepository jobs;

	@Autowired
	TransactionTemplate tx;

	@Autowired
	JdbcClient jdbc;

	private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

	@BeforeEach
	void cleanDatabase() {
		jdbc.sql("TRUNCATE jobs RESTART IDENTITY").update();
	}

	@Test
	void deletesOnlyJobsPastTheRetentionOfTheirStatus() {
		long oldSucceeded = insert("SUCCEEDED", "90 minutes");
		long recentSucceeded = insert("SUCCEEDED", "30 minutes");
		long oldDead = insert("DEAD", "3 hours");
		long recentDead = insert("DEAD", "90 minutes"); // past SUCCEEDED's retention, not DEAD's
		long oldPending = insert("PENDING", null, "3 hours");
		long oldRunning = insert("RUNNING", null, "3 hours");

		assertThat(sweeper(100).sweep()).isEqualTo(2);

		assertThat(remainingIds()).containsExactly(recentSucceeded, recentDead, oldPending, oldRunning);
		assertThat(deletedCount("SUCCEEDED")).isEqualTo(1);
		assertThat(deletedCount("DEAD")).isEqualTo(1);
	}

	@Test
	void revivedJobIsKept() {
		long id = insert("DEAD", "3 hours");
		assertThat(jobs.revive(id)).isPresent(); // clears finished_at

		assertThat(sweeper(100).sweep()).isZero();
		assertThat(remainingIds()).containsExactly(id);
	}

	@Test
	void deletesABacklogLargerThanABatchInOneSweep() {
		jdbc.sql("""
				INSERT INTO jobs (type, status, finished_at)
				SELECT 'old', 'SUCCEEDED', now() - interval '2 hours' - make_interval(secs => i)
				  FROM generate_series(1, 2500) AS i
				""").update();
		long recent = insert("SUCCEEDED", "1 minute");

		assertThat(sweeper(1000).sweep()).isEqualTo(2500);
		assertThat(remainingIds()).containsExactly(recent);
	}

	@Test
	void skipsALockedRowInsteadOfWaitingForIt() throws Exception {
		long locked = insert("DEAD", "3 hours");
		long free = insert("DEAD", "3 hours");
		var holding = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		// Someone else holds the row, e.g. a revive that hasn't committed yet.
		CompletableFuture<Void> other = CompletableFuture.runAsync(() -> tx.executeWithoutResult(status -> {
			jdbc.sql("SELECT 1 FROM jobs WHERE id = :id FOR UPDATE").param("id", locked).query(Integer.class).single();
			holding.countDown();
			await(release);
		}));
		assertThat(holding.await(5, TimeUnit.SECONDS)).isTrue();

		int deleted = CompletableFuture.supplyAsync(() -> sweeper(100).sweep()).get(5, TimeUnit.SECONDS);

		release.countDown();
		other.get(5, TimeUnit.SECONDS);
		assertThat(deleted).isEqualTo(1);
		assertThat(remainingIds()).containsExactly(locked).doesNotContain(free);
		// Next sweep gets it.
		assertThat(sweeper(100).sweep()).isEqualTo(1);
	}

	@Test
	void concurrentSweepsSplitTheWorkWithoutDeletingTwice() throws Exception {
		jdbc.sql("""
				INSERT INTO jobs (type, status, finished_at)
				SELECT 'old', 'SUCCEEDED', now() - interval '2 hours'
				  FROM generate_series(1, 5000)
				""").update();
		RetentionSweeper a = sweeper(100);
		RetentionSweeper b = sweeper(100);

		var sweepA = CompletableFuture.supplyAsync(a::sweep);
		var sweepB = CompletableFuture.supplyAsync(b::sweep);

		assertThat(sweepA.get(30, TimeUnit.SECONDS) + sweepB.get(30, TimeUnit.SECONDS)).isEqualTo(5000);
		assertThat(remainingIds()).isEmpty();
	}

	@Test
	void findsOldJobsThroughTheIndex() {
		jdbc.sql("""
				INSERT INTO jobs (type, status, finished_at)
				SELECT 't', 'SUCCEEDED', now() - make_interval(secs => i)
				  FROM generate_series(1, 20000) AS i
				""").update();
		jdbc.sql("ANALYZE jobs").update();

		String plan = String.join("\n", jdbc.sql("""
				EXPLAIN SELECT id FROM jobs
				 WHERE status = 'SUCCEEDED' AND finished_at < now() - interval '1 hour'
				 ORDER BY finished_at LIMIT 1000
				""").query(String.class).list());

		assertThat(plan).contains("jobs_finished_idx").doesNotContain("Sort");
	}

	private RetentionSweeper sweeper(int batchSize) {
		return new RetentionSweeper(jobs, new RetentionSweeper.Settings(Duration.ofHours(1), Duration.ofHours(2),
				batchSize), meterRegistry);
	}

	private long insert(String status, String finishedAgo) {
		return insert(status, finishedAgo, finishedAgo);
	}

	private long insert(String status, String finishedAgo, String createdAgo) {
		return jdbc.sql("""
				INSERT INTO jobs (type, status, created_at, finished_at)
				VALUES ('t', :status, now() - CAST(:createdAgo AS interval),
				        now() - CAST(:finishedAgo AS interval))
				RETURNING id
				""")
				.param("status", status)
				.param("createdAgo", createdAgo)
				.param("finishedAgo", finishedAgo)
				.query(Long.class)
				.single();
	}

	private List<Long> remainingIds() {
		return jdbc.sql("SELECT id FROM jobs ORDER BY id").query(Long.class).list();
	}

	private double deletedCount(String status) {
		return meterRegistry.get("conveyor.retention.deleted").tag("status", status).counter().count();
	}

	private static void await(CountDownLatch latch) {
		try {
			latch.await(10, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}
