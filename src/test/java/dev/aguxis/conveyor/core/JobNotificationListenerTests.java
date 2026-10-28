package dev.aguxis.conveyor.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.BooleanSupplier;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.autoconfigure.JdbcConnectionDetails;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import dev.aguxis.conveyor.TestcontainersConfiguration;

/**
 * Pools here poll only once a minute, so a job that runs within a couple of seconds can only have been
 * picked up because of a notification.
 */
@SpringBootTest(properties = { "conveyor.worker.enabled=false", "conveyor.examples.enabled=false" })
@Import(TestcontainersConfiguration.class)
class JobNotificationListenerTests {

	private static final Duration SLOW_POLL = Duration.ofMinutes(1);
	private static final Duration FAST_ENOUGH = Duration.ofSeconds(3);

	@Autowired
	JobRepository jobs;

	@Autowired
	JdbcClient jdbc;

	@Autowired
	JdbcConnectionDetails db;

	private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
	private WorkerPool pool;
	private JobNotificationListener listener;

	@BeforeEach
	void start() {
		jdbc.sql("TRUNCATE jobs RESTART IDENTITY").update();
		JobHandler ok = new JobHandler() {
			@Override
			public String type() {
				return "ok";
			}

			@Override
			public void handle(Job job) {
			}
		};
		pool = new WorkerPool(jobs, new JobHandlerRegistry(List.of(ok)), new RetryPolicy(Duration.ZERO, Duration.ZERO),
				new WorkerPool.Settings("w1", "default", 2, SLOW_POLL, Duration.ofSeconds(30), Duration.ofSeconds(10),
						Duration.ofSeconds(5)),
				meters);
		var dataSource = new DriverManagerDataSource(db.getJdbcUrl(), db.getUsername(), db.getPassword());
		var properties = new Properties();
		properties.setProperty("ApplicationName", "conveyor-listener");
		dataSource.setConnectionProperties(properties);
		listener = new JobNotificationListener(dataSource, List.of(pool), meters);
		pool.start();
		listener.start();
		await(listener::isConnected, FAST_ENOUGH);
		// Let the pool finish its first (empty) poll and go idle for the full minute.
		sleep(Duration.ofMillis(300));
	}

	@AfterEach
	void stop() {
		listener.stop();
		pool.stop();
	}

	@Test
	void enqueuedJobIsPickedUpWithoutWaitingForThePoll() {
		Job job = jobs.enqueue(NewJob.of("ok", "{}"));

		awaitStatus(job.id(), JobStatus.SUCCEEDED);
		assertThat(meters.get("conveyor.listener.connected").gauge().value()).isEqualTo(1);
	}

	@Test
	void jobsForOtherQueuesDoNotWakeThePool() {
		jobs.enqueue(new NewJob("other", "ok", "{}", 0, 5, Duration.ZERO, null));
		Job job = jobs.enqueue(NewJob.of("ok", "{}"));

		awaitStatus(job.id(), JobStatus.SUCCEEDED);
		assertThat(countByStatus(JobStatus.PENDING)).isEqualTo(1);
	}

	@Test
	void revivedDeadJobIsPickedUpRightAway() {
		Job job = jobs.enqueue(NewJob.of("nobody.handles.this", "{}"));
		awaitStatus(job.id(), JobStatus.DEAD);
		jdbc.sql("UPDATE jobs SET type = 'ok' WHERE id = :id").param("id", job.id()).update();

		jobs.revive(job.id());

		awaitStatus(job.id(), JobStatus.SUCCEEDED);
	}

	@Test
	void reconnectsAfterLosingTheConnection() {
		long killed = listenerPid();
		jdbc.sql("SELECT pg_terminate_backend(CAST(:pid AS int))").param("pid", killed).query(Boolean.class).single();
		await(() -> listener.isConnected() && listenerPid() != killed, Duration.ofSeconds(10));
		sleep(Duration.ofMillis(300));

		Job job = jobs.enqueue(NewJob.of("ok", "{}"));

		awaitStatus(job.id(), JobStatus.SUCCEEDED);
	}

	// --- helpers ---------------------------------------------------------------------------------

	private long listenerPid() {
		return jdbc.sql("SELECT coalesce(max(pid), 0) FROM pg_stat_activity WHERE application_name = 'conveyor-listener'")
				.query(Long.class)
				.single();
	}

	private void awaitStatus(long id, JobStatus status) {
		await(() -> jobs.findById(id).orElseThrow().status() == status, FAST_ENOUGH);
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
