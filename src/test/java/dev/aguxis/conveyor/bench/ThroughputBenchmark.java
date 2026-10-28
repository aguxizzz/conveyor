package dev.aguxis.conveyor.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import dev.aguxis.conveyor.core.Job;
import dev.aguxis.conveyor.core.JobHandler;
import dev.aguxis.conveyor.core.JobHandlerRegistry;
import dev.aguxis.conveyor.core.JobRepository;
import dev.aguxis.conveyor.core.NewJob;
import dev.aguxis.conveyor.core.RetryPolicy;
import dev.aguxis.conveyor.core.WorkerPool;

/**
 * Measures how fast a fleet of workers drains the queue, against a real Postgres.
 * <p>
 * Each simulated worker gets its own {@link WorkerPool} and its own connection pool, exactly like a
 * separate process would, so the only thing they share is the database. Not part of the normal build:
 * <pre>./mvnw test -Dtest=ThroughputBenchmark -Dbenchmark=true</pre>
 * Results are printed and written to {@code target/benchmark-results.md}.
 */
@EnabledIfSystemProperty(named = "benchmark", matches = "true")
class ThroughputBenchmark {

	private static final int CONCURRENCY = 8;
	private static final int REPEATS = 3;
	private static final int[] WORKER_COUNTS = { 1, 2, 4, 8 };

	private static PostgreSQLContainer postgres;
	private static HikariDataSource admin;
	private static JdbcClient jdbc;
	private static final StringBuilder report = new StringBuilder();

	@BeforeAll
	static void startDatabase() {
		postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:17-alpine"));
		postgres.start();
		admin = dataSource(4);
		Flyway.configure().dataSource(admin).load().migrate();
		jdbc = JdbcClient.create(admin);
		report.append("# Conveyor benchmark results\n\n")
				.append("- CPU cores: ").append(Runtime.getRuntime().availableProcessors())
				.append(", OS: ").append(System.getProperty("os.name")).append(' ').append(System.getProperty("os.arch"))
				.append(", Java ").append(System.getProperty("java.version")).append('\n')
				.append("- ").append(jdbc.sql("SELECT version()").query(String.class).single()).append('\n')
				.append("- Each worker: concurrency ").append(CONCURRENCY).append(", own connection pool; median of ")
				.append(REPEATS).append(" runs\n\n");
		// Warm-up: JIT, connection setup and Postgres caches, so the first measured run isn't penalized.
		drain(2, 2_000, noop());
	}

	@AfterAll
	static void stopDatabase() throws IOException {
		if (admin != null) {
			admin.close();
		}
		if (postgres != null) {
			postgres.stop();
		}
		Path out = Path.of("target", "benchmark-results.md");
		Files.createDirectories(out.getParent());
		Files.writeString(out, report);
		System.out.println("\n" + report);
	}

	/** Pure queue overhead: handlers do nothing, so every millisecond is claim + complete in Postgres. */
	@Test
	void noOpJobs() {
		int jobs = 20_000;
		section("Queue overhead: no-op jobs (" + jobs + " jobs)",
				"| Workers | Threads | Jobs/s | vs. 1 worker |", "|---:|---:|---:|---:|");
		double baseline = 0;
		for (int workers : WORKER_COUNTS) {
			double rate = median(() -> drain(workers, jobs, noop()));
			baseline = baseline == 0 ? rate : baseline;
			row(workers, workers * CONCURRENCY, fmt(rate), String.format("%.2fx", rate / baseline));
		}
	}

	/** Jobs that wait on "I/O" for 10 ms: the ideal rate is threads / 10 ms, the gap is queue overhead. */
	@Test
	void tenMillisecondJobs() {
		int jobs = 8_000;
		JobHandler handler = handler(job -> Thread.sleep(10));
		section("Realistic work: 10 ms I/O-bound jobs (" + jobs + " jobs)",
				"| Workers | Threads | Jobs/s | Ideal (threads / 10 ms) | Efficiency |", "|---:|---:|---:|---:|---:|");
		for (int workers : WORKER_COUNTS) {
			double rate = median(() -> drain(workers, jobs, handler));
			double ideal = workers * CONCURRENCY / 0.010;
			row(workers, workers * CONCURRENCY, fmt(rate), fmt(ideal), String.format("%.0f%%", 100 * rate / ideal));
		}
	}

	/** Partial indexes should make the claim cost independent of how many finished jobs the table holds. */
	@Test
	void largeHistoryOfFinishedJobs() {
		int jobs = 20_000;
		int workers = 4;
		section("Finished-job history: " + workers + " workers, no-op jobs (" + jobs + " jobs)",
				"| Finished rows already in table | Jobs/s |", "|---:|---:|");
		for (int history : new int[] { 0, 1_000_000 }) {
			double rate = median(() -> {
				jdbc.sql("TRUNCATE jobs RESTART IDENTITY").update();
				jdbc.sql("""
						INSERT INTO jobs (type, status, attempts, finished_at)
						SELECT 'bench', 'SUCCEEDED', 1, now() FROM generate_series(1, :n)
						""").param("n", history).update();
				jdbc.sql("ANALYZE jobs").update();
				return seedAndDrain(workers, jobs, noop());
			});
			row(String.format("%,d", history), fmt(rate));
		}
	}

	/** Enqueue path: many HTTP-like clients inserting one job per transaction. */
	@Test
	void enqueueRate() {
		int clients = 16;
		Duration window = Duration.ofSeconds(10);
		section("Enqueue: " + clients + " concurrent clients, one job per call (" + window.toSeconds() + " s)",
				"| Idempotency key | Jobs/s |", "|---|---:|");
		for (boolean keyed : new boolean[] { false, true }) {
			double rate = median(() -> enqueueFor(clients, window, keyed));
			row(keyed ? "yes (unique per job)" : "no", fmt(rate));
		}
	}

	/** Seeds {@code jobs} runnable jobs, starts {@code workers} pools and returns jobs/s until all succeed. */
	private static double drain(int workers, int jobs, JobHandler handler) {
		jdbc.sql("TRUNCATE jobs RESTART IDENTITY").update();
		return seedAndDrain(workers, jobs, handler);
	}

	private static double seedAndDrain(int workers, int jobs, JobHandler handler) {
		jdbc.sql("""
				INSERT INTO jobs (type, payload)
				SELECT 'bench', jsonb_build_object('i', i) FROM generate_series(1, :n) i
				""").param("n", jobs).update();
		jdbc.sql("ANALYZE jobs").update();

		List<HikariDataSource> dataSources = new ArrayList<>();
		List<WorkerPool> pools = new ArrayList<>();
		var registry = new JobHandlerRegistry(List.of(handler));
		var retry = new RetryPolicy(Duration.ofMillis(10), Duration.ofMillis(50));
		for (int i = 0; i < workers; i++) {
			// Executor threads + poller + heartbeat, like one real instance.
			HikariDataSource ds = dataSource(CONCURRENCY + 2);
			dataSources.add(ds);
			pools.add(new WorkerPool(transactional(ds), registry, retry,
					new WorkerPool.Settings("bench-" + i, "default", CONCURRENCY, Duration.ofMillis(500),
							Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofSeconds(20)),
					new SimpleMeterRegistry()));
		}

		long start = System.nanoTime();
		pools.forEach(WorkerPool::start);
		// Uses the partial indexes, so polling stays cheap even with a large finished-job history.
		String unfinished = "SELECT count(*) FROM jobs WHERE status IN ('PENDING', 'RUNNING')";
		while (jdbc.sql(unfinished).query(Long.class).single() > 0) {
			sleep(Duration.ofMillis(10));
		}
		double seconds = (System.nanoTime() - start) / 1e9;
		pools.forEach(WorkerPool::stop);
		dataSources.forEach(HikariDataSource::close);

		// No crashes in a benchmark, so every job must have run exactly once.
		long reran = jdbc.sql("SELECT count(*) FROM jobs WHERE attempts <> 1").query(Long.class).single();
		assertThat(reran).as("jobs claimed more than once").isZero();
		return jobs / seconds;
	}

	/** Single worker, single claimer: average time of one claim of {@code CONCURRENCY} jobs. */
	@Test
	void claimLatencyVsBacklog() {
		int claims = 300;
		section("Claim cost vs. backlog: one claim of " + CONCURRENCY + " jobs, average of " + claims,
				"| Pending jobs | ms per claim |", "|---:|---:|");
		HikariDataSource ds = dataSource(2);
		JobRepository repository = transactional(ds);
		for (int backlog : new int[] { 1_000, 10_000, 100_000 }) {
			double ms = median(() -> {
				jdbc.sql("TRUNCATE jobs RESTART IDENTITY").update();
				jdbc.sql("INSERT INTO jobs (type) SELECT 'bench' FROM generate_series(1, :n)").param("n", backlog)
						.update();
				jdbc.sql("ANALYZE jobs").update();
				long start = System.nanoTime();
				for (int i = 0; i < claims; i++) {
					repository.claim("default", "bench", CONCURRENCY, Duration.ofSeconds(30));
				}
				return (System.nanoTime() - start) / 1e6 / claims;
			});
			row(String.format("%,d", backlog), String.format("%.2f", ms));
		}
		ds.close();
	}

	/**
	 * The repository as the app uses it: Spring wraps {@code claim} in one transaction through
	 * {@code @Transactional}, which a plain {@code new JobRepository(..)} wouldn't get.
	 */
	private static JobRepository transactional(HikariDataSource ds) {
		var tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
		return new JobRepository(JdbcClient.create(ds)) {
			@Override
			public List<Job> claim(String queue, String workerId, int limit, Duration lease) {
				return tx.execute(status -> super.claim(queue, workerId, limit, lease));
			}
		};
	}

	private static double enqueueFor(int clients, Duration window, boolean keyed) {
		jdbc.sql("TRUNCATE jobs RESTART IDENTITY").update();
		HikariDataSource ds = dataSource(clients);
		var repository = new JobRepository(JdbcClient.create(ds));
		var enqueued = new AtomicLong();
		long deadline = System.nanoTime() + window.toNanos();
		ExecutorService executor = Executors.newFixedThreadPool(clients);
		for (int c = 0; c < clients; c++) {
			executor.execute(() -> {
				while (System.nanoTime() < deadline) {
					long n = enqueued.incrementAndGet();
					NewJob job = NewJob.of("bench", "{\"n\":" + n + "}");
					repository.enqueue(keyed ? job.withIdempotencyKey("key-" + n) : job);
				}
			});
		}
		executor.shutdown();
		try {
			executor.awaitTermination(window.toSeconds() + 30, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		ds.close();
		long stored = jdbc.sql("SELECT count(*) FROM jobs").query(Long.class).single();
		return stored / (window.toNanos() / 1e9);
	}

	private static HikariDataSource dataSource(int size) {
		var config = new HikariConfig();
		config.setJdbcUrl(postgres.getJdbcUrl());
		config.setUsername(postgres.getUsername());
		config.setPassword(postgres.getPassword());
		config.setMaximumPoolSize(size);
		return new HikariDataSource(config);
	}

	private interface Work {
		void run(Job job) throws Exception;
	}

	private static JobHandler handler(Work work) {
		return new JobHandler() {
			@Override
			public String type() {
				return "bench";
			}

			@Override
			public void handle(Job job) throws Exception {
				work.run(job);
			}
		};
	}

	private static JobHandler noop() {
		return handler(job -> {
		});
	}

	private interface Measurement {
		double run();
	}

	private static double median(Measurement measurement) {
		double[] samples = new double[REPEATS];
		for (int i = 0; i < REPEATS; i++) {
			samples[i] = measurement.run();
		}
		Arrays.sort(samples);
		return samples[REPEATS / 2];
	}

	private static void section(String title, String header, String separator) {
		report.append("## ").append(title).append("\n\n").append(header).append('\n').append(separator).append('\n');
		System.out.println("== " + title);
	}

	private static void row(Object... cells) {
		StringBuilder line = new StringBuilder("|");
		for (Object cell : cells) {
			line.append(' ').append(cell).append(" |");
		}
		report.append(line).append('\n');
		System.out.println(line);
	}

	private static String fmt(double rate) {
		return String.format("%,.0f", rate);
	}

	private static void sleep(Duration duration) {
		try {
			Thread.sleep(duration.toMillis());
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}
