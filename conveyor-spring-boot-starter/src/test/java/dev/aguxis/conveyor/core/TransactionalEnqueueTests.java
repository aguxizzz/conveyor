package dev.aguxis.conveyor.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

import dev.aguxis.conveyor.TestcontainersConfiguration;

/**
 * Enqueue as part of the app's own transaction: the business row and its job commit or roll back
 * together, and no worker can claim the job while that transaction is still open.
 */
@SpringBootTest(properties = { "conveyor.worker.enabled=false", "conveyor.scheduler.enabled=false",
		"conveyor.retention.enabled=false" })
@Import(TestcontainersConfiguration.class)
class TransactionalEnqueueTests {

	private static final Duration LEASE = Duration.ofSeconds(30);

	@Autowired
	JobRepository jobs;

	@Autowired
	JdbcClient jdbc;

	@Autowired
	TransactionTemplate transaction;

	@BeforeEach
	void cleanDatabase() {
		// stands in for the app's own table, written in the same transaction as the job
		jdbc.sql("CREATE TABLE IF NOT EXISTS orders (id bigserial PRIMARY KEY)").update();
		jdbc.sql("TRUNCATE jobs, orders RESTART IDENTITY").update();
	}

	@Test
	void rollingBackTheAppsTransactionDropsTheJobWithIt() {
		transaction.executeWithoutResult(status -> {
			placeOrderAndEnqueue(NewJob.of("order.confirm", "{}"));
			status.setRollbackOnly(); // e.g. a validation further down the same service method failed
		});

		assertThat(count("orders")).isZero();
		assertThat(count("jobs")).isZero();
	}

	@Test
	void theJobCantBeClaimedUntilTheAppsTransactionCommits() {
		transaction.executeWithoutResult(status -> {
			placeOrderAndEnqueue(NewJob.of("order.confirm", "{}"));
			// a worker polls meanwhile, on its own connection and transaction
			assertThat(claimFromAnotherThread()).isEmpty();
		});

		List<Job> claimed = claimFromAnotherThread();
		assertThat(claimed).singleElement().extracting(Job::type).isEqualTo("order.confirm");
		assertThat(count("orders")).isEqualTo(1);
	}

	@Test
	void aRolledBackEnqueueDoesntKeepItsIdempotencyKey() {
		var job = NewJob.of("order.confirm", "{}").withIdempotencyKey("order-42");
		transaction.executeWithoutResult(status -> {
			jobs.enqueue(job);
			status.setRollbackOnly();
		});

		// the retry after the failed request is a first enqueue, not a duplicate of a job that never existed
		Job retried = transaction.execute(status -> jobs.enqueue(job));

		assertThat(jobs.findByIdempotencyKey("order-42")).get().extracting(Job::id).isEqualTo(retried.id());
		assertThat(count("jobs")).isEqualTo(1);
	}

	private void placeOrderAndEnqueue(NewJob job) {
		jdbc.sql("INSERT INTO orders DEFAULT VALUES").update();
		jobs.enqueue(job);
	}

	private List<Job> claimFromAnotherThread() {
		return CompletableFuture.supplyAsync(() -> jobs.claim("default", "worker", 10, LEASE)).join();
	}

	private long count(String table) {
		return jdbc.sql("SELECT count(*) FROM " + table).query(Long.class).single();
	}
}
