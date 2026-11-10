package dev.aguxis.conveyor.autoconfigure;

import java.net.InetAddress;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import javax.sql.DataSource;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.JdbcConnectionDetails;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import dev.aguxis.conveyor.core.CronScheduler;
import dev.aguxis.conveyor.core.JobHandler;
import dev.aguxis.conveyor.core.JobHandlerRegistry;
import dev.aguxis.conveyor.core.JobNotificationListener;
import dev.aguxis.conveyor.core.JobRepository;
import dev.aguxis.conveyor.core.LeaderLease;
import dev.aguxis.conveyor.core.RetentionSweeper;
import dev.aguxis.conveyor.core.RetryPolicy;
import dev.aguxis.conveyor.core.ScheduleRepository;
import dev.aguxis.conveyor.core.WorkerPool;

/**
 * Wires the core classes, which know nothing about Spring Boot, from {@link ConveyorProperties}, on top
 * of the app's own DataSource. Sharing that DataSource is what lets {@link JobRepository#enqueue} join a
 * transaction the app already has open.
 */
@AutoConfiguration(afterName = {
		"org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
		"org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration",
		"org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration",
		// so @ConditionalOnBean(MeterRegistry) below sees the app's registry when it has one
		"org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration" })
@ConditionalOnBean(DataSource.class)
@EnableConfigurationProperties(ConveyorProperties.class)
// RetentionSweeper and QueueMetrics run on @Scheduled; enabling it twice is harmless if the app does too
@EnableScheduling
public class ConveyorAutoConfiguration {

	/** Identifies this process in worker ids and as scheduler leader. Unique per start, not per host. */
	private final String instance = hostname() + "-" + UUID.randomUUID().toString().substring(0, 8);

	// JdbcClient.create goes through DataSourceUtils, so inside a @Transactional method it uses the
	// connection already bound to that transaction instead of taking a new one from the pool.
	// Bean names are prefixed so they can't clash with an app's own "jobRepository".
	@Bean
	JobRepository conveyorJobRepository(DataSource dataSource) {
		return new JobRepository(JdbcClient.create(dataSource));
	}

	@Bean
	ScheduleRepository conveyorScheduleRepository(DataSource dataSource) {
		return new ScheduleRepository(JdbcClient.create(dataSource));
	}

	@Bean
	LeaderLease conveyorLeaderLease(DataSource dataSource) {
		return new LeaderLease(JdbcClient.create(dataSource));
	}

	/** The app's handlers. An app that only enqueues (workers elsewhere) can have none. */
	@Bean
	JobHandlerRegistry jobHandlerRegistry(ObjectProvider<JobHandler> handlers) {
		return new JobHandlerRegistry(handlers.orderedStream().toList());
	}

	@Bean
	RetryPolicy retryPolicy(ConveyorProperties properties) {
		return new RetryPolicy(properties.retry().base(), properties.retry().max());
	}

	/** One {@link WorkerPool} per configured queue, each managed by the Spring lifecycle. */
	@Bean
	WorkerPools workerPools(ConveyorProperties properties, JobRepository jobs, JobHandlerRegistry handlers,
			RetryPolicy retryPolicy, ObjectProvider<MeterRegistry> meterRegistry) {
		var worker = properties.worker();
		if (!worker.enabled()) {
			// enqueue-only instance: it still writes jobs, it just never claims them
			return new WorkerPools(List.of());
		}
		var meters = meters(meterRegistry);
		var pools = worker.queues().stream()
				.map(queue -> new WorkerPool(jobs, handlers, retryPolicy, new WorkerPool.Settings(
						instance + "/" + queue, queue, worker.concurrency(), worker.pollInterval(),
						worker.lease(), worker.heartbeatInterval(), worker.shutdownTimeout()), meters))
				.toList();
		return new WorkerPools(pools);
	}

	/**
	 * Wakes the pools when jobs are enqueued. Connects with the same settings as the pool but through an
	 * unpooled data source, so the listener's session is its own and never borrowed from the workers.
	 * Needs Boot's connection details; an app that builds its DataSource by hand still works, its
	 * workers just find new jobs by polling.
	 */
	@Bean
	@ConditionalOnBooleanProperty(name = "conveyor.worker.listen", matchIfMissing = true)
	@ConditionalOnBean(JdbcConnectionDetails.class)
	JobNotificationListener jobNotificationListener(WorkerPools workerPools, JdbcConnectionDetails db,
			ObjectProvider<MeterRegistry> meterRegistry) {
		var dataSource = new DriverManagerDataSource(db.getJdbcUrl(), db.getUsername(), db.getPassword());
		var connectionProperties = new Properties();
		// Shows up in pg_stat_activity, so the session is easy to spot (and to kill in tests).
		connectionProperties.setProperty("ApplicationName", "conveyor-listener");
		dataSource.setConnectionProperties(connectionProperties);
		return new JobNotificationListener(dataSource, workerPools.pools(), meters(meterRegistry));
	}

	@Bean
	@ConditionalOnBooleanProperty(name = "conveyor.scheduler.enabled", matchIfMissing = true)
	CronScheduler cronScheduler(ConveyorProperties properties, ScheduleRepository schedules, JobRepository jobs,
			LeaderLease leases, PlatformTransactionManager transactionManager,
			ObjectProvider<MeterRegistry> meterRegistry) {
		var scheduler = properties.scheduler();
		return new CronScheduler(schedules, jobs, leases, new TransactionTemplate(transactionManager),
				new CronScheduler.Settings(instance, scheduler.tick(), scheduler.lease()), meters(meterRegistry));
	}

	@Bean
	@ConditionalOnBooleanProperty(name = "conveyor.retention.enabled", matchIfMissing = true)
	RetentionSweeper retentionSweeper(ConveyorProperties properties, JobRepository jobs,
			ObjectProvider<MeterRegistry> meterRegistry) {
		var retention = properties.retention();
		return new RetentionSweeper(jobs,
				new RetentionSweeper.Settings(retention.succeeded(), retention.dead(), retention.batchSize()),
				meters(meterRegistry));
	}

	// Backlog gauges cost a query on every refresh, so they only exist when someone can read them.
	@Bean
	@ConditionalOnBean(MeterRegistry.class)
	QueueMetrics queueMetrics(JobRepository jobs, MeterRegistry meterRegistry) {
		return new QueueMetrics(jobs, meterRegistry);
	}

	// Apps without Micrometer set up still get counters, on the global registry, where they cost next to nothing.
	private static MeterRegistry meters(ObjectProvider<MeterRegistry> registry) {
		return registry.getIfAvailable(() -> Metrics.globalRegistry);
	}

	// only for readability in locked_by and logs; uniqueness comes from the UUID part
	private static String hostname() {
		try {
			return InetAddress.getLocalHost().getHostName();
		}
		catch (Exception e) {
			return "unknown";
		}
	}
}
