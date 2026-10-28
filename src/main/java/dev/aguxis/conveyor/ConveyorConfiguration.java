package dev.aguxis.conveyor;

import java.net.InetAddress;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.JdbcConnectionDetails;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.annotation.EnableScheduling;

import dev.aguxis.conveyor.core.JobHandler;
import dev.aguxis.conveyor.core.JobHandlerRegistry;
import dev.aguxis.conveyor.core.JobNotificationListener;
import dev.aguxis.conveyor.core.JobRepository;
import dev.aguxis.conveyor.core.RetryPolicy;
import dev.aguxis.conveyor.core.WorkerPool;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ConveyorProperties.class)
@EnableScheduling
class ConveyorConfiguration {

	@Bean
	JobHandlerRegistry jobHandlerRegistry(List<JobHandler> handlers) {
		return new JobHandlerRegistry(handlers);
	}

	@Bean
	RetryPolicy retryPolicy(ConveyorProperties properties) {
		return new RetryPolicy(properties.retry().base(), properties.retry().max());
	}

	/** One {@link WorkerPool} per configured queue, each managed by the Spring lifecycle. */
	@Bean
	WorkerPools workerPools(ConveyorProperties properties, JobRepository jobs, JobHandlerRegistry handlers,
			RetryPolicy retryPolicy, MeterRegistry meterRegistry) {
		var worker = properties.worker();
		if (!worker.enabled()) {
			return new WorkerPools(List.of());
		}
		String instance = hostname() + "-" + UUID.randomUUID().toString().substring(0, 8);
		List<WorkerPool> pools = worker.queues().stream()
				.map(queue -> new WorkerPool(jobs, handlers, retryPolicy, new WorkerPool.Settings(
						instance + "/" + queue, queue, worker.concurrency(), worker.pollInterval(),
						worker.lease(), worker.heartbeatInterval(), worker.shutdownTimeout()), meterRegistry))
				.toList();
		return new WorkerPools(pools);
	}

	/**
	 * Wakes the pools when jobs are enqueued. Connects with the same settings as the pool but through an
	 * unpooled data source, so the listener's session is its own and never borrowed from the workers.
	 */
	@Bean
	@ConditionalOnBooleanProperty(name = "conveyor.worker.listen", matchIfMissing = true)
	JobNotificationListener jobNotificationListener(WorkerPools workerPools, JdbcConnectionDetails db,
			MeterRegistry meterRegistry) {
		var dataSource = new DriverManagerDataSource(db.getJdbcUrl(), db.getUsername(), db.getPassword());
		var connectionProperties = new Properties();
		// Shows up in pg_stat_activity, so the session is easy to spot (and to kill in tests).
		connectionProperties.setProperty("ApplicationName", "conveyor-listener");
		dataSource.setConnectionProperties(connectionProperties);
		return new JobNotificationListener(dataSource, workerPools.pools(), meterRegistry);
	}

	@Bean
	QueueMetrics queueMetrics(JobRepository jobs, MeterRegistry meterRegistry) {
		return new QueueMetrics(jobs, meterRegistry);
	}

	private static String hostname() {
		try {
			return InetAddress.getLocalHost().getHostName();
		}
		catch (Exception e) {
			return "unknown";
		}
	}
}
