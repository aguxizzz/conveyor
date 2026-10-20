package dev.aguxis.conveyor;

import java.net.InetAddress;
import java.util.List;
import java.util.UUID;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import dev.aguxis.conveyor.core.JobHandler;
import dev.aguxis.conveyor.core.JobHandlerRegistry;
import dev.aguxis.conveyor.core.JobRepository;
import dev.aguxis.conveyor.core.RetryPolicy;
import dev.aguxis.conveyor.core.WorkerPool;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ConveyorProperties.class)
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
			RetryPolicy retryPolicy) {
		var worker = properties.worker();
		if (!worker.enabled()) {
			return new WorkerPools(List.of());
		}
		String instance = hostname() + "-" + UUID.randomUUID().toString().substring(0, 8);
		List<WorkerPool> pools = worker.queues().stream()
				.map(queue -> new WorkerPool(jobs, handlers, retryPolicy, new WorkerPool.Settings(
						instance + "/" + queue, queue, worker.concurrency(), worker.pollInterval(),
						worker.lease(), worker.heartbeatInterval(), worker.shutdownTimeout())))
				.toList();
		return new WorkerPools(pools);
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
