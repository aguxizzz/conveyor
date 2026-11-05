package dev.aguxis.conveyor;

import java.util.List;

import org.springframework.context.SmartLifecycle;

import dev.aguxis.conveyor.core.WorkerPool;

/** Starts/stops all worker pools together with the application context. */
public class WorkerPools implements SmartLifecycle {

	private final List<WorkerPool> pools;
	private volatile boolean running;

	public WorkerPools(List<WorkerPool> pools) {
		this.pools = pools;
	}

	@Override
	public void start() {
		pools.forEach(WorkerPool::start);
		running = true;
	}

	@Override
	public void stop() {
		// in parallel so the shutdown timeouts of several queues overlap instead of adding up
		pools.parallelStream().forEach(WorkerPool::stop);
		running = false;
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	public List<WorkerPool> pools() {
		return pools;
	}
}
