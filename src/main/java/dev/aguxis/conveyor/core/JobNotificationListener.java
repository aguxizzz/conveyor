package dev.aguxis.conveyor.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Turns Postgres {@code NOTIFY}s into {@link WorkerPool#wakeUp()} calls, so a job enqueued into an idle
 * queue is claimed within milliseconds instead of on the next poll.
 * <p>
 * Notifications are an optimization, never the source of truth: if one is missed (listener
 * reconnecting, a silently dropped connection), the job is still picked up by the regular poll. That's
 * why the listener can afford to be simple: one connection, and on any error reconnect and wake every
 * pool, since whatever was announced while it was gone went to nobody.
 * <p>
 * It holds its own long-lived connection, outside the pool: {@code LISTEN} is per session, and a
 * connection parked in {@code getNotifications} for hours would otherwise be one less for the workers.
 */
public class JobNotificationListener implements SmartLifecycle {

	public static final String CHANNEL = "conveyor_jobs";

	private static final Logger log = LoggerFactory.getLogger(JobNotificationListener.class);
	private static final int RECEIVE_TIMEOUT_MILLIS = 500;
	/** Idle connections can die without a TCP reset (NAT, firewalls); a query is the only way to notice. */
	private static final Duration HEALTH_CHECK_INTERVAL = Duration.ofSeconds(30);
	private static final Duration NETWORK_TIMEOUT = Duration.ofSeconds(10);
	private static final Duration RECONNECT_DELAY = Duration.ofSeconds(1);

	private final DataSource dataSource;
	private final Collection<WorkerPool> pools;
	private final Map<String, WorkerPool> poolsByQueue;

	private volatile boolean running;
	private volatile boolean connected;
	private Thread listener;

	/** @param dataSource must hand out new, unpooled connections */
	public JobNotificationListener(DataSource dataSource, Collection<WorkerPool> pools, MeterRegistry meterRegistry) {
		this.dataSource = dataSource;
		this.pools = pools;
		this.poolsByQueue = pools.stream().collect(Collectors.toMap(WorkerPool::queue, Function.identity()));
		Gauge.builder("conveyor.listener.connected", this, l -> l.connected ? 1 : 0)
				.description("1 while the LISTEN connection is up; at 0, pickup latency falls back to the poll interval")
				.register(meterRegistry);
	}

	@Override
	public synchronized void start() {
		if (running || pools.isEmpty()) {
			return;
		}
		running = true;
		listener = new Thread(this::listenLoop, "conveyor-listener");
		listener.setDaemon(true);
		listener.start();
	}

	@Override
	public synchronized void stop() {
		if (!running) {
			return;
		}
		running = false;
		try {
			// Wakes up within RECEIVE_TIMEOUT_MILLIS at most.
			listener.join(NETWORK_TIMEOUT.toMillis());
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	public boolean isConnected() {
		return connected;
	}

	private void listenLoop() {
		while (running) {
			try (Connection connection = dataSource.getConnection()) {
				listen(connection);
			}
			catch (SQLException e) {
				if (running) {
					log.warn("Notification listener lost its connection; retrying in {}", RECONNECT_DELAY, e);
				}
			}
			finally {
				connected = false;
			}
			if (running) {
				sleepQuietly(RECONNECT_DELAY);
			}
		}
	}

	private void listen(Connection connection) throws SQLException {
		connection.setNetworkTimeout(Runnable::run, (int) NETWORK_TIMEOUT.toMillis());
		try (Statement statement = connection.createStatement()) {
			statement.execute("LISTEN " + CHANNEL);
		}
		PGConnection pg = connection.unwrap(PGConnection.class);
		connected = true;
		log.info("Listening for new jobs on queues {}", poolsByQueue.keySet());
		// Anything enqueued before LISTEN took effect wasn't announced to us.
		pools.forEach(WorkerPool::wakeUp);

		long lastActivity = System.nanoTime();
		while (running) {
			PGNotification[] notifications = pg.getNotifications(RECEIVE_TIMEOUT_MILLIS);
			if (notifications != null && notifications.length > 0) {
				for (PGNotification notification : notifications) {
					WorkerPool pool = poolsByQueue.get(notification.getParameter());
					if (pool != null) {
						pool.wakeUp();
					}
				}
				lastActivity = System.nanoTime();
			}
			else if (System.nanoTime() - lastActivity > HEALTH_CHECK_INTERVAL.toNanos()) {
				try (Statement statement = connection.createStatement()) {
					statement.execute("SELECT 1");
				}
				lastActivity = System.nanoTime();
			}
		}
	}

	private static void sleepQuietly(Duration duration) {
		try {
			Thread.sleep(duration.toMillis());
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}
