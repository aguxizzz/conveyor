package dev.aguxis.conveyor.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import tools.jackson.databind.json.JsonMapper;

import dev.aguxis.conveyor.core.Job;
import dev.aguxis.conveyor.core.JobHandler;

/**
 * {@code order.confirm}: "sends" the confirmation email (a log line here) and stamps the order. Runs
 * on a conveyor worker, after the order's transaction committed, so the order is always there to read.
 */
@Component
class OrderConfirmationHandler implements JobHandler {

	private static final Logger log = LoggerFactory.getLogger(OrderConfirmationHandler.class);

	private final JdbcClient jdbc;
	private final JsonMapper json;

	OrderConfirmationHandler(JdbcClient jdbc, JsonMapper json) {
		this.jdbc = jdbc;
		this.json = json;
	}

	@Override
	public String type() {
		return "order.confirm";
	}

	@Override
	public void handle(Job job) {
		long orderId = json.readTree(job.payload()).path("orderId").asLong();
		String email = jdbc.sql("SELECT email FROM orders WHERE id = :id").param("id", orderId)
				.query(String.class).single();
		log.info("Sending confirmation of order {} to {}", orderId, email);
		// Only the first run stamps it: a retry after a crash mid-send must not move the time.
		jdbc.sql("UPDATE orders SET confirmation_sent_at = now() WHERE id = :id AND confirmation_sent_at IS NULL")
				.param("id", orderId)
				.update();
	}
}
