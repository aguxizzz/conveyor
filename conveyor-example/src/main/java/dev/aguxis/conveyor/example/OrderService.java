package dev.aguxis.conveyor.example;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import dev.aguxis.conveyor.core.JobRepository;
import dev.aguxis.conveyor.core.NewJob;

/**
 * Places orders. The order row, the job that emails its confirmation and the stock update happen in one
 * transaction, so there's never an order without its email, nor an email for an order that failed.
 */
@Service
class OrderService {

	/** No stock left for the requested quantity. Thrown after the job was enqueued, to show the rollback. */
	static class OutOfStockException extends RuntimeException {
		OutOfStockException(long productId) {
			super("product " + productId + " doesn't have enough stock");
		}
	}

	private final JdbcClient jdbc;
	private final JobRepository jobs;

	OrderService(JdbcClient jdbc, JobRepository jobs) {
		this.jdbc = jdbc;
		this.jobs = jobs;
	}

	/** Returns the new order's id. */
	@Transactional
	long placeOrder(long productId, int quantity, String email) {
		long orderId = jdbc.sql("""
				INSERT INTO orders (product_id, quantity, email) VALUES (:productId, :quantity, :email)
				RETURNING id
				""")
				.param("productId", productId)
				.param("quantity", quantity)
				.param("email", email)
				.query(Long.class)
				.single();

		// Joins this method's transaction. The key makes a retried request reuse the job, not add a second email.
		jobs.enqueue(NewJob.of("order.confirm", "{\"orderId\": " + orderId + "}")
				.withIdempotencyKey("order-confirm-" + orderId));

		// Deliberately last: when it fails, the order and the job above are already written, and the
		// rollback has to take both back.
		int updated = jdbc.sql("UPDATE products SET stock = stock - :quantity WHERE id = :id AND stock >= :quantity")
				.param("quantity", quantity)
				.param("id", productId)
				.update();
		if (updated == 0) {
			throw new OutOfStockException(productId);
		}
		return orderId;
	}
}
