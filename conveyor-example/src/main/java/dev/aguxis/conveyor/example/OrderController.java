package dev.aguxis.conveyor.example;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** {@code POST /orders} to place one, {@code GET /orders/{id}} to see whether its confirmation went out. */
@RestController
class OrderController {

	record PlaceOrder(long productId, int quantity, String email) {
	}

	record Order(long id, long productId, int quantity, String email, OffsetDateTime confirmationSentAt) {
	}

	private final OrderService orders;
	private final JdbcClient jdbc;

	OrderController(OrderService orders, JdbcClient jdbc) {
		this.orders = orders;
		this.jdbc = jdbc;
	}

	@PostMapping("/orders")
	ResponseEntity<Map<String, Long>> place(@RequestBody PlaceOrder request) {
		long id = orders.placeOrder(request.productId(), request.quantity(), request.email());
		return ResponseEntity.created(URI.create("/orders/" + id)).body(Map.of("id", id));
	}

	@GetMapping("/orders/{id}")
	ResponseEntity<Order> get(@PathVariable long id) {
		return ResponseEntity.of(jdbc.sql("""
				SELECT id, product_id, quantity, email, confirmation_sent_at FROM orders WHERE id = :id
				""")
				.param("id", id)
				.query(Order.class)
				.optional());
	}

	@ExceptionHandler(OrderService.OutOfStockException.class)
	ResponseEntity<Map<String, String>> outOfStock(OrderService.OutOfStockException e) {
		return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
	}
}
