package dev.aguxis.conveyor.example;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.MockMvc;

/**
 * The example app end to end, with the starter's real workers running: a placed order gets its
 * confirmation from a job, and an order that fails on stock leaves neither the order nor the job behind.
 * Starting at all also proves the app's Flyway and conveyor's migrate the same fresh database.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class OrderFlowTests {

	@Autowired
	MockMvc mvc;

	@Autowired
	JdbcClient jdbc;

	@BeforeEach
	void resetShop() {
		jdbc.sql("TRUNCATE jobs, orders RESTART IDENTITY").update();
		jdbc.sql("UPDATE products SET stock = CASE name WHEN 'keyboard' THEN 3 ELSE 0 END").update();
	}

	@Test
	void aPlacedOrderGetsItsConfirmationFromAJob() throws Exception {
		mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON)
				.content("{\"productId\": 1, \"quantity\": 2, \"email\": \"ana@example.com\"}"))
				.andExpect(status().isCreated())
				.andExpect(jsonPath("$.id").value(1));

		// the worker picks the job up through LISTEN/NOTIFY, well within this
		long deadline = System.nanoTime() + 10_000_000_000L;
		while (confirmationSentAt(1) == null && System.nanoTime() < deadline) {
			Thread.sleep(50);
		}
		mvc.perform(get("/orders/1")).andExpect(jsonPath("$.confirmationSentAt").isNotEmpty());
		assertThat(jdbc.sql("SELECT stock FROM products WHERE id = 1").query(Integer.class).single()).isEqualTo(1);
	}

	@Test
	void anOrderThatFailsOnStockLeavesNoJobBehind() throws Exception {
		mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON)
				.content("{\"productId\": 2, \"quantity\": 1, \"email\": \"ana@example.com\"}"))
				.andExpect(status().isConflict());

		// the job was enqueued before the stock check failed; the rollback took it back with the order
		assertThat(jdbc.sql("SELECT count(*) FROM orders").query(Long.class).single()).isZero();
		assertThat(jdbc.sql("SELECT count(*) FROM jobs").query(Long.class).single()).isZero();
	}

	@Test
	void conveyorKeepsItsMigrationHistoryApartFromTheApps() {
		assertThat(jdbc.sql("SELECT script FROM flyway_schema_history WHERE success").query(String.class).list())
				.containsExactly("V1__shop.sql");
		assertThat(jdbc.sql("SELECT count(*) FROM conveyor_schema_history WHERE version = '6' AND success")
				.query(Long.class).single()).isEqualTo(1);
	}

	private Object confirmationSentAt(long orderId) {
		return jdbc.sql("SELECT confirmation_sent_at FROM orders WHERE id = :id").param("id", orderId)
				.query(Object.class).optional().orElse(null);
	}
}
