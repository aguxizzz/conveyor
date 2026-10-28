package dev.aguxis.conveyor.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
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
import org.springframework.test.web.servlet.ResultActions;

import com.jayway.jsonpath.JsonPath;

import dev.aguxis.conveyor.TestcontainersConfiguration;

@SpringBootTest(properties = { "conveyor.worker.enabled=false", "conveyor.examples.enabled=false" })
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class JobControllerTests {

	@Autowired
	MockMvc mvc;

	@Autowired
	JdbcClient jdbc;

	@BeforeEach
	void cleanDatabase() {
		jdbc.sql("TRUNCATE jobs, schedules RESTART IDENTITY").update();
	}

	@Test
	void enqueuesWithDefaultsAndReadsTheJobBack() throws Exception {
		long id = idOf(enqueue("{\"type\": \"email.send\"}")
				.andExpect(status().isCreated())
				.andExpect(jsonPath("$.queue").value("default"))
				.andExpect(jsonPath("$.status").value("PENDING"))
				.andExpect(jsonPath("$.priority").value(0))
				.andExpect(jsonPath("$.maxAttempts").value(5))
				.andExpect(jsonPath("$.attempts").value(0)));

		mvc.perform(get("/api/jobs/" + id))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.type").value("email.send"))
				// No payload means an empty object, not null.
				.andExpect(jsonPath("$.payload").isMap());
	}

	@Test
	void returnsThePayloadAsJsonAndPointsLocationAtTheNewJob() throws Exception {
		var result = enqueue("""
				{"type": "report", "queue": "reports", "payload": {"format": "pdf", "pages": [1, 2]}}
				""")
				.andExpect(status().isCreated())
				.andExpect(jsonPath("$.payload.format").value("pdf"))
				.andExpect(jsonPath("$.payload.pages.length()").value(2));

		long id = idOf(result);
		result.andExpect(header().string("Location", "/api/jobs/" + id));
	}

	@Test
	void sameIdempotencyKeyReturnsTheExistingJob() throws Exception {
		String body = "{\"type\": \"charge\", \"idempotencyKey\": \"order-42\"}";
		long first = idOf(enqueue(body).andExpect(status().isCreated()));
		long second = idOf(enqueue(body).andExpect(status().isCreated()));

		assertThat(second).isEqualTo(first);
		assertThat(jdbc.sql("SELECT count(*) FROM jobs").query(Long.class).single()).isEqualTo(1);
	}

	@Test
	void rejectsInvalidJobs() throws Exception {
		enqueue("{}").andExpect(status().isBadRequest());
		enqueue("{\"type\": \"  \"}").andExpect(status().isBadRequest());
		enqueue("{\"type\": \"x\", \"maxAttempts\": 0}").andExpect(status().isBadRequest());
		enqueue("{\"type\": \"x\", \"maxAttempts\": 101}").andExpect(status().isBadRequest());
		enqueue("{\"type\": \"x\", \"delaySeconds\": -1}").andExpect(status().isBadRequest());
		enqueue("{\"type\": ").andExpect(status().isBadRequest());

		assertThat(jdbc.sql("SELECT count(*) FROM jobs").query(Long.class).single()).isZero();
	}

	@Test
	void listsNewestFirstAndFiltersByStatusAndQueue() throws Exception {
		enqueue("{\"type\": \"a\", \"queue\": \"emails\"}");
		enqueue("{\"type\": \"b\", \"queue\": \"emails\"}");
		long dead = idOf(enqueue("{\"type\": \"c\", \"queue\": \"reports\"}"));
		kill(dead);

		mvc.perform(get("/api/jobs"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[*].type").value(org.hamcrest.Matchers.contains("c", "b", "a")));
		mvc.perform(get("/api/jobs").param("queue", "emails"))
				.andExpect(jsonPath("$[*].type").value(org.hamcrest.Matchers.contains("b", "a")));
		mvc.perform(get("/api/jobs").param("status", "DEAD"))
				.andExpect(jsonPath("$[*].type").value(org.hamcrest.Matchers.contains("c")));
		mvc.perform(get("/api/jobs").param("status", "DEAD").param("queue", "emails"))
				.andExpect(jsonPath("$.length()").value(0));
		mvc.perform(get("/api/jobs").param("limit", "1"))
				.andExpect(jsonPath("$[*].type").value(org.hamcrest.Matchers.contains("c")));
	}

	@Test
	void rejectsBadListParameters() throws Exception {
		mvc.perform(get("/api/jobs").param("limit", "0")).andExpect(status().isBadRequest());
		mvc.perform(get("/api/jobs").param("limit", "501")).andExpect(status().isBadRequest());
		mvc.perform(get("/api/jobs").param("limit", "lots")).andExpect(status().isBadRequest());
		mvc.perform(get("/api/jobs").param("status", "SLEEPING")).andExpect(status().isBadRequest());
	}

	@Test
	void unknownJobIsNotFound() throws Exception {
		mvc.perform(get("/api/jobs/999")).andExpect(status().isNotFound());
		mvc.perform(post("/api/jobs/999/retry")).andExpect(status().isNotFound());
	}

	@Test
	void retryOnlyRevivesDeadJobs() throws Exception {
		long id = idOf(enqueue("{\"type\": \"flaky\"}"));
		mvc.perform(post("/api/jobs/" + id + "/retry")).andExpect(status().isNotFound());

		kill(id);
		mvc.perform(post("/api/jobs/" + id + "/retry"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.status").value("PENDING"))
				.andExpect(jsonPath("$.attempts").value(0))
				.andExpect(jsonPath("$.finishedAt").isEmpty());
	}

	@Test
	void reportsCountsPerQueue() throws Exception {
		enqueue("{\"type\": \"a\", \"queue\": \"emails\"}");
		enqueue("{\"type\": \"b\", \"queue\": \"emails\"}");
		kill(idOf(enqueue("{\"type\": \"c\", \"queue\": \"reports\"}")));

		mvc.perform(get("/api/queues"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[?(@.queue == 'emails')].pending").value(2))
				.andExpect(jsonPath("$[?(@.queue == 'reports')].dead").value(1))
				.andExpect(jsonPath("$[?(@.queue == 'reports')].pending").value(0));
	}

	private ResultActions enqueue(String body) throws Exception {
		return mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content(body));
	}

	private static long idOf(ResultActions result) throws Exception {
		return JsonPath.<Number>read(result.andReturn().getResponse().getContentAsString(), "$.id").longValue();
	}

	/** Dead-letters a job directly, as the worker would after its last failed attempt. */
	private void kill(long id) {
		jdbc.sql("UPDATE jobs SET status = 'DEAD', last_error = 'boom', finished_at = now() WHERE id = :id")
				.param("id", id)
				.update();
	}
}
