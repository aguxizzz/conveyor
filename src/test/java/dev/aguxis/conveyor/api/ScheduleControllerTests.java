package dev.aguxis.conveyor.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
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

import dev.aguxis.conveyor.TestcontainersConfiguration;

@SpringBootTest(properties = { "conveyor.worker.enabled=false", "conveyor.examples.enabled=false",
		"conveyor.scheduler.tick=50ms" })
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class ScheduleControllerTests {

	@Autowired
	MockMvc mvc;

	@Autowired
	JdbcClient jdbc;

	@BeforeEach
	void cleanDatabase() {
		jdbc.sql("TRUNCATE jobs, schedules RESTART IDENTITY").update();
	}

	@Test
	void createsReadsUpdatesAndDeletesASchedule() throws Exception {
		mvc.perform(put("/api/schedules/nightly").contentType(MediaType.APPLICATION_JSON).content("""
				{"cron": "0 0 3 * * *", "timeZone": "America/Argentina/Buenos_Aires",
				 "type": "report", "payload": {"format": "pdf"}}
				"""))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.name").value("nightly"))
				.andExpect(jsonPath("$.payload.format").value("pdf"))
				.andExpect(jsonPath("$.enabled").value(true))
				// 3 AM in Buenos Aires (UTC-3) is 06:00 UTC.
				.andExpect(jsonPath("$.nextRunAt").value(org.hamcrest.Matchers.endsWith("T06:00:00Z")));

		mvc.perform(put("/api/schedules/nightly").contentType(MediaType.APPLICATION_JSON)
				.content("{\"cron\": \"@hourly\", \"type\": \"report\", \"enabled\": false}"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.cron").value("@hourly"))
				.andExpect(jsonPath("$.timeZone").value("UTC"));

		mvc.perform(get("/api/schedules"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.length()").value(1))
				.andExpect(jsonPath("$[0].enabled").value(false));

		mvc.perform(delete("/api/schedules/nightly")).andExpect(status().isNoContent());
		mvc.perform(get("/api/schedules/nightly")).andExpect(status().isNotFound());
		mvc.perform(delete("/api/schedules/nightly")).andExpect(status().isNotFound());
	}

	@Test
	void rejectsInvalidSchedules() throws Exception {
		putSchedule("bad", "{\"cron\": \"every monday\", \"type\": \"report\"}").andExpect(status().isBadRequest());
		putSchedule("bad", "{\"cron\": \"0 0 3 * * *\", \"timeZone\": \"Mars/Olympus\", \"type\": \"report\"}")
				.andExpect(status().isBadRequest());
		putSchedule("bad", "{\"cron\": \"0 0 0 30 2 *\", \"type\": \"report\"}").andExpect(status().isBadRequest());
		putSchedule("bad", "{\"cron\": \"@daily\"}").andExpect(status().isBadRequest());
		putSchedule("no spaces", "{\"cron\": \"@daily\", \"type\": \"report\"}").andExpect(status().isBadRequest());
	}

	@Test
	void theAppsOwnSchedulerLeadsAndFires() throws Exception {
		putSchedule("tick", "{\"cron\": \"* * * * * *\", \"type\": \"tick\"}").andExpect(status().isOk());

		long deadline = System.nanoTime() + 5_000_000_000L;
		while (jdbc.sql("SELECT count(*) FROM jobs WHERE type = 'tick'").query(Long.class).single() == 0) {
			if (System.nanoTime() > deadline) {
				throw new AssertionError("Schedule never fired");
			}
			Thread.sleep(50);
		}
		mvc.perform(get("/api/scheduler/leader"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.term").isNumber());
	}

	private org.springframework.test.web.servlet.ResultActions putSchedule(String name, String body) throws Exception {
		return mvc.perform(put("/api/schedules/" + name).contentType(MediaType.APPLICATION_JSON).content(body));
	}
}
