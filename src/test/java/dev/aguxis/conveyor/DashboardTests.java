package dev.aguxis.conveyor;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.forwardedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = { "conveyor.worker.enabled=false", "conveyor.examples.enabled=false" })
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class DashboardTests {

	@Autowired
	MockMvc mvc;

	@Test
	void servesTheDashboardAtTheRoot() throws Exception {
		mvc.perform(get("/")).andExpect(forwardedUrl("index.html"));

		mvc.perform(get("/index.html"))
				.andExpect(status().isOk())
				.andExpect(content().string(containsString("<title>Conveyor</title>")));
		mvc.perform(get("/dashboard.js"))
				.andExpect(status().isOk())
				.andExpect(content().contentTypeCompatibleWith("text/javascript"));
		mvc.perform(get("/dashboard.css")).andExpect(status().isOk());
	}
}
