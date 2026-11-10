package dev.aguxis.conveyor;

import org.springframework.boot.SpringApplication;

/** Runs the app locally against a throwaway Postgres container, no compose needed: {@code ./mvnw spring-boot:test-run}. */
public class TestConveyorApplication {

	public static void main(String[] args) {
		SpringApplication.from(ConveyorApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
