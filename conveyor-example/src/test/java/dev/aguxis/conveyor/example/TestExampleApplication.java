package dev.aguxis.conveyor.example;

import org.springframework.boot.SpringApplication;

/** Runs the example against a throwaway Postgres container, no setup needed: {@code ./mvnw -pl conveyor-example spring-boot:test-run}. */
public class TestExampleApplication {

	public static void main(String[] args) {
		SpringApplication.from(ExampleApplication::main).with(TestcontainersConfiguration.class).run(args);
	}
}
