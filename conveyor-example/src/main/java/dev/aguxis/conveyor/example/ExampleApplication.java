package dev.aguxis.conveyor.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point of the example shop. Nothing here configures conveyor: adding the starter is enough for
 * the jobs tables, the workers and an injectable {@code JobRepository}.
 */
@SpringBootApplication
public class ExampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(ExampleApplication.class, args);
	}
}
