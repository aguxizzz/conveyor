package dev.aguxis.conveyor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point. One process serves the REST API and the dashboard, runs workers for its configured
 * queues and takes part in scheduler leader election; any of those can be turned off by config.
 */
@SpringBootApplication
public class ConveyorApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConveyorApplication.class, args);
	}

}
