package dev.aguxis.conveyor;

import org.springframework.boot.SpringApplication;

public class TestConveyorApplication {

	public static void main(String[] args) {
		SpringApplication.from(ConveyorApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
