package dev.aguxis.conveyor.core;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JobHandlerRegistry {

	private final Map<String, JobHandler> handlers;

	public JobHandlerRegistry(List<JobHandler> handlers) {
		this.handlers = handlers.stream().collect(Collectors.toUnmodifiableMap(JobHandler::type, Function.identity()));
	}

	public Optional<JobHandler> find(String type) {
		return Optional.ofNullable(handlers.get(type));
	}
}
