package dev.aguxis.conveyor.api;

import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import dev.aguxis.conveyor.core.CronScheduler;
import dev.aguxis.conveyor.core.LeaderLease;
import dev.aguxis.conveyor.core.ScheduleRepository;

@RestController
@RequestMapping("/api")
class ScheduleController {

	private static final String NAME = "[A-Za-z0-9._-]{1,100}";

	private final ScheduleRepository schedules;
	private final LeaderLease leases;

	ScheduleController(ScheduleRepository schedules, LeaderLease leases) {
		this.schedules = schedules;
		this.leases = leases;
	}

	/** Creates or replaces the schedule; PUT because the client picks the name, so retries are safe. */
	@PutMapping("/schedules/{name}")
	ScheduleResponse save(@PathVariable @Pattern(regexp = NAME) String name,
			@Valid @RequestBody ScheduleRequest request) {
		try {
			return ScheduleResponse.from(schedules.save(request.toNewSchedule(name)));
		}
		catch (IllegalArgumentException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
		}
	}

	@GetMapping("/schedules")
	List<ScheduleResponse> list() {
		return schedules.findAll().stream().map(ScheduleResponse::from).toList();
	}

	@GetMapping("/schedules/{name}")
	ResponseEntity<ScheduleResponse> get(@PathVariable String name) {
		return ResponseEntity.of(schedules.findByName(name).map(ScheduleResponse::from));
	}

	@DeleteMapping("/schedules/{name}")
	ResponseEntity<Void> delete(@PathVariable String name) {
		return schedules.delete(name) ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
	}

	/** Which instance is firing schedules right now. 404 while there is no leader. */
	@GetMapping("/scheduler/leader")
	ResponseEntity<LeaderLease.Holder> leader() {
		return ResponseEntity.of(leases.current(CronScheduler.LEASE_NAME));
	}
}
