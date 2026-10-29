package dev.aguxis.conveyor.api;

import java.net.URI;
import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import dev.aguxis.conveyor.core.JobRepository;
import dev.aguxis.conveyor.core.JobStatus;
import dev.aguxis.conveyor.core.QueueStats;

@RestController
@RequestMapping("/api")
class JobController {

	private final JobRepository jobs;

	JobController(JobRepository jobs) {
		this.jobs = jobs;
	}

	@PostMapping("/jobs")
	ResponseEntity<JobResponse> enqueue(@Valid @RequestBody EnqueueRequest request) {
		var job = jobs.enqueue(request.toNewJob());
		return ResponseEntity.created(URI.create("/api/jobs/" + job.id())).body(JobResponse.from(job));
	}

	@GetMapping("/jobs/{id}")
	ResponseEntity<JobResponse> get(@PathVariable long id) {
		return ResponseEntity.of(jobs.findById(id).map(JobResponse::from));
	}

	@GetMapping("/jobs")
	List<JobResponse> list(@RequestParam(required = false) JobStatus status,
			@RequestParam(required = false) String queue,
			@RequestParam(defaultValue = "50") @Min(1) @Max(500) int limit) {
		return jobs.list(status, queue, limit).stream().map(JobResponse::from).toList();
	}

	/** Re-queues a DEAD job. 404 if it doesn't exist or isn't dead. */
	@PostMapping("/jobs/{id}/retry")
	ResponseEntity<JobResponse> retry(@PathVariable long id) {
		return ResponseEntity.of(jobs.revive(id).map(JobResponse::from));
	}

	@GetMapping("/queues")
	List<QueueStats> queues() {
		return jobs.stats();
	}
}
