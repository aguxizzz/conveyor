# Conveyor

[![CI](https://github.com/aguxizzz/conveyor/actions/workflows/ci.yml/badge.svg)](https://github.com/aguxizzz/conveyor/actions/workflows/ci.yml)

A distributed background-job queue for the JVM, backed only by PostgreSQL.

Enqueue work over HTTP (or from Java), and any number of worker instances pick it up, run it with
retries and exponential backoff, and survive crashes, without Redis, Kafka or RabbitMQ.
Postgres is already in most stacks; Conveyor makes it your job queue.

**Stack:** Java 17 · Spring Boot 4 · PostgreSQL 17 · Flyway · JdbcClient · Micrometer · Prometheus · Grafana · Testcontainers · Docker · GitHub Actions

## Features

- **Safe concurrent claiming**: many workers poll one queue with `FOR UPDATE SKIP LOCKED`; a job is
  never handed to two workers at once and pollers never block each other.
- **Leases + heartbeats**: a running job holds a time-boxed lease that its worker keeps extending.
  If the worker dies, the lease expires and another instance takes over.
- **Fencing tokens**: the attempt counter fences every state transition, so a "zombie" worker that
  wakes up after losing its lease cannot overwrite the result of the attempt that replaced it.
- **Retries with full-jitter exponential backoff** and a dead-letter state (`DEAD`) with manual revive.
- **Idempotent enqueue** via `idempotencyKey`: clients can safely retry the enqueue request itself.
- **Priorities, delayed jobs, multiple queues**, and bounded concurrency per worker.
- **Observability**: Micrometer metrics scraped by Prometheus, with a provisioned Grafana dashboard
  (throughput, backlog, pickup latency, execution time, worker utilization).
- **Database time only**: all scheduling uses Postgres `now()`, so clock skew between hosts doesn't matter.

## Architecture

```
            POST /api/jobs                      ┌──────────────── worker instance ───────────────┐
 client ──────────────────────┐                 │  poller ──claim(n free)──┐                      │
                              ▼                 │     ▲                    ▼                      │
                     ┌─────────────────┐  SKIP  │     │ capacity   executor (N threads) → handler │
                     │  PostgreSQL     │◄───────┤     └──────────────────  │                      │
                     │  jobs table     │ LOCKED │  heartbeat ── extend lease of in-flight jobs    │
                     └─────────────────┘        └─────────────────────────────────────────────────┘
                              ▲                  ┌──────────────── worker instance ────────────────┐
                              └──────────────────┤                    ...                          │
                                                 └─────────────────────────────────────────────────┘
```

Job lifecycle:

```
PENDING ──claim──► RUNNING ──success──► SUCCEEDED
   ▲                 │  │
   └──fail (retry)───┘  └──fail, attempts exhausted──► DEAD ──POST /retry──► PENDING
   ▲                 │
   └─lease expired───┘   (worker crashed: re-claimed by another worker)
```

## Quick start

```bash
docker compose up --build          # Postgres, 2 competing workers (:8080 + next free port), Prometheus, Grafana

curl -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"type":"demo.flaky","payload":{"failRate":0.6},"maxAttempts":5}'

curl localhost:8080/api/jobs?status=DEAD
curl localhost:8080/api/queues
```

Then generate some load and watch it in Grafana at http://localhost:3000 (no login):

```bash
scripts/load.sh 30 120             # 30 jobs/s for 2 minutes; try 100 to see the backlog build up
```

## Performance

Measured with a reproducible benchmark against real Postgres (details, method and the bug it found in
[BENCHMARK.md](BENCHMARK.md)):

| | Result |
|---|---:|
| Queue overhead, 1 worker (no-op jobs) | ~3,300 jobs/s |
| Peak, 2–4 workers on a 4-core laptop shared with Postgres | ~5,000 jobs/s |
| Claim latency with 100,000 pending jobs | ~1 ms (was 42 ms before the fix) |
| Throughput with 1M finished jobs in the table | unchanged |

Profiling the first run showed the claim query sorting the entire backlog on every poll; splitting
lease recovery out and matching the index to the `ORDER BY` made claims O(1) in backlog size and
raised single-worker throughput 6×.

## Metrics

Exposed at `/actuator/prometheus`. Two sources, on purpose:

| Metric | Source | What it answers |
|--------|--------|-----------------|
| `conveyor_job_execution_seconds` (histogram; `queue`, `type`, `outcome`) | each worker | Throughput, failure rate and p95 per job type. `outcome` is `succeeded`, `retried`, `dead` or `lost` (finished after its lease was taken over). |
| `conveyor_job_pickup_latency_seconds` (histogram; `queue`) | each worker | How long a runnable job waits before a worker claims it. |
| `conveyor_worker_in_flight`, `conveyor_worker_concurrency` | each worker | Utilization: are workers saturated? |
| `conveyor_queue_jobs` (`queue`, `status`) | Postgres | Backlog by status. |
| `conveyor_queue_oldest_ready_age_seconds` (`queue`) | Postgres | The alerting signal: grows without bound when workers can't keep up. |

Worker metrics are per process and get summed across instances. Queue metrics come from the shared
table, so every instance reports the same value (dashboards use `max`); they're refreshed every 15s
rather than on each scrape, so scrapes never turn into database load. Pickup latency is computed from
two Postgres timestamps (`run_at` and the claim's `now()`), so it's immune to clock skew too. The
`type` tag only takes values that have a registered handler (anything else becomes `unknown`):
job types are client input, and must not be able to create unbounded time series.

## API

| Method | Path                    | Description                                  |
|--------|-------------------------|----------------------------------------------|
| POST   | `/api/jobs`             | Enqueue (`type`, `payload`, `queue`, `priority`, `maxAttempts`, `delaySeconds`, `idempotencyKey`) |
| GET    | `/api/jobs/{id}`        | Job details                                  |
| GET    | `/api/jobs?status=&queue=&limit=` | List jobs                          |
| POST   | `/api/jobs/{id}/retry`  | Revive a `DEAD` job                          |
| GET    | `/api/queues`           | Per-queue counts by status                   |

## Writing a handler

```java
@Component
class SendInvoiceEmail implements JobHandler {
    public String type() { return "invoice.email"; }

    public void handle(Job job) throws Exception {
        // Delivery is at-least-once: make this idempotent.
    }
}
```

## Testing

Integration tests run against a real Postgres via Testcontainers (Docker required):

```bash
./mvnw verify
```

They cover: 4 competing workers × 300 jobs, each run exactly once; lease expiry and recovery
after a simulated crash; fencing of a stale worker; heartbeats keeping long jobs leased;
retries, dead-lettering, delayed jobs and idempotent enqueue; recorded metrics per outcome
(including a fenced-off attempt), and the full app exposing them through `/actuator/prometheus`.

The throughput benchmark is opt-in: `./mvnw test -Dtest=ThroughputBenchmark -Dbenchmark=true`.

## Roadmap

- [ ] `LISTEN/NOTIFY` wake-ups (sub-millisecond pickup without tight polling)
- [x] Micrometer metrics + Prometheus/Grafana dashboard
- [ ] Recurring (cron) jobs with single-leader scheduling
- [ ] Web dashboard
- [x] Load test & benchmark write-up (jobs/sec vs. worker count)
- [ ] Retention: archive/purge finished jobs
