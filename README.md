# Conveyor

[![CI](https://github.com/aguxizzz/conveyor/actions/workflows/ci.yml/badge.svg)](https://github.com/aguxizzz/conveyor/actions/workflows/ci.yml)

A distributed background-job queue for the JVM, backed only by PostgreSQL.

Enqueue work over HTTP (or from Java), and any number of worker instances pick it up, run it with
retries and exponential backoff, and survive crashes, without Redis, Kafka or RabbitMQ.
Postgres is already in most stacks; Conveyor makes it your job queue.

**Stack:** Java 17 · Spring Boot 4 · PostgreSQL 17 · Flyway · JdbcClient · Testcontainers · Docker · GitHub Actions

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
docker compose up --build          # Postgres + 2 competing worker instances (ports 8080, 8081)

curl -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"type":"demo.flaky","payload":{"failRate":0.6},"maxAttempts":5}'

curl localhost:8080/api/jobs?status=DEAD
curl localhost:8080/api/queues
```

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
retries, dead-lettering, delayed jobs and idempotent enqueue.

## Roadmap

- [ ] `LISTEN/NOTIFY` wake-ups (sub-millisecond pickup without tight polling)
- [ ] Micrometer metrics + Prometheus/Grafana dashboard
- [ ] Recurring (cron) jobs with single-leader scheduling
- [ ] Web dashboard
- [ ] Load test & benchmark write-up (jobs/sec vs. worker count)
- [ ] Retention: archive/purge finished jobs
