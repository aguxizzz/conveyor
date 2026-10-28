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
- **Instant pickup with `LISTEN/NOTIFY`**: a trigger announces new jobs, so idle workers claim them in
  ~3 ms instead of waiting for their next poll. Polling stays as the safety net, so a missed
  notification only costs latency, never a job.
- **Recurring jobs (cron)** with time zones, fired by a single elected leader. Leadership is a lease
  with a fencing term, so a paused ex-leader can't fire, and a crashed leader is replaced within ~20 s.
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
| Idle-queue latency, enqueue → done (p50) | ~3 ms with `LISTEN/NOTIFY` (264 ms polling every 500 ms) |

Profiling the first run showed the claim query sorting the entire backlog on every poll; splitting
lease recovery out and matching the index to the `ORDER BY` made claims O(1) in backlog size and
raised single-worker throughput 6×.

## Wake-ups with LISTEN/NOTIFY

A trigger on `jobs` runs `pg_notify('conveyor_jobs', queue)` whenever a row becomes runnable now
(inserted, or revived from `DEAD`). Each instance keeps one dedicated connection `LISTEN`ing on that
channel, outside the connection pool, and wakes the pool for the named queue.

- **Delivered on commit only**, so a woken worker can always see the job; a rolled-back enqueue wakes
  nobody.
- **Polling remains the source of truth.** Delayed jobs and retries (`run_at` in the future) aren't
  announced, and a notification sent while the listener is reconnecting is simply lost. The regular
  poll (now every 2 s by default) picks up both, so notifications only ever make things faster.
- **Lost connections are detected and healed**: the listener runs a `SELECT 1` after 30 s of silence
  (idle connections can die without a TCP reset), reconnects on any error, and then wakes every pool,
  since anything announced while it was gone went to nobody. `conveyor_listener_connected` shows when
  it's down.
- **Every instance wakes, one wins.** All idle workers race for the new job; `SKIP LOCKED` hands it to
  one and the rest get an empty claim, which is cheap.
- **The cost is on enqueue**: notifying commits are serialized by Postgres, which cut peak enqueue rate
  by ~27% in the benchmark (still twice the drain rate). `conveyor.worker.listen=false` turns the
  listener off.

## Recurring jobs

```bash
curl -X PUT localhost:8080/api/schedules/nightly-report -H 'Content-Type: application/json' \
  -d '{"cron":"0 0 3 * * *","timeZone":"America/Argentina/Buenos_Aires","type":"report","payload":{"format":"pdf"}}'

curl localhost:8080/api/scheduler/leader     # {"holder":"host-1a2b3c4d","term":2,"expiresAt":"..."}
```

A schedule doesn't run anything itself: each time it's due, it enqueues an ordinary job, which then
gets retries, leases and metrics like any other. Cron uses Spring's six-field format (seconds first)
or `@hourly`/`@daily`/`@weekly`/`@monthly`, evaluated in the schedule's time zone (DST included).

Every instance runs a scheduler, but only the **leader** fires:

- **Leader election is a lease row** (`leader_leases`): one `INSERT … ON CONFLICT DO UPDATE` acquires
  it if it's free or expired, or renews it for the current holder. The leader renews every 5 s
  (a third of its 15 s lease); followers ask just as often, so a crashed leader is replaced within
  ~20 s, and one that shuts down cleanly releases the lease for an immediate hand-off.
- **Leadership alone isn't trusted.** A leader that pauses (GC, network) past its lease doesn't know it
  was replaced. Every change of holder increments a `term`, a fencing token, and each firing is one
  transaction that first locks the lease row `FOR SHARE` for that term. A stale term fires nothing;
  and since taking over has to update that row, a new leader waits for a firing in progress to commit
  instead of overlapping with it. The row is never deleted, so terms never start over.
- **Each occurrence is one job**: due schedules are locked `FOR UPDATE SKIP LOCKED`, and the job's
  idempotency key is `schedule:<name>:<due time>`, so even a repeated firing would enqueue nothing new.
- **Missed firings are coalesced.** If no leader ran for 10 minutes, an every-minute schedule fires
  once, late, and continues from the current time, rather than releasing a burst of 10 jobs.
- All times come from Postgres, so the instances' clocks don't matter here either.

`conveyor_scheduler_leader` is 1 on the leader, and `conveyor_scheduler_fired_total` counts jobs it
enqueued. `conveyor.scheduler.enabled=false` keeps an instance out of the election.

## Metrics

Exposed at `/actuator/prometheus`. Two sources, on purpose:

| Metric | Source | What it answers |
|--------|--------|-----------------|
| `conveyor_job_execution_seconds` (histogram; `queue`, `type`, `outcome`) | each worker | Throughput, failure rate and p95 per job type. `outcome` is `succeeded`, `retried`, `dead` or `lost` (finished after its lease was taken over). |
| `conveyor_job_pickup_latency_seconds` (histogram; `queue`) | each worker | How long a runnable job waits before a worker claims it. |
| `conveyor_worker_in_flight`, `conveyor_worker_concurrency` | each worker | Utilization: are workers saturated? |
| `conveyor_listener_connected` | each worker | 0 means wake-ups are down and pickup falls back to the poll interval. |
| `conveyor_scheduler_leader`, `conveyor_scheduler_fired_total` | each instance | Which instance fires schedules (exactly one should be 1), and how many jobs it enqueued. |
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
| PUT    | `/api/schedules/{name}` | Create or replace a schedule (`cron`, `timeZone`, `type`, `payload`, `queue`, `priority`, `maxAttempts`, `enabled`) |
| GET    | `/api/schedules`, `/api/schedules/{name}` | List / get schedules, with `nextRunAt` and `lastRunAt` |
| DELETE | `/api/schedules/{name}` | Delete a schedule                            |
| GET    | `/api/scheduler/leader` | Current scheduler leader and term (404 if none) |

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
Wake-up tests poll only once a minute, so a job finishing within seconds proves the notification
path: new and revived jobs, queue filtering, and reconnecting after the listener's session is killed
with `pg_terminate_backend`.
Scheduler tests run three competing schedulers (one leads, every occurrence becomes exactly one job),
hand-off on shutdown, takeover from a crashed leader's expired lease, a stale leader fenced off by
its term, a takeover blocking until the old leader's firing commits, and an hour of missed firings
coalesced into one.

The throughput benchmark is opt-in: `./mvnw test -Dtest=ThroughputBenchmark -Dbenchmark=true`.

## Roadmap

- [x] `LISTEN/NOTIFY` wake-ups (~3 ms pickup without tight polling)
- [x] Micrometer metrics + Prometheus/Grafana dashboard
- [x] Recurring (cron) jobs with single-leader scheduling
- [ ] Web dashboard
- [x] Load test & benchmark write-up (jobs/sec vs. worker count)
- [ ] Retention: archive/purge finished jobs
