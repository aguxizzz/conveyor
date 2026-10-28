# Benchmark: jobs/sec vs. worker count

How fast can a fleet of Conveyor workers drain one queue, and what limits it? The benchmark answered
that, and also turned up a query-plan bug that capped throughput at ~540 jobs/s per worker. This page
covers both.

## Setup

[`ThroughputBenchmark`](src/test/java/dev/aguxis/conveyor/bench/ThroughputBenchmark.java) runs
against a real PostgreSQL 17 in Testcontainers. Each simulated worker is a `WorkerPool` with its own
connection pool (concurrency 8, plus poller and heartbeat connections), the same as a separate
instance, so the database is the only thing they share. Jobs are pre-seeded, the clock runs from
worker start until the last job is `SUCCEEDED`, and each figure is the median of 3 runs after a
warm-up. Every run also asserts that **each job ran exactly once** (`attempts = 1` for every row).

```bash
./mvnw test -Dtest=ThroughputBenchmark -Dbenchmark=true   # ~5 min; writes target/benchmark-results.md
```

Machine: AMD Ryzen 3 2200G (4 cores, no SMT), 16 GB RAM, Linux, Java 17. Postgres and all workers
run on this same box, so they compete for the same 4 cores. Absolute numbers are a lower bound and
the useful part is the shape of the curves.

## Results

### Queue overhead: no-op jobs

Handlers do nothing, so throughput is pure queue cost (claim + complete in Postgres). 20,000 jobs.

| Workers | Threads | Before fix | After fix |
|---:|---:|---:|---:|
| 1 | 8 | 537 jobs/s | **3,269 jobs/s** |
| 2 | 16 | 1,008 | **4,620** |
| 4 | 32 | 1,263 | **4,988** |
| 8 | 64 | 1,441 | **4,654** |

### Realistic work: 10 ms I/O-bound jobs

Each job sleeps 10 ms (a typical HTTP call or query). The ideal rate is `threads / 10 ms`, and any
gap below it is queue overhead. 8,000 jobs.

| Workers | Threads | Ideal | Before fix | After fix | Efficiency after |
|---:|---:|---:|---:|---:|---:|
| 1 | 8 | 800 | 515 | **670** | 84% |
| 2 | 16 | 1,600 | 1,007 | **1,354** | 85% |
| 4 | 32 | 3,200 | 1,704 | **2,273** | 71% |
| 8 | 64 | 6,400 | 2,241 | **3,244** | 51% |

### Claim cost vs. backlog

A single claim of 8 jobs, average of 300. A queue should not get slower as it falls behind.

| Pending jobs | Before fix | After fix |
|---:|---:|---:|
| 1,000 | 1.07 ms | 0.52 ms |
| 10,000 | 4.65 ms | 1.09 ms |
| 100,000 | **41.72 ms** | **1.05 ms** |

### Finished-job history and enqueue

| Scenario | Result |
|---|---:|
| 4 workers, no-op, empty table | 5,202 jobs/s |
| 4 workers, no-op, 1,000,000 `SUCCEEDED` rows already in the table | 4,994 jobs/s |
| Enqueue, 16 concurrent clients, one job per transaction | 14,276 jobs/s |
| Same, with a unique `idempotencyKey` per job | 12,744 jobs/s |

Finished jobs don't slow the queue down, because every hot index is partial (`WHERE status = 'PENDING'`
/ `'RUNNING'`). The idempotency key costs about 10%, from maintaining one more unique index.

## What the first run found

One worker topped out at ~540 jobs/s **even with no-op jobs**, and 10 ms jobs ran at almost the same
rate. That meant job execution wasn't the bottleneck. Instrumenting `claim()` showed 4 jobs per claim at
~6 ms each, so a single poller can't exceed about 4 / 6 ms ≈ 650 jobs/s.

Turning off `synchronous_commit` changed nothing, which ruled out WAL fsync. `EXPLAIN ANALYZE` on
the claim query found the cause:

```
Limit (actual time=10.160..10.164 rows=4)
  ->  LockRows
        ->  Sort  (Sort Key: priority DESC, run_at, id)
              ->  Seq Scan on jobs  (rows=20000)
                    Filter: ... ((status = 'PENDING' AND run_at <= now())
                              OR (status = 'RUNNING' AND locked_until < now()))
```

The claim accepted both pending jobs and jobs with an expired lease in one `WHERE ... OR ...`. No
single index covers both branches in `ORDER BY` order, so Postgres read and sorted **the whole
backlog** just to return 4 rows. Claim cost was O(backlog), which made the queue slowest exactly
when it was furthest behind (42 ms per claim at 100k pending).

The fix has two parts:

1. **Lease recovery became its own statement.** In the same transaction, before claiming, expired
   `RUNNING` jobs go back to `PENDING` through `jobs_leased_idx`. That statement is a near-empty
   index range scan in the normal case. As a side effect, a recovered job now rejoins the queue in its
   normal priority order instead of jumping ahead. Fencing is unchanged: the stale worker's
   `complete()` still fails, because the status, owner and attempt number no longer match.
2. **The ready index matches the `ORDER BY` exactly**, including the `id` tie-breaker
   ([`V2__claim_index_matches_order.sql`](src/main/resources/db/migration/V2__claim_index_matches_order.sql)).
   Jobs enqueued in one burst share `run_at`, and without `id` Postgres still had to sort them all.

After the fix:

```
Limit (actual time=0.055..0.060 rows=4)
  ->  LockRows
        ->  Index Scan using jobs_ready_idx on jobs  (rows=4)
```

## Reading the curves

- **One worker went from ~540 to ~3,300 jobs/s (6×).** Its single poller is no longer waiting on a sort.
- **No-op throughput plateaus around 5,000 jobs/s from 2 workers up.** At that point Postgres, the
  JVM and up to 8 worker pools are all fighting over 4 cores. Every job costs at least one commit
  (its `complete()`), plus a share of a claim commit. That plateau is this machine's ceiling, not a
  limit of the design, and the next step would be to measure with Postgres on its own host.
- **For real work, scaling is close to linear up to 2 workers** (85% of ideal), then flattens as the
  shared CPU saturates. In production, workers and the database run on different machines.

## Next optimizations (not done)

- **Batch completions**: one `UPDATE ... WHERE id = ANY(...)` for several finished jobs would cut
  commits per job, which are now the dominant cost for short jobs.
- **`LISTEN/NOTIFY`** to wake idle pollers, which matters for latency rather than throughput (already on
  the roadmap).
