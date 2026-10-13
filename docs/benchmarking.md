# Benchmarks and capacity

> **Read this first.** Everything here was measured on **one low-end laptop** (a 2012-era 2-core CPU, 8 GB RAM) with the load generator, the API, PostgreSQL, Redis and all workers sharing the same two physical cores. The absolute numbers describe *that machine*, not what QueueForge can do on real hardware, and they must not be quoted as capacity figures. What the results are good for: showing **where the bottlenecks are**, finding **problems** (several were found and fixed), and checking that **safety properties hold under load**. Nothing here is a production load test, and no number in this document was estimated: each comes from a run recorded in `docs/benchmark-results/`.

## 1. What was measured, and how

A repeatable harness (`scripts/benchmark/bench.py`) starts an **isolated copy of the whole stack** (its own Docker Compose project, database volume and ports, so it never touches development data) and runs real workloads against it over real HTTP.

**Timings come from the database, not from the load generator's clock**, so they include everything the system does:

| Measure | Definition |
|---|---|
| Queue wait | the first attempt's `started_at` minus the job's `created_at` |
| Run time | the attempt's recorded `duration_ms` |
| End-to-end | `completed_at` minus `created_at` |
| Drain throughput | jobs finished divided by the time from the first attempt starting to the last attempt finishing |
| Error rate | jobs ending `failed` or `dead_lettered`, divided by jobs |
| Extra attempts | attempts beyond the first. Anything above 0 means a retry or a re-run happened |

The throughput scenarios (API cost, drain capacity, deep queue) were each repeated 3 times and their tables show **median (min–max)**, because single numbers on a busy laptop are misleading. The idle-latency, fairness, overload, steady-state and soak scenarios were run **once each**, so treat their figures as single observations.

**Guardrails.** Nothing was loosened to improve a number. Leases, heartbeats, fenced writes, `fsync` and synchronous commits are as in the code. PostgreSQL runs with its default settings. The full application settings of every run are stored in its result file. There is exactly **one deliberate deviation from the defaults**: the per-key rate limit is raised to 100,000,000 requests a minute, because the default of 120 would cap the load generator. The limiter is still switched on (the Redis check still runs on every request), and its cost is measured separately in section 3.1.

## 2. Environment

| Item | Value |
|---|---|
| Host CPU | Intel(R) Core(TM) i5-3210M CPU @ 2.50GHz (2 cores, 4 threads) |
| Host RAM | 7.9 GB |
| Host OS | Windows-10-10.0.19045-SP0 |
| Docker | ncpu=4 memory_bytes=4041818112 server=29.8.2 (the VM that runs every container) |
| PostgreSQL | PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) on x86_64-pc-linux-gnu |
| PostgreSQL settings | all defaults: shared_buffers 128MB, work_mem 4MB, max_connections 100, fsync on, synchronous_commit on, random_page_cost 4 |
| Redis | 7 (rate limiting only) |
| Python | 3.12.15 for the harness; 3.12 in the containers |
| Application settings | RATE_LIMIT_PER_MINUTE=100000000, MAX_QUEUED_JOBS_PER_CLIENT=10000, MAX_QUEUED_JOBS_TOTAL=100000, MAX_REQUEST_BODY_BYTES=65536, QUEUE_DEPTH_CACHE_SECONDS=1, METRICS_CACHE_SECONDS=10, WORKER_LEASE_SECONDS=30, WORKER_HEARTBEAT_INTERVAL_SECONDS=10, WORKER_SWEEP_INTERVAL_SECONDS=10, WORKER_SHUTDOWN_DRAIN_SECONDS=25, RETRY_BASE_DELAY_SECONDS=2, RETRY_MAX_DELAY_SECONDS=300, LOG_LEVEL=INFO, ENVIRONMENT=development |
| Code version of the last run | git ef3d334 (uncommitted changes: True) |

(Taken from the result file `20261013T143808Z_drain_abab-r2-B-without-indexes.json`; every result file records its own copy.)

**Which code produced which numbers.** The core and extended matrices (the first runs) used the code of the merged Phase A: it has **neither** the two partial indexes **nor** the two short caches described in section 4. Rows labelled "after", "abab", "reordered" or "vacuumed" come from the later code. Each result file records the `git_commit` it ran on and whether the working tree had uncommitted changes (it usually did, because the harness itself was being edited).

Other things that matter when reading the numbers:
- The API runs as a **single uvicorn process** (sync endpoints in a thread pool, a pool of 10 + 10 database connections).
- Workers run **one job at a time each**. Idle workers poll every second.
- Job kinds: `echo` (does nothing, so it measures the queue machinery) and `mixed` (a `sleep` task of 0.05, 0.1, 0.15 or 0.2 s, mean 0.125 s, so it measures concurrency).
- "Drain" scenarios put jobs straight into the database (bypassing the API) to measure the **workers' own** capacity, because on this machine the API is slower than the workers and could never build a backlog.
- Arrivals in the steady-state test are evenly spaced, not random, which understates queueing compared with bursty real traffic.

## 3. Results

### 3.1 The API

| Label | Concurrency | Jobs already queued | Accepted/s | Latency p50 (s) | p95 | p99 | Statuses |
|---|---|---|---|---|---|---|---|
| limiter-on-c1 | 1 | 0 | 36.5 | 0.022 | 0.050 | 0.093 | {'201': 500} |
| limiter-on-c4 | 4 | 0 | 51.8 | 0.074 | 0.112 | 0.139 | {'201': 500} |
| limiter-on-c16 | 16 | 0 | 40.9 | 0.345 | 0.615 | 0.795 | {'201': 500} |
| limiter-on-c64 | 64 | 0 | 37.0 | 1.557 | 2.167 | 2.327 | {'201': 500} |
| limiter-on-c16-rep2 | 16 | 0 | 50.9 | 0.300 | 0.444 | 0.520 | {'201': 500} |
| limiter-on-c16-rep3 | 16 | 0 | 43.8 | 0.333 | 0.605 | 0.856 | {'201': 500} |
| limiter-off-c16-rep1 | 16 | 0 | 45.0 | 0.334 | 0.535 | 0.669 | {'201': 500} |
| limiter-off-c16-rep2 | 16 | 0 | 49.1 | 0.312 | 0.455 | 0.542 | {'201': 500} |
| limiter-off-c16-rep3 | 16 | 0 | 48.3 | 0.312 | 0.459 | 0.611 | {'201': 500} |
| limiter-on-c16-1m-history | 16 | 0 | 36.1 | 0.349 | 0.719 | 2.016 | {'201': 500} |
| deepq90k-cache1s-rep1 | 16 | 90000 | 44.2 | 0.333 | 0.557 | 0.767 | {'201': 500} |
| deepq90k-cache0-rep1 | 16 | 90000 | 6.3 | 1.840 | 4.831 | 6.003 | {'201': 476, '503': 24} |
| deepq90k-cache1s-rep2 | 16 | 90000 | 33.7 | 0.411 | 0.834 | 1.024 | {'201': 500} |
| deepq90k-cache0-rep2 | 16 | 90000 | 5.6 | 2.495 | 5.210 | 5.469 | {'201': 477, '503': 23} |
| deepq90k-cache1s-rep3 | 16 | 90000 | 39.0 | 0.331 | 0.721 | 1.294 | {'201': 500} |
| deepq90k-cache0-rep3 | 16 | 90000 | 6.2 | 2.512 | 4.998 | 5.300 | {'201': 486, '503': 14} |
| reordered-deepq90k-cache0-rep1 | 16 | 90000 | 37.9 | 0.391 | 0.580 | 0.918 | {'201': 500} |
| reordered-deepq90k-cache1s-rep1 | 16 | 90000 | 42.7 | 0.316 | 0.628 | 0.910 | {'201': 500} |
| reordered-deepq90k-cache0-rep2 | 16 | 90000 | 37.8 | 0.388 | 0.602 | 0.939 | {'201': 500} |
| reordered-deepq90k-cache1s-rep2 | 16 | 90000 | 47.3 | 0.309 | 0.457 | 0.878 | {'201': 500} |
| reordered-deepq90k-cache0-rep3 | 16 | 90000 | 35.0 | 0.407 | 0.744 | 0.876 | {'201': 500} |
| reordered-deepq90k-cache1s-rep3 | 16 | 90000 | 39.3 | 0.297 | 0.878 | 1.468 | {'201': 500} |
| reordered-no-deep-queue-baseline | 16 | 0 | 51.0 | 0.283 | 0.429 | 0.504 | {'201': 500} |

Concurrency 16, limiter ON: 3 runs, accepted/s median (min-max) = 43.8 (40.9-50.9); p50 latency median (min-max) = 0.333 (0.300-0.345) s

Concurrency 16, limiter OFF: 3 runs, accepted/s median (min-max) = 48.3 (45.0-49.1); p50 latency median (min-max) = 0.312 (0.312-0.334) s

**Reading it.** On this machine the API tops out at roughly **50 accepted submits per second** (best at a concurrency of 4–16), and is CPU-bound: its container used more than one full core while the workers were mostly idle. More concurrent clients beyond that only lengthen latency (64 concurrent clients: 37/s and a 1.6 s median). The rate limiter's cost is **not distinguishable from run-to-run noise** here (43.8 vs 48.3 accepted/s median, with overlapping ranges over 3 runs each). The 90,000-row rows are explained in section 4.

### 3.2 What the workers can do (drain capacity)

| Label | Workers | Job kind | Setup | Runs | Jobs/s median (min-max) | Per-worker share spread | Errors / extra attempts | CPU% of one core: db / worker (mean per container) |
|---|---|---|---|---|---|---|---|---|
| echo-w1 | 1 | echo | empty database | 3 | 38.3 (35.8-40.1) | 3000-3000 jobs/worker | 0 / 0 | 24 / 62 |
| echo-w2 | 2 | echo | empty database | 3 | 43.4 (42.7-55.8) | 1483-1517 jobs/worker | 0 / 0 | 47 / 61 |
| echo-w4 | 4 | echo | empty database | 3 | 81.9 (81.6-92.1) | 736-769 jobs/worker | 0 / 0 | 64 / 50 |
| echo-w8 | 8 | echo | empty database | 3 | 90.7 (89.2-91.4) | 365-388 jobs/worker | 0 / 0 | 72 / 29 |
| mixed-w1 | 1 | mixed | empty database | 3 | 6.0 (5.5-6.1) | 600-600 jobs/worker | 0 / 0 | 8 / 16 |
| mixed-w2 | 2 | mixed | empty database | 3 | 11.9 (9.7-12.2) | 299-301 jobs/worker | 0 / 0 | 21 / 19 |
| mixed-w4 | 4 | mixed | empty database | 3 | 23.9 (22.2-24.4) | 148-152 jobs/worker | 0 / 0 | 17 / 13 |
| mixed-w8 | 8 | mixed | empty database | 3 | 35.0 (32.1-42.9) | 71-82 jobs/worker | 0 / 0 | 38 / 17 |
| echo-w4-1m-history | 4 | echo | 1M history | 3 | 58.7 (57.7-61.3) | 741-761 jobs/worker | 0 / 0 | 88 / 45 |
| echo-w4-1m-history-after | 4 | echo | 1M history | 3 | 37.0 (29.1-44.2) | 742-757 jobs/worker | 0 / 0 | 91 / 47 |
| echo-w4-1m-vacuumed-with-indexes | 4 | echo | 1M history, vacuumed first | 3 | 53.9 (42.0-74.0) | 732-770 jobs/worker | 0 / 0 | 71 / 49 |
| echo-w4-1m-vacuumed-without-indexes | 4 | echo | 1M history, vacuumed first, new indexes dropped | 3 | 60.9 (49.5-62.3) | 737-766 jobs/worker | 0 / 0 | 99 / 45 |
| echo-w4-1m-vacuumed-with-indexes-again | 4 | echo | 1M history, vacuumed first | 3 | 66.1 (64.8-71.9) | 732-771 jobs/worker | 0 / 0 | 67 / 49 |
| after-echo-w1 | 1 | echo | empty database | 3 | 31.6 (20.6-33.4) | 3000-3000 jobs/worker | 0 / 0 | 26 / 61 |
| after-echo-w4 | 4 | echo | empty database | 3 | 57.3 (49.3-80.1) | 736-766 jobs/worker | 0 / 0 | 62 / 48 |
| after-echo-w8 | 8 | echo | empty database | 3 | 74.1 (63.8-82.4) | 367-382 jobs/worker | 0 / 0 | 76 / 29 |
| after-mixed-w4 | 4 | mixed | empty database | 3 | 24.6 (24.0-25.3) | 149-151 jobs/worker | 0 / 0 | 15 / 11 |
| abab-r1-A-with-indexes | 4 | echo | empty database | 3 | 86.2 (79.8-87.3) | 743-757 jobs/worker | 0 / 0 | 66 / 51 |
| abab-r1-B-without-indexes | 4 | echo | empty database, new indexes dropped | 3 | 87.5 (83.2-87.6) | 743-762 jobs/worker | 0 / 0 | 64 / 52 |
| abab-r2-A-with-indexes | 4 | echo | empty database | 3 | 87.9 (86.5-89.7) | 742-760 jobs/worker | 0 / 0 | 67 / 53 |
| abab-r2-B-without-indexes | 4 | echo | empty database, new indexes dropped | 3 | 85.9 (81.0-92.5) | 738-760 jobs/worker | 0 / 0 | 66 / 50 |

**Reading it.**
- With sleep-dominated `mixed` jobs, throughput scales close to linearly up to 4 workers (6.0, 11.9, 23.9 jobs/s for 1, 2, 4 workers), at about 75% of the theoretical `workers / 0.125 s`: roughly 40 ms of overhead per job (claim, lease, heartbeat thread, two commits). At 8 workers it reaches 35 jobs/s, only 55% of theoretical, because the machine runs out of CPU.
- With `echo` jobs (pure queue overhead), **one worker handles about 38 jobs/s, four about 82–88, and eight about 91**. The curve flattens because PostgreSQL and the workers saturate the two cores: this is a machine limit, not evidence about the design at scale. (The 2-worker `echo` result, 43 jobs/s, is lower than one would expect and had a wide range; I did not find out why.)
- **Work is shared evenly**: in every run each worker's share was within about 6% of the others (the widest spread, 365 to 388 jobs, was at 8 workers).
- **No job was lost or run twice** in any drain run: errors and extra attempts are 0 in every row.
- The rows with a 1M history, "vacuumed first" and "new indexes dropped" are the experiments discussed in section 4. **The row `echo-w4-1m-history-after` (37.0 jobs/s) is confounded**: it was measured right after a Docker restart (cold caches) and before a vacuum, with about 165,000 dead rows in the table. It must not be read as an effect of the indexes. The vacuumed rows after it, and the interleaved A-B-A-B rows at the bottom, are the controlled comparisons.
- The `after-echo-*` rows (empty database, run after the Docker restart) are lower than the first runs; the interleaved A-B-A-B rows taken afterwards returned to the original level, so those were transient machine slowness (section 6).

### 3.3 Idle pickup latency

40 jobs, 2 idle workers (poll interval 1 s). Queue wait p50 / p95 / p99 / max = 0.563 / 0.931 / 0.996 / 1.011 s. End-to-end p50 = 0.589 s.

A job arriving at an idle system waits about half the 1-second poll interval on average (median 0.56 s, never more than 1.01 s). This is a direct, expected consequence of polling. Lowering it means a shorter poll interval (more idle database load) or `LISTEN/NOTIFY`, which has not been built.

### 3.4 Steady load

| Arrival rate (jobs/s) | Jobs | Completed/s | Queue wait p50 (s) | p95 | p99 | End-to-end p95 (s) | Errors |
|---|---|---|---|---|---|---|---|
| 6.0 | 360 | 6.0 | 0.257 | 0.813 | 0.945 | 0.977 | 0 |
| 9.5 | 570 | 9.4 | 0.291 | 0.722 | 0.895 | 0.863 | 0 |
| 11.3 | 678 | 11.1 | 0.379 | 1.053 | 1.230 | 1.216 | 0 |

Two workers on `mixed` jobs can drain about 11.9 jobs/s. At 50%, 80% and 95% of that, queue wait stayed under about 1.2 s at p99 with no errors. Because arrivals here are evenly spaced, this is the *best case*; real, bursty traffic would queue more at the same average rate.

### 3.5 Overload and recovery

4 clients x 15.0/s for 90.0 s against 2 workers; queue caps 150 per client / 400 total.

- HTTP statuses returned: {'201': 2720, '503': 2680}
  - status 201: n=2720, latency p50/p95/p99 = 3.118 / 11.385 / 13.684 s
  - status 503: n=2680, latency p50/p95/p99 = 2.512 / 4.883 / 7.320 s
- Accepted jobs: 2720; succeeded 2720; failed/dead-lettered 0; unfinished 0; extra attempts 0
- Accepted jobs' queue wait p50/p95/p99 = 32.443 / 36.445 / 37.472 s
- Drained after load stopped: True in 29.5 s; submit after recovery: HTTP 201
- Peak CPU% (of one core) per container: worker 65, api 253, db 115, redis 33

**Reading it.** About 5,400 requests were offered (about 60/s against an API that handles roughly 50/s and 2 workers that drain about 12 jobs/s). The system **protected itself**: it accepted 2,720 jobs, refused 2,680 with a clean `503` (the global queue cap of 400 was the limit that engaged first, so no `429`s appear; that path is covered by automated tests), returned **no `500`s and no timeouts**, kept every accepted job (all 2,720 succeeded, none re-run), drained the backlog in 29.5 s after the load stopped, and **accepted new work again**. The cost of the protection was latency: with the API itself overloaded, even refusals took a median 2.5 s. The queue cap also bounds waiting time: with a cap of 400 and a throughput of about 12 jobs/s, accepted jobs waited about 32 s. **The default caps (10,000 per client, 100,000 total) would allow waits of hours at this throughput. Size the caps as throughput × the longest wait you accept.**

### 3.6 Fairness between clients

| Case | Jobs queued when B started | B queue wait p50 (s) | p95 | max |
|---|---|---|---|---|
| baseline_b_alone | 0 | 0.192 | 0.536 | 0.584 |
| b_behind_a_backlog | 3000 | 115.898 | 120.329 | 120.833 |

This is a **measured weakness, not yet fixed.** Claiming is oldest-first across all clients, so a small client's job waits behind another client's entire backlog: client B waited a median **116 s** (instead of 0.19 s, about 600 times longer) behind a 3,000-job backlog from client A. The queue caps limit how bad this can get but do not remove it. Section 5 describes the planned fix.

### 3.7 Soak

600 s at 15.0/s with 4 workers: 8997 jobs, 8997 succeeded, 0 failed, 0 unfinished, 0 extra attempts; queue wait p50/p95/p99 = 0.260/1.855/2.762 s

| Elapsed (s) | Completed in window | Queue wait p95 in window (s) | Queued now | jobs table dead rows | jobs table size (MB) |
|---|---|---|---|---|---|
| 30 | 446 | 0.787 | 3 | 620 | 0.3 |
| 60 | 455 | 0.511 | 0 | 670 | 0.4 |
| 90 | 448 | 0.880 | 1 | 1576 | 0.5 |
| 121 | 446 | 0.960 | 7 | 667 | 0.6 |
| 151 | 456 | 0.906 | 2 | 1584 | 0.8 |
| 181 | 436 | 0.650 | 18 | 660 | 0.9 |
| 211 | 454 | 0.749 | 4 | 1579 | 1.0 |
| 242 | 464 | 1.127 | 9 | 744 | 1.1 |
| 272 | 459 | 3.328 | 1 | 1642 | 1.3 |
| 302 | 450 | 1.906 | 2 | 700 | 1.4 |
| 332 | 450 | 0.903 | 2 | 1595 | 1.5 |
| 362 | 425 | 0.766 | 12 | 652 | 1.7 |
| 393 | 477 | 1.254 | 6 | 1608 | 1.8 |
| 423 | 456 | 0.880 | 1 | 742 | 1.9 |
| 453 | 447 | 0.745 | 2 | 1641 | 2.0 |
| 483 | 453 | 1.260 | 3 | 732 | 2.1 |
| 513 | 443 | 2.676 | 9 | 1641 | 2.3 |
| 545 | 431 | 1.154 | 25 | 713 | 2.3 |
| 575 | 490 | 2.778 | 6 | 1694 | 2.5 |

Container peaks: worker: CPU 63% of a core, memory 55 MiB; api: CPU 231% of a core, memory 92 MiB; db: CPU 139% of a core, memory 119 MiB; redis: CPU 40% of a core, memory 16 MiB

**This is a 10-minute run, not a real soak.** Over 8,997 jobs nothing failed or was re-run, and queue wait stayed in the same range throughout (p95 in each 30 s window between about 0.5 and 3.3 s, with no upward trend). The jobs table grew with the data only (to about 2.5 MB), and dead rows stayed under about 1,700 (autovacuum kept up at this size). Memory use stayed small (worker 55 MiB, API 92 MiB). A leak or bloat that takes hours to appear would not show up here.

### 3.8 A large history (1,000,000 finished jobs)

**Preloaded history**

Tables after preload: {'jobs': {'live_rows': 1000000, 'dead_rows': 0, 'total_bytes': 235896832, 'table_bytes': 189669376}, 'job_attempts': {'live_rows': 1000000, 'dead_rows': 0, 'total_bytes': 216596480, 'table_bytes': 101138432}} (took 154 s)

**Query timings with 1,000,000 finished jobs and 20,000 queued: before and after the two partial indexes**

Median of 3 runs of EXPLAIN (ANALYZE), milliseconds.

| Query | Before (ms) | After (ms) | Before / After |
|---|---|---|---|
| claim_next_job | 0.47 | 1.89 | 0.25x |
| sweeper_find_expired | 254.12 | 0.43 | 584.19x |
| per_client_queue_count | 8.09 | 13.60 | 0.59x |
| global_queue_count | 10.50 | 34.85 | 0.30x |
| dead_letter_listing | 132.83 | 0.18 | 737.94x |
| metrics_jobs_by_status | 499.60 | 540.81 | 0.92x |
| metrics_oldest_queued | 0.22 | 1.33 | 0.17x |
| metrics_oldest_running | 90.57 | 0.48 | 188.30x |
| metrics_expired_leases | 136.89 | 0.17 | 800.51x |
| metrics_attempts_by_outcome | 261.57 | 1595.52 | 0.16x |
| get_job_by_id | 0.05 | 0.07 | 0.65x |
| attempt_history_for_job | 0.07 | 0.13 | 0.51x |

**Cost of the queue-cap checks as the queue gets deeper (before any cache)**

| Query | 20,000 queued | 100,000 queued |
|---|---|---|
| per_client_queue_count | 8.09 ms | 5.20 ms |
| global_queue_count | 10.50 ms | 47.83 ms |
| claim_next_job | 0.47 ms | 0.76 ms |

**GET /metrics scrape time (seconds), repeated back to back**

| Label | Scrapes in order |
|---|---|
| 1m-history-idle-queue | [0.996, 0.713, 1.499, 3.092, 0.833] |
| 1m-cache10s | [1.168, 0.072, 0.083, 0.072, 0.058, 0.059] |
| 1m-cache0 | [0.736, 0.791, 0.888, 0.716, 0.906, 0.802] |

plans run '1m': GET /metrics seconds [4.44, 3.99, 1.95]

plans run '1m-queue100k': GET /metrics seconds [0.76, 0.97, 0.79]

plans run '1m-after-indexes': GET /metrics seconds [1.95, 0.17, 0.11]

## 4. What the measurements led to (and whether it worked)

Each change below was made because a measurement showed a problem, and was re-measured afterwards.

**1. Two small partial indexes (migration 0004).** With 1,000,000 finished jobs, the lease sweeper's query (run by every worker every 10 s) took about 254 ms, and the admin dead-letter listing about 133 ms: both were full table scans. With a partial index each on the few `running` / `dead_lettered` rows, they take about 0.4 ms and 0.2 ms (**about 580 and 740 times faster**), as do two of the `/metrics` queries (about 190 and 800 times). *Cost check:* an interleaved A-B-A-B test on an empty database (with and without the indexes, 4 workers, `echo` jobs) gave **86.2, 87.5, 87.9 and 85.9 jobs/s** (medians): no measurable write cost. In the "before / after" table above, the rows for queries the indexes do not touch are noise, not changes (for example the attempts-by-outcome scan took 262 ms and then 1,596 ms): the "after" measurements were taken right after a Docker restart, with cold caches.

**2. A short cache on the global queue-depth check.** At 100,000 queued jobs the global cap check cost about **48 ms per submit** (it counts up to the cap each time). With 90,000 jobs queued, submits fell to **5.6–6.3 per second**, and 14–24 of 500 requests were wrongly refused with `503` (the check ran while holding the client's lock, so concurrent submits queued behind it and timed out). Fixes: reuse the answer for 1 second (the global cap was already a soft limit) and do the check **before** taking the lock. Result at 90,000 queued: **35–38 accepted/s with the cache off, 39–47/s with it on** (51/s with an empty queue), and no refusals. A new automated test pins the order, and fails if the old order is restored.

**3. A short cache on `/metrics`.** A scrape took 0.7–4.4 s with 1,000,000 jobs (two aggregate queries scan whole tables, and the indexes do not help them). With a 10-second cache the first scrape still takes about 1.2 s, then **0.06–0.08 s**. The staleness is up to 10 s, and the scans themselves remain; retention (planned) is the real fix.

**4. The exact per-client cap** (an earlier fix) was found to be missing from `main` because its pull request had been merged before the commit was pushed; it is included in this branch.

## 5. Findings not acted on yet

- **No fairness between clients (section 3.6).** The data justify fixing it. Planned design, **not implemented**: when claiming, pick the client with the fewest jobs currently running (ties: oldest waiting job), then that client's oldest job. It needs a per-client queued index (one exists) and the new running-jobs index, and its cost must be measured against the 0.3–0.5 ms the current claim takes.
- **The API is the bottleneck on this machine** (about 50 submits/s, more than one core busy). Not investigated: profiling the request path, and running several uvicorn processes.
- **Throughput drops with a large history.** With 1M finished jobs, 4-worker `echo` drain was 54–66 jobs/s (vacuumed) against about 82–88 with an empty database, a drop of roughly 25–35%. The indexes do not explain or fix it. A likely cause is that the data (about 450 MB) no longer fits in PostgreSQL's default 128 MB of shared buffers, but this was **not tested**. Retention (deleting old finished jobs) would avoid it.
- **Dead rows pile up.** Before a manual vacuum, the 1M-row `jobs` table held about 165,000 dead rows (the default autovacuum waits for 20% of the table). Per-table autovacuum settings should be tuned and tested.
- **Polling latency** (section 3.3) and the **`/metrics` full-table scans** (section 4.3) remain as described.

## 6. What went wrong while measuring, and the limits of these numbers

Things I found wrong with my own harness during testing, all fixed before the results above were taken (the failed attempts' logs are in `docs/benchmark-results/logs/`):
- A stray `RATE_LIMIT_PER_MINUTE=120` from my `.env` silently re-enabled the default rate limit (30 of 100 requests were refused). The harness now sets every setting explicitly and ignores `.env`.
- A test that inserted 100,000 queued jobs hit the global cap of 100,000, so every submit was correctly refused. The deep-queue test now uses 90,000.
- The `plans` scenario ran while workers were running, so they processed its synthetic jobs mid-measurement; it now runs with no workers and cleans up. One such result was thrown away. A related cleanup race (workers picking up stray queued jobs while data was being deleted) now cannot happen, because data is cleared before any worker starts.

Limits you should keep in mind:
- **One machine, one operating system, one Docker VM** (4 vCPUs, 4 GB); the host's two physical cores run everything including the load generator. No multi-host test.
- **The machine drifted.** A repeat of the empty-database `echo` runs once gave 20–80 jobs/s where earlier runs gave 82–92; an interleaved re-test then reproduced 80–92. Drain figures for `echo` can vary by ±30% between runs; sleep-dominated `mixed` figures were far steadier. Differences smaller than that should not be read as real, which is why the index and cache comparisons use interleaving or very large effects.
- Run counts are small (3 per configuration). Medians and ranges are shown; no statistical tests were done.
- The soak was 10 minutes. Overload used one load shape. The steady test used evenly spaced arrivals.
- No test of PostgreSQL with larger memory settings, a separate database host, or a network between components.
- Docker Hub was unreachable for part of the session, so the images used for the final deep-queue re-measurements were built as a local overlay of the existing image plus the changed package files (same source as the repository). A normal `up --build` gives the same code.

## 7. How to reproduce

From the repository root, with the database credentials exported (`set -a; . ./.env; set +a`). The first time (or after changing application code) build the images: `uv run python scripts/benchmark/bench.py up --workers 2 --build`. Stop the development stack first so the two do not compete for CPU.

| Script | What it runs |
|---|---|
| `scripts/benchmark/run_core.sh` | API cost, worker drain capacity, idle latency (sections 3.1 to 3.3) |
| `scripts/benchmark/run_extended.sh` | fairness, overload, steady state, soak, 1M-row history and query plans |
| `scripts/benchmark/run_after_changes.sh` and `..._part2.sh`, `..._part3.sh`, `..._part4.sh` | the re-measurements after the changes in section 4 (run in four parts because of the harness problems described above; the exact commands are in each file) |

`uv run python scripts/benchmark/summarize.py` regenerates every table in this document from the files in `docs/benchmark-results/`. Remove the benchmark stack and its database with `uv run python scripts/benchmark/bench.py down`.

## 8. Raw data

`docs/benchmark-results/*.json`: one file per run, each with the full parameters, the environment, and the raw measurements. `docs/benchmark-results/logs/`: the console output of each script, including the attempts that stopped early.
