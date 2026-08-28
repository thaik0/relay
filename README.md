# Relay

Relay is a small distributed job-processing system built to make concurrency,
failure recovery, at-least-once execution, and performance measurable. A
FastAPI control plane persists work in PostgreSQL; independent C++20 workers
claim it with row locks, execute outside the transaction, and use leases to
recover jobs abandoned by dead workers.

The project is intentionally narrow. It is an engineering study of durable
queue coordination and failure semantics, not a feature-complete task system.

## Architecture

```text
                  FastAPI
                     |
                     v
                PostgreSQL
                     |
          -------------------------
          |           |           |
          v           v           v
       Worker 1    Worker 2    Worker N
        C++20       C++20       C++20
```

- `POST /jobs` creates a durable job; `GET /jobs/{job_id}` returns its state.
- PostgreSQL owns job state, retry schedules, lease deadlines, and uniqueness
  constraints.
- Workers atomically claim eligible rows, commit, run handlers, and then record
  success or a retry decision.

Supported demonstration payloads are:

```json
{"type": "sleep", "duration_ms": 500}
{"type": "fail"}
{"type": "write_effect", "operation_id": "operation-123", "value": "hello"}
```

`sleep` accepts an integer duration from 0 through 60000 ms. `fail` exercises
retries deterministically. `write_effect` demonstrates duplicate execution
with a logically idempotent side effect.

## Job lifecycle

```text
                         handler succeeds
queued  --->  running  -------------------->  succeeded
  ^             |
  |             | handler fails and attempts remain
  |             v
  +----- queued until available_at (exponential backoff)
                |
                | handler fails at attempt limit, or an exhausted lease expires
                v
              failed

running with an expired lease ---> running again with attempts + 1
```

`attempts` is the number of executions that have started. A new job begins at
zero; every claim or reclaim increments it once inside the claim transaction.
Terminal jobs are never eligible again.

## Atomic claiming

Each worker runs one short PostgreSQL transaction:

```text
BEGIN
finalize one expired running job already at the attempt limit, if present
select the oldest eligible queued or expired running job
  FOR UPDATE SKIP LOCKED
set status = running, increment attempts, and assign a lease
COMMIT
```

`FOR UPDATE` protects the selected row until commit. `SKIP LOCKED` lets another
worker select different work instead of waiting on that row. Selection and the
state transition occur in the same transaction, so workers cannot own the same
attempt simultaneously. The same rule protects expired-job reclaims.

Execution happens after commit. Arbitrary job runtime therefore does not hold a
row lock or an open database transaction. Success and failure updates match
both job ID and attempt number; a stale worker cannot overwrite a newer attempt
after its lease has expired.

## Leases, recovery, and retries

A claim sets `lease_expires_at` using PostgreSQL time. If a worker disappears,
it cannot run cleanup, so the row remains `running`. Once its fixed lease
expires, any worker may reclaim it safely. An unexpired lease is never eligible.

Ordinary handler failures either return the job to `queued` with:

```text
RETRY_BASE_SECONDS * 2^(attempt - 1)
```

or mark it permanently `failed` at `MAX_JOB_ATTEMPTS`. Retry eligibility also
uses PostgreSQL time. Workers continue polling the whole queue rather than
sleeping for a particular retry.

| Setting | Compose default | Meaning |
| --- | ---: | --- |
| `JOB_LEASE_SECONDS` | `10` | Fixed lease assigned at claim time |
| `MAX_JOB_ATTEMPTS` | `3` | Maximum executions that may begin |
| `RETRY_BASE_SECONDS` | `1` | Exponential-backoff base delay |

## At-least-once execution and idempotency

The queue cannot atomically combine an arbitrary external side effect with its
own PostgreSQL acknowledgement:

```text
perform side effect
        |
worker crashes before recording success
        |
lease expires and another worker executes the job again
```

Relay therefore provides at-least-once execution, not exactly-once execution.
Two separate idempotency mechanisms address different duplicate sources:

- **Submission idempotency:** an optional `idempotency_key` on `POST /jobs`
  prevents client retries from creating multiple jobs. A partial PostgreSQL
  unique index and `INSERT ... ON CONFLICT DO NOTHING` make concurrent matching
  requests converge on one job. Reusing a key with a different payload returns
  `409 Conflict`.
- **Execution-side idempotency:** `write_effect` inserts a logical result into
  `effects`, keyed by `operation_id`, with `ON CONFLICT DO NOTHING`. The
  non-idempotent `effect_attempts` audit still records every execution, so a
  post-effect crash visibly produces two executions but one logical effect.

Atomic claim, at-least-once recovery, and idempotent effects are distinct
guarantees. Production consumers need an idempotency facility owned by the
side-effecting system, such as a payment API key or deterministic resource ID.

## Run Relay

Requirements: Docker with Docker Compose.

```bash
docker compose up --build --scale worker=4
```

The API is available at <http://localhost:8000>, its OpenAPI UI at
<http://localhost:8000/docs>, and health at `GET /health`. Submit a job with:

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"type":"sleep","duration_ms":500},"idempotency_key":"request-abc"}'
```

Compose assigns independent hostnames to scaled workers. Follow their
machine-parseable event logs with `docker compose logs -f worker`.

## Benchmark methodology

The main benchmark used one synchronized batch per trial:

- worker counts: 1, 4, and 8;
- three trials per worker count;
- 600 `sleep` jobs per trial, each 25 ms;
- 30 warm-up jobs after every scale change;
- 10 s leases, three maximum attempts, 1 s retry base, and 250 ms empty-queue
  polling;
- benchmark-owned idempotency keys, with only those rows removed between runs.

The harness stages rows directly in PostgreSQL with the same future
`created_at` and `available_at`. This deliberate synchronized release excludes
sequential HTTP submission and container startup from the worker-scaling
measurement. It means job latency is **batch completion latency**: later jobs
wait behind earlier jobs, so the percentiles are not single-job service time.
The FastAPI submission path is covered separately by integration tests.

Metrics are defined as:

- throughput = 600 successful jobs / (release time to final `completed_at`);
- job completion latency = `completed_at - created_at`;
- claim transaction latency = worker monotonic time immediately before `BEGIN`
  through successful `COMMIT` for a claimed job.

Claim latency includes libpq, container/network, PostgreSQL, and commit
overhead; it is not pure server execution time. Instrumentation adds a timer and
one numeric field to the existing successful-claim log line. It performs no
extra query, transaction, metrics write, or additional log flush.

The recorded run was on 2026-08-28 using an ARM64 Mac with 12 logical CPUs,
Docker 29.6.1, Docker Compose 5.1.4, and the Compose `postgres:17-alpine`
database. Local/container benchmark results are environment-dependent.

Reproduce the full run from the repository root:

```bash
python3 benchmarks/run_benchmark.py
```

Useful explicit options are:

```bash
python3 benchmarks/run_benchmark.py \
  --worker-counts 1 4 8 --trials 3 --jobs 600 \
  --duration-ms 25 --warmup-jobs 30
```

The script builds/scales Compose services, refuses to run alongside unrelated
active jobs, prints a concise table, and writes raw and summarized CSV files in
[`benchmarks/results`](benchmarks/results). `summarize.py` can regenerate the
summary from `raw_trials.csv`.

## Benchmark results

Each value below is the median of the three trial-level measurements. Throughput
is jobs/s; all latency columns are milliseconds.

| Workers | Throughput | p50 Job | p95 Job | p50 Claim | p95 Claim |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 32.41 | 9284.10 | 17600.65 | 1.772 | 3.662 |
| 4 | 131.70 | 2367.86 | 4350.92 | 1.738 | 4.143 |
| 8 | 250.42 | 1289.58 | 2270.59 | 1.838 | 3.879 |

Raw trial rows are in
[`raw_trials.csv`](benchmarks/results/raw_trials.csv), with all 5,400 per-job
and per-claim samples in `job_metrics.csv` and `claim_metrics.csv`.

### Findings

- Throughput scaled 4.06× from one to four workers and 7.73× from one to eight.
  Four to eight workers added 1.90× throughput, a small decline from ideal 2×
  scaling but still a strong gain. Eight workers had not reached a clear
  saturation point on this machine.
- Parallelism reduced synchronized-batch p95 completion latency from 17.60 s to
  4.35 s to 2.27 s. This is the expected reduction in time spent waiting behind
  the released batch.
- Claim p50 remained around 1.7–1.8 ms. Claim p95 moved from 3.66 ms to 4.14 ms
  and then 3.88 ms rather than increasing monotonically. PostgreSQL claim
  contention was therefore not measurably worsening through eight workers in
  this run.
- The modest four-to-eight scaling loss is not enough to identify a database
  bottleneck; normal scheduling, logging, client, and container variation are
  also present. The data bounds the result: the centralized claim path was not
  the limiting factor at the tested concurrency.

## Queue-overload observation

`run_overload.py` scheduled 300 jobs at 60 arrivals/s against one worker, whose
measured batch capacity was about 32.4 jobs/s. Queue depth counts only arrived
jobs still in `queued`, not future scheduled rows.

```bash
python3 benchmarks/run_overload.py --no-build
```

While arrivals exceeded completion capacity, queued depth grew to 139 jobs near
the end of the five-second arrival window. Once arrivals stopped, the worker
drained the backlog; all 300 jobs were complete and queue depth was zero at
9.59 s. Samples are in
[`overload.csv`](benchmarks/results/overload.csv).

## Failure demonstrations

Measure force-kill recovery with a 5 s lease:

```bash
python3 scripts/crash_recovery_demo.py
```

The script starts two workers, finds the container that claimed a 2.5 s sleep
job, sends it `SIGKILL`, verifies the row remains temporarily stranded, and
observes a survivor reclaim attempt 2. The recorded completed-kill-to-observed-
reclaim interval was **4.977 s**, close to the 5 s lease; the small difference
reflects how soon after claim the kill completed and 50 ms observation polling.
Recovery is primarily bounded by the remaining lease plus claim/poll delay. The
machine-readable result is
[`recovery.json`](benchmarks/results/recovery.json).

Demonstrate the side-effect/acknowledgement failure window with:

```bash
python3 scripts/idempotent_effect_demo.py
```

Attempt 1 commits the effect and intentionally exits before acknowledging the
job. Attempt 2 runs after lease expiry. The demo requires two execution-audit
rows, one logical effect row, and final `succeeded` state.

## Design decisions and tradeoffs

**Why PostgreSQL?** It provides durable state, transactions, row-level locks,
`SKIP LOCKED`, database time, and uniqueness constraints. That is enough to
study queue coordination without introducing another distributed service.

**Why execute outside the claim transaction?** Holding a transaction and row
lock while arbitrary code runs would create long transactions and unnecessary
contention. The lease and attempt-matched completion update handle lost or
stale owners instead.

**Why leases?** A crashed or force-killed worker cannot reliably deregister or
run cleanup. A persisted deadline makes abandoned work recoverable without its
cooperation.

**Why at-least-once rather than exactly-once?** An external side effect and the
queue acknowledgement cannot generally share one atomic transaction. A crash
between them makes re-execution necessary for recovery.

**Why idempotency?** Submission keys collapse repeated client requests, while
side-effect keys make unavoidable repeated executions logically safe. Neither
mechanism substitutes for the other.

**Where does scaling bottleneck?** This experiment found no monotonic claim-
latency increase through eight workers, so it does not support naming
PostgreSQL contention as the current limit. Eventually the single PostgreSQL
coordinator, one claim transaction per job, per-job execution logging, or host
resources must cap scaling; locating that point would require higher worker
counts and separate profiling, outside this milestone.

## Tests

All tests use real PostgreSQL. Run API, locking, and benchmark-calculation tests
with workers stopped:

```bash
docker compose up -d --build db api
docker compose stop worker
docker compose exec api pytest -q \
  tests/test_jobs.py tests/test_claiming.py tests/test_benchmarks.py
```

Run ordinary worker behavior plus four-worker claim/reclaim concurrency:

```bash
docker compose up -d --build --scale worker=4
docker compose exec \
  -e RUN_WORKER_TESTS=1 -e RUN_MULTI_WORKER_TESTS=1 \
  api pytest -q tests/test_worker.py
```

Run the self-terminating post-effect crash integration test:

```bash
JOB_LEASE_SECONDS=1 RETRY_BASE_SECONDS=0.25 \
  docker compose up -d --build --scale worker=2
docker compose exec \
  -e RUN_WORKER_TESTS=1 -e RUN_EFFECT_CRASH_TESTS=1 \
  api pytest -q tests/test_worker.py -k post_effect_crash
```

Coverage includes API submission races and conflicts, PostgreSQL locked-row
skipping, unexpired-lease rejection, expired reclaim, stale-completion
protection, retries and 1×/2× backoff, attempt exhaustion, concurrent claims,
duplicate execution, idempotent effects, percentile calculation, aggregation,
and claim-log parsing.

## Limitations

- PostgreSQL is the centralized coordinator; there is no queue partitioning.
- Leases are fixed and not renewed. A legitimate job longer than its lease can
  be reclaimed and run concurrently on another worker.
- Execution is at-least-once, never exactly-once; consumers own side-effect
  idempotency.
- Workers poll rather than receive push notifications.
- Retries have exponential backoff but no jitter, specialized error classes,
  or dead-letter infrastructure.
- There are no priorities, DAGs, worker registry, heartbeats, admission
  control, authentication, or production observability stack.
- The benchmark is a synchronized, local, containerized experiment on one
  laptop, not a large-cluster or steady-state production load test.

These are deliberate scope decisions. Relay's value is the combination of
correctness, explicit failure semantics, reproducible measurements, and a
small enough implementation to understand end to end.
