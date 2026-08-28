# Relay

Relay is a small distributed job queue built in milestones. Milestone 4 adds
fixed-duration leases, worker crash recovery, bounded retries, and exponential
backoff to the FastAPI control plane, PostgreSQL queue, and concurrent C++20
workers.

## Architecture

```text
                  FastAPI
                     |
                     v
                PostgreSQL
                     |
          -----------------------
          |          |          |
          v          v          v
       Worker 1   Worker 2   Worker 3
```

- FastAPI accepts jobs with `POST /jobs` and returns their persisted state with
  `GET /jobs/{job_id}`.
- PostgreSQL stores jobs, retry schedules, and lease deadlines and coordinates
  independent workers with row-level locking.
- Workers atomically claim eligible queued or expired running jobs, commit,
  execute outside the transaction, and record success or a retry decision.

The worker supports these payloads:

```json
{"type": "sleep", "duration_ms": 500}
{"type": "fail"}
```

`duration_ms` must be an integer from 0 through 60000. The `fail` job always
fails and exists to demonstrate deterministic retries. Missing or invalid
fields and unsupported job types are also execution failures: they retry until
the configured attempt limit and do not stop the worker.

## Run multiple workers

Requirements: Docker with Docker Compose.

```bash
docker compose up --build --scale worker=4
```

This starts PostgreSQL, the API at <http://localhost:8000>, and four independent
worker containers connected to the same queue. The API documentation is at
<http://localhost:8000/docs>. The API health check is `GET /health`.
PostgreSQL data is held in the named `postgres_data` volume.

Submit and inspect a sleep job:

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"type":"sleep","duration_ms":500}}'

curl http://localhost:8000/jobs/<JOB_ID>
```

Every worker uses `WORKER_ID` when configured, otherwise its unique Compose
hostname. Follow all replicas with:

```bash
docker compose logs -f worker
```

## Leases and crash recovery

`attempts` has one meaning: **the number of execution attempts that have
started**. A new job has `attempts = 0`. Every successful claim or reclaim
increments it exactly once inside the claim transaction.

Each worker performs this short transaction:

```text
BEGIN
finalize one expired running job already at the attempt limit, if present
select the oldest eligible queued or expired running job
  with attempts below the limit
  FOR UPDATE SKIP LOCKED
set status = running
increment attempts
set lease_expires_at = database time + lease duration
COMMIT
```

Both new claims and reclaims therefore retain Milestone 3's PostgreSQL
concurrency safety. `FOR UPDATE` locks the selected row until commit, while
`SKIP LOCKED` lets competing workers choose other work without waiting. Several
workers can discover an expired lease, but only one can lock and begin its next
attempt. Unexpired running jobs, future retries, succeeded jobs, and permanently
failed jobs are not eligible.

Execution still happens after commit. Long work does not hold a database
transaction, block queue progress, or retain a row lock. If a worker dies after
commit, the `running` row and lease deadline remain in PostgreSQL. Once database
time reaches `lease_expires_at`, any surviving worker can safely reclaim it.
Recovery never depends on shutdown cleanup from the dead process.

Successful completion sets `status = succeeded`, records `completed_at`, and
clears the lease. Completion and failure updates also match the attempt number
the worker claimed. If a stale worker finishes after another worker has already
reclaimed the row, its outdated update cannot overwrite the newer attempt.

Worker policy is configured with environment variables. Compose supplies these
defaults:

| Variable | Default | Meaning |
| --- | ---: | --- |
| `JOB_LEASE_SECONDS` | `10` | Fixed lease duration assigned at claim time |
| `MAX_JOB_ATTEMPTS` | `3` | Maximum number of executions that may begin |
| `RETRY_BASE_SECONDS` | `1` | Base delay for execution-failure retries |

Positive fractional second values are accepted for lease and retry timing,
which keeps focused integration tests fast.

## Retries and exponential backoff

After an ordinary execution failure, the worker makes one database update:

```text
if attempts < MAX_JOB_ATTEMPTS:
    status = queued
    available_at = database time + backoff
    completed_at = null
    lease_expires_at = null
else:
    status = failed
    completed_at = database time
    lease_expires_at = null
```

The deterministic delay after attempt `n` is:

```text
RETRY_BASE_SECONDS * 2^(n - 1)
```

With the defaults, failures after attempts 1 and 2 wait 1 and 2 seconds. A
failure on attempt 3 is permanent. Retry eligibility uses
`available_at <= CURRENT_TIMESTAMP` in PostgreSQL. Workers schedule the retry
and immediately return to polling the whole queue; no worker sleeps waiting for
one particular job.

To observe the complete retry sequence, submit a deterministic failure and
watch its state and logs:

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"type":"fail"}}'

watch -n 0.25 curl -s http://localhost:8000/jobs/<JOB_ID>
docker compose logs -f worker
```

The logs expose `claimed`, `reclaimed`, `retry_scheduled`, `succeeded`, and
`failed_permanently` events with the worker, job, attempt, and relevant lease or
retry timestamp.

## Forced worker-crash demonstration

Run the deterministic demo from the repository root:

```bash
python3 scripts/crash_recovery_demo.py
```

The helper starts PostgreSQL, the API, and two workers with a 5-second lease;
submits a 2.5-second sleep job; identifies the container that logged its claim;
and sends that container `SIGKILL` with `docker kill --signal KILL`. It verifies
all of these externally visible states:

```text
running, attempts = 1
worker is force-killed
still running, attempts = 1 before lease expiry
running, attempts = 2 after another worker reclaims it
succeeded, attempts = 2, lease_expires_at = null
```

The demo prints the relevant worker logs and exits nonzero if recovery does not
occur. It exercises actual worker death, not graceful shutdown or an ordinary
job exception.

## Batch demonstration

With four workers running, submit 100 short jobs:

```bash
python3 scripts/submit_jobs.py --count 100 --duration-ms 100
```

The helper periodically prints state totals and exits after all jobs succeed.
This validates concurrent operation; it is not the formal throughput and
latency benchmark planned for Milestone 6.

## Run the tests

All tests use real PostgreSQL. Run the API and direct PostgreSQL locking tests
with polling workers stopped:

```bash
docker compose up -d --build db api
docker compose stop worker
docker compose exec api pytest -q tests/test_jobs.py tests/test_claiming.py
```

Run ordinary lease, retry, backoff, and attempt-exhaustion behavior with one
worker:

```bash
docker compose up -d --build --scale worker=1
docker compose exec -e RUN_WORKER_TESTS=1 api pytest -q tests/test_worker.py
```

Run the queued-claim and expired-reclaim concurrency coverage with four workers:

```bash
docker compose up -d --build --scale worker=4
docker compose exec \
  -e RUN_WORKER_TESTS=1 \
  -e RUN_MULTI_WORKER_TESTS=1 \
  api pytest -q tests/test_worker.py
```

Coverage includes lease assignment and clearing, rejection of unexpired leases,
expired reclaim with a replaced lease, concurrent `SKIP LOCKED` behavior,
successful terminal-state exclusion, future retry scheduling, 1x/2x backoff,
attempt exhaustion, and finalization of an abandoned job already at the limit.

## Delivery guarantee and current limitations

Relay now provides **at-least-once execution behavior**, not exactly-once
execution. A payload can execute more than once if a worker completes its work
but dies before recording success: its lease eventually expires and another
worker executes the payload again. Duplicate side-effect protection and use of
the existing `idempotency_key` field are deliberately deferred to Milestone 5.

Leases are fixed and are not renewed. There are no heartbeats, renewal threads,
worker registry, or reaper service. A legitimate job whose runtime exceeds its
lease can therefore be reclaimed prematurely and execute concurrently on more
than one worker. Configure the lease comfortably above normal job runtime.

Relay also does not yet include retry jitter, specialized retry classes, dead
letter infrastructure, priority scheduling, or formal throughput/latency
benchmarks.

## Schema setup

On startup, the API runs SQLAlchemy's idempotent `metadata.create_all`. The
Milestone 1 schema already contained `attempts`, `available_at`, and
`lease_expires_at`, so Milestone 4 does not require a table migration. A
versioned migration tool can replace this setup if later schema changes need it.
