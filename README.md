# Relay

Relay is a small distributed job queue built in milestones. Milestone 3 adds
multiple concurrent C++20 worker processes to the FastAPI control plane and
PostgreSQL queue.

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

- FastAPI accepts jobs with `POST /jobs` and returns their state with
  `GET /jobs/{job_id}`.
- PostgreSQL persists jobs and coordinates independent worker processes.
- Each polling C++ worker atomically claims an eligible queued job, executes it
  outside the claim transaction, and records a terminal state.

The worker currently supports only this payload:

```json
{"type": "sleep", "duration_ms": 500}
```

`duration_ms` must be an integer from 0 through 60000. Missing or invalid
fields and unsupported job types cause the job to become `failed`; they do not
stop the worker. A successful or failed job receives a `completed_at` value.

## Run multiple workers

Requirements: Docker with Docker Compose.

```bash
docker compose up --build --scale worker=4
```

This starts PostgreSQL, the API at <http://localhost:8000>, and four independent
worker containers connected to the same queue. The API documentation is at
<http://localhost:8000/docs>. The API health check is `GET /health`.
PostgreSQL data is held in the named `postgres_data` volume.

Submit one sleep job:

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"type":"sleep","duration_ms":500}}'
```

Use the returned `id` to read its state:

```bash
curl http://localhost:8000/jobs/<JOB_ID>
```

Every worker uses `WORKER_ID` when it is configured, otherwise its unique
Compose hostname. Logs include `worker`, `event`, and `job` fields:

```text
worker=worker-9c22d6f36e90 event=claimed job=...
worker=worker-28ed40947f89 event=claimed job=...
worker=worker-9c22d6f36e90 event=succeeded job=...
```

Follow all worker replicas with:

```bash
docker compose logs -f worker
```

## Batch demonstration

With four workers running, open another terminal and submit 100 short jobs:

```bash
python scripts/submit_jobs.py --count 100 --duration-ms 100
```

The helper periodically prints state totals and exits successfully after all
jobs succeed. The worker logs show different worker IDs claiming different
jobs concurrently. This is a correctness demonstration, not the formal
throughput benchmark planned for a later milestone.

The worker image uses CMake to build the C++20 source and links directly to
PostgreSQL's `libpq` client. Its `DATABASE_URL` is supplied by Compose; no
database credentials are compiled into the binary.

## Atomic claiming

Each worker performs this short transaction:

```text
BEGIN
select the oldest queued job with available_at <= current time
  FOR UPDATE SKIP LOCKED
update that row from queued to running and increment attempts
COMMIT
```

Selection and the `queued` to `running` transition happen in the same
transaction, so two workers cannot successfully claim the same queued row.
`FOR UPDATE` makes PostgreSQL hold a row-level lock until the transition
commits. `SKIP LOCKED` lets another worker immediately choose a different
eligible job instead of waiting behind that lock. Ordering by `available_at`,
`created_at`, and `id` provides deterministic oldest-first selection among
unlocked rows.

The worker commits before executing the payload. Consequently, a long-running
job does not hold a database transaction or prevent other workers from
claiming work.

## Run the tests

All tests use real PostgreSQL. First run the API suite and the targeted
two-connection `SKIP LOCKED` test with workers stopped so polling cannot race
the API assertions:

```bash
docker compose up -d --build db api
docker compose stop worker
docker compose exec api pytest -q tests/test_jobs.py tests/test_claiming.py
```

Run ordinary worker behavior with one worker:

```bash
docker compose up -d --build --scale worker=1
docker compose exec -e RUN_WORKER_TESTS=1 api pytest -q tests/test_worker.py
```

Run the batch concurrency test with four independent worker processes:

```bash
docker compose up -d --build --scale worker=4
docker compose exec \
  -e RUN_WORKER_TESTS=1 \
  -e RUN_MULTI_WORKER_TESTS=1 \
  api pytest -q tests/test_worker.py
```

The concurrency coverage verifies that two PostgreSQL connections skip locked
rows, that multiple jobs are observed in `running` simultaneously, that every
job in a 24-job batch succeeds, and that every job has exactly one claim
attempt. Existing invalid-payload coverage confirms failures do not stop a
worker.

## Current reliability limits

This milestone provides atomic claiming, but it does **not** claim exactly-once
execution. A worker killed after committing a claim can still leave its job
permanently in `running`. The existing `lease_expires_at` field remains unused.

Lease ownership, lease renewal, crash recovery, retries, and exponential
backoff are intentionally deferred to Milestone 4. Idempotency enforcement,
priority scheduling, and benchmarking also remain out of scope.

## Schema setup

On startup, the API runs SQLAlchemy's idempotent `metadata.create_all`. This is
a small setup mechanism for the current single table. A versioned migration
tool can replace it when future milestones require schema evolution.
