# Relay

Relay is a small distributed job queue built in milestones. Milestone 2 adds a
single C++20 worker to the existing FastAPI control plane and PostgreSQL queue.

## Architecture

```text
client -> FastAPI -> PostgreSQL <- C++ worker
                            queued -> running -> succeeded/failed
```

- FastAPI accepts jobs with `POST /jobs` and returns their state with
  `GET /jobs/{job_id}`.
- PostgreSQL persists jobs across API and worker container restarts.
- One polling C++ worker claims the oldest eligible queued job, executes it
  outside the claim transaction, and records a terminal state.

The worker currently supports only this payload:

```json
{"type": "sleep", "duration_ms": 500}
```

`duration_ms` must be an integer from 0 through 60000. Missing or invalid
fields and unsupported job types cause the job to become `failed`; they do not
stop the worker. A successful or failed job receives a `completed_at` value.

## Run the system

Requirements: Docker with Docker Compose.

```bash
docker compose up --build
```

This starts PostgreSQL, the API at <http://localhost:8000>, and one worker. The
API documentation is at <http://localhost:8000/docs>. The API health check is
`GET /health`. PostgreSQL data is held in the named `postgres_data` volume.

Submit a sleep job:

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"type":"sleep","duration_ms":500}}'
```

The response describes the new job in the `queued` state. Use its `id` to
observe the worker transition it through `running` to `succeeded`:

```bash
curl http://localhost:8000/jobs/<JOB_ID>
```

Worker lifecycle messages are available with:

```bash
docker compose logs -f worker
```

The worker image uses CMake to build the C++20 source and links directly to
PostgreSQL's `libpq` client. Its `DATABASE_URL` is supplied by Compose and no
database credentials are compiled into the binary.

## Run the tests

The original API tests should run without a polling worker so their immediate
database assertions stay deterministic:

```bash
docker compose up -d --build db api
docker compose stop worker
docker compose exec api pytest -q
```

The two worker tests are skipped in that mode. Run the PostgreSQL-backed worker
integration tests with all three services running:

```bash
docker compose up -d --build
docker compose exec -e RUN_WORKER_TESTS=1 api pytest -q tests/test_worker.py
```

Those tests cover successful sleep execution, `completed_at`, unsupported and
invalid payloads, and continued processing after failures.

## Claiming behavior and current limits

For clarity, the worker claims one eligible job in a short transaction:

```text
BEGIN
select the oldest queued job with available_at <= current time
lock it and update it to running
COMMIT
execute the job outside the transaction
update it to succeeded or failed
```

This milestone intentionally runs only one worker. If the worker process is
killed after claiming a job, that job can remain stranded in `running`.

**Multi-worker concurrency, leases, retries, and crash recovery have not been
implemented yet.** Idempotency enforcement, priority scheduling, and
benchmarking are also deferred to later milestones.

## Schema setup

On startup, the API runs SQLAlchemy's idempotent `metadata.create_all`. This is
a small setup mechanism for the current single table. A versioned migration
tool can replace it when future milestones require schema evolution.
