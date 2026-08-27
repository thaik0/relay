# Relay

Relay is the persistent control-plane foundation for a small distributed job
queue. Later milestones will add workers, leasing, retries, and benchmarking;
Milestone 1 is intentionally limited to an HTTP API and PostgreSQL storage.

## Milestone 1

The API can create a job in the `queued` state and retrieve it by UUID. Jobs
are stored in PostgreSQL and survive API container restarts. There is no worker
or job execution yet.

## Run the service

Requirements: Docker with Docker Compose.

```bash
docker compose up --build
```

The API listens on <http://localhost:8000>. Its interactive documentation is at
<http://localhost:8000/docs>, and `GET /health` checks database connectivity.
PostgreSQL data is held in the named `postgres_data` volume, so restarting or
recreating only the API container does not remove jobs.

Submit a job:

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"type":"sleep","duration_ms":500}}'
```

Retrieve it using the `id` from that response:

```bash
curl http://localhost:8000/jobs/<JOB_ID>
```

An optional idempotency key can be stored, but Milestone 1 deliberately does
not enforce idempotency:

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"type":"sleep","duration_ms":500},"idempotency_key":"demo-1"}'
```

## Run the tests

Start PostgreSQL, build the API image, and run the integration tests inside it:

```bash
docker compose up -d db
docker compose build api
docker compose run --rm api pytest -q
```

The tests use the same PostgreSQL service and truncate only the `jobs` table
between test cases. To test against another PostgreSQL instance, install
`requirements.txt`, set `DATABASE_URL`, and run `pytest -q`.

## Schema setup

On startup, the API runs SQLAlchemy's idempotent `metadata.create_all`. This is
a small, reproducible setup mechanism for the single Milestone 1 table. A
versioned migration tool can replace it when later milestones require schema
evolution.
