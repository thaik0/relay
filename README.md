# Relay

**Stack:** C++20, Python, FastAPI, PostgreSQL, Docker

Relay is a small distributed job-processing system built to explore what happens when multiple workers compete for work and can fail in the middle of processing it.

Jobs are stored in PostgreSQL, submitted through a FastAPI service, and executed by independent C++20 workers. Workers use PostgreSQL row locking to claim jobs without taking th esame work, leases to recover jobs left behind by crashed workers, and attempt numbers to prevent an older worker from overwriting the result of a newer attempt.

Relay has **at-least-once execution** rather than exactly-once execution, so the project also has examples of how idempotency can make repeated submissions and repeated side effects safe.

## Architecture

```
FastAPI -> PostgreSQL -> Workers [1..N]
```
- `POST /jobs` creates a durable job
- `GET /jobs/{job_id}` returns its current state
- PostgreSQL is the source of truth for job state, retries, leases, and submission idempotency
- C++ workers independently poll PostgreSQL for work, claim eligible jobs, execute them, and record the success or failure

## Concurrent job claiming
A worked claims a job in short PostgreSQL transactions using `FOR UPDATE SKIP LOCKED`, locking the selected row until the transaction commits while allows other workers to skip that row and claim different work. 

The worker changes the job to `running`, increments its attempt number, assigns a lease, and commits before executing the handler. Keeping execution outside the transaction means a slow job does not hold a database lock for its entire runtime.

The attempt number acts as a fencing value. When a worker records success or failure, the update must match both the job ID and the attempt that it originally claimed. If its lease expired and another worker has already reclaimed the job, the older worker can no longer overwrite the newer job state.

## Failure recovery and retries

Every claim receives a fixed lease stored in PostgreSQL.

If a worker crashes, the job remains running until its lease expires. Another worker can then start a new attempt without requiring the failed process to clean up after itself.

Handler failures are retried with exponential backoff until the configured attempt limit is reached. Retry times are stored in PostgreSQL, so workers can continue processing other jobs while failed jobs wait.

## At-least once execution
A worker can perform an external side effect and then crash before recording that the job succeeded:
```
perform side effect -> worker crashes -> success never recorded -> lease expires -> another worker executes the job again
```

Because the queue cannot generally make an arbitrary external side effect and its own acknowledgement one atomic operation, duplicate execution is possible during recovery.

Relay demonstrates two forms of idempotency:

- **Submission idempotency:** an optional `idempotency_key` prevents retried client requests from creating duplicate jobs.

- **Side-effect idempotency:** the `write_effect` demo uses an `operation_id` so multiple executions can still produce one logical result.

The second cases means the job may execute twice even though the resulting side effect only occurs once.

## Performance
I benchmarked synchronized batches of 600 jobs, each performing 25 ms of work, with 1, 4, and 8 workers. Values below are medians across three trials.

| Workers | Throughput | p50 Job | p95 Job | p50 Claim | p95 Claim |
| ------: | ---------: | ------: | ------: | --------: | --------: |
| 1 | 32.41 jobs/s | 9.28 s | 17.60 s | 1.77 ms | 3.66 ms |
| 4 | 131.70 jobs/s | 2.37 s | 4.35 s | 1.74 ms | 4.14 ms |
| 8 | 250.42 jobs/s | 1.29 s | 2.27 s | 1.84 ms | 3.88 ms |

Throughput increased **7.73x** movig from 1 to 8 workers. Claim p95 remained below 4.2 ms across the tested range, so the PostgreSQL claim path did not show a clear contention bottleneck at eight workers.

The job-latency values measure completion time within a synchronized batch, so later jobs include time spent waiting behind earlier jobs rather than only their 25 ms handler runtime.

A separate crash-recovery experiment force-killed a worker while it owned a job. With a five-second lease, another worker reclaimed the job 4.977 seconds after the kill completed.

## Run Relay
Reqs: Docker and Docker Compose
```
docker compose up --build --scale worker=4
```
Example job submission:
```
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"type":"sleep","duration_ms":500},"idempotency_key":"request-abc"}'
```
Run benchmark via:
```
python3 benchmarks/run_benchmark.py
```

## Limitations
Relay doesn't have features of many production grade task systems:
- jobs are not partitioned across queue servers
- leases are fixed rather than renewed
- workers poll the database rather than receiving notifications
- no priorities, DAGs, authentication, worker heartbeats, admission control, or production observatility stack
- no production cluster
