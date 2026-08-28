#!/usr/bin/env python3
"""Schedule arrivals above one worker's capacity and sample queue depth."""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.run_benchmark import (
    BENCHMARK_KEY_PREFIX,
    DEFAULT_RESULTS,
    Benchmark,
    nonnegative_int,
    positive_int,
)
from benchmarks.summarize import write_csv


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=positive_int, default=1)
    parser.add_argument("--jobs", type=positive_int, default=300)
    parser.add_argument("--arrival-rate", type=positive_float, default=60.0)
    parser.add_argument("--duration-ms", type=nonnegative_int, default=25)
    parser.add_argument("--warmup-jobs", type=nonnegative_int, default=30)
    parser.add_argument("--sample-interval-ms", type=positive_int, default=250)
    parser.add_argument("--timeout-seconds", type=positive_int, default=60)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args()
    # Benchmark owns Compose/database mechanics. These fields are its fixed
    # queue configuration for this small observation experiment.
    args.worker_counts = [args.workers]
    args.trials = 1
    args.lease_seconds = 10
    args.max_attempts = 3
    args.retry_base_seconds = 1
    return args


def stage_arrivals(
    benchmark: Benchmark,
    run_id: str,
    job_count: int,
    duration_ms: int,
    arrival_rate: float,
) -> str:
    release_at = benchmark.scalar(
        "SELECT (clock_timestamp() + INTERVAL '1 second')::text"
    )
    payload = json.dumps(
        {
            "type": "sleep",
            "duration_ms": duration_ms,
            "benchmark_run_id": run_id,
            "experiment": "overload",
        },
        separators=(",", ":"),
    ).replace("'", "''")
    values = ",\n".join(
        f"('{uuid4()}'::uuid, {index}, "
        f"'{BENCHMARK_KEY_PREFIX}{run_id}:{index}')"
        for index in range(job_count)
    )
    benchmark.sql(
        f"""
        INSERT INTO jobs (
            id, payload, status, attempts, available_at,
            lease_expires_at, idempotency_key, created_at, completed_at
        )
        SELECT
            staged.id,
            '{payload}'::jsonb,
            'queued',
            0,
            '{release_at}'::timestamptz
                + (staged.sequence / {arrival_rate} * INTERVAL '1 second'),
            NULL,
            staged.idempotency_key,
            '{release_at}'::timestamptz
                + (staged.sequence / {arrival_rate} * INTERVAL '1 second'),
            NULL
        FROM (VALUES {values}) AS staged(id, sequence, idempotency_key);
        """
    )
    return release_at


def sample(benchmark: Benchmark, run_id: str, release_at: str) -> dict[str, int | float]:
    output = benchmark.sql(
        "SELECT "
        f"GREATEST(EXTRACT(EPOCH FROM (clock_timestamp() - '{release_at}'::timestamptz)), 0) AS elapsed_seconds, "
        "count(*) FILTER (WHERE created_at <= clock_timestamp()) AS arrived_jobs, "
        "count(*) FILTER (WHERE created_at <= clock_timestamp() AND status = 'queued') AS queued_jobs, "
        "count(*) FILTER (WHERE created_at <= clock_timestamp() AND status = 'running') AS running_jobs, "
        "count(*) FILTER (WHERE status = 'succeeded') AS completed_jobs "
        "FROM jobs "
        f"WHERE idempotency_key LIKE '{BENCHMARK_KEY_PREFIX}{run_id}:%'",
        csv_output=True,
    )
    row = next(csv.DictReader(io.StringIO(output)))
    return {
        "elapsed_seconds": round(float(row["elapsed_seconds"]), 6),
        "arrived_jobs": int(row["arrived_jobs"]),
        "queued_jobs": int(row["queued_jobs"]),
        "running_jobs": int(row["running_jobs"]),
        "completed_jobs": int(row["completed_jobs"]),
    }


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    args = parse_args()
    benchmark = Benchmark(args)
    run_id = f"overload-{uuid4()}"
    try:
        benchmark.initialize()
        benchmark.scale_workers(args.workers)
        benchmark.warm_up(args.workers)
        release_at = stage_arrivals(
            benchmark,
            run_id,
            args.jobs,
            args.duration_ms,
            args.arrival_rate,
        )
        print(
            f"scheduled jobs={args.jobs} arrival_rate={args.arrival_rate:.2f}/s "
            f"workers={args.workers}",
            flush=True,
        )
        samples = []
        deadline = time.monotonic() + args.timeout_seconds
        while time.monotonic() < deadline:
            observation = sample(benchmark, run_id, release_at)
            samples.append(observation)
            print(
                f"t={observation['elapsed_seconds']:.2f}s "
                f"queued={observation['queued_jobs']} "
                f"completed={observation['completed_jobs']}",
                flush=True,
            )
            if observation["completed_jobs"] == args.jobs:
                break
            time.sleep(args.sample_interval_ms / 1000)
        else:
            raise RuntimeError(f"overload experiment timed out: {samples[-1]}")

        args.results_dir.mkdir(parents=True, exist_ok=True)
        write_csv(args.results_dir / "overload.csv", samples)
        metadata = {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "worker_count": args.workers,
            "job_count": args.jobs,
            "job_duration_ms": args.duration_ms,
            "arrival_rate_jobs_per_second": args.arrival_rate,
            "sample_interval_ms": args.sample_interval_ms,
            "release_time": release_at,
            "queue_depth_definition": "arrived jobs whose status is queued",
        }
        (args.results_dir / "overload.json").write_text(
            json.dumps(metadata, indent=2) + "\n",
            encoding="utf-8",
        )
        print(
            f"peak_queued_jobs={max(row['queued_jobs'] for row in samples)} "
            f"drain_completed_seconds={samples[-1]['elapsed_seconds']:.3f}"
        )
        benchmark.cleanup_run(run_id)
    except Exception as error:
        print(f"overload experiment failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
