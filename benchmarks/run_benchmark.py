#!/usr/bin/env python3
"""Run synchronized sleep-job trials against scaled Compose workers."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import platform
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.summarize import format_table, percentile, summarize_trials, write_csv


DEFAULT_RESULTS = Path(__file__).resolve().parent / "results"
BENCHMARK_KEY_PREFIX = "relay-benchmark:"
POLL_INTERVAL_MS = 250
CLAIM_PATTERN = re.compile(
    r"event=(?:claimed|reclaimed) job=([0-9a-f-]+).*"
    r"claim_transaction_latency_ms=([0-9]+(?:\.[0-9]+)?)"
)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker-counts", nargs="+", type=positive_int, default=[1, 4, 8])
    parser.add_argument("--trials", type=positive_int, default=3)
    parser.add_argument("--jobs", type=positive_int, default=600)
    parser.add_argument("--duration-ms", type=nonnegative_int, default=25)
    parser.add_argument("--warmup-jobs", type=nonnegative_int, default=30)
    parser.add_argument("--lease-seconds", type=positive_int, default=10)
    parser.add_argument("--max-attempts", type=positive_int, default=3)
    parser.add_argument("--retry-base-seconds", type=positive_int, default=1)
    parser.add_argument("--timeout-seconds", type=positive_int, default=180)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="reuse existing images rather than building once before the run",
    )
    return parser.parse_args()


class Benchmark:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.environment = os.environ.copy()
        self.environment.update(
            {
                "JOB_LEASE_SECONDS": str(args.lease_seconds),
                "MAX_JOB_ATTEMPTS": str(args.max_attempts),
                "RETRY_BASE_SECONDS": str(args.retry_base_seconds),
            }
        )
        self.raw_trials: list[dict[str, Any]] = []
        self.job_metrics: list[dict[str, Any]] = []
        self.claim_metrics: list[dict[str, Any]] = []

    def command(
        self,
        arguments: list[str],
        *,
        input_text: str | None = None,
    ) -> str:
        result = subprocess.run(
            arguments,
            cwd=PROJECT_ROOT,
            env=self.environment,
            input=input_text,
            check=True,
            text=True,
            capture_output=True,
        )
        return result.stdout.strip()

    def compose(self, *arguments: str) -> str:
        return self.command(["docker", "compose", *arguments])

    def sql(self, statement: str, *, csv_output: bool = False) -> str:
        command = [
            "docker",
            "compose",
            "exec",
            "-T",
            "db",
            "psql",
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "relay",
            "-d",
            "relay",
        ]
        if csv_output:
            command.append("--csv")
        command.extend(["-f", "-"])
        return self.command(command, input_text=statement)

    def scalar(self, statement: str) -> str:
        return self.command(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "db",
                "psql",
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                "relay",
                "-d",
                "relay",
                "-At",
                "-c",
                statement,
            ]
        )

    def initialize(self) -> None:
        up = ["up", "-d"]
        if not self.args.no_build:
            up.append("--build")
        up.extend(["--scale", f"worker={self.args.worker_counts[0]}"])
        print("preparing Compose services", flush=True)
        self.compose(*up)
        self.sql(
            f"DELETE FROM jobs WHERE idempotency_key LIKE '{BENCHMARK_KEY_PREFIX}%';"
        )
        unrelated = int(
            self.scalar(
                "SELECT count(*) FROM jobs "
                "WHERE status IN ('queued', 'running') "
                f"AND (idempotency_key IS NULL OR idempotency_key NOT LIKE '{BENCHMARK_KEY_PREFIX}%')"
            )
        )
        if unrelated:
            raise RuntimeError(
                f"refusing to benchmark with {unrelated} unrelated active job(s)"
            )

    def scale_workers(self, worker_count: int) -> None:
        self.compose("up", "-d", "--scale", f"worker={worker_count}")
        running = len(self.compose("ps", "-q", "worker").splitlines())
        if running != worker_count:
            raise RuntimeError(
                f"expected {worker_count} worker containers, found {running}"
            )

    def stage_jobs(self, run_id: str, job_count: int, duration_ms: int) -> list[str]:
        job_ids = [str(uuid4()) for _ in range(job_count)]
        release_at = self.scalar(
            "SELECT (clock_timestamp() + INTERVAL '1 second')::text"
        )
        payload = json.dumps(
            {
                "type": "sleep",
                "duration_ms": duration_ms,
                "benchmark_run_id": run_id,
            },
            separators=(",", ":"),
        ).replace("'", "''")
        values = ",\n".join(
            f"('{job_id}'::uuid, '{BENCHMARK_KEY_PREFIX}{run_id}:{index}')"
            for index, job_id in enumerate(job_ids)
        )
        self.sql(
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
                '{release_at}'::timestamptz,
                NULL,
                staged.idempotency_key,
                '{release_at}'::timestamptz,
                NULL
            FROM (VALUES {values}) AS staged(id, idempotency_key);
            """
        )
        return job_ids

    def status_counts(self, run_id: str) -> dict[str, int]:
        output = self.scalar(
            "SELECT status || '=' || count(*) FROM jobs "
            f"WHERE idempotency_key LIKE '{BENCHMARK_KEY_PREFIX}{run_id}:%' "
            "GROUP BY status ORDER BY status"
        )
        return {
            status: int(count)
            for line in output.splitlines()
            if line
            for status, count in [line.split("=", 1)]
        }

    def wait_for_success(self, run_id: str, job_count: int) -> None:
        deadline = time.monotonic() + self.args.timeout_seconds
        last_counts: dict[str, int] = {}
        while time.monotonic() < deadline:
            last_counts = self.status_counts(run_id)
            if last_counts.get("failed", 0):
                raise RuntimeError(f"benchmark job failed: {last_counts}")
            if last_counts.get("succeeded", 0) == job_count:
                return
            time.sleep(0.25)
        raise RuntimeError(
            f"trial {run_id} timed out after {self.args.timeout_seconds}s: {last_counts}"
        )

    def fetch_job_metrics(self, run_id: str) -> list[dict[str, str]]:
        output = self.sql(
            "SELECT id::text AS job_id, created_at, completed_at, "
            "EXTRACT(EPOCH FROM (completed_at - created_at)) * 1000 AS latency_ms "
            "FROM jobs "
            f"WHERE idempotency_key LIKE '{BENCHMARK_KEY_PREFIX}{run_id}:%' "
            "ORDER BY id",
            csv_output=True,
        )
        return list(csv.DictReader(io.StringIO(output)))

    def fetch_claim_metrics(self, job_ids: set[str], since: str) -> list[dict[str, Any]]:
        logs = self.compose("logs", "--no-color", "--since", since, "worker")
        metrics = []
        for match in CLAIM_PATTERN.finditer(logs):
            if match.group(1) in job_ids:
                metrics.append(
                    {
                        "job_id": match.group(1),
                        "claim_transaction_latency_ms": float(match.group(2)),
                    }
                )
        return metrics

    def cleanup_run(self, run_id: str) -> None:
        self.sql(
            "DELETE FROM jobs "
            f"WHERE idempotency_key LIKE '{BENCHMARK_KEY_PREFIX}{run_id}:%';"
        )

    def warm_up(self, worker_count: int) -> None:
        if self.args.warmup_jobs == 0:
            return
        run_id = f"warmup-{worker_count}-{uuid4()}"
        print(
            f"warming workers={worker_count} jobs={self.args.warmup_jobs}",
            flush=True,
        )
        self.stage_jobs(run_id, self.args.warmup_jobs, self.args.duration_ms)
        self.wait_for_success(run_id, self.args.warmup_jobs)
        self.cleanup_run(run_id)

    def run_trial(self, worker_count: int, trial: int) -> None:
        run_id = str(uuid4())
        log_since = datetime.now(timezone.utc).isoformat()
        job_ids = self.stage_jobs(run_id, self.args.jobs, self.args.duration_ms)
        print(
            f"running workers={worker_count} trial={trial} jobs={self.args.jobs}",
            flush=True,
        )
        self.wait_for_success(run_id, self.args.jobs)
        jobs = self.fetch_job_metrics(run_id)
        claims = self.fetch_claim_metrics(set(job_ids), log_since)
        if len(jobs) != self.args.jobs:
            raise RuntimeError(f"expected {self.args.jobs} job metrics, found {len(jobs)}")
        if len(claims) != self.args.jobs:
            raise RuntimeError(
                f"expected {self.args.jobs} successful claim metrics, found {len(claims)}"
            )

        job_latencies = [float(job["latency_ms"]) for job in jobs]
        claim_latencies = [claim["claim_transaction_latency_ms"] for claim in claims]
        elapsed_seconds = max(job_latencies) / 1000
        row = {
            "worker_count": worker_count,
            "trial": trial,
            "run_id": run_id,
            "job_count": self.args.jobs,
            "job_duration_ms": self.args.duration_ms,
            "benchmark_start_time": jobs[0]["created_at"],
            "elapsed_seconds": round(elapsed_seconds, 6),
            "throughput_jobs_per_second": round(self.args.jobs / elapsed_seconds, 6),
            "p50_job_latency_ms": round(percentile(job_latencies, 50), 6),
            "p95_job_latency_ms": round(percentile(job_latencies, 95), 6),
            "p50_claim_latency_ms": round(percentile(claim_latencies, 50), 6),
            "p95_claim_latency_ms": round(percentile(claim_latencies, 95), 6),
            "lease_seconds": self.args.lease_seconds,
            "max_attempts": self.args.max_attempts,
            "retry_base_seconds": self.args.retry_base_seconds,
            "poll_interval_ms": POLL_INTERVAL_MS,
        }
        self.raw_trials.append(row)
        self.job_metrics.extend(
            {
                "worker_count": worker_count,
                "trial": trial,
                "run_id": run_id,
                **job,
            }
            for job in jobs
        )
        self.claim_metrics.extend(
            {
                "worker_count": worker_count,
                "trial": trial,
                "run_id": run_id,
                **claim,
            }
            for claim in claims
        )
        self.write_results()
        self.cleanup_run(run_id)
        print(
            f"completed workers={worker_count} trial={trial} "
            f"throughput={row['throughput_jobs_per_second']:.2f} jobs/s",
            flush=True,
        )

    def environment_metadata(self) -> dict[str, Any]:
        return {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "os": platform.platform(),
            "architecture": platform.machine(),
            "logical_cpu_count": os.cpu_count(),
            "docker_version": self.command(
                ["docker", "version", "--format", "{{.Server.Version}}"]
            ),
            "docker_compose_version": self.compose("version", "--short"),
            "git_commit": self.command(["git", "rev-parse", "HEAD"]),
            "worker_counts": self.args.worker_counts,
            "trials_per_worker_count": self.args.trials,
            "job_count_per_trial": self.args.jobs,
            "job_duration_ms": self.args.duration_ms,
            "warmup_jobs_per_worker_count": self.args.warmup_jobs,
            "lease_seconds": self.args.lease_seconds,
            "max_attempts": self.args.max_attempts,
            "retry_base_seconds": self.args.retry_base_seconds,
            "worker_poll_interval_ms": POLL_INTERVAL_MS,
        }

    def write_results(self) -> None:
        results = self.args.results_dir
        write_csv(results / "raw_trials.csv", self.raw_trials)
        write_csv(results / "job_metrics.csv", self.job_metrics)
        write_csv(results / "claim_metrics.csv", self.claim_metrics)
        summary = summarize_trials(self.raw_trials)
        write_csv(results / "summary.csv", summary)

    def run(self) -> None:
        self.initialize()
        metadata = self.environment_metadata()
        self.args.results_dir.mkdir(parents=True, exist_ok=True)
        (self.args.results_dir / "environment.json").write_text(
            json.dumps(metadata, indent=2) + "\n",
            encoding="utf-8",
        )
        for worker_count in self.args.worker_counts:
            self.scale_workers(worker_count)
            self.warm_up(worker_count)
            for trial in range(1, self.args.trials + 1):
                self.run_trial(worker_count, trial)
        print("\nMedian across trials (jobs/s and milliseconds):")
        print(format_table(summarize_trials(self.raw_trials)))


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    args = parse_args()
    if args.duration_ms > 60_000:
        raise SystemExit("--duration-ms must be at most 60000")
    try:
        Benchmark(args).run()
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"benchmark failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
