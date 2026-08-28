#!/usr/bin/env python3
"""Aggregate Relay benchmark trials and print the main results table."""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable


SUMMARY_METRICS = (
    "throughput_jobs_per_second",
    "p50_job_latency_ms",
    "p95_job_latency_ms",
    "p50_claim_latency_ms",
    "p95_claim_latency_ms",
)


def percentile(values: Iterable[float], percent: float) -> float:
    """Return a linearly interpolated percentile using a zero-based rank."""
    ordered = sorted(values)
    if not ordered:
        raise ValueError("percentile requires at least one value")
    if not 0 <= percent <= 100:
        raise ValueError("percent must be between 0 and 100")

    rank = (len(ordered) - 1) * percent / 100
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def summarize_trials(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Take the median trial result for every worker count."""
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[int(row["worker_count"])].append(row)

    summary = []
    for worker_count, trials in sorted(grouped.items()):
        result: dict[str, Any] = {
            "worker_count": worker_count,
            "trial_count": len(trials),
        }
        for metric in SUMMARY_METRICS:
            result[metric] = statistics.median(
                float(trial[metric]) for trial in trials
            )
        summary.append(result)
    return summary


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise ValueError(f"cannot write empty result file: {path}")
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def format_table(rows: Iterable[dict[str, Any]]) -> str:
    def formatted(value: Any, places: int) -> str:
        quantum = Decimal(1).scaleb(-places)
        return str(Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP))

    lines = [
        "Workers | Throughput | p50 Job | p95 Job | p50 Claim | p95 Claim",
        "------- | ---------- | ------- | ------- | --------- | ---------",
    ]
    for row in rows:
        lines.append(
            f"{int(row['worker_count']):7d} | "
            f"{formatted(row['throughput_jobs_per_second'], 2):>10} | "
            f"{formatted(row['p50_job_latency_ms'], 2):>7} | "
            f"{formatted(row['p95_job_latency_ms'], 2):>7} | "
            f"{formatted(row['p50_claim_latency_ms'], 3):>9} | "
            f"{formatted(row['p95_claim_latency_ms'], 3):>9}"
        )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "raw_trials",
        nargs="?",
        type=Path,
        default=Path(__file__).parent / "results" / "raw_trials.csv",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent / "results" / "summary.csv",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with args.raw_trials.open(newline="", encoding="utf-8") as source:
        trials = list(csv.DictReader(source))
    summary = summarize_trials(trials)
    write_csv(args.output, summary)
    print(format_table(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
