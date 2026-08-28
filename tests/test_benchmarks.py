import pytest

from benchmarks.run_benchmark import CLAIM_PATTERN
from benchmarks.summarize import format_table, percentile, summarize_trials


def test_percentile_uses_linear_interpolation() -> None:
    values = [40.0, 10.0, 30.0, 20.0]

    assert percentile(values, 50) == 25.0
    assert percentile(values, 95) == pytest.approx(38.5)


def test_summarize_trials_uses_median_trial_metrics() -> None:
    rows = [
        {
            "worker_count": 1,
            "throughput_jobs_per_second": value,
            "p50_job_latency_ms": value + 1,
            "p95_job_latency_ms": value + 2,
            "p50_claim_latency_ms": value + 3,
            "p95_claim_latency_ms": value + 4,
        }
        for value in (10, 30, 20)
    ]

    summary = summarize_trials(rows)

    assert summary == [
        {
            "worker_count": 1,
            "trial_count": 3,
            "throughput_jobs_per_second": 20.0,
            "p50_job_latency_ms": 21.0,
            "p95_job_latency_ms": 22.0,
            "p50_claim_latency_ms": 23.0,
            "p95_claim_latency_ms": 24.0,
        }
    ]


def test_claim_log_parser_extracts_successful_claim_metric() -> None:
    line = (
        "relay-worker-1 | worker=worker-abc event=claimed "
        "job=123e4567-e89b-12d3-a456-426614174000 attempt=1 "
        "claim_transaction_latency_ms=1.796 lease_expires_at=..."
    )

    match = CLAIM_PATTERN.search(line)

    assert match is not None
    assert match.groups() == ("123e4567-e89b-12d3-a456-426614174000", "1.796")


def test_result_table_uses_decimal_half_up_rounding() -> None:
    row = {
        "worker_count": 8,
        "throughput_jobs_per_second": 250.424156,
        "p50_job_latency_ms": 1289.58,
        "p95_job_latency_ms": 2270.5897,
        "p50_claim_latency_ms": 1.8375,
        "p95_claim_latency_ms": 3.8788,
    }

    assert "250.42 | 1289.58 | 2270.59 |     1.838 |     3.879" in format_table([row])
