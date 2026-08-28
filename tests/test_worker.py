import os
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from fastapi.testclient import TestClient

from app.db import engine
from app.models import JobStatus, jobs


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_WORKER_TESTS") != "1",
    reason="worker integration tests require the Compose worker service",
)


def wait_for_job(
    client: TestClient,
    job_id: str,
    predicate: Callable[[dict[str, Any]], bool],
    timeout_seconds: float = 5,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_job: dict[str, Any] | None = None

    while time.monotonic() < deadline:
        response = client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        last_job = response.json()
        if predicate(last_job):
            return last_job
        time.sleep(0.05)

    pytest.fail(f"job {job_id} did not reach the expected state; last value: {last_job}")


def submit_job(client: TestClient, payload: dict[str, Any]) -> dict[str, Any]:
    response = client.post("/jobs", json={"payload": payload})
    assert response.status_code == 201
    return response.json()


def persisted_batch(job_ids: list[UUID]) -> list[dict[str, Any]]:
    statement = sa.select(jobs.c.id, jobs.c.status, jobs.c.attempts).where(
        jobs.c.id.in_(job_ids)
    )
    with engine.connect() as connection:
        return [dict(row) for row in connection.execute(statement).mappings()]


def test_worker_executes_sleep_job(client: TestClient) -> None:
    created = submit_job(client, {"type": "sleep", "duration_ms": 20})
    assert created["status"] == "queued"

    completed = wait_for_job(
        client,
        created["id"],
        lambda job: job["status"] == "succeeded",
    )

    assert completed["attempts"] == 1
    assert completed["completed_at"] is not None


def test_invalid_jobs_fail_and_worker_continues(client: TestClient) -> None:
    unsupported = submit_job(client, {"type": "unknown"})
    invalid_sleep = submit_job(client, {"type": "sleep", "duration_ms": -1})

    for created in (unsupported, invalid_sleep):
        failed = wait_for_job(
            client,
            created["id"],
            lambda job: job["status"] == "failed",
        )
        assert failed["attempts"] == 1
        assert failed["completed_at"] is not None

    following_job = submit_job(client, {"type": "sleep", "duration_ms": 0})
    succeeded = wait_for_job(
        client,
        following_job["id"],
        lambda job: job["status"] == "succeeded",
    )
    assert succeeded["completed_at"] is not None


@pytest.mark.skipif(
    os.getenv("RUN_MULTI_WORKER_TESTS") != "1",
    reason="requires at least two scaled Compose worker processes",
)
def test_multiple_workers_complete_a_batch_without_duplicate_claims() -> None:
    count = 24
    job_ids = [uuid4() for _ in range(count)]
    now = datetime.now(timezone.utc)
    available_at = now + timedelta(seconds=1)
    batch = [
        {
            "id": job_id,
            "payload": {"type": "sleep", "duration_ms": 200},
            "status": JobStatus.QUEUED.value,
            "attempts": 0,
            "available_at": available_at,
            "created_at": now,
        }
        for job_id in job_ids
    ]
    with engine.begin() as connection:
        connection.execute(sa.insert(jobs), batch)

    deadline = time.monotonic() + 15
    largest_running_count = 0
    last_batch: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        last_batch = persisted_batch(job_ids)
        statuses = [job["status"] for job in last_batch]
        largest_running_count = max(
            largest_running_count,
            statuses.count(JobStatus.RUNNING.value),
        )
        assert JobStatus.FAILED.value not in statuses
        if len(statuses) == count and all(
            status == JobStatus.SUCCEEDED.value for status in statuses
        ):
            break
        time.sleep(0.02)
    else:
        pytest.fail(f"batch did not complete; last values: {last_batch}")

    assert largest_running_count >= 2
    assert len(last_batch) == count
    assert all(job["attempts"] == 1 for job in last_batch)
