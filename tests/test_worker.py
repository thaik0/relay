import os
import time
from collections.abc import Callable
from typing import Any

import pytest
from fastapi.testclient import TestClient


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
