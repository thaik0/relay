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
from app.models import JobStatus, effect_attempts, effects, jobs


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


def insert_running_job(
    payload: dict[str, Any],
    *,
    attempts: int = 1,
    lease_delta: timedelta,
) -> str:
    job_id = uuid4()
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        connection.execute(
            sa.insert(jobs),
            {
                "id": job_id,
                "payload": payload,
                "status": JobStatus.RUNNING.value,
                "attempts": attempts,
                "available_at": now - timedelta(seconds=1),
                "lease_expires_at": now + lease_delta,
                "created_at": now - timedelta(seconds=1),
            },
        )
    return str(job_id)


def database_now() -> datetime:
    with engine.connect() as connection:
        return connection.execute(sa.select(sa.func.current_timestamp())).scalar_one()


def test_worker_executes_sleep_job(client: TestClient) -> None:
    created = submit_job(client, {"type": "sleep", "duration_ms": 500})
    assert created["status"] == "queued"

    running = wait_for_job(
        client,
        created["id"],
        lambda job: job["status"] == "running",
    )
    assert running["attempts"] == 1
    assert running["lease_expires_at"] is not None

    completed = wait_for_job(
        client,
        created["id"],
        lambda job: job["status"] == "succeeded",
    )

    assert completed["attempts"] == 1
    assert completed["completed_at"] is not None
    assert completed["lease_expires_at"] is None

    time.sleep(0.3)
    still_completed = client.get(f"/jobs/{created['id']}").json()
    assert still_completed["status"] == "succeeded"
    assert still_completed["attempts"] == 1


def test_write_effect_is_idempotent_across_repeated_execution(
    client: TestClient,
) -> None:
    payload = {
        "type": "write_effect",
        "operation_id": "operation-repeat",
        "value": "hello",
    }
    created = [submit_job(client, payload), submit_job(client, payload)]

    completed = [
        wait_for_job(
            client,
            job["id"],
            lambda persisted: persisted["status"] == "succeeded",
        )
        for job in created
    ]

    assert [job["attempts"] for job in completed] == [1, 1]
    with engine.connect() as connection:
        persisted_effects = connection.execute(sa.select(effects)).mappings().all()
        attempts = connection.execute(sa.select(effect_attempts)).mappings().all()
    assert len(persisted_effects) == 1
    assert persisted_effects[0]["operation_id"] == "operation-repeat"
    assert persisted_effects[0]["value"] == "hello"
    assert len(attempts) == 2
    assert {attempt["operation_id"] for attempt in attempts} == {"operation-repeat"}


def test_worker_does_not_reclaim_an_unexpired_lease(client: TestClient) -> None:
    job_id = insert_running_job(
        {"type": "sleep", "duration_ms": 0},
        lease_delta=timedelta(seconds=2),
    )

    time.sleep(0.5)
    persisted = client.get(f"/jobs/{job_id}").json()
    assert persisted["status"] == "running"
    assert persisted["attempts"] == 1


def test_worker_reclaims_an_expired_lease(client: TestClient) -> None:
    old_expiration = datetime.now(timezone.utc) - timedelta(seconds=1)
    job_id = insert_running_job(
        {"type": "sleep", "duration_ms": 500},
        lease_delta=timedelta(seconds=-1),
    )

    reclaimed = wait_for_job(
        client,
        job_id,
        lambda job: job["status"] == "running" and job["attempts"] == 2,
    )
    assert reclaimed["lease_expires_at"] is not None
    assert datetime.fromisoformat(reclaimed["lease_expires_at"]) > old_expiration

    completed = wait_for_job(
        client,
        job_id,
        lambda job: job["status"] == "succeeded",
    )
    assert completed["attempts"] == 2
    assert completed["lease_expires_at"] is None


def test_invalid_jobs_fail_and_worker_continues(client: TestClient) -> None:
    unsupported = submit_job(client, {"type": "unknown"})
    invalid_sleep = submit_job(client, {"type": "sleep", "duration_ms": -1})

    for created in (unsupported, invalid_sleep):
        failed = wait_for_job(
            client,
            created["id"],
            lambda job: job["status"] == "failed",
            timeout_seconds=8,
        )
        assert failed["attempts"] == 3
        assert failed["completed_at"] is not None
        assert failed["lease_expires_at"] is None

    following_job = submit_job(client, {"type": "sleep", "duration_ms": 0})
    succeeded = wait_for_job(
        client,
        following_job["id"],
        lambda job: job["status"] == "succeeded",
    )
    assert succeeded["completed_at"] is not None


def test_failing_job_uses_exponential_backoff_and_exhausts_attempts(
    client: TestClient,
) -> None:
    created = submit_job(client, {"type": "fail"})

    first_retry = wait_for_job(
        client,
        created["id"],
        lambda job: job["status"] == "queued" and job["attempts"] == 1,
    )
    first_delay_remaining = (
        datetime.fromisoformat(first_retry["available_at"]) - database_now()
    ).total_seconds()
    assert 0.4 < first_delay_remaining <= 1.05
    assert first_retry["lease_expires_at"] is None
    assert first_retry["completed_at"] is None

    second_retry = wait_for_job(
        client,
        created["id"],
        lambda job: job["status"] == "queued" and job["attempts"] == 2,
        timeout_seconds=3,
    )
    second_delay_remaining = (
        datetime.fromisoformat(second_retry["available_at"]) - database_now()
    ).total_seconds()
    assert 1.4 < second_delay_remaining <= 2.05
    assert second_retry["lease_expires_at"] is None

    failed = wait_for_job(
        client,
        created["id"],
        lambda job: job["status"] == "failed",
        timeout_seconds=4,
    )
    assert failed["attempts"] == 3
    assert failed["completed_at"] is not None
    assert failed["lease_expires_at"] is None

    time.sleep(0.4)
    still_failed = client.get(f"/jobs/{created['id']}").json()
    assert still_failed["status"] == "failed"
    assert still_failed["attempts"] == 3


def test_expired_job_at_attempt_limit_is_finalized_without_execution(
    client: TestClient,
) -> None:
    job_id = insert_running_job(
        {"type": "sleep", "duration_ms": 0},
        attempts=3,
        lease_delta=timedelta(seconds=-1),
    )

    failed = wait_for_job(
        client,
        job_id,
        lambda job: job["status"] == "failed",
    )
    assert failed["attempts"] == 3
    assert failed["completed_at"] is not None
    assert failed["lease_expires_at"] is None


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


@pytest.mark.skipif(
    os.getenv("RUN_MULTI_WORKER_TESTS") != "1",
    reason="requires at least two scaled Compose worker processes",
)
def test_multiple_workers_reclaim_one_expired_job_once(client: TestClient) -> None:
    job_id = insert_running_job(
        {"type": "sleep", "duration_ms": 500},
        lease_delta=timedelta(seconds=-1),
    )

    completed = wait_for_job(
        client,
        job_id,
        lambda job: job["status"] == "succeeded",
    )
    assert completed["attempts"] == 2
    assert completed["lease_expires_at"] is None


@pytest.mark.skipif(
    os.getenv("RUN_EFFECT_CRASH_TESTS") != "1",
    reason="requires two workers because the first process intentionally exits",
)
def test_post_effect_crash_reexecutes_but_does_not_duplicate_effect(
    client: TestClient,
) -> None:
    created = submit_job(
        client,
        {
            "type": "write_effect",
            "operation_id": "operation-crash-recovery",
            "value": "hello",
            "crash_after_effect_on_attempt": 1,
        },
    )

    completed = wait_for_job(
        client,
        created["id"],
        lambda job: job["status"] == "succeeded",
        timeout_seconds=8,
    )

    assert completed["attempts"] == 2
    assert completed["lease_expires_at"] is None
    with engine.connect() as connection:
        persisted_effects = connection.execute(
            sa.select(effects).where(
                effects.c.operation_id == "operation-crash-recovery"
            )
        ).mappings().all()
        executions = connection.execute(
            sa.select(effect_attempts)
            .where(effect_attempts.c.job_id == UUID(created["id"]))
            .order_by(effect_attempts.c.attempt)
        ).mappings().all()

    assert len(persisted_effects) == 1
    assert persisted_effects[0]["value"] == "hello"
    assert [execution["attempt"] for execution in executions] == [1, 2]
