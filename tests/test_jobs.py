from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import sqlalchemy as sa
from fastapi.testclient import TestClient

from app.db import engine
from app.models import jobs


def test_submit_job_creates_persisted_queued_job(client: TestClient) -> None:
    response = client.post(
        "/jobs",
        json={
            "payload": {"type": "sleep", "duration_ms": 500},
            "idempotency_key": "request-123",
        },
    )

    assert response.status_code == 201
    created = response.json()
    assert created["status"] == "queued"
    assert created["attempts"] == 0
    assert created["idempotency_key"] == "request-123"
    assert created["lease_expires_at"] is None
    assert created["completed_at"] is None
    assert created["available_at"].endswith("Z")
    assert created["created_at"].endswith("Z")

    retrieved = client.get(f"/jobs/{created['id']}")
    assert retrieved.status_code == 200
    assert retrieved.json() == created


def test_payload_json_round_trips(client: TestClient) -> None:
    payload = {
        "type": "transform",
        "inputs": [1, 2.5, None, True, {"nested": "value"}],
    }

    created = client.post("/jobs", json={"payload": payload}).json()
    retrieved = client.get(f"/jobs/{created['id']}")

    assert retrieved.status_code == 200
    assert retrieved.json()["payload"] == payload


def test_matching_idempotent_submission_returns_original_job(
    client: TestClient,
) -> None:
    request = {
        "payload": {"type": "sleep", "duration_ms": 100},
        "idempotency_key": "request-repeat",
    }

    created = client.post("/jobs", json=request)
    replayed = client.post("/jobs", json=request)

    assert created.status_code == 201
    assert replayed.status_code == 200
    assert replayed.json() == created.json()
    with engine.connect() as connection:
        count = connection.execute(
            sa.select(sa.func.count()).select_from(jobs)
        ).scalar_one()
    assert count == 1


def test_concurrent_matching_submissions_return_one_job(client: TestClient) -> None:
    request = {
        "payload": {"type": "sleep", "duration_ms": 100},
        "idempotency_key": "request-concurrent",
    }

    with ThreadPoolExecutor(max_workers=8) as executor:
        responses = list(
            executor.map(lambda _: client.post("/jobs", json=request), range(8))
        )

    assert sorted(response.status_code for response in responses) == [200] * 7 + [201]
    assert len({response.json()["id"] for response in responses}) == 1
    with engine.connect() as connection:
        count = connection.execute(
            sa.select(sa.func.count()).select_from(jobs)
        ).scalar_one()
    assert count == 1


def test_idempotency_key_reuse_with_different_payload_conflicts(
    client: TestClient,
) -> None:
    created = client.post(
        "/jobs",
        json={
            "payload": {"type": "sleep", "duration_ms": 100},
            "idempotency_key": "request-conflict",
        },
    )
    conflict = client.post(
        "/jobs",
        json={
            "payload": {"type": "sleep", "duration_ms": 200},
            "idempotency_key": "request-conflict",
        },
    )

    assert created.status_code == 201
    assert conflict.status_code == 409
    assert conflict.json() == {
        "detail": "Idempotency key is already associated with a different payload"
    }
    with engine.connect() as connection:
        persisted = connection.execute(sa.select(jobs)).mappings().one()
    assert str(persisted["id"]) == created.json()["id"]
    assert persisted["payload"] == {"type": "sleep", "duration_ms": 100}


def test_get_nonexistent_job_returns_404(client: TestClient) -> None:
    response = client.get(f"/jobs/{uuid4()}")

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}


def test_malformed_job_id_returns_validation_error(client: TestClient) -> None:
    response = client.get("/jobs/not-a-uuid")

    assert response.status_code == 422
