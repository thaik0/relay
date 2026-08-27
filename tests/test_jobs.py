from uuid import uuid4

from fastapi.testclient import TestClient


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


def test_get_nonexistent_job_returns_404(client: TestClient) -> None:
    response = client.get(f"/jobs/{uuid4()}")

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}


def test_malformed_job_id_returns_validation_error(client: TestClient) -> None:
    response = client.get("/jobs/not-a-uuid")

    assert response.status_code == 422
