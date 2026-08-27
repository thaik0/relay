from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from app.models import JobStatus


class JobCreate(BaseModel):
    payload: dict[str, Any]
    idempotency_key: str | None = None


class Job(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    payload: dict[str, Any]
    status: JobStatus
    attempts: int
    available_at: datetime
    lease_expires_at: datetime | None
    idempotency_key: str | None
    created_at: datetime
    completed_at: datetime | None
