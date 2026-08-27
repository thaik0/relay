from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID, uuid4

import sqlalchemy as sa
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.engine import Connection

from app.db import get_connection
from app.models import JobStatus, jobs
from app.schemas import Job, JobCreate


router = APIRouter(prefix="/jobs", tags=["jobs"])
ConnectionDependency = Annotated[Connection, Depends(get_connection)]


@router.post("", response_model=Job, status_code=status.HTTP_201_CREATED)
def create_job(job: JobCreate, connection: ConnectionDependency) -> Job:
    now = datetime.now(timezone.utc)
    statement = (
        sa.insert(jobs)
        .values(
            id=uuid4(),
            payload=job.payload,
            status=JobStatus.QUEUED.value,
            attempts=0,
            available_at=now,
            lease_expires_at=None,
            idempotency_key=job.idempotency_key,
            created_at=now,
            completed_at=None,
        )
        .returning(jobs)
    )

    # begin() makes the insert atomic and commits before the response is returned.
    with connection.begin():
        created = connection.execute(statement).mappings().one()

    return Job.model_validate(created)


@router.get("/{job_id}", response_model=Job)
def get_job(job_id: UUID, connection: ConnectionDependency) -> Job:
    statement = sa.select(jobs).where(jobs.c.id == job_id)
    persisted = connection.execute(statement).mappings().one_or_none()

    if persisted is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    return Job.model_validate(persisted)
