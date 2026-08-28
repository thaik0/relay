import logging
from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID, uuid4

import sqlalchemy as sa
from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection

from app.db import get_connection
from app.models import JobStatus, jobs
from app.schemas import Job, JobCreate


router = APIRouter(prefix="/jobs", tags=["jobs"])
ConnectionDependency = Annotated[Connection, Depends(get_connection)]
logger = logging.getLogger(__name__)


@router.post("", response_model=Job, status_code=status.HTTP_201_CREATED)
def create_job(
    job: JobCreate,
    connection: ConnectionDependency,
    response: Response,
) -> Job:
    now = datetime.now(timezone.utc)
    statement = insert(jobs).values(
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
    if job.idempotency_key is not None:
        statement = statement.on_conflict_do_nothing(
            index_elements=[jobs.c.idempotency_key],
            index_where=jobs.c.idempotency_key.is_not(None),
        )
    statement = statement.returning(jobs)

    # A conflicting INSERT waits for the concurrent transaction that owns the
    # key. The following SELECT is a new READ COMMITTED snapshot, so it sees the
    # winner without application locks or blind retries.
    with connection.begin():
        created = connection.execute(statement).mappings().one_or_none()
        if created is not None:
            return Job.model_validate(created)

        persisted = (
            connection.execute(
                sa.select(jobs).where(
                    jobs.c.idempotency_key == job.idempotency_key
                )
            )
            .mappings()
            .one()
        )
        if persisted["payload"] != job.payload:
            logger.info("event=idempotency_conflict key=%s", job.idempotency_key)
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency key is already associated with a different payload",
            )

        response.status_code = status.HTTP_200_OK
        logger.info(
            "event=duplicate_submission key=%s job=%s",
            job.idempotency_key,
            persisted["id"],
        )
        return Job.model_validate(persisted)


@router.get("/{job_id}", response_model=Job)
def get_job(job_id: UUID, connection: ConnectionDependency) -> Job:
    statement = sa.select(jobs).where(jobs.c.id == job_id)
    persisted = connection.execute(statement).mappings().one_or_none()

    if persisted is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    return Job.model_validate(persisted)
