from datetime import datetime, timedelta, timezone
from uuid import uuid4

import sqlalchemy as sa

from app.db import engine
from app.models import JobStatus, jobs


CLAIMABLE_JOB = sa.text(
    """
    SELECT id
    FROM jobs
    WHERE status = 'queued'
      AND available_at <= CURRENT_TIMESTAMP
    ORDER BY available_at, created_at, id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
    """
)


def test_claim_query_skips_a_row_locked_by_another_connection() -> None:
    """Exercise PostgreSQL row locking with two real, concurrent connections."""
    now = datetime.now(timezone.utc)
    first_id = uuid4()
    second_id = uuid4()
    values = [
        {
            "id": first_id,
            "payload": {"type": "sleep", "duration_ms": 0},
            "status": JobStatus.QUEUED.value,
            "available_at": now - timedelta(seconds=2),
            "created_at": now - timedelta(seconds=2),
        },
        {
            "id": second_id,
            "payload": {"type": "sleep", "duration_ms": 0},
            "status": JobStatus.QUEUED.value,
            "available_at": now - timedelta(seconds=1),
            "created_at": now - timedelta(seconds=1),
        },
    ]
    with engine.begin() as connection:
        connection.execute(sa.insert(jobs), values)

    with engine.connect() as first_connection, engine.connect() as second_connection:
        with first_connection.begin():
            locked_id = first_connection.execute(CLAIMABLE_JOB).scalar_one()
            assert locked_id == first_id

            # The first transaction remains open and holds its row lock while
            # the second connection selects the next eligible job.
            with second_connection.begin():
                skipped_to_id = second_connection.execute(CLAIMABLE_JOB).scalar_one()

            assert skipped_to_id == second_id

