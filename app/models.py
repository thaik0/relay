import enum

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID


class JobStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


metadata = sa.MetaData()

jobs = sa.Table(
    "jobs",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("payload", JSONB, nullable=False),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("attempts", sa.Integer, nullable=False, server_default=sa.text("0")),
    sa.Column(
        "available_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("idempotency_key", sa.String, nullable=True),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint(
        "status IN ('queued', 'running', 'succeeded', 'failed')",
        name="ck_jobs_valid_status",
    ),
)

jobs_idempotency_key_index = sa.Index(
    "uq_jobs_idempotency_key_not_null",
    jobs.c.idempotency_key,
    unique=True,
    postgresql_where=jobs.c.idempotency_key.is_not(None),
)

effects = sa.Table(
    "effects",
    metadata,
    sa.Column("operation_id", sa.String, primary_key=True),
    sa.Column("value", sa.String, nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
)

# This deliberately non-idempotent audit makes repeated handler execution
# visible while `effects` represents the one logical, idempotent side effect.
effect_attempts = sa.Table(
    "effect_attempts",
    metadata,
    sa.Column("id", sa.BigInteger, sa.Identity(), primary_key=True),
    sa.Column("job_id", UUID(as_uuid=True), nullable=False),
    sa.Column("attempt", sa.Integer, nullable=False),
    sa.Column("operation_id", sa.String, nullable=False),
    sa.Column("value", sa.String, nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint("attempt > 0", name="ck_effect_attempts_positive_attempt"),
)
