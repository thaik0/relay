import os
from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from app.models import jobs_idempotency_key_index, metadata


def database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable is required")
    return url


engine: Engine = create_engine(database_url(), pool_pre_ping=True)


def create_schema() -> None:
    # create_all opens a connection and commits the DDL in one transaction.
    metadata.create_all(engine)
    # create_all does not add a newly declared index when its table already
    # exists, so create the Milestone 5 index explicitly for existing installs.
    jobs_idempotency_key_index.create(engine, checkfirst=True)


def get_connection() -> Iterator[Connection]:
    # FastAPI closes the connection after the request finishes.
    with engine.connect() as connection:
        yield connection
