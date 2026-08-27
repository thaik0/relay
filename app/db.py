import os
from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from app.models import metadata


def database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable is required")
    return url


engine: Engine = create_engine(database_url(), pool_pre_ping=True)


def create_schema() -> None:
    # create_all opens a connection and commits the DDL in one transaction.
    metadata.create_all(engine)


def get_connection() -> Iterator[Connection]:
    # FastAPI closes the connection after the request finishes.
    with engine.connect() as connection:
        yield connection
