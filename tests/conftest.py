import pytest
from fastapi.testclient import TestClient

from app.db import create_schema, engine
from app.main import app


@pytest.fixture(scope="session", autouse=True)
def schema() -> None:
    create_schema()


@pytest.fixture(autouse=True)
def clean_jobs(schema: None) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql("TRUNCATE TABLE jobs")


@pytest.fixture
def client() -> TestClient:
    with TestClient(app) as test_client:
        yield test_client
