import pytest
from fastapi.testclient import TestClient

from app.db import engine
from app.main import app
from app.models import metadata


@pytest.fixture(scope="session", autouse=True)
def schema() -> None:
    metadata.create_all(engine)


@pytest.fixture(autouse=True)
def clean_jobs(schema: None) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql("TRUNCATE TABLE jobs")


@pytest.fixture
def client() -> TestClient:
    with TestClient(app) as test_client:
        yield test_client
