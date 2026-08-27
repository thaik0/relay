from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

import sqlalchemy as sa
from fastapi import FastAPI, HTTPException, status

from app.db import create_schema, engine
from app.jobs import router as jobs_router


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    create_schema()
    yield


app = FastAPI(title="Relay Job Queue API", version="0.1.0", lifespan=lifespan)
app.include_router(jobs_router)


@app.get("/health")
def health() -> dict[str, str]:
    try:
        with engine.connect() as connection:
            connection.execute(sa.text("SELECT 1"))
    except sa.exc.SQLAlchemyError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        ) from exc
    return {"status": "ok"}
