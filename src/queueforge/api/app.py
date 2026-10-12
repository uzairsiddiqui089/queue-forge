"""FastAPI application factory."""

from fastapi import FastAPI
from sqlalchemy import Engine
from sqlalchemy.orm import sessionmaker

from queueforge.api import health, jobs
from queueforge.config import get_settings
from queueforge.db.engine import make_engine


def create_app(engine: Engine | None = None) -> FastAPI:
    app = FastAPI(title="QueueForge", version="0.2.0")
    app.state.engine = engine if engine is not None else make_engine(get_settings().database_url)
    # expire_on_commit=False lets us read a job's fields after its commit
    # without triggering another query.
    app.state.session_factory = sessionmaker(app.state.engine, expire_on_commit=False)
    app.include_router(health.router)
    app.include_router(jobs.router)
    return app
