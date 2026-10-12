"""FastAPI application factory."""

from fastapi import FastAPI
from sqlalchemy import Engine

from queueforge.api import health
from queueforge.config import get_settings
from queueforge.db.engine import make_engine


def create_app(engine: Engine | None = None) -> FastAPI:
    app = FastAPI(title="QueueForge", version="0.1.0")
    app.state.engine = engine if engine is not None else make_engine(get_settings().database_url)
    app.include_router(health.router)
    return app
