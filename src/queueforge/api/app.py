"""FastAPI application factory."""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import Engine
from sqlalchemy.orm import sessionmaker

from queueforge.api import admin, health, jobs, metrics
from queueforge.api.middleware import BodySizeLimitMiddleware, log_requests
from queueforge.config import get_settings
from queueforge.db.engine import make_engine
from queueforge.logging import configure_logging
from queueforge.services.ratelimit import RateLimiter, RedisRateLimiter

log = logging.getLogger("queueforge.api")

DEFAULT_MAX_BODY_BYTES = 65_536


def create_app(
    engine: Engine | None = None,
    rate_limiter: RateLimiter | None = None,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
) -> FastAPI:
    """Build the app. With no arguments it configures itself from the environment."""
    if engine is None:
        settings = get_settings()
        configure_logging(settings.log_level)
        engine = make_engine(settings.database_url)
        max_body_bytes = settings.max_request_body_bytes
        if rate_limiter is None and settings.redis_url:
            rate_limiter = RedisRateLimiter.from_url(
                settings.redis_url, settings.rate_limit_per_minute
            )

    app = FastAPI(title="QueueForge", version="0.5.0")
    app.state.engine = engine
    # expire_on_commit=False lets us read a job's fields after its commit
    # without triggering another query.
    app.state.session_factory = sessionmaker(engine, expire_on_commit=False)
    app.state.rate_limiter = rate_limiter
    app.state.metrics_registry = metrics.make_registry(engine)

    # The last middleware added is the outermost. The size limit is inside the request
    # logger, so rejected (413) requests are logged too.
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=max_body_bytes)
    app.middleware("http")(log_requests)

    @app.exception_handler(Exception)
    async def unhandled_error(request: Request, exc: Exception) -> JSONResponse:
        # The full trace goes to the log. The client only ever sees a generic message.
        log.error("unhandled error", exc_info=exc, extra={"path": request.url.path})
        return JSONResponse({"detail": "Internal server error"}, status_code=500)

    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(admin.router)
    app.include_router(metrics.router)
    return app
