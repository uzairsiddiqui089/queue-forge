from collections.abc import Iterator
from typing import Annotated

from fastapi import Depends, HTTPException, Request, Response, status
from fastapi.security import APIKeyHeader
from sqlalchemy.orm import Session, sessionmaker

from queueforge.db.models import ApiClient
from queueforge.services.auth import authenticate
from queueforge.services.ratelimit import RateLimiter

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_session(request: Request) -> Iterator[Session]:
    factory: sessionmaker[Session] = request.app.state.session_factory
    with factory() as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]


def get_current_client(
    request: Request,
    response: Response,
    session: SessionDep,
    api_key: Annotated[str | None, Depends(api_key_header)],
) -> ApiClient:
    client = authenticate(session, api_key) if api_key else None
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    request.state.client_id = str(client.id)  # for the request log

    limiter: RateLimiter | None = request.app.state.rate_limiter
    if limiter is not None:
        result = limiter.check(str(client.id))
        response.headers["X-RateLimit-Limit"] = str(result.limit)
        response.headers["X-RateLimit-Remaining"] = str(result.remaining)
        if not result.allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded",
                headers={
                    "Retry-After": str(result.retry_after_seconds),
                    "X-RateLimit-Limit": str(result.limit),
                    "X-RateLimit-Remaining": "0",
                },
            )
    return client


CurrentClient = Annotated[ApiClient, Depends(get_current_client)]


def get_admin(client: CurrentClient) -> ApiClient:
    if client.role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return client


AdminClient = Annotated[ApiClient, Depends(get_admin)]
