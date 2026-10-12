from collections.abc import Iterator
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader
from sqlalchemy.orm import Session, sessionmaker

from queueforge.db.models import ApiClient
from queueforge.services.auth import authenticate

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_session(request: Request) -> Iterator[Session]:
    factory: sessionmaker[Session] = request.app.state.session_factory
    with factory() as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]


def get_current_client(
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
    return client


CurrentClient = Annotated[ApiClient, Depends(get_current_client)]
