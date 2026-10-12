"""Liveness and readiness endpoints.

- /healthz (liveness): the process is up. It never touches the database, so a
  database outage does not make an orchestrator restart a healthy API.
- /readyz (readiness): the process can do useful work, i.e. the database answers.
"""

from typing import Any

from fastapi import APIRouter, Request, Response, status

from queueforge.db.engine import database_is_reachable

router = APIRouter(tags=["health"])


@router.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/readyz")
def readyz(request: Request, response: Response) -> dict[str, Any]:
    if database_is_reachable(request.app.state.engine):
        return {"status": "ready"}
    response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "unavailable", "reason": "database unreachable"}
