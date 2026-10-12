"""Job endpoints: submit a job and look it up."""

import uuid
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Request, Response, status
from pydantic import BaseModel, ConfigDict

from queueforge.api.deps import CurrentClient, SessionDep
from queueforge.services import jobs as job_service
from queueforge.tasks.schemas import JobCreate

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    task_type: str
    status: str
    payload: dict[str, Any]
    attempts: int
    max_attempts: int
    result: dict[str, Any] | None
    last_error: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None


@router.post(
    "",
    response_model=JobOut,
    status_code=status.HTTP_201_CREATED,
    responses={200: {"description": "Idempotent replay: the original job is returned"}},
)
def submit_job(
    body: JobCreate,
    request: Request,
    response: Response,
    client: CurrentClient,
    session: SessionDep,
    idempotency_key: Annotated[str | None, Header(min_length=1, max_length=255)] = None,
) -> JobOut:
    try:
        job, created = job_service.submit_job(
            session,
            client.id,
            body.task_type,
            body.payload.model_dump(mode="json"),
            idempotency_key,
            request.app.state.queue_limits,
        )
    except job_service.QueueFull as full:
        # Backpressure. A client over its own cap gets 429; a full queue overall gets 503.
        raise HTTPException(
            status_code=(
                status.HTTP_429_TOO_MANY_REQUESTS
                if full.scope == "client"
                else status.HTTP_503_SERVICE_UNAVAILABLE
            ),
            detail="Too many queued jobs" if full.scope == "client" else "Queue is full",
            headers={"Retry-After": str(full.retry_after_seconds)},
        ) from None
    except job_service.IdempotencyConflict:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request",
        ) from None
    if not created:
        response.status_code = status.HTTP_200_OK
    return JobOut.model_validate(job)


@router.get("/{job_id}", response_model=JobOut)
def read_job(job_id: uuid.UUID, client: CurrentClient, session: SessionDep) -> JobOut:
    job = job_service.get_job(session, client.id, job_id)
    if job is None:
        # Same answer for "does not exist" and "belongs to someone else".
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return JobOut.model_validate(job)
