"""Admin endpoints (admin API key only): inspect and re-queue dead-lettered jobs."""

import uuid
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict

from queueforge.api.deps import AdminClient, SessionDep
from queueforge.api.jobs import JobOut
from queueforge.services import admin as admin_service

router = APIRouter(prefix="/v1/admin", tags=["admin"])


class AdminJobOut(JobOut):
    client_id: uuid.UUID


class AttemptOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt_number: int
    worker_id: str
    started_at: datetime
    finished_at: datetime | None
    duration_ms: int | None
    outcome: str | None
    error_type: str | None
    error_message: str | None


class AdminJobDetail(AdminJobOut):
    attempt_history: list[AttemptOut]


@router.get("/dead-letter", response_model=list[AdminJobOut])
def list_dead_letter(
    _admin: AdminClient,
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[AdminJobOut]:
    jobs = admin_service.list_dead_lettered(session, limit, offset)
    return [AdminJobOut.model_validate(j) for j in jobs]


@router.get("/jobs/{job_id}", response_model=AdminJobDetail)
def read_job_with_history(
    job_id: uuid.UUID, _admin: AdminClient, session: SessionDep
) -> AdminJobDetail:
    job = admin_service.get_any_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    attempts = admin_service.list_attempts(session, job_id)
    return AdminJobDetail(
        **AdminJobOut.model_validate(job).model_dump(),
        attempt_history=[AttemptOut.model_validate(a) for a in attempts],
    )


@router.post("/jobs/{job_id}/requeue", response_model=AdminJobOut)
def requeue_job(job_id: uuid.UUID, _admin: AdminClient, session: SessionDep) -> AdminJobOut:
    result = admin_service.requeue_dead_lettered(session, job_id)
    if result == admin_service.RequeueResult.NOT_FOUND:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    if result == admin_service.RequeueResult.NOT_DEAD_LETTERED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="Only dead-lettered jobs can be re-queued"
        )
    job = admin_service.get_any_job(session, job_id)
    assert job is not None
    return AdminJobOut.model_validate(job)
