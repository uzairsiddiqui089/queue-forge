"""Allow-list of task types and the validated payload each one accepts.

Clients can only submit these four task types. Nothing a client sends is ever
executed as code or as a shell command; the payload is plain, bounded data.
"""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EchoPayload(_Strict):
    message: str = Field(max_length=1000)


class SleepPayload(_Strict):
    seconds: float = Field(ge=0, le=30)  # bounded so a job cannot hold a worker forever


class FlakyPayload(_Strict):
    fail_times: int = Field(ge=0, le=10)  # fails this many attempts, then succeeds


class NotificationPayload(_Strict):
    recipient: str = Field(min_length=1, max_length=200)
    message: str = Field(max_length=1000)


class EchoJob(_Strict):
    task_type: Literal["echo"]
    payload: EchoPayload


class SleepJob(_Strict):
    task_type: Literal["sleep"]
    payload: SleepPayload


class FlakyJob(_Strict):
    task_type: Literal["flaky"]
    payload: FlakyPayload


class NotificationJob(_Strict):
    task_type: Literal["simulated_notification"]
    payload: NotificationPayload


# FastAPI picks the right model from `task_type`; anything else is a 422.
JobCreate = Annotated[
    EchoJob | SleepJob | FlakyJob | NotificationJob,
    Field(discriminator="task_type"),
]
