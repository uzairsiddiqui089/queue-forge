"""Request size limit and request logging."""

import json
import logging
import time
from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any

from fastapi import HTTPException, Request, Response

log = logging.getLogger("queueforge.api")

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]


class BodySizeLimitMiddleware:
    """Reject request bodies over `max_bytes` with 413.

    Checks the Content-Length header first (cheap, rejects before reading anything),
    and also counts the bytes actually received, because a client can lie about
    the length or send a chunked body that has no length at all.
    """

    def __init__(self, app: Callable[[Scope, Receive, Send], Awaitable[None]], max_bytes: int):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        declared = dict(scope["headers"]).get(b"content-length")
        if declared is not None and declared.isdigit() and int(declared) > self.max_bytes:
            await self._reject(send)
            return

        received = 0

        async def limited_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    # A real HTTPException: FastAPI lets these through (it would turn any
                    # other error while reading the body into a 400) and answers 413.
                    raise HTTPException(status_code=413, detail="Request body too large")
            return message

        await self.app(scope, limited_receive, send)

    async def _reject(self, send: Send) -> None:
        body = json.dumps({"detail": "Request body too large"}).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


async def log_requests(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """One log line per request. Logs the path only: never the query string, headers or body."""
    started = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        log.info(
            "request",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": status_code,
                "duration_ms": round((time.perf_counter() - started) * 1000, 1),
                "client_id": getattr(request.state, "client_id", None),
            },
        )
