import time
from uuid import uuid4

import structlog
from starlette.datastructures import Headers, MutableHeaders
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from shiftledger.observability import HTTP_DURATION, HTTP_REQUESTS

log = structlog.get_logger()


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp, upload_max_bytes: int) -> None:
        self.app = app
        self.upload_limit = upload_max_bytes + 1024 * 1024

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request_id = str(uuid4())
        scope.setdefault("state", {})["request_id"] = request_id
        context = structlog.contextvars.bind_contextvars(request_id=request_id)
        started = time.monotonic()
        status = 500
        limit = self.upload_limit if scope["path"].endswith("/attachments") else 512 * 1024
        received = 0

        async def bounded_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > limit:
                    raise HTTPException(413, "Request body exceeds the permitted limit")
            return message

        async def send_with_context(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                headers = MutableHeaders(scope=message)
                headers["X-Request-ID"] = request_id
                headers["X-Content-Type-Options"] = "nosniff"
                headers["Cache-Control"] = "no-store"
            await send(message)

        try:
            content_length = Headers(scope=scope).get("content-length")
            if content_length and (not content_length.isdigit() or int(content_length) > limit):
                response = JSONResponse(
                    {
                        "error": {
                            "code": "request_too_large",
                            "message": "Request body exceeds the permitted limit",
                            "request_id": request_id,
                        }
                    },
                    status_code=413,
                )
                await response(scope, receive, send_with_context)
            else:
                await self.app(scope, bounded_receive, send_with_context)
        finally:
            route = getattr(scope.get("route"), "path", "unmatched")
            duration = time.monotonic() - started
            HTTP_REQUESTS.labels(scope["method"], route, str(status)).inc()
            HTTP_DURATION.labels(scope["method"], route).observe(duration)
            if route not in {"/health/live", "/health/ready", "/metrics"}:
                log.info(
                    "http_request",
                    method=scope["method"],
                    route=route,
                    status=status,
                    duration_ms=round(duration * 1000, 2),
                )
            structlog.contextvars.reset_contextvars(**context)
