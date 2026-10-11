import asyncio
import hmac
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from shiftledger.api.dependencies import ResourcesDep
from shiftledger.api.middleware import RequestContextMiddleware
from shiftledger.attachments.router import router as attachments_router
from shiftledger.bootstrap import Resources
from shiftledger.config import Environment, Settings, get_settings
from shiftledger.errors import ApplicationError
from shiftledger.identity.router import router as identity_router
from shiftledger.observability import configure_logging
from shiftledger.reconciliation.router import router as reconciliation_router
from shiftledger.reports.router import router as reports_router
from shiftledger.templates.router import router as templates_router

log = structlog.get_logger()


def _error(request: Request, code: str, message: str, status: int) -> JSONResponse:
    headers = {
        "X-Request-ID": getattr(request.state, "request_id", ""),
        "X-Content-Type-Options": "nosniff",
        "Cache-Control": "no-store",
    }
    if status == 401:
        headers["WWW-Authenticate"] = "Bearer"
    if status == 429:
        headers["Retry-After"] = "60"
    return JSONResponse(
        {
            "error": {
                "code": code,
                "message": message,
                "request_id": getattr(request.state, "request_id", None),
            }
        },
        status_code=status,
        headers=headers,
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    config = settings or get_settings()
    configure_logging(config.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        resources = Resources.create(config)
        app.state.resources = resources
        try:
            yield
        finally:
            await resources.close()

    public_docs = config.app_env != Environment.PRODUCTION
    app = FastAPI(
        title="ShiftLedger",
        version="0.1.0",
        lifespan=lifespan,
        description="Configurable shift reports with asynchronous deterministic reconciliation.",
        docs_url="/docs" if public_docs else None,
        redoc_url=None,
        openapi_url="/openapi.json" if public_docs else None,
    )
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=config.allowed_hosts)
    if config.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=config.cors_origins,
            allow_methods=["GET", "POST"],
            allow_headers=["Authorization", "Content-Type", "Idempotency-Key"],
        )
    app.add_middleware(RequestContextMiddleware, upload_max_bytes=config.upload_max_bytes)

    @app.exception_handler(ApplicationError)
    async def application_error(request: Request, exc: ApplicationError) -> JSONResponse:
        return _error(request, exc.code, exc.message, exc.status_code)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        # Omit input and ctx: credentials, employee notes and document data are private.
        details = [
            {"location": list(error["loc"]), "message": error["msg"], "type": error["type"]}
            for error in exc.errors()
        ]
        return JSONResponse(
            {
                "error": {
                    "code": "validation_error",
                    "message": "Request validation failed",
                    "details": details,
                    "request_id": getattr(request.state, "request_id", None),
                }
            },
            status_code=422,
        )

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException) -> JSONResponse:
        return _error(request, "http_error", str(exc.detail), exc.status_code)

    @app.exception_handler(SQLAlchemyError)
    async def database_error(request: Request, exc: SQLAlchemyError) -> JSONResponse:
        log.error("database_request_failed", error_type=type(exc).__name__)
        return _error(request, "dependency_unavailable", "Database temporarily unavailable", 503)

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        log.error(
            "unhandled_request_error",
            error_type=type(exc).__name__,
            request_id=getattr(request.state, "request_id", None),
        )
        return _error(request, "internal_error", "Unexpected server error", 500)

    @app.get("/health/live", tags=["operations"])
    async def live() -> dict[str, str]:
        return {"status": "ok", "service": "shiftledger"}

    @app.get("/health/ready", tags=["operations"])
    async def ready(resources: ResourcesDep) -> JSONResponse:
        async def database_check() -> None:
            async with resources.engine.connect() as connection:
                await connection.execute(text("SELECT 1"))

        checks = await asyncio.gather(
            database_check(),
            resources.redis.ping(),
            resources.storage.check(),
            return_exceptions=True,
        )
        statuses = {
            name: not isinstance(result, BaseException)
            for name, result in zip(["database", "redis", "storage"], checks, strict=True)
        }
        healthy = all(statuses.values())
        return JSONResponse(
            {
                "status": "ready" if healthy else "unavailable",
                "service": "shiftledger",
                "dependencies": statuses,
            },
            status_code=200 if healthy else 503,
        )

    @app.get("/metrics", include_in_schema=False)
    async def metrics(request: Request) -> Response:
        if config.metrics_token:
            expected = "Bearer " + config.metrics_token.get_secret_value()
            if not hmac.compare_digest(
                request.headers.get("Authorization", "").encode(), expected.encode()
            ):
                raise ApplicationError("unauthorized", "Metrics authentication required", 401)
        return Response(generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})

    for router in [
        identity_router,
        templates_router,
        reports_router,
        attachments_router,
        reconciliation_router,
    ]:
        app.include_router(router, prefix="/api/v1")
    return app
