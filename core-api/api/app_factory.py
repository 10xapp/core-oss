"""Shared FastAPI app factories for the main API and webhook ingress."""

from __future__ import annotations

import logging
import time
import traceback
from datetime import datetime

import sentry_sdk
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from api.config import settings
from api.rate_limit import limiter
from api.schemas import HealthResponse
from lib.supabase_client import start_supabase_request_scope, reset_supabase_request_scope

logger = logging.getLogger(__name__)
_SENTRY_INITIALIZED = False


def _sentry_filter_noise(event, hint):
    """Drop expected HTTP errors (4xx) from Sentry to reduce noise."""
    exc = hint.get("exc_info", (None, None, None))[1]
    if isinstance(exc, HTTPException) and exc.status_code < 500:
        return None
    return event


def _ensure_sentry_initialized() -> None:
    global _SENTRY_INITIALIZED
    if _SENTRY_INITIALIZED:
        return

    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        environment=settings.api_env,
        traces_sample_rate=0.05,
        send_default_pii=True,
        before_send=_sentry_filter_noise,
    )
    _SENTRY_INITIALIZED = True


def _install_exception_handler(app: FastAPI) -> None:
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error(
            f"Unhandled exception on {request.method} {request.url.path}: {exc}\n"
            f"{''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}"
        )
        sentry_sdk.capture_exception(exc)
        return JSONResponse(
            status_code=500,
            content={
                "detail": "Internal server error",
                "error_type": type(exc).__name__,
            },
        )

    app.add_exception_handler(Exception, global_exception_handler)


def _install_middlewares(
    app: FastAPI,
    *,
    include_cors: bool,
    include_rate_limit: bool,
) -> None:
    if include_rate_limit:
        app.state.limiter = limiter
        app.add_middleware(SlowAPIMiddleware)
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

    if include_cors:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.get_allowed_origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            allow_headers=["*"],
        )

    @app.middleware("http")
    async def supabase_request_scope_middleware(request: Request, call_next):
        scope_token = start_supabase_request_scope()
        try:
            return await call_next(request)
        finally:
            reset_supabase_request_scope(scope_token)

    @app.middleware("http")
    async def security_headers_middleware(request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        return response

    @app.middleware("http")
    async def timing_middleware(request: Request, call_next):
        start_time = time.perf_counter()

        try:
            response = await call_next(request)
        except Exception as exc:
            process_time_ms = (time.perf_counter() - start_time) * 1000
            logger.error(
                f"[PERF] {request.method} {request.url.path} - {process_time_ms:.2f}ms - "
                f"EXCEPTION: {type(exc).__name__}"
            )
            raise

        process_time_ms = (time.perf_counter() - start_time) * 1000
        response.headers["X-Process-Time-Ms"] = f"{process_time_ms:.2f}"
        logger.info(
            f"[PERF] {request.method} {request.url.path} - {process_time_ms:.2f}ms - "
            f"Status: {response.status_code}"
        )
        return response


def _install_health_routes(
    app: FastAPI,
    *,
    service_name: str,
    root_message: str,
) -> None:
    @app.get("/", response_model=HealthResponse)
    async def root():
        return {
            "status": "healthy",
            "service": service_name,
            "message": root_message,
            "version": settings.app_version,
        }

    @app.get("/api/health", response_model=HealthResponse)
    async def health_check():
        return {
            "status": "healthy",
            "service": service_name,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }


def _create_base_app(
    *,
    service_name: str,
    description: str,
    root_message: str,
    include_cors: bool,
    include_rate_limit: bool,
) -> FastAPI:
    _ensure_sentry_initialized()

    app = FastAPI(
        title=settings.app_name,
        description=description,
        version=settings.app_version,
        debug=settings.debug,
    )

    _install_exception_handler(app)
    _install_middlewares(
        app,
        include_cors=include_cors,
        include_rate_limit=include_rate_limit,
    )
    _install_health_routes(
        app,
        service_name=service_name,
        root_message=root_message,
    )
    return app


def create_full_app() -> FastAPI:
    """Build the full application used by the main API deployment."""
    from api.routers import (
        app_drawer,
        auth,
        builder,
        calendar,
        chat,
        chat_attachments,
        cron,
        documents,
        email,
        files,
        init,
        invitations,
        messages,
        notifications,
        permissions,
        preferences,
        projects,
        public,
        sync,
        users,
        webhooks,
        workers,
        workspaces,
    )

    app = _create_base_app(
        service_name="core-api",
        description="FastAPI backend for the all-in-one productivity app",
        root_message="Core Productivity API is running",
        include_cors=True,
        include_rate_limit=True,
    )

    app.include_router(auth.router)
    app.include_router(workspaces.router)
    app.include_router(invitations.router)
    app.include_router(calendar.router)
    app.include_router(email.router)
    app.include_router(documents.router)
    app.include_router(files.router)
    if settings.enable_webhook_routes:
        app.include_router(webhooks.router)
    app.include_router(cron.router)
    app.include_router(sync.router)
    app.include_router(chat.router)
    app.include_router(chat_attachments.router)
    app.include_router(app_drawer.router)
    app.include_router(preferences.router)
    app.include_router(messages.router)
    app.include_router(users.router)
    app.include_router(projects.router)
    app.include_router(notifications.router)
    app.include_router(permissions.router)
    app.include_router(init.router)
    app.include_router(public.router)
    app.include_router(workers.router)
    app.include_router(builder.router)
    return app


def create_webhooks_app() -> FastAPI:
    """Build the minimal public webhook ingress service."""
    from api.routers import webhooks

    app = _create_base_app(
        service_name="core-webhooks",
        description="Dedicated public webhook ingress for provider notifications",
        root_message="Core webhook ingress is running",
        include_cors=False,
        include_rate_limit=False,
    )
    app.include_router(webhooks.router)
    return app
