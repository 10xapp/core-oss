"""
FastAPI application for Vercel
Vercel auto-detects and deploys FastAPI apps at index.py
NO vercel.json or Mangum needed!
"""
import sys
import os

# Add project root to path so we can import from api/ and lib/
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import sentry_sdk
import time
import logging
import traceback
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from datetime import datetime
from api.config import settings
from api.schemas import HealthResponse, StatusResponse
from lib.supabase_client import start_supabase_request_scope, reset_supabase_request_scope

logger = logging.getLogger(__name__)


def _sentry_filter_noise(event, hint):
    """Drop expected HTTP errors (4xx) from Sentry to reduce noise."""
    exc = hint.get("exc_info", (None, None, None))[1]
    if isinstance(exc, HTTPException) and exc.status_code < 500:
        return None
    return event


sentry_sdk.init(
    dsn=settings.sentry_dsn,
    environment=settings.api_env,
    traces_sample_rate=0.05,
    send_default_pii=True,
    before_send=_sentry_filter_noise,
)

from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from api.rate_limit import limiter

from api.routers import auth, calendar, email, webhooks, cron, sync, documents, files, chat, chat_attachments, app_drawer, preferences, workspaces, invitations, messages, users, projects, notifications, init, agents, agent_dispatch, agent_memory, permissions, public, workers, builder

# Create FastAPI app - Vercel will auto-detect this
app = FastAPI(
    title=settings.app_name,
    description="FastAPI backend for the all-in-one productivity app",
    version=settings.app_version,
    debug=settings.debug
)

# Rate limiting — middleware runs BEFORE FastAPI validation/dependencies
# Without this, invalid requests (422) bypass the decorator-based limiter
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# Global exception handler for unhandled exceptions
# This ensures CORS headers are included even on 500 errors
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Catch-all exception handler that returns proper JSON error responses.

    This is critical for CORS: when an unhandled exception occurs, FastAPI's
    default error handler may not include CORS headers if the exception happens
    before response headers are sent. By catching all exceptions here and
    returning a proper JSONResponse, we ensure the CORS middleware can add
    its headers to the response.
    """
    # Log the full traceback for debugging
    logger.error(
        f"Unhandled exception on {request.method} {request.url.path}: {exc}\n"
        f"{''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}"
    )

    # Report to Sentry
    sentry_sdk.capture_exception(exc)

    # Return a proper JSON response (CORS middleware will add headers)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "error_type": type(exc).__name__,
        }
    )


# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.get_allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


# Security headers middleware
@app.middleware("http")
async def supabase_request_scope_middleware(request: Request, call_next):
    scope_token = start_supabase_request_scope()
    try:
        return await call_next(request)
    finally:
        reset_supabase_request_scope(scope_token)


@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    try:
        response = await call_next(request)
    except Exception as exc:
        # Let the global exception handler deal with it, but ensure we don't swallow errors
        raise
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    return response


# Request timing middleware for performance monitoring
@app.middleware("http")
async def timing_middleware(request: Request, call_next):
    start_time = time.perf_counter()

    try:
        response = await call_next(request)
    except Exception as exc:
        # Log timing even for failed requests
        process_time_ms = (time.perf_counter() - start_time) * 1000
        logger.error(
            f"[PERF] {request.method} {request.url.path} - {process_time_ms:.2f}ms - EXCEPTION: {type(exc).__name__}"
        )
        raise

    process_time_ms = (time.perf_counter() - start_time) * 1000

    # Add timing to response headers
    response.headers["X-Process-Time-Ms"] = f"{process_time_ms:.2f}"

    # Log the request timing
    logger.info(
        f"[PERF] {request.method} {request.url.path} - {process_time_ms:.2f}ms - Status: {response.status_code}"
    )

    return response


# Include routers
app.include_router(auth.router)
app.include_router(workspaces.router)
app.include_router(invitations.router)
app.include_router(calendar.router)
app.include_router(email.router)
app.include_router(documents.router)
app.include_router(files.router)
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
app.include_router(agents.router)
app.include_router(agent_dispatch.router)
app.include_router(agent_memory.router)
app.include_router(init.router)
app.include_router(public.router)
app.include_router(workers.router)
app.include_router(builder.router)

@app.get("/", response_model=HealthResponse)
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "message": "Core Productivity API is running",
        "version": settings.app_version
    }

@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """Detailed health check"""
    return {
        "status": "healthy",
        "service": "core-api",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
