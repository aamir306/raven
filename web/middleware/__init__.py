"""
RAVEN Web — Middleware
=======================
Authentication and rate limiting middleware.
"""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict

from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)


class BasicAuthMiddleware(BaseHTTPMiddleware):
    """
    Optional basic API key authentication.
    
    Enable by setting RAVEN_API_KEY environment variable.
    Public endpoints (health, docs) are always open.
    """

    PUBLIC_PATHS = {"/health", "/docs", "/redoc", "/openapi.json"}

    async def dispatch(self, request: Request, call_next):
        api_key = os.getenv("RAVEN_API_KEY")

        # If no key configured, auth is disabled
        if not api_key:
            return await call_next(request)

        # Public endpoints skip auth
        if request.url.path in self.PUBLIC_PATHS:
            return await call_next(request)

        # Check Authorization header
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]
            if token == api_key:
                return await call_next(request)

        # Check X-API-Key header
        header_key = request.headers.get("X-API-Key", "")
        if header_key == api_key:
            return await call_next(request)

        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid or missing API key"},
        )


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Simple in-memory rate limiter.
    
    Configure via environment:
        RAVEN_RATE_LIMIT_RPM: requests per minute per IP (default: 60)
    """

    def __init__(self, app, rpm: int | None = None):
        super().__init__(app)
        self.rpm = rpm or int(os.getenv("RAVEN_RATE_LIMIT_RPM", "60"))
        self._buckets: dict[str, list[float]] = defaultdict(list)

    async def dispatch(self, request: Request, call_next):
        # Skip rate limiting for health checks
        if request.url.path == "/health":
            return await call_next(request)

        client_ip = request.client.host if request.client else "unknown"
        now = time.time()
        window = 60.0  # 1 minute

        # Clean old entries
        bucket = self._buckets[client_ip]
        self._buckets[client_ip] = [t for t in bucket if now - t < window]

        if len(self._buckets[client_ip]) >= self.rpm:
            retry_after = int(window - (now - self._buckets[client_ip][0]))
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(max(1, retry_after))},
            )

        self._buckets[client_ip].append(now)
        response = await call_next(request)

        # Add rate limit headers
        remaining = self.rpm - len(self._buckets[client_ip])
        response.headers["X-RateLimit-Limit"] = str(self.rpm)
        response.headers["X-RateLimit-Remaining"] = str(remaining)

        return response


class RequestTimingMiddleware(BaseHTTPMiddleware):
    """Add X-Response-Time header to all responses."""

    async def dispatch(self, request: Request, call_next):
        start = time.monotonic()
        response = await call_next(request)
        elapsed_ms = (time.monotonic() - start) * 1000
        response.headers["X-Response-Time"] = f"{elapsed_ms:.0f}ms"
        return response
