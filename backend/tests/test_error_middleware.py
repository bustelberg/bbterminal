"""Regression guard for `routers/_error_middleware.cors_safe_errors`.

Pins the one property that matters: an unhandled exception must become a RESPONSE, because
`CORSMiddleware` can only attach `Access-Control-Allow-Origin` to a response. If this layer ever
re-raises, a 500 goes out headerless and every browser reports it as a CORS failure on an
allow-listed origin — the misdiagnosis this module exists to prevent.

Driven directly like `test_auth_middleware.py`: a constructed Starlette Request and a `call_next`
we control, no app boot and no Supabase.
"""
from __future__ import annotations

import asyncio

import pytest
from fastapi.responses import JSONResponse
from starlette.requests import Request

from routers._error_middleware import cors_safe_errors


def _request(path: str = "/api/airs/portfolios/overview") -> Request:
    return Request({
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [],
        "query_string": b"",
        "scheme": "http",
        "server": ("testserver", 80),
    })


def _run(call_next):
    return asyncio.run(cors_safe_errors(_request(), call_next))


class TestPassthrough:
    def test_a_normal_response_is_returned_untouched(self):
        ok = JSONResponse({"ok": True})

        async def call_next(_req):
            return ok

        assert _run(call_next) is ok

    def test_a_handled_error_response_is_not_rewritten(self):
        """FastAPI's inner ExceptionMiddleware turns HTTPException into a response before this
        layer sees it. A 403 must stay a 403 — swallowing it into a generic 500 would erase the
        auth gate's answer."""
        async def call_next(_req):
            return JSONResponse({"detail": "Admin access required"}, status_code=403)

        resp = _run(call_next)
        assert resp.status_code == 403


class TestUnhandledExceptions:
    def test_an_unhandled_exception_becomes_a_500_response(self):
        """The whole point: a raised exception must not propagate past CORS."""
        async def call_next(_req):
            raise RuntimeError("boom")

        resp = _run(call_next)
        assert resp.status_code == 500

    def test_the_body_does_not_leak_the_exception(self):
        """The traceback goes to the log, never to the browser."""
        async def call_next(_req):
            raise RuntimeError("SUPABASE_SERVICE_KEY=sekrit")

        assert b"sekrit" not in _run(call_next).body

    def test_a_missing_table_error_is_caught_too(self):
        """The measured 2026-07-31 case — postgrest raises its own APIError class, not a
        RuntimeError, so the catch must be broad rather than a list of known types."""
        class APIError(Exception):
            pass

        async def call_next(_req):
            raise APIError("PGRST205: Could not find the table 'public.airs_account_display_name'")

        assert _run(call_next).status_code == 500


class TestCancellationIsNotSwallowed:
    def test_client_disconnect_still_propagates(self):
        """⚠ `CancelledError` is a BaseException and must NOT be converted into a 500. A cancelled
        request has no client left to answer, and catching it would keep dead requests alive."""
        async def call_next(_req):
            raise asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            _run(call_next)
