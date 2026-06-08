"""Regression guard for the `/api/*` auth gate (`routers/_auth_middleware.py`).

Pins the security properties the audit cared about so they can't silently
regress:
  * M1 — EVERY non-public /api/* request needs a valid token (reads too).
  * H1 — earnings-refresh is no longer unauthenticated; it requires a
    logged-in user (and is intentionally allowed for non-admins).
  * Role tiers — public (health/cron), self-auth (/api/auth), user reads,
    user writes, and "everything else is admin-only".

We drive the real `enforce_api_auth` middleware with a stubbed
`verify_token` (injected via sys.modules so no Supabase client is needed)
and a constructed Starlette Request, asserting both the HTTP status and
whether the request was allowed through to the handler.
"""
from __future__ import annotations

import asyncio
import sys
import types

from fastapi.responses import JSONResponse
from starlette.requests import Request

from routers import _auth_middleware as mw


def _request(method: str, path: str, auth: str | None = "Bearer t") -> Request:
    headers = [(b"authorization", auth.encode())] if auth is not None else []
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": headers,
        "query_string": b"",
        "scheme": "http",
        "server": ("testserver", 80),
    }
    return Request(scope)


def _run(monkeypatch, method: str, path: str, role: str | None, auth: str | None = "Bearer t"):
    """Returns (status_code, reached_handler). `role=None` simulates an
    invalid/absent token (verify_token returns None)."""
    fake_auth = types.SimpleNamespace(
        verify_token=lambda _authz: (None if role is None else {"role": role})
    )
    monkeypatch.setitem(sys.modules, "routers.auth", fake_auth)

    reached = {"v": False}

    async def call_next(_req):
        reached["v"] = True
        return JSONResponse({"ok": True})

    resp = asyncio.run(mw.enforce_api_auth(_request(method, path, auth), call_next))
    return resp.status_code, reached["v"]


class TestPublicAndPassthrough:
    def test_options_preflight_passes(self, monkeypatch):
        assert _run(monkeypatch, "OPTIONS", "/api/companies", None) == (200, True)

    def test_non_api_path_passes(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/openapi.json", None) == (200, True)

    def test_health_is_public(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/health", None) == (200, True)

    def test_both_cron_endpoints_are_public(self, monkeypatch):
        # Cron endpoints self-gate on X-Cron-Secret, so the JWT gate lets
        # them through.
        assert _run(monkeypatch, "POST", "/api/ingest/scheduled-refresh/cron", None) == (200, True)
        assert _run(monkeypatch, "POST", "/api/momentum/current-picks/cron", None) == (200, True)

    def test_auth_router_is_self_gated(self, monkeypatch):
        assert _run(monkeypatch, "POST", "/api/auth/login", None) == (200, True)


class TestM1_ReadsRequireAuth:
    def test_unauthenticated_read_is_401(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/companies", None) == (401, False)

    def test_unauthenticated_arbitrary_read_is_401(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/momentum/saved", None) == (401, False)

    def test_authenticated_user_read_allowed(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/companies", "user") == (200, True)

    def test_non_admin_read_of_admin_path_is_403(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/admin/health", "user") == (403, False)


class TestH1_EarningsRefreshNeedsAuth:
    def test_unauthenticated_refresh_is_401(self, monkeypatch):
        # The whole point of the fix: no longer reachable without a token.
        assert _run(monkeypatch, "POST", "/api/earnings/1/refresh/financials", None) == (401, False)
        assert _run(monkeypatch, "POST", "/api/earnings/1/refresh-all", None) == (401, False)

    def test_authenticated_user_may_refresh(self, monkeypatch):
        # Intentionally allowed for non-admins (it's a user-facing page).
        assert _run(monkeypatch, "POST", "/api/earnings/1/refresh/financials", "user") == (200, True)


class TestWriteTiers:
    def test_non_admin_write_to_protected_path_is_403(self, monkeypatch):
        # /api/companies is a user READ surface but not a user WRITE one.
        assert _run(monkeypatch, "POST", "/api/companies", "user") == (403, False)

    def test_portfolio_parse_allowed_for_user(self, monkeypatch):
        assert _run(monkeypatch, "POST", "/api/portfolios/parse", "user") == (200, True)

    def test_admin_may_write_anything(self, monkeypatch):
        assert _run(monkeypatch, "POST", "/api/momentum/backtest", "admin") == (200, True)

    def test_admin_may_read_anything(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/admin/health", "admin") == (200, True)
