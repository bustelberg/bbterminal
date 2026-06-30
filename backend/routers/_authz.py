"""Shared request-level authorization helpers.

`is_admin_request` resolves whether the verified caller should be treated as
an admin for THIS request. The auth-gate middleware stamps the verified
identity on `request.state.auth`; this also honors the admin "view as regular
user" preview — when an admin sends `X-View-As: user` (forwarded by
`frontend/lib/apiFetch.ts` from the `view_as` cookie the Sidebar toggle sets)
we report non-admin so role-filtered endpoints render the genuine user view.

Downgrade-only: a non-admin JWT can never become admin via the header (its
role isn't admin to begin with), so the header is safe to honor.

Single source of truth — `routers.scheduled_strategies._is_admin` and the
momentum read-only gates all delegate here so the view-as semantics can't
drift between endpoints.
"""
from __future__ import annotations

from fastapi import Request


def is_admin_request(request: Request) -> bool:
    info = getattr(request.state, "auth", None) or {}
    if info.get("role") != "admin":
        return False
    if request.headers.get("x-view-as", "").lower() == "user":
        return False
    return True
