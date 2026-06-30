"""Regression guards for the read-only /schedule visibility path.

Two independent bugs let a non-admin see ALL scheduled strategies instead
of only the `user_visible` ones:

  1. `_resolve_role` (auth) force-promoted any hardcoded-admin-email account
     to admin even when its `app_metadata.role` was an EXPLICIT "user" — so
     an admin's designated user-test account got admin data from the backend
     while the frontend (reading `app_metadata.role`) rendered the read-only
     user UI. The two desynced and admin-only strategies leaked through.

  2. `_is_admin` (scheduled_strategies) ignored the admin "view as user"
     preview, so an admin previewing the user view still saw every strategy.
     `apiFetch` forwards the `view_as=user` cookie as `X-View-As: user`;
     `_is_admin` must honor it.
"""
from __future__ import annotations

from starlette.requests import Request

from routers import auth
from routers.scheduled_strategies import _is_admin


def _request(auth_state, headers: list[tuple[bytes, bytes]] | None = None) -> Request:
    req = Request({
        "type": "http",
        "method": "GET",
        "path": "/api/scheduled-strategies",
        "headers": headers or [],
        "query_string": b"",
    })
    req.state.auth = auth_state
    return req


class TestResolveRole:
    def test_explicit_user_role_is_never_promoted(self, monkeypatch):
        # Even for a hardcoded-admin email, an EXPLICIT "user" wins — it's an
        # intentional demotion (admin's user-test account).
        monkeypatch.setattr(auth, "_is_hardcoded_admin_email", lambda _e: True)
        assert auth._resolve_role("user", "anything@example.com") == "user"

    def test_explicit_admin_role_stays_admin(self, monkeypatch):
        monkeypatch.setattr(auth, "_is_hardcoded_admin_email", lambda _e: False)
        assert auth._resolve_role("admin", "anything@example.com") == "admin"

    def test_blank_role_falls_back_to_allowlist(self, monkeypatch):
        # The fallback the allowlist exists for: role wiped / predates trigger.
        monkeypatch.setattr(auth, "_is_hardcoded_admin_email", lambda _e: True)
        assert auth._resolve_role(None, "admin@example.com") == "admin"
        assert auth._resolve_role("", "admin@example.com") == "admin"

    def test_blank_role_non_allowlisted_is_user(self, monkeypatch):
        monkeypatch.setattr(auth, "_is_hardcoded_admin_email", lambda _e: False)
        assert auth._resolve_role(None, "stranger@example.com") == "user"


class TestIsAdminViewAs:
    def test_admin_is_admin(self):
        assert _is_admin(_request({"role": "admin"})) is True

    def test_user_is_not_admin(self):
        assert _is_admin(_request({"role": "user"})) is False

    def test_missing_auth_state_is_not_admin(self):
        assert _is_admin(_request({})) is False

    def test_admin_with_view_as_user_header_is_downgraded(self):
        req = _request({"role": "admin"}, [(b"x-view-as", b"user")])
        assert _is_admin(req) is False

    def test_view_as_header_is_case_insensitive(self):
        req = _request({"role": "admin"}, [(b"x-view-as", b"USER")])
        assert _is_admin(req) is False

    def test_unrelated_view_as_value_keeps_admin(self):
        req = _request({"role": "admin"}, [(b"x-view-as", b"admin")])
        assert _is_admin(req) is True
