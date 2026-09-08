"""The second-factor gate on the API, and why it is shaped the way it is.

⚠⚠ THE FIRST VERSION OF THIS GATE WOULD HAVE LOCKED OUT EVERY ADMIN, AND THESE TESTS AGREED WITH
IT. It refused any admin whose token said `aal1`, on the assumption that an account with no
authenticator carries no `aal` claim. Measured end to end against the live stack, it carries
`aal1` anyway:

    no factor yet            -> aal1
    enrolled, password only  -> aal1
    after verifying the code -> aal2

So `aal` alone cannot tell "skipped the factor" from "has no factor", and the rule needs both
halves: `aal1` AND a verified factor exists. That is exactly what the browser's
`getAuthenticatorAssuranceLevel().nextLevel === 'aal2'` computes, so the two sides now express one
rule. The tests below are written the way round the measurement forced.
"""
from __future__ import annotations

import importlib

from fastapi.responses import JSONResponse


def _mod(monkeypatch, require: str | None = None):
    """Re-import the middleware with `REQUIRE_MFA` set, and hand back the module."""
    if require is None:
        monkeypatch.delenv("REQUIRE_MFA", raising=False)
    else:
        monkeypatch.setenv("REQUIRE_MFA", require)
    import routers._auth_middleware as m  # noqa: PLC0415

    return importlib.reload(m)


class TestWhoIsRefused:
    def test_an_admin_who_skipped_their_factor_is_refused(self, monkeypatch):
        m = _mod(monkeypatch)
        denied = m._mfa_denial({"role": "admin", "aal": "aal1", "has_verified_factor": True})
        assert isinstance(denied, JSONResponse)
        assert denied.status_code == 403

    def test_an_admin_who_used_it_passes(self, monkeypatch):
        m = _mod(monkeypatch)
        assert m._mfa_denial(
            {"role": "admin", "aal": "aal2", "has_verified_factor": True}) is None

    def test_someone_who_never_enrolled_is_also_refused(self, monkeypatch):
        """⚠⚠ THE RULE CHANGED HERE (2026-09-08, on request): two-factor is REQUIRED, so this is a
        refusal rather than an exemption. It was an exemption while enrolment was optional."""
        m = _mod(monkeypatch)
        denied = m._mfa_denial({"role": "user", "aal": "aal1", "has_verified_factor": False})
        assert denied is not None and denied.status_code == 403

    def test_the_two_refusals_name_DIFFERENT_actions(self, monkeypatch):
        """⚠ Somebody with no authenticator cannot "enter their code", and somebody who has one
        does not need to set it up. One message for both would be wrong for half the readers."""
        m = _mod(monkeypatch)
        no_factor = bytes(m._mfa_denial(
            {"role": "user", "aal": "aal1", "has_verified_factor": False}).body).decode()
        has_factor = bytes(m._mfa_denial(
            {"role": "user", "aal": "aal1", "has_verified_factor": True}).body).decode()
        assert "/account/security" in no_factor
        assert "/account/security" not in has_factor
        assert no_factor != has_factor

    def test_a_read_only_user_is_held_to_it_too(self, monkeypatch):
        """⚠ It sits BEFORE the role split. Inside the admin branch — where it started — every
        non-admin would be exempt, which is the soft way in the rule exists to remove."""
        m = _mod(monkeypatch)
        assert m._mfa_denial({"role": "user", "aal": "aal1", "has_verified_factor": True}) is not None

    def test_an_unreadable_level_passes_and_does_not_fail_closed(self, monkeypatch):
        """`_token_aal` returns None when a token GoTrue ACCEPTED cannot be parsed here.

        That is a disagreement between us and the identity provider, not evidence about the user —
        the same distinction `AuthBackendUnavailable` exists to make. Failing closed on it turns a
        library upgrade into every admin locked out of production at once.
        """
        m = _mod(monkeypatch)
        assert m._mfa_denial({"role": "admin", "aal": None, "has_verified_factor": True}) is None

    def test_the_message_names_the_action_not_the_state(self, monkeypatch):
        # A client that reaches this bypassed the UI gate, so there is no redirect behind the
        # sentence — "aal1" would tell that reader nothing they can act on.
        m = _mod(monkeypatch)
        body = bytes(m._mfa_denial(
            {"role": "admin", "aal": "aal1", "has_verified_factor": True}).body).decode()
        assert "authenticator" in body.lower()
        assert "aal" not in body.lower()


class TestTheKillSwitch:
    def test_it_can_be_switched_off_for_an_incident(self, monkeypatch):
        """⚠ `REQUIRE_MFA=0` is the way back in when the factor itself is the problem —
        a lost device, a broken clock, GoTrue rejecting valid codes. Without it the recovery path
        for a locked-out sole admin runs through SQL against production."""
        m = _mod(monkeypatch, "0")
        assert m._mfa_denial(
            {"role": "admin", "aal": "aal1", "has_verified_factor": True}) is None

    def test_it_is_ON_by_default(self, monkeypatch):
        """⚠ A security control that defaults to off is a control nobody has. Absent env ⇒ on."""
        m = _mod(monkeypatch)
        assert m._REQUIRE_MFA is True
        assert m._mfa_denial(
            {"role": "admin", "aal": "aal1", "has_verified_factor": True}) is not None

    def test_only_an_explicit_zero_disables_it(self, monkeypatch):
        # ⚠ Not truthiness: "false", "no" and "" are the spellings people reach for, and a control
        # that silently accepts them turns a typo into an unprotected API.
        for value in ("1", "true", "yes", "false", ""):
            m = _mod(monkeypatch, value)
            assert m._mfa_denial(
                {"role": "admin", "aal": "aal1", "has_verified_factor": True}) is not None, value


class TestScope:
    def test_it_runs_for_every_authenticated_caller(self, monkeypatch):
        """Pinned on the source of the call site because the alternative is booting the app, which
        the unit-tests-only rule bans — and the helper is role-agnostic on purpose, so its POSITION
        in `enforce_api_auth` is the only place the scope is expressed."""
        import inspect  # noqa: PLC0415

        m = _mod(monkeypatch)
        src = inspect.getsource(m.enforce_api_auth)
        # ⚠ BEFORE the role split, so it covers every authenticated caller rather than admins.
        before_roles, after_roles = src.split('if info.get("role") == "admin":', 1)
        assert "_mfa_denial" in before_roles
        assert "_mfa_denial" not in after_roles
