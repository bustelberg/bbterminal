"""Regression guard for the production error-clarity helper
(`common.db_errors.explain_db_error`).

Ensures an opaque PostgREST statement-timeout becomes a message that names
the operation AND the actionable cause (COPY disabled vs COPY failing),
and that non-timeout errors still get the operation context prepended so
nothing reaches production context-free.
"""
from __future__ import annotations

from common.db_errors import explain_db_error


class _FakePostgrestError(Exception):
    """Mimics postgrest-py's APIError: carries .code + .message and a
    dict-ish str()."""
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"{{'message': '{message}', 'code': '{code}'}}")


def test_timeout_with_copy_disabled_explains_and_points_to_env():
    e = _FakePostgrestError("57014", "canceling statement due to statement timeout")
    out = explain_db_error(e, what="loading close_price for 1700 companies", copy_enabled=False)
    msg = str(out)
    assert isinstance(out, RuntimeError)
    assert "statement timeout" in msg.lower()
    assert "loading close_price for 1700 companies" in msg
    assert "SUPABASE_DB_URL" in msg  # tells the operator what to set
    assert "5432" in msg             # the session-pooler hint


def test_timeout_with_copy_enabled_points_to_copy_status():
    e = _FakePostgrestError("57014", "canceling statement due to statement timeout")
    out = explain_db_error(e, what="loading universe membership for 'LEONTEQ'", copy_enabled=True)
    msg = str(out)
    assert "copy-status" in msg.lower()  # directs to the diagnostic endpoint
    assert "LEONTEQ" in msg


def test_timeout_detected_from_str_only():
    # Even an error without .code/.message but a timeout-shaped str is caught.
    e = Exception("canceling statement due to statement timeout")
    out = explain_db_error(e, what="loading volume", copy_enabled=False)
    assert "statement timeout" in str(out).lower()


def test_non_timeout_error_keeps_context():
    e = ValueError("connection refused")
    out = explain_db_error(e, what="loading prices", copy_enabled=True)
    msg = str(out)
    assert "loading prices" in msg
    assert "connection refused" in msg
    assert "ValueError" in msg
