"""Regression guard for the cron-secret verification (L1 security fix).

Pins three properties of `common.cron.verify_cron_secret`:
  * fails CLOSED when CRON_SECRET is unset (500, never a silent pass),
  * rejects a wrong secret (401),
  * accepts the correct secret,
and that the compare goes through `hmac.compare_digest` (constant-time),
not a plain `!=`.
"""
from __future__ import annotations

import hmac

import pytest
from fastapi import HTTPException

from common.cron import verify_cron_secret


def test_fails_closed_when_secret_unset(monkeypatch):
    monkeypatch.delenv("CRON_SECRET", raising=False)
    with pytest.raises(HTTPException) as exc:
        verify_cron_secret("anything")
    assert exc.value.status_code == 500


def test_fails_closed_when_secret_empty(monkeypatch):
    monkeypatch.setenv("CRON_SECRET", "")
    with pytest.raises(HTTPException) as exc:
        verify_cron_secret("")
    assert exc.value.status_code == 500


def test_rejects_wrong_secret(monkeypatch):
    monkeypatch.setenv("CRON_SECRET", "s3cret-value")
    with pytest.raises(HTTPException) as exc:
        verify_cron_secret("wrong")
    assert exc.value.status_code == 401


def test_rejects_none_provided(monkeypatch):
    monkeypatch.setenv("CRON_SECRET", "s3cret-value")
    with pytest.raises(HTTPException) as exc:
        verify_cron_secret("")
    assert exc.value.status_code == 401


def test_accepts_correct_secret(monkeypatch):
    monkeypatch.setenv("CRON_SECRET", "s3cret-value")
    # No raise == accepted.
    assert verify_cron_secret("s3cret-value") is None


def test_uses_constant_time_compare(monkeypatch):
    """Guard against a regression back to `!=`: assert the module calls
    hmac.compare_digest. If someone swaps it for `==`/`!=`, this fails."""
    monkeypatch.setenv("CRON_SECRET", "s3cret-value")
    calls = {"n": 0}
    real = hmac.compare_digest

    def spy(a, b):
        calls["n"] += 1
        return real(a, b)

    monkeypatch.setattr("common.cron.hmac.compare_digest", spy)
    verify_cron_secret("s3cret-value")
    assert calls["n"] == 1, "verify_cron_secret must use hmac.compare_digest"
