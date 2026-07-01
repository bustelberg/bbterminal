"""Unit tests for the GuruFocus curl_cffi impersonation ladder's
recently-failed-target deprioritisation (`ingest._gurufocus_http`).

A fingerprint Cloudflare silently drops (typically the newest Chrome, tried
first) used to make every worker re-eat a 30s timeout on it on every call. Once
a target times out / is CF-blocked it's now pushed to the BACK of the ladder for
a TTL, so the pool converges on a working profile instead of hammering the dead
one. These tests stub `cf_requests.get` — no network.
"""
from __future__ import annotations

import pytest

import ingest._gurufocus_http as gh


class _Resp:
    def __init__(self, status_code, text, headers=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}


class _FakeReq:
    def __init__(self, get):
        self.get = get


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    if not gh._HAS_CURL_CFFI or len(gh._TARGETS) < 2:
        pytest.skip("curl_cffi ladder unavailable")
    # Fresh state per test: no failures remembered, preferred = newest.
    monkeypatch.setattr(gh, "_recently_failed", {})
    monkeypatch.setattr(gh, "_preferred", gh._TARGETS[0])
    monkeypatch.setattr(gh, "_FAILED_TTL_S", 300.0)
    # Circuit breaker must start closed.
    gh._circuit.note_success()


def _install(monkeypatch, calls, *, timeout_targets):
    def fake_get(url, headers=None, timeout=None, impersonate=None, proxies=None):
        calls.append(impersonate)
        if impersonate in timeout_targets:
            raise TimeoutError("simulated 30s timeout, 0 bytes")
        return _Resp(200, '{"ok": true}')
    monkeypatch.setattr(gh, "cf_requests", _FakeReq(fake_get))


def test_timed_out_target_deprioritised_next_call(monkeypatch):
    bad = gh._TARGETS[0]  # the newest Chrome — tried first by default
    calls: list[str] = []
    _install(monkeypatch, calls, timeout_targets={bad})

    # First call: bad first (times out), then falls through to a working profile.
    r1 = gh.cf_get("https://example.com")
    assert r1.ok
    assert calls[0] == bad and r1.used_target != bad
    assert bad in gh._active_failed_targets()

    # Second call: bad is now deprioritised → NOT tried first (no wasted timeout).
    calls.clear()
    r2 = gh.cf_get("https://example.com")
    assert r2.ok
    assert calls[0] != bad
    assert bad not in calls  # a good profile succeeded before the fallback


def test_recovered_target_cleared_on_success(monkeypatch):
    bad = gh._TARGETS[0]
    calls: list[str] = []
    _install(monkeypatch, calls, timeout_targets={bad})
    gh.cf_get("https://example.com")
    assert bad in gh._active_failed_targets()

    # Now everything EXCEPT `bad` fails, so the ladder falls all the way through
    # to `bad` (deprioritised, at the back), which now succeeds → cleared.
    calls.clear()
    _install(monkeypatch, calls, timeout_targets=set(gh._TARGETS) - {bad})
    r = gh.cf_get("https://example.com")
    assert r.ok and r.used_target == bad
    assert bad not in gh._active_failed_targets()


def test_ttl_expiry_prunes(monkeypatch):
    gh._note_target_failure("chromeXYZ")
    assert "chromeXYZ" in gh._active_failed_targets()
    # Expire it.
    monkeypatch.setattr(gh, "_FAILED_TTL_S", 300.0)
    with gh._failed_lock:
        gh._recently_failed["chromeXYZ"] = 0.0  # already in the past
    assert "chromeXYZ" not in gh._active_failed_targets()


def test_disabled_by_zero_ttl(monkeypatch):
    monkeypatch.setattr(gh, "_FAILED_TTL_S", 0.0)
    gh._note_target_failure(gh._TARGETS[0])
    assert gh._active_failed_targets() == set()
