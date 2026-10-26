from types import SimpleNamespace

import pytest

import airs_scanner as scanner


def test_login_uses_dom_readiness_and_includes_navigation_failures_in_diagnostics(monkeypatch):
    monkeypatch.setenv("BROKER_USERNAME", "test-user")
    monkeypatch.setenv("BROKER_PASSWORD", "test-password")
    calls = []

    def goto(url, **kwargs):
        calls.append((url, kwargs))
        raise TimeoutError("navigation timed out")

    monkeypatch.setattr(scanner, "_capture_login_diagnostics", lambda _page: "diagnostic page state")
    with pytest.raises(RuntimeError, match="diagnostic page state"):
        scanner._login(SimpleNamespace(goto=goto))

    assert calls == [(f"{scanner.BASE_URL}/login.php", {"wait_until": "domcontentloaded"})]
