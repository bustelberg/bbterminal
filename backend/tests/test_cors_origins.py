"""The CORS allow-list is hardcoded plus `CORS_ORIGINS` — and a mistake in it is INVISIBLE.

A missing origin is rejected by the BROWSER, before the request reaches a handler: nothing is
logged server-side, every call fails at once, and it reads like the backend is down. That is why
the parsing is pinned rather than eyeballed — the three ways to get it subtly wrong (trailing
slash, whitespace, an empty segment from a stray comma) all produce an entry that matches nothing
and looks fine in the dashboard.

⚠ `RAILWAY_PUBLIC_DOMAIN` is the BACKEND's own domain. It is not, and has never been, the frontend
that calls it — a second Railway environment does not get its Vercel project allowed by existing.
"""
from __future__ import annotations

import importlib


def _origins(monkeypatch, value: str | None) -> list[str]:
    """Re-import `main` with `CORS_ORIGINS` set, and read the list it built."""
    if value is None:
        monkeypatch.delenv("CORS_ORIGINS", raising=False)
    else:
        monkeypatch.setenv("CORS_ORIGINS", value)
    monkeypatch.delenv("RAILWAY_PUBLIC_DOMAIN", raising=False)
    import main  # noqa: PLC0415

    return list(importlib.reload(main)._cors_origins)


class TestTheAllowList:
    def test_the_defaults_survive(self, monkeypatch):
        got = _origins(monkeypatch, None)
        assert "http://localhost:3000" in got
        assert "https://bbterminal.vercel.app" in got

    def test_an_extra_frontend_is_added(self, monkeypatch):
        got = _origins(monkeypatch, "https://bbterminal-dev.vercel.app")
        assert "https://bbterminal-dev.vercel.app" in got
        assert "https://bbterminal.vercel.app" in got      # additive, never a replacement

    def test_several_are_split_on_commas(self, monkeypatch):
        got = _origins(monkeypatch, "https://a.vercel.app,https://b.vercel.app")
        assert "https://a.vercel.app" in got
        assert "https://b.vercel.app" in got

    def test_whitespace_around_a_comma_is_stripped(self, monkeypatch):
        """A space after the comma is the natural way to type a list, and an origin with a leading
        space matches nothing."""
        got = _origins(monkeypatch, "https://a.vercel.app, https://b.vercel.app")
        assert "https://b.vercel.app" in got
        assert " https://b.vercel.app" not in got

    def test_a_trailing_slash_is_dropped(self, monkeypatch):
        """⚠ Starlette compares the `Origin` header verbatim, and a browser NEVER sends a trailing
        slash. Copy a URL out of the address bar and you get one — an entry that can never match,
        failing identically to having forgotten it."""
        got = _origins(monkeypatch, "https://a.vercel.app/")
        assert "https://a.vercel.app" in got
        assert "https://a.vercel.app/" not in got

    def test_an_empty_segment_is_dropped_not_added_as_blank(self, monkeypatch):
        """A stray trailing comma must not put "" in the list."""
        got = _origins(monkeypatch, "https://a.vercel.app,,")
        assert "" not in got
        assert "https://a.vercel.app" in got

    def test_unset_changes_nothing(self, monkeypatch):
        assert _origins(monkeypatch, "") == _origins(monkeypatch, None)
