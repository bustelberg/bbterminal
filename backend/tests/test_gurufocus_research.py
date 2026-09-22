from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from ingest.earnings import _api_client
from routers import admin


def test_research_returns_every_full_payload_and_counts_vendor_calls(monkeypatch):
    monkeypatch.setattr(admin, "_require_admin", lambda _authorization: None)
    monkeypatch.setenv("GURUFOCUS_BASE_URL", "https://example.test")
    monkeypatch.setenv("GURUFOCUS_API_KEY", "secret")

    calls: list[str] = []
    tracked: list[tuple[str, int]] = []
    large_payload = {"rows": [{"value": "x" * 3_000}]}

    def fake_request(url: str, timeout: int = 30):
        calls.append(url)
        if url.endswith("/forward_pe_ratio"):
            return _api_client.ApiResult(None, "vendor failure", 503)
        return _api_client.ApiResult(large_payload, "OK", 200)

    monkeypatch.setattr(_api_client, "_api_request", fake_request)

    import ingest.api_usage as api_usage

    monkeypatch.setattr(
        api_usage,
        "track_api_call",
        lambda _supabase, exchange, count=1: tracked.append((exchange, count)),
    )

    result = asyncio.run(admin.gurufocus_research(None, " nvda "))

    assert result["symbol"] == "NVDA"
    assert result["guru_focus_requests"] == len(admin._GURUFOCUS_RESEARCH_ENDPOINTS) == 14
    assert len(result["sections"]) == len(calls) == 14
    assert result["sections"][0]["data"] == large_payload
    assert len(result["sections"][0]["data"]["rows"][0]["value"]) == 3_000
    failed = next(section for section in result["sections"] if section["endpoint"] == "forward_pe_ratio")
    assert failed == {
        "key": "forward_pe_history",
        "label": "Historical forward P/E",
        "endpoint": "forward_pe_ratio",
        "status_code": 503,
        "ok": False,
        "error": "vendor failure",
        "data": None,
    }
    assert tracked == [("NASDAQ", 14)]


def test_research_rejects_a_path_instead_of_proxying_it(monkeypatch):
    monkeypatch.setattr(admin, "_require_admin", lambda _authorization: None)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin.gurufocus_research(None, "NVDA/financials"))

    assert exc.value.status_code == 422
