"""The decision-log button refreshes prices in one explicit batch."""
from __future__ import annotations

import asyncio
from datetime import date

from asset_pipeline import price_refresh
from routers import log_dashboard as log
from tests._fake_supabase import FakeSupabase


def _fake_db() -> FakeSupabase:
    return FakeSupabase({
        "asset_execution": [{"isin": "US0000000001", "analysis_id": 7}],
        "asset_price": [
            {"analysis_id": 7, "target_date": "2026-01-02", "close": 100},
            {"analysis_id": 7, "target_date": "2026-10-09", "close": 125},
        ],
    })


def test_batch_refresh_updates_series_then_returns_every_dated_entry(monkeypatch):
    monkeypatch.setattr(log, "supabase", _fake_db())
    monkeypatch.setattr(log, "_entries", lambda: [
        {"id": 12, "isin": "US0000000001", "review_on": "2026-01-01"},
        {"id": 13, "isin": "US0000000001", "review_on": None},
    ])
    seen: list[set[str]] = []
    monkeypatch.setattr(price_refresh, "backfill_missing", lambda isins: seen.append(isins) or {
        "missing": 0, "backfilled": 0, "failed": 0,
    })
    monkeypatch.setattr(price_refresh, "refresh_stale", lambda **kw: {
        "moved": 1, "unchanged": 0, "failed": 0, "considered": len(kw["isins"]),
    })

    result = asyncio.run(log.refresh_yahoo_returns())

    assert seen == [{"US0000000001"}]
    assert result["entries"] == 1
    assert result["prices"]["moved"] == 1
    assert result["returns"]["12"]["return_pct"] == 25.0
    assert "13" not in result["returns"]


def test_return_reader_stays_a_read_of_the_persisted_series(monkeypatch):
    monkeypatch.setattr(log, "supabase", _fake_db())

    result = log._yahoo_return_since("us0000000001", date(2026, 1, 1))

    assert result["available"] is True
    assert result["start_date"] == "2026-01-02"
    assert result["as_of"] == "2026-10-09"
