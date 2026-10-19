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
        {"id": 14, "isin": "", "review_on": "2026-01-01"},
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


def test_return_reader_explains_a_future_review_date(monkeypatch):
    monkeypatch.setattr(log, "supabase", _fake_db())
    class FrozenDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 10, 19)
    monkeypatch.setattr(log, "date", FrozenDate)

    result = log._yahoo_return_since("US0000000001", date(2027, 10, 16))

    assert result == {
        "available": False,
        "message": "The review date 2027-10-16 is in the future; a Yahoo return cannot be calculated yet.",
    }


def test_decision_can_be_saved_without_an_isin():
    values = {"company_name": "Private company", "decision": "Watch", "conviction": 3}
    assert log.LogEntryIn(**values).isin == ""
    assert log.LogEntryIn(**values, isin="").model_dump()["isin"] == ""


def test_create_and_edit_preserve_every_decision_field(monkeypatch):
    db = FakeSupabase({"bc_log_entry": []})
    monkeypatch.setattr(log, "supabase", db)
    values = dict(company_name="Private instrument", isin="", decision="Watch",
                  notes="First line\nSecond line", conviction=4, portfolio_weight=0,
                  review_on="2026-11-01")
    created = asyncio.run(log.create_entry(log.LogEntryIn(**values)))
    assert {key: created[key] for key in values} == values
    # The database supplies these; keep them unchanged when editing.
    db.tables["bc_log_entry"][0].update(id=42, meeting_date="2026-10-19")
    changed = dict(values, company_name="Edited instrument", isin="US0378331005",
                   decision="Buy", notes="Updated", conviction=5,
                   portfolio_weight=12.75, review_on=None)
    updated = asyncio.run(log.update_entry(42, log.LogEntryIn(**changed)))
    assert {key: updated[key] for key in changed} == changed
    assert updated["meeting_date"] == "2026-10-19"
    loaded = asyncio.run(log.get_entries())
    assert {key: loaded[0][key] for key in changed} == changed


def test_create_persists_zero_weight_and_partial_edit_cannot_erase_a_weight(monkeypatch):
    db = FakeSupabase({"bc_log_entry": [{
        "id": 9, "company_name": "Existing", "isin": "US0000000001", "decision": "Hold",
        "notes": "", "conviction": 3, "portfolio_weight": 4.5, "review_on": None,
    }]})
    monkeypatch.setattr(log, "supabase", db)

    created = asyncio.run(log.create_entry(log.LogEntryIn(
        company_name="Zero weight", isin="US0000000002", decision="Watch", conviction=3,
        portfolio_weight=0,
    )))
    assert created["portfolio_weight"] == 0

    # Simulate a legacy client that does not know about the optional weight field.
    body = log.LogEntryIn(company_name="Renamed", isin="US0000000001", decision="Hold", conviction=3)
    updated = asyncio.run(log.update_entry(9, body))
    assert updated["portfolio_weight"] == 4.5


def test_logbook_rows_include_the_yahoo_symbol_for_links(monkeypatch):
    monkeypatch.setattr(log, "supabase", FakeSupabase({
        "bc_log_entry": [
            {"id": 1, "meeting_date": "2026-10-19", "isin": "US0378331005"},
            {"id": 2, "meeting_date": "2026-10-19", "isin": ""},
        ],
        "asset_execution": [{"isin": "US0378331005", "yahoo_symbol": "AAPL"}],
    }))
    rows = {row["id"]: row for row in log._entries()}
    assert rows[1]["yahoo_symbol"] == "AAPL"
    assert rows[2]["yahoo_symbol"] is None


def test_immediate_price_fetch_preserves_mapping_and_targets_one_isin(monkeypatch):
    db = _fake_db()
    db.tables["asset_execution"][0]["yahoo_symbol"] = "TEST"
    db.tables["bc_log_entry"] = [{"id": 1, "isin": "US0000000001"}]
    monkeypatch.setattr(log, "supabase", db)
    seen = []
    monkeypatch.setattr(price_refresh, "backfill_missing", lambda isins: seen.append(isins) or {"failed": 0})
    monkeypatch.setattr(price_refresh, "refresh_stale", lambda **kw: seen.append(kw) or {"failed": 0})
    result = asyncio.run(log.refresh_entry_price(1))
    assert result["available"] is True
    assert seen == [{"US0000000001"}, {"held_only": False, "stale_days": 0, "isins": {"US0000000001"}}]


def test_immediate_price_fetch_without_isin_never_calls_yahoo(monkeypatch):
    monkeypatch.setattr(log, "supabase", FakeSupabase({"bc_log_entry": [{"id": 1, "isin": ""}]}))
    result = asyncio.run(log.refresh_entry_price(1))
    assert result["available"] is False
    assert "No ISIN" in result["message"]


def test_immediate_price_fetch_reports_failure_without_removing_decision(monkeypatch):
    import pytest
    from fastapi import HTTPException
    db = _fake_db()
    db.tables["asset_execution"][0]["yahoo_symbol"] = "TEST"
    db.tables["bc_log_entry"] = [{"id": 1, "isin": "US0000000001"}]
    monkeypatch.setattr(log, "supabase", db)
    monkeypatch.setattr(price_refresh, "backfill_missing", lambda isins: {"failed": 0})
    monkeypatch.setattr(price_refresh, "refresh_stale", lambda **kw: {"failed": 1})
    with pytest.raises(HTTPException) as error:
        asyncio.run(log.refresh_entry_price(1))
    assert error.value.status_code == 502
    assert db.tables["bc_log_entry"][0]["id"] == 1


def test_refresh_job_reports_real_outcomes_for_every_row(monkeypatch):
    from jobs import Job, JobCtx
    db = _fake_db()
    db.tables["asset_execution"][0]["yahoo_symbol"] = "TEST"
    monkeypatch.setattr(log, "supabase", db)
    monkeypatch.setattr(log, "_entries", lambda: [
        {"id": 1, "company_name": "Mapped", "isin": "US0000000001", "review_on": "2026-01-01"},
        {"id": 2, "company_name": "Manual", "isin": "", "review_on": "2026-01-01"},
        {"id": 3, "company_name": "Unmapped", "isin": "UNKNOWN", "review_on": "2026-01-01"},
        {"id": 4, "company_name": "Undated", "isin": "US0000000001", "review_on": None},
    ])
    seen = []
    monkeypatch.setattr(price_refresh, "backfill_missing", lambda isins: {"failed": 0})
    monkeypatch.setattr(price_refresh, "refresh_stale", lambda **kw: seen.append(kw["isins"]) or {"failed": 0})
    job = Job(id="test", kind="log-yahoo-returns", label="test")
    summary = log._refresh_logbook_job(JobCtx(job))
    assert summary == "1 returns available; 3 unavailable/skipped; 0 failed. 4 decisions checked."
    assert seen == [{"US0000000001"}]
    messages = [event["message"] for event in job.events]
    assert any("Mapped" in m and "+25%" in m for m in messages)
    assert any("Manual" in m and "no ISIN" in m for m in messages)
    assert any("Unmapped" in m and "no Yahoo mapping" in m for m in messages)
    assert any("Undated" in m and "no review date" in m for m in messages)
    assert job.done == job.total == 4
    assert [event["done"] for event in job.events] == sorted(event["done"] for event in job.events)
