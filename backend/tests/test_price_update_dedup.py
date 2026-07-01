"""Regression tests for the price-update same-period dedup
(`ingest.phases.momentum._dedupe_price_update`).

The daily price-update op dedupes a re-priced snapshot against the prior one for
the same open period + `latest_price_date`. The date-only key was WRONG: because
`latest_price_date` is the MAX exit date across holdings, a lagging ETF catching
up to a company's already-max date left the max unchanged while the ETF's own
mark moved — so the fresh snapshot was deleted and the ETF froze on that
strategy. The dedup must also require identical per-holding marks.
"""
from __future__ import annotations

import ingest.phases.momentum as mom
from tests._fake_supabase import FakeSupabase


def _holdings(etf_exit_date: str, etf_exit_local: float, etf_ret: float) -> list[dict]:
    return [
        {"company_id": 10, "side": "long", "exit_date": "2026-06-30",
         "exit_price_local": 100.0, "forward_return_pct": 5.0},
        {"company_id": -5, "side": "long", "exit_date": etf_exit_date,
         "exit_price_local": etf_exit_local, "forward_return_pct": etf_ret},
    ]


def _snap(sid: int, holdings: list[dict], created: str) -> dict:
    return {
        "snapshot_id": sid, "scheduled_strategy_id": 7,
        "as_of_date": "2026-06-01", "latest_price_date": "2026-06-30",
        "created_at": created, "holdings": [dict(h) for h in holdings],
    }


def test_sig_differs_when_etf_mark_moves():
    stale = _holdings("2026-06-23", 50.0, 1.0)
    fresh = _holdings("2026-06-30", 52.0, 4.0)
    assert mom._price_update_marks_sig(stale) != mom._price_update_marks_sig(fresh)


def test_sig_order_independent():
    h = _holdings("2026-06-30", 52.0, 4.0)
    assert mom._price_update_marks_sig(h) == mom._price_update_marks_sig(list(reversed(h)))


def test_keeps_new_when_etf_caught_up(monkeypatch):
    # Same as_of + same latest_price_date (06-30, driven by the company), but the
    # ETF caught up from 06-23 → 06-30. Must NOT be deduped.
    prior = _snap(1, _holdings("2026-06-23", 50.0, 1.0), "2026-06-30T05:00:00Z")
    new = _snap(2, _holdings("2026-06-30", 52.0, 4.0), "2026-07-01T05:00:00Z")
    fake = FakeSupabase(tables={"current_picks_snapshot": [prior, new]})
    monkeypatch.setattr(mom, "supabase", fake)

    assert mom._dedupe_price_update(7, 2, new) is None
    ids = {r["snapshot_id"] for r in fake.tables["current_picks_snapshot"]}
    assert ids == {1, 2}  # the fresh snapshot survives


def test_dedupes_when_marks_identical(monkeypatch):
    holds = _holdings("2026-06-30", 52.0, 4.0)
    prior = _snap(1, holds, "2026-06-30T05:00:00Z")
    new = _snap(2, holds, "2026-07-01T05:00:00Z")
    fake = FakeSupabase(tables={"current_picks_snapshot": [prior, new]})
    monkeypatch.setattr(mom, "supabase", fake)

    assert mom._dedupe_price_update(7, 2, new) == 1
    ids = {r["snapshot_id"] for r in fake.tables["current_picks_snapshot"]}
    assert ids == {1}  # the redundant new snapshot was deleted
