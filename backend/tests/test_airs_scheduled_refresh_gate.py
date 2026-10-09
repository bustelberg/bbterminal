"""The morning AIRS checks must be cheap without missing a new valuation."""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import airs_vermogen as V
import scheduler
from tests._fake_supabase import FakeSupabase


def test_gate_skips_when_the_newest_stored_canary_is_still_current(monkeypatch):
    monkeypatch.setattr(V, "supabase", FakeSupabase({"airs_holding": [
        {"portefeuille": "BUS_Neutraal_Dyn", "as_of_date": "2026-10-08"},
    ], "airs_account_roster": [
        {"portefeuille": "BUS_Neutraal_Dyn", "last_seen_at": "2026-10-08T11:00:00Z"},
    ]}))
    monkeypatch.setattr(V, "_discover_portfolios", lambda: ["BUS_Neutraal_Dyn"])
    monkeypatch.setattr(V, "_vermogen_most_recent",
                        lambda name, _van: ("2026-10-08", b"xlsx"))

    gate = V.scheduled_account_refresh_decision()

    assert gate["run"] is False
    assert gate["canary"] == "BUS_Neutraal_Dyn"
    assert gate["observed_date"] == gate["stored_date"] == "2026-10-08"
    assert V._LOCK.acquire(blocking=False), "the readiness probe must release the AIRS lock"
    V._LOCK.release()


def test_gate_runs_when_airs_has_a_newer_valuation(monkeypatch):
    monkeypatch.setattr(V, "supabase", FakeSupabase({"airs_holding": [
        {"portefeuille": "BUS_Neutraal_Dyn", "as_of_date": "2026-10-08"},
    ], "airs_account_roster": [
        {"portefeuille": "BUS_Neutraal_Dyn", "last_seen_at": "2026-10-08T11:00:00Z"},
    ]}))
    monkeypatch.setattr(V, "_discover_portfolios", lambda: ["BUS_Neutraal_Dyn"])
    monkeypatch.setattr(V, "_vermogen_most_recent",
                        lambda name, _van: ("2026-10-09", b"xlsx"))

    gate = V.scheduled_account_refresh_decision()

    assert gate["run"] is True
    assert gate["reason"] == "AIRS has a newer valuation"


def test_gate_falls_back_to_a_full_scan_when_the_probe_fails(monkeypatch):
    monkeypatch.setattr(V, "supabase", FakeSupabase({"airs_holding": [
        {"portefeuille": "BUS_Neutraal_Dyn", "as_of_date": "2026-10-08"},
    ], "airs_account_roster": [
        {"portefeuille": "BUS_Neutraal_Dyn", "last_seen_at": "2026-10-08T11:00:00Z"},
    ]}))
    monkeypatch.setattr(V, "_discover_portfolios", lambda: ["BUS_Neutraal_Dyn"])

    def boom(*_args):
        raise RuntimeError("AIRS unavailable")

    monkeypatch.setattr(V, "_vermogen_most_recent", boom)

    gate = V.scheduled_account_refresh_decision()

    assert gate["run"] is True
    assert "probe failed" in gate["reason"]


def test_gate_runs_when_airs_has_a_new_portfolio_even_without_a_new_valuation(monkeypatch):
    monkeypatch.setattr(V, "supabase", FakeSupabase({"airs_holding": [
        {"portefeuille": "BUS_Neutraal_Dyn", "as_of_date": "2026-10-08"},
    ], "airs_account_roster": [
        {"portefeuille": "BUS_Neutraal_Dyn", "last_seen_at": "2026-10-08T11:00:00Z"},
    ]}))
    monkeypatch.setattr(V, "_discover_portfolios",
                        lambda: ["BUS_Neutraal_Dyn", "TOPS_NEU_BEH_DYN"])
    monkeypatch.setattr(V, "_vermogen_most_recent",
                        lambda *_args: ("2026-10-08", b"xlsx"))

    gate = V.scheduled_account_refresh_decision()

    assert gate["run"] is True
    assert gate["new_portfolios"] == ["TOPS_NEU_BEH_DYN"]


def test_models_are_deferred_before_eleven(monkeypatch):
    monkeypatch.setattr(
        scheduler, "_amsterdam_now",
        lambda: datetime(2026, 10, 9, 10, 0, tzinfo=ZoneInfo("Europe/Amsterdam")),
    )
    monkeypatch.setattr(V, "scheduled_account_refresh_decision", lambda: {
        "run": False, "reason": "AIRS valuation is already stored",
    })

    message, summary = scheduler._body_airs_vermogen()

    assert summary["accounts_skipped"] is True
    assert summary["models_deferred"] is True
    assert summary["pricing_deferred"] is True
    assert "deferred to 11:00" in message


def test_models_are_due_at_eleven_amsterdam():
    assert scheduler._airs_models_due(datetime(2026, 10, 9, 10, 59,
                                               tzinfo=ZoneInfo("Europe/Amsterdam"))) is False
    assert scheduler._airs_models_due(datetime(2026, 10, 9, 11, 0,
                                               tzinfo=ZoneInfo("Europe/Amsterdam"))) is True
