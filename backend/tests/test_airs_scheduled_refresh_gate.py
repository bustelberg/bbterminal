"""The morning AIRS checks must be cheap without missing a new valuation."""
from __future__ import annotations

import inspect

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


def test_every_morning_slot_runs_the_full_accounts_models_and_pricing_refresh():
    src = inspect.getsource(scheduler._body_airs_vermogen)

    assert 'triggered_by="auto", force=True' in src
    assert "scheduled_account_refresh_decision" not in src
    assert "models_deferred" not in src
    assert "pricing_deferred" not in src
    assert "count_model_portfolio_holdings_sync(" in src
    assert "_body_airs_model_prices(ctx)" in src
