"""Expected Growth Model assumptions are durable, company-keyed analyst inputs."""
from __future__ import annotations

import asyncio

from routers import earnings
from tests._fake_supabase import FakeSupabase


def test_egm_assumption_round_trips_for_a_company(monkeypatch):
    db = FakeSupabase({"company": [{"company_id": 42, "company_name": "Example"}]})
    monkeypatch.setattr(earnings, "supabase", db)

    saved = asyncio.run(earnings.save_egm_assumption(
        42, earnings.EgmAssumptionIn(eps_growth=0.125, exit_pe=18.5)))
    loaded = asyncio.run(earnings.get_egm_assumptions(company_id=[42]))

    assert saved["eps_growth"] == 0.125
    assert saved["exit_pe"] == 18.5
    assert loaded["rows"] == [saved]


def test_editing_egm_assumptions_updates_the_existing_company_record(monkeypatch):
    db = FakeSupabase({"company": [{"company_id": 42, "company_name": "Example"}]})
    monkeypatch.setattr(earnings, "supabase", db)
    asyncio.run(earnings.save_egm_assumption(
        42, earnings.EgmAssumptionIn(eps_growth=0.125, exit_pe=18.5)))

    updated = asyncio.run(earnings.save_egm_assumption(
        42, earnings.EgmAssumptionIn(eps_growth=0.15, exit_pe=22)))
    loaded = asyncio.run(earnings.get_egm_assumptions(company_id=[42]))

    assert len(loaded["rows"]) == 1
    assert loaded["rows"][0] == updated
    assert updated["eps_growth"] == 0.15
    assert updated["exit_pe"] == 22


def test_clearing_one_egm_input_preserves_the_other(monkeypatch):
    db = FakeSupabase({"company": [{"company_id": 42, "company_name": "Example"}]})
    monkeypatch.setattr(earnings, "supabase", db)
    asyncio.run(earnings.save_egm_assumption(
        42, earnings.EgmAssumptionIn(eps_growth=0.125, exit_pe=18.5)))
    asyncio.run(earnings.save_egm_assumption(
        42, earnings.EgmAssumptionIn(eps_growth=None, exit_pe=18.5)))

    loaded = asyncio.run(earnings.get_egm_assumptions(company_id=[42]))
    assert len(loaded["rows"]) == 1
    assert loaded["rows"][0]["eps_growth"] is None
    assert loaded["rows"][0]["exit_pe"] == 18.5
