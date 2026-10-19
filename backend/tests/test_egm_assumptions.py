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
