"""A MISSING CHIP MUST NOT COST THE WHOLE ANALYSE MODAL.

⚠⚠ MEASURED IN PRODUCTION 2026-09-04. `GET /api/airs/model-portfolios/1935/analysis` returned a
500 with `PGRST205 — Could not find the table 'public.relative_momentum' in the schema cache`:
`supabase/migrations/20260902010000_relative_momentum.sql` had been committed and never pushed to
prod. The modal had already done and LOGGED all of its expensive work — per-holding returns, the
split rescale, the realised leg, 52 holdings — and then died reading a distribution whose only job
is to put a state chip beside a momentum number.

⚠ THE CODE ALREADY INTENDED THE OPPOSITE, WHICH IS WHY THIS IS A GAP AND NOT A DESIGN CHOICE.
`load_distribution` returns None for an empty table, the caller reads that as "no chip", and its
comment says in terms: "A missing precompute must degrade to the old behaviour, never to a default
distribution." That covers NOT PRECOMPUTED. It did not cover CANNOT BE READ — a table that does not
exist raises instead of returning None, and the read sat outside the `_daily_eur` try/except that
guards its neighbour with the words "one missing column must never cost the whole modal".

⚠ AND IT STILL LOGS. The point is not to hide a missing migration — it is that a missing migration
must not decide whether fifteen panels render.
"""
from __future__ import annotations

import pytest

from momentum import relative as rel
from routers import _airs_portfolio_analysis as apa


@pytest.fixture()
def one_holding(monkeypatch):
    """One ISIN with enough daily history to produce a momentum figure, and nothing else stubbed —
    so what this exercises is the distribution read, not the risk maths."""
    import routers._analysis_cache as ac

    # Five years of a steadily rising daily close, weekdays only — enough for every floor.
    from datetime import date, timedelta
    start = date(2020, 1, 1)
    series = {"TESTISIN00001": [((start + timedelta(days=i)).isoformat(), 100.0 + i * 0.05)
                                for i in range(5 * 365)]}
    monkeypatch.setattr(apa, "_daily_eur", lambda isins, years: series)
    # ⚠ A COLD CACHE PER TEST, or the second test reads the first one's answer and asserts nothing.
    monkeypatch.setattr(ac, "leg_get_many", lambda keys: ({}, list(keys)))
    monkeypatch.setattr(ac, "leg_put_many", lambda pairs: None)
    return series


class TestTheDistributionReadCannotTakeTheModalDown:
    def test_an_unreadable_distribution_is_survived(self, one_holding, monkeypatch, caplog):
        """The production case: the table is not there, so the read RAISES rather than returning
        None."""
        def boom(_label):
            raise RuntimeError("PGRST205 Could not find the table 'public.relative_momentum'")

        monkeypatch.setattr(rel, "load_distribution", boom)
        out = apa._holding_risk(["TESTISIN00001"], "ACWI")
        assert "TESTISIN00001" in out, "the holding must still get its risk columns"
        assert "mom_state" not in out["TESTISIN00001"], "and no chip, since there is no ranking"

    def test_and_the_reason_is_logged_rather_than_swallowed(self, one_holding, monkeypatch,
                                                            caplog):
        """⚠ A missing migration has to stay findable — the guard is about blast radius, not about
        being quiet."""
        def boom(_label):
            raise RuntimeError("PGRST205 the table is not there")

        monkeypatch.setattr(rel, "load_distribution", boom)
        with caplog.at_level("WARNING"):
            apa._holding_risk(["TESTISIN00001"], "ACWI")
        assert any("relative-momentum distribution unavailable" in r.message for r in caplog.records)

    def test_an_EMPTY_distribution_still_degrades_the_way_it_always_did(self, one_holding,
                                                                       monkeypatch):
        """⚠ THE OTHER ARM, UNCHANGED. `None` means "nothing precomputed yet" and has always been a
        supported answer; this pins that the new try/except did not turn it into something else."""
        monkeypatch.setattr(rel, "load_distribution", lambda _label: None)
        out = apa._holding_risk(["TESTISIN00001"], "ACWI")
        assert "TESTISIN00001" in out
        assert "mom_state" not in out["TESTISIN00001"]

    def test_the_momentum_NUMBER_survives_either_way(self, one_holding, monkeypatch):
        """⚠ WHAT IS LOST IS THE CHIP, NOT THE COLUMN. The reader still gets the 12-1 return; only
        the "where this sits in the universe" state is missing."""
        monkeypatch.setattr(rel, "load_distribution", lambda _label: None)
        out = apa._holding_risk(["TESTISIN00001"], "ACWI")
        assert out["TESTISIN00001"].get("mom_12_1_pct") is not None
