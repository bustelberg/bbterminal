"""`_year_perf` — assembling an account's YEAR out of AIRS's monthly rows.

Every fixture below is a real shape measured from AirSPMS on 2026-07-17 (the numbers are
AITopSelectie OFF DYN's and BUS_Offensief_Dyn's own). Two things make this hard, and both
produce a plausible number rather than an error:

  1. one ATT row is one MONTH, so the freshest row is July, not the year;
  2. the rows are not all distinct months — the daily refresh re-downloads Jan-1..today and
     writes a fresh PARTIAL row for the month in progress every time it runs.
"""
from __future__ import annotations

import pytest

from routers import _airs_accounts


# AITopSelectie OFF DYN, 2026 — the real seven-row chain. Note stortingen is 0 THROUGHOUT:
# this account's -8.37% / +42.21% split has nothing to do with flows.
_AI = [
    # periode,      begin,       eind,        koers,       opbr,   rendement, cumulatief
    ("2026-01-31", 1000000.00, 1044066.01,   44066.01,    0.00,     4.4066,    4.4066),
    ("2026-02-28", 1044066.01, 1032391.90,  -11674.11,    0.00,    -1.1181,    3.2392),
    ("2026-03-31", 1032391.90,  995759.40,  -36632.50,    0.00,    -3.5483,   -0.4241),
    ("2026-04-30",  995759.40, 1195799.16,  200039.76,    0.00,    20.0892,   19.5799),
    ("2026-05-31", 1195799.16, 1369842.48,  172337.54, 1705.78,    14.5546,   36.9842),
    ("2026-06-30", 1369842.48, 1551994.38,  182151.90,    0.00,    13.2973,   55.1994),
    ("2026-07-16", 1551994.38, 1422087.64, -130063.51,  156.77,    -8.3703,   42.2088),
]


def _rows(spec, name="AITopSelectie OFF DYN"):
    out = []
    for periode, begin, eind, koers, opbr, rend, cumul in spec:
        out.append({
            "portefeuille": name, "periode": periode,
            "beginvermogen": begin, "eindvermogen": eind,
            "stortingen": 0.0, "onttrekkingen": 0.0,
            "koersresultaat": koers, "opbrengsten": opbr,
            "kosten": 0.0, "mutatie_opgelopen_rente": 0.0,
            "beleggingsresultaat": round(koers + opbr, 2),
            "rendement": rend, "cumulatief_rendement": cumul,
        })
    return out


@pytest.fixture
def perf(monkeypatch):
    """Point `_year_perf` at a fixed row set instead of Supabase."""
    def _install(rows):
        class _Q:
            def select(self, *a, **k): return self
            def order(self, *a, **k): return self
            def limit(self, *a, **k): return self
            def execute(self): return type("R", (), {"data": rows})()
        monkeypatch.setattr(_airs_accounts, "supabase",
                            type("S", (), {"table": staticmethod(lambda n: _Q())})())
        return _airs_accounts._year_perf()
    return _install


class TestOneRowIsOneMonthNotTheYear:
    """The bug this module shipped with: the freshest row read as the year."""

    def test_the_year_is_summed_not_taken_from_the_freshest_row(self, perf):
        a = perf(_rows(_AI))["AITopSelectie OFF DYN"]
        # July alone is -130,063.51 and NEGATIVE. The year is +420,225.09.
        assert a["koersresultaat"] == pytest.approx(420225.09, abs=0.01)
        assert a["koersresultaat"] > 0, "the year's price result must not be July's"
        assert a["opbrengsten"] == pytest.approx(1862.55, abs=0.01)

    def test_beginvermogen_is_the_years_opening_not_the_months(self, perf):
        a = perf(_rows(_AI))["AITopSelectie OFF DYN"]
        assert a["beginvermogen"] == 1000000.00     # January's, not July's 1,551,994.38
        assert a["eindvermogen"] == 1422087.64

    def test_the_latest_months_return_is_not_served_as_a_year_figure(self, perf):
        a = perf(_rows(_AI))["AITopSelectie OFF DYN"]
        assert a["cumulatief_rendement"] == 42.2088          # the year — AIRS's own
        assert a["rendement_latest_month"] == -8.3703        # July — a different window
        # These are both right, of different periods. Conflating them was the whole bug.
        assert a["cumulatief_rendement"] != a["rendement_latest_month"]

    def test_the_year_return_is_never_recomputed_from_the_values(self, perf):
        """`eind/begin - 1` is 42.21% here only by coincidence of zero flows; the number
        served must be AIRS's own regardless."""
        rows = _rows(_AI)
        rows[-1]["cumulatief_rendement"] = 99.99            # AIRS says 99.99
        a = perf(rows)["AITopSelectie OFF DYN"]
        assert a["cumulatief_rendement"] == 99.99           # we say what AIRS says


class TestTheRowsAreNotAllDistinctMonths:
    def test_repeated_partial_months_are_not_double_counted(self, perf):
        """The daily refresh writes a new partial row for the month in progress.

        BUS_Offensief_Dyn really holds 20 rows for 7 months — eight of them July, all
        sharing June's close as their `beginvermogen`. Summing the lot counts July eight
        times; only the freshest look at a month is that month's answer.
        """
        spec = list(_AI)
        # three earlier looks at July, as the daily job would have written them
        spec.insert(6, ("2026-07-02", 1551994.38, 1500000.00, -51994.38, 0.00, -3.35, 50.00))
        spec.insert(7, ("2026-07-09", 1551994.38, 1480000.00, -71994.38, 0.00, -4.64, 48.00))
        spec.insert(8, ("2026-07-14", 1551994.38, 1430000.00, -121994.38, 50.00, -7.86, 43.00))
        a = perf(_rows(spec))["AITopSelectie OFF DYN"]
        assert a["months"] == 7, "seven months, whatever the row count"
        assert a["koersresultaat"] == pytest.approx(420225.09, abs=0.01)
        assert a["eindvermogen"] == 1422087.64      # the freshest July, not an earlier one

    def test_the_freshest_look_at_a_month_wins_regardless_of_row_order(self, perf):
        spec = list(_AI)
        spec.insert(0, ("2026-07-02", 1551994.38, 1500000.00, -51994.38, 0.00, -3.35, 50.00))
        a = perf(_rows(spec))["AITopSelectie OFF DYN"]
        assert a["months"] == 7
        assert a["eindvermogen"] == 1422087.64


class TestTheIdentityIsAssertedNotAssumed:
    def test_a_complete_year_reconciles(self, perf):
        a = perf(_rows(_AI))["AITopSelectie OFF DYN"]
        # eind - begin - stortingen + onttrekkingen == sum(beleggingsresultaat)
        assert a["reconciles"] is True
        assert a["residual_eur"] == pytest.approx(0.0, abs=1.0)
        assert a["beleggingsresultaat"] == pytest.approx(422087.64, abs=0.01)

    def test_a_missing_month_shows_up_as_a_discrepancy(self, perf):
        """A month we failed to store must not silently shorten the year.

        Dropping April leaves the sums 200k light while begin/eind still span January to
        July — the residual is the only thing that can say so.
        """
        a = perf(_rows([r for r in _AI if not r[0].startswith("2026-04")]))["AITopSelectie OFF DYN"]
        assert a["reconciles"] is False
        assert abs(a["residual_eur"]) > 1000

    def test_flows_are_carried_into_the_identity(self, perf):
        """A deposit is not a gain. It must come out of the result, not inflate it."""
        rows = _rows(_AI)
        rows[3]["stortingen"] = 100000.0
        rows[3]["eindvermogen"] += 100000.0
        for r in rows[4:]:
            r["beginvermogen"] += 100000.0
            r["eindvermogen"] += 100000.0
        a = perf(rows)["AITopSelectie OFF DYN"]
        assert a["stortingen"] == 100000.0
        assert a["reconciles"] is True, "the deposit must be subtracted, not counted as result"


class TestYearsAreNotMixed:
    def test_a_prior_year_is_not_summed_into_this_one(self, perf):
        """`cumulatief_rendement` restarts each January, so two years cannot be added."""
        prior = [("2025-12-31", 800000.00, 1000000.00, 200000.00, 0.00, 25.0, 25.0)]
        a = perf(_rows(prior + _AI))["AITopSelectie OFF DYN"]
        assert a["months"] == 7
        assert a["beginvermogen"] == 1000000.00          # 2026's opening, not 2025's
        assert a["koersresultaat"] == pytest.approx(420225.09, abs=0.01)
