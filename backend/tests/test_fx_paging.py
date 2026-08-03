"""The FX readers must PAGE, and the reason is a measured production incident.

`AITopSelectie OFF FX` reported +36.64% locally and +44.14% in production, off identical code
and (for the holdings that mattered) identical prices. Neither number was right.

The cause was not pricing. `_fx` read every currency's whole history in ONE PostgREST request:
19,037 rows over 27 currencies on the day it was found. PostgREST caps a response and truncates
SILENTLY — 10,000 rows locally, **1,000 on Supabase cloud** — so the two environments each got a
different arbitrary slice of the FX table. TWD came back locally as 20 rows starting 2026-05-27,
against a real history reaching 2014.

What a missing rate does is invisible twice over. `_eur_series` drops any close that has no rate
on or before it, so a currency cut at the head has no EUR series before the cut; the holding then
has no mark at the YTD anchor, is classed unpriceable, silently leaves the basket, and the return
is renormalised over whatever survived. Taiwan Semiconductor — 5% of that model, 6,606 price bars,
perfectly resolved — simply vanished from its own portfolio's return. No error, no gap, no blank
cell. Just a confident number computed over 19 holdings out of 20, and a DIFFERENT confident
number in production because the cap there is ten times tighter.

So these tests do not check FX arithmetic. They check that the readers keep asking until the
table is exhausted, against a fake that truncates the way the real one does.
"""
from __future__ import annotations

import pytest

from tests._fake_supabase import FakeSupabase

# Well under the 1,000-row page, so a single unpaged request cannot accidentally satisfy these.
_CAP = 250


def _fx_rows(currencies: dict[str, float], days: int) -> list[dict]:
    """`days` consecutive daily rates per currency, from 2024-01-01. Row ORDER is deliberately
    interleaved by date (the order the table would naturally hold them in), so a reader that
    takes the first N rows gets a slice of every currency rather than whole ones."""
    out: list[dict] = []
    for d in range(days):
        day = f"2024-{1 + d // 28:02d}-{1 + d % 28:02d}"
        for code, base in currencies.items():
            out.append({"currency_code": code, "rate_date": day, "rate": base + d * 0.001})
    return out


@pytest.fixture
def capped(monkeypatch):
    """A Supabase whose every response is cut at `_CAP` rows, silently."""
    currencies = {"USD": 1.08, "TWD": 34.0, "JPY": 160.0, "GBP": 0.85}
    rows = _fx_rows(currencies, days=200)          # 800 rows, cap 250
    fake = FakeSupabase({"fx_rate": rows}, max_rows=_CAP)
    monkeypatch.setattr("deps.supabase", fake)
    return fake, currencies, 200


class TestAirsPerfFxPages:
    def test_every_currency_comes_back_whole(self, capped, monkeypatch):
        fake, currencies, days = capped
        import routers._airs_portfolio_perf as perf
        monkeypatch.setattr(perf, "supabase", fake)

        got = perf._fx(set(currencies), "2024-01-01", "2024-12-31")

        assert set(got) == set(currencies)
        for code in currencies:
            assert len(got[code]) == days, (
                f"{code} came back with {len(got[code])} of {days} rates — the reader stopped "
                f"at the row cap instead of paging past it."
            )

    def test_the_EARLIEST_rate_survives(self, capped, monkeypatch):
        """The head of the history is the part that matters and the part a truncation takes.

        A YTD anchor sits at the START of the window, so a currency missing its early rates
        loses its opening mark — which is how a fully-priced holding leaves the basket without
        anything anywhere reporting a problem.
        """
        fake, currencies, _ = capped
        import routers._airs_portfolio_perf as perf
        monkeypatch.setattr(perf, "supabase", fake)

        got = perf._fx(set(currencies), "2024-01-01", "2024-12-31")

        for code in currencies:
            assert min(got[code]) == "2024-01-01", (
                f"{code}'s history now starts at {min(got[code])} — every close before that "
                f"date converts to nothing and the holding drops out of its own portfolio."
            )

    def test_a_holding_keeps_its_opening_mark_under_the_cap(self, capped, monkeypatch):
        """The incident end-to-end, in miniature: TWD closes reaching back to the window's
        start still produce an EUR series reaching back to the window's start."""
        fake, currencies, _ = capped
        import routers._airs_portfolio_perf as perf
        monkeypatch.setattr(perf, "supabase", fake)

        fx = perf._fx(set(currencies), "2024-01-01", "2024-12-31")
        closes = [(f"2024-{1 + d // 28:02d}-{1 + d % 28:02d}", 1000.0 + d) for d in range(200)]
        eur = perf._eur_series(closes, "TWD", fx)

        assert len(eur) == len(closes)
        assert eur[0][0] == closes[0][0]
        assert perf._mark_at(eur, "2024-01-15") is not None


class TestBenchmarkFxPages:
    """`_benchmark_index._fx_to_eur` is the same reader for the SP500/ACWI index, and had the
    same defect. A truncated currency there drops that country's constituents out of a
    cap-weighted index and renormalises over the rest — the index still renders, at the wrong
    level, differently per environment."""

    def test_every_currency_comes_back_whole(self, capped, monkeypatch):
        fake, currencies, days = capped
        import routers._benchmark_index as bi
        monkeypatch.setattr(bi, "supabase", fake)

        got = bi._fx_to_eur(set(currencies), "2024-01-01", "2024-12-31")

        assert set(got) == set(currencies)
        for code in currencies:
            assert len(got[code]) == days
            assert min(got[code]) == "2024-01-01"


class TestPagingIsDeterministic:
    """A page boundary that falls inside a tie can serve a row twice or never — Postgres makes
    no promise about the order of equal keys across two separate LIMIT/OFFSET queries. Both
    readers therefore sort on `(rate_date, currency_code)`, which IS unique in `fx_rate`.
    """

    def test_the_sort_key_is_unique(self, capped, monkeypatch):
        fake, currencies, days = capped
        import routers._airs_portfolio_perf as perf
        monkeypatch.setattr(perf, "supabase", fake)

        got = perf._fx(set(currencies), "2024-01-01", "2024-12-31")
        # Every (currency, date) pair present exactly once — no duplicate would be visible in a
        # dict, but a SKIPPED one would show up as a short series, which is the real risk.
        assert sum(len(v) for v in got.values()) == days * len(currencies)
