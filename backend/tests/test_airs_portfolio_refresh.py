"""Refreshing one AIRS model portfolio: all five inputs, not just the one AIRS owns.

`AITopSelectie OFF FX` read +36.64% locally and +55.20% in production off IDENTICAL code — the
FX paging fix (3cec3eb) was live in both. Same code, two answers, means the INPUTS differ, and a
YTD has five of them:

    composition   airs_model_portfolio_position   weights + ISINs        <- AIRS
    instruments   asset_execution                 ISIN -> symbol, ccy    <- Yahoo/OpenFIGI
    prices        asset_price                     the two closes         <- Yahoo
    FX            fx_rate                         EUR conversion         <- ECB / Yahoo
    links         airs_portfolio_link             certificates           <- our own choices

⚠ WHICH IS WHY THE OLD "Refresh from AIRS" COULD NOT FIX IT. It re-read the composition and
nothing else, so a wrong return caused by a missing price series or a short FX history survived
every press — and looked like the button was broken rather than aimed at the wrong input.

⚠ THE FX STEP IS THE ONE NOTHING ELSE IN THE APP DOES. `sync_fx_rates_to_db` reads the stored max
and fetches from max+1, so it can only extend FORWARD. A currency whose history STARTS after the
window opens is therefore never repaired by anything, in any environment, ever — and it fails in
the worst direction: `_eur_series` drops every close with no rate on or before it, the holding
loses its opening mark, it is classed unpriceable, it leaves the basket, and the return is
renormalised over the survivors. A portfolio missing its laggards reads HIGH, and reads like a
number. That is the exact shape of +55.20% against +36.64%.
"""
from __future__ import annotations

import inspect

import pytest

from tests._fake_supabase import FakeSupabase


def _rate_rows(code: str, dates: list[str]) -> list[dict]:
    return [{"currency_code": code, "rate_date": d, "rate": 1.1} for d in dates]


@pytest.fixture
def fx_env(monkeypatch):
    """A `fx_rate` table plus a stubbed ECB fetch that records what was asked for."""
    fake = FakeSupabase({"fx_rate": []})
    monkeypatch.setattr("routers._airs_portfolio_refresh.supabase", fake)
    asked: list[tuple[str, str]] = []

    def _fetch(code, start=None):
        asked.append((code, start))
        # Whatever was asked for, from `start` — one row is enough to prove the write path.
        return [{"date": start, "rate": 1.23}]

    monkeypatch.setattr("fx_rates.fetch_history", _fetch)
    return fake, asked


class TestTheFxStepRepairsHistoryBACKWARDS:
    def test_a_currency_starting_after_the_window_is_backfilled(self, fx_env):
        """The production failure, in one currency. TWD's real history reaches 2014; a database
        holding it only from May would drop Taiwan Semiconductor — 5% of the model, 6,606 price
        bars, perfectly resolved — out of its own portfolio's return, with no error anywhere."""
        from routers._airs_portfolio_refresh import _fx

        fake, asked = fx_env
        fake.tables["fx_rate"] = _rate_rows("TWD", ["2026-05-27", "2026-08-01"])

        out = _fx({"TWD"}, "2026-01-01", lambda *a, **k: None)

        assert out["backfilled"] == 1
        assert out["extended"] == 0
        # Asked from BEFORE the anchor: the opening mark is the last close on or before 1 Jan and
        # can sit days earlier over a holiday break, so a rate exactly at the anchor is not enough.
        code, start = asked[0]
        assert code == "TWD"
        assert start < "2026-01-01"

    def test_a_covered_currency_is_not_refetched(self, fx_env):
        """A press over an already-healthy portfolio must cost nothing at the ECB."""
        from datetime import date

        from routers._airs_portfolio_refresh import _fx

        fake, asked = fx_env
        fake.tables["fx_rate"] = _rate_rows("USD", ["2020-01-01", date.today().isoformat()])

        out = _fx({"USD"}, "2026-01-01", lambda *a, **k: None)

        assert out == {"checked": 1, "backfilled": 0, "extended": 0, "failed": 0,
                       "currencies": {"USD": {"from": "2020-01-01",
                                              "to": date.today().isoformat(),
                                              "action": "none"}}}
        assert asked == []

    def test_a_currency_with_no_rows_at_all_is_fetched(self, fx_env):
        from routers._airs_portfolio_refresh import _fx

        fake, asked = fx_env
        fake.tables["fx_rate"] = []

        out = _fx({"JPY"}, "2026-01-01", lambda *a, **k: None)

        assert out["backfilled"] == 1
        assert asked and asked[0][0] == "JPY"

    def test_it_asks_for_the_MAJOR_currency_never_the_minor_unit(self, fx_env):
        """⚠ `GBp` IS PENCE, NOT A CURRENCY CODE. `fx_rate` has GBP and has never had GBp, so
        requesting the literal string returns nothing, `_rate` finds no table, and every
        pence-quoted holding reads as unpriceable with all its bars present — measured across 13
        AIRS portfolios. The minor unit resolves through the shared `SUBUNIT` map."""
        from routers._airs_portfolio_refresh import _fx

        _fake, asked = fx_env

        _fx({"GBp"}, "2026-01-01", lambda *a, **k: None)

        assert asked and asked[0][0] == "GBP"

    def test_EUR_is_never_asked_for(self, fx_env):
        from routers._airs_portfolio_refresh import _fx

        _fake, asked = fx_env

        assert _fx({"EUR"}, "2026-01-01", lambda *a, **k: None)["checked"] == 0
        assert asked == []

    def test_one_dead_currency_does_not_end_the_run(self, fx_env, monkeypatch):
        from routers._airs_portfolio_refresh import _fx

        fake, _asked = fx_env
        fake.tables["fx_rate"] = _rate_rows("MXN", ["2026-05-01"])
        monkeypatch.setattr("fx_rates.fetch_history",
                            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("ECB down")))

        out = _fx({"MXN"}, "2026-01-01", lambda *a, **k: None)

        assert out["failed"] == 1
        assert out["currencies"]["MXN"]["action"] == "failed"

    def test_it_says_when_the_backfill_did_not_reach(self, fx_env, monkeypatch):
        """A currency with no published history that far back is a LIMIT, not a success. Saying
        nothing there would report a repair that did not happen."""
        from routers._airs_portfolio_refresh import _fx

        fake, _asked = fx_env
        fake.tables["fx_rate"] = _rate_rows("TWD", ["2026-05-27"])
        monkeypatch.setattr("fx_rates.fetch_history",
                            lambda *a, **k: [{"date": "2026-04-01", "rate": 1.0}])
        said: list[str] = []

        _fx({"TWD"}, "2026-01-01", lambda _t, **k: said.append(k.get("message") or ""))

        assert any("STILL SHORT" in m for m in said)


class TestTheStepsRunInDependencyOrder:
    """The composition decides which ISINs and which window; the instruments decide which
    currencies; the currencies decide what FX must cover; only then can prices be fetched and
    converted. Running FX first would sync the currencies of the composition we USED to hold."""

    def test_composition_then_instruments_then_fx_then_prices_then_recompute(self):
        from routers._airs_portfolio_refresh import refresh_portfolio

        src = inspect.getsource(refresh_portfolio)
        order = [src.index(f"_{s}(") for s in
                 ("composition", "instruments", "fx", "prices", "recompute")]
        assert order == sorted(order)

    def test_the_window_comes_from_the_composition_just_read(self):
        """`ytd_anchor_for(comp["datum"])` — not from the row we had before the scrape. The
        effective date IS the anchor for a model younger than the year, so anchoring on the stale
        one would price the new weights over the old window."""
        from routers._airs_portfolio_refresh import refresh_portfolio

        src = inspect.getsource(refresh_portfolio)
        assert 'ytd_anchor_for(comp["datum"])' in src


class TestIdentityIsDecidedInExactlyOnePlace:
    """Yahoo answers an overloaded caller with an EMPTY search rather than a 429, so a second
    concurrent resolver is how Alphabet moved from GOOGL to a Vienna line 75,000x thinner. Step 2
    resolves, through the queue's own paced slice; step 4 fetches by the symbol we already hold
    and never reopens the question."""

    def test_step_two_uses_the_queues_slice(self):
        from routers._airs_portfolio_refresh import _instruments

        src = inspect.getsource(_instruments)
        assert "_drain_now" in src and "_queue.enqueue" in src
        for forbidden in ("store_one", "fast_resolve"):
            assert forbidden not in src, forbidden

    def test_step_four_never_resolves(self):
        from routers._airs_portfolio_refresh import _prices

        src = inspect.getsource(_prices)
        assert "extend_series" in src
        for forbidden in ("enqueue", "resolve(", "store_one"):
            assert forbidden not in src, forbidden

    def test_step_four_extends_rather_than_re_downloading(self):
        """`store_series` re-downloads every bar an instrument ever had and derives
        `bars`/`price_from` from the slice it fetched; `extend_series` fetches the gap and
        recomputes them from the DB. The full path is the FALLBACK, taken only on None."""
        from routers._airs_portfolio_refresh import _prices

        src = inspect.getsource(_prices)
        assert src.index("extend_series") < src.index("store_series")


class TestTheRecomputeIsNotASecondOpinion:
    """A refresh that computed the YTD its own way could agree with itself while disagreeing with
    the table it just refreshed. It reads `explain_portfolio_ytd`, which INSTRUMENTS
    `compute_portfolio_performance` rather than reproducing it."""

    def test_it_reads_the_instrumented_derivation(self):
        from routers._airs_portfolio_refresh import _recompute

        src = inspect.getsource(_recompute)
        assert "explain_portfolio_ytd" in src
        assert "reconciles" in src
