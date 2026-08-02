"""Hand-set cash + ETF sleeves on a scheduled strategy.

A scheduled strategy's book is three sleeves — cash, ETF overlays, and the stock
picks the momentum engine selected — and the admin sets the first two by hand.
The stocks are not set: they take what is left, at the RELATIVE weights the
underlying strategy chose.

⚠ WHICH MEANS EVERY EDIT MUST START FROM THE STRATEGY'S OWN WEIGHTS, NOT FROM
THE LAST EDIT'S OUTPUT. What is stored is already scaled by whatever sleeves
were applied last time (a 24-name book at 70% of the portfolio holds each name
at 70% of its selected weight). Scaling THOSE by the new sleeve compounds:

    set 10% cash three times → 0.9 × 0.9 × 0.9 = 0.729 invested, and a portfolio
    that reports 10% cash while holding 27% of it

so `apply_sleeves` renormalizes the stock sleeve to sum-1 first, and the result
depends only on the sleeves you asked for — not on how many times you asked.

The second trap is the storage convention. The input is ABSOLUTE (an ETF's share
of the whole portfolio, which is what a person means by "20% in this ETF"), while
`config.etf_overlay[].weight_pct` — written by the diversifier, read by the
blended backtest — is a share of the INVESTED book. With 10% cash those differ by
exactly the cash haircut: store 20 and you hold 18.
"""
from __future__ import annotations

import inspect

import pytest

from momentum.portfolio_math import apply_sleeves, split_book


def _stocks(*weights: float) -> list[dict]:
    return [
        {"company_id": i + 1, "weight": w, "forward_return_pct": 0.0}
        for i, w in enumerate(weights)
    ]


def _etf(bid: int, weight: float) -> dict:
    return {"company_id": -bid, "weight": weight, "forward_return_pct": 0.0}


def _w(holdings: list[dict], cid: int) -> float:
    return next(h["weight"] for h in holdings if h["company_id"] == cid)


class TestSplitBook:
    def test_the_id_SIGN_is_the_discriminator(self):
        """Positive = company, negative = ETF sleeve (`-benchmark_id`), 0 = cash.
        Reading the sleeves out of the HOLDINGS (not the config) is what lets an
        edit rebuild the book from what is actually held."""
        book = [*_stocks(0.5, 0.3), _etf(7, 0.15), {"company_id": 0, "is_cash": True, "weight": 0.05}]
        stocks, etfs, cash = split_book(book)
        assert [h["company_id"] for h in stocks] == [1, 2]
        assert [h["company_id"] for h in etfs] == [-7]
        assert cash == pytest.approx(0.05)


class TestTheStockSleeveIsRederivedNotRescaled:
    def test_repeated_edits_do_not_compound(self):
        """THE bug this renormalize exists to prevent."""
        book = apply_sleeves(_stocks(0.5, 0.5), [], 0.10)
        for _ in range(3):
            stocks, etfs, _cash = split_book(book)
            book = apply_sleeves(stocks, etfs, 0.10)
        cash = next(h["weight"] for h in book if h.get("is_cash"))
        assert cash == pytest.approx(0.10)
        assert sum(h["weight"] for h in book) == pytest.approx(1.0)
        # ...and the stocks still hold 90%, not 0.9³ = 72.9%.
        assert _w(book, 1) == pytest.approx(0.45)

    def test_the_relative_weights_the_ENGINE_chose_survive(self):
        """Renormalizing must recover the strategy's own proportions — a 3:1 pick
        stays 3:1 whatever sleeve sits on top."""
        book = apply_sleeves(_stocks(0.75, 0.25), [_etf(7, 0.20)], 0.10)
        assert _w(book, 1) / _w(book, 2) == pytest.approx(3.0)

    def test_lowering_cash_gives_the_weight_BACK(self):
        """Not just "doesn't shrink further" — going 20% → 0% must restore the
        fully-invested book, which only works if the base is re-derived."""
        book = apply_sleeves(_stocks(0.6, 0.4), [], 0.20)
        stocks, etfs, _c = split_book(book)
        restored = apply_sleeves(stocks, etfs, 0.0)
        assert _w(restored, 1) == pytest.approx(0.6)
        assert not any(h.get("is_cash") for h in restored)


class TestTheSleevesLandWhereTheyWereAsked:
    def test_cash_plus_etf_plus_stocks_is_the_whole_book(self):
        book = apply_sleeves(_stocks(0.5, 0.5), [_etf(7, 0.25)], 0.10)
        cash = next(h["weight"] for h in book if h.get("is_cash"))
        etf = _w(book, -7)
        assert cash == pytest.approx(0.10)
        # The ETF weight is INVESTED-relative: 25% of the 90% that isn't cash.
        assert etf == pytest.approx(0.225)
        assert sum(h["weight"] for h in book) == pytest.approx(1.0)

    def test_a_book_that_is_all_sleeve_leaves_the_stocks_at_zero(self):
        """100% ETF is a legal (if odd) book. The stocks going to 0 is an
        answer; the weights still sum to 1."""
        book = apply_sleeves(_stocks(0.5, 0.5), [_etf(7, 1.0)], 0.0)
        assert _w(book, 1) == pytest.approx(0.0)
        assert _w(book, -7) == pytest.approx(1.0)

    def test_no_sleeves_is_the_fully_invested_book(self):
        book = apply_sleeves(_stocks(0.7, 0.3), [], 0.0)
        assert [h["weight"] for h in book] == pytest.approx([0.7, 0.3])

    def test_an_empty_stock_sleeve_does_not_divide_by_zero(self):
        book = apply_sleeves([], [_etf(7, 1.0)], 0.0)
        assert _w(book, -7) == pytest.approx(1.0)


class TestTheAbsoluteToInvestedConversion:
    """⚠ The input is a share of the WHOLE portfolio; the storage is a share of
    the INVESTED book. Skip the conversion and 20% typed becomes 18% held."""

    def test_the_endpoint_converts(self):
        from routers import scheduled_strategies as ss

        src = inspect.getsource(ss.set_strategy_sleeves)
        assert "invested = 1.0 - cash" in src
        assert "float(e.weight_pct) / invested" in src

    def test_the_round_trip_holds_what_was_typed(self):
        """20% of the book, with 10% cash: stored 22.222…, held 0.2222 × 0.9 = 20%."""
        cash, typed_pct = 0.10, 20.0
        stored = typed_pct / (1.0 - cash)                     # what the endpoint writes
        book = apply_sleeves(_stocks(0.5, 0.5), [_etf(7, stored / 100.0)], cash)
        assert _w(book, -7) == pytest.approx(typed_pct / 100.0)

    def test_over_100_percent_is_REFUSED_not_scaled_to_fit(self):
        """Scaling a 130% book down to 100% would hold weights nobody chose."""
        from routers import scheduled_strategies as ss

        src = inspect.getsource(ss.set_strategy_sleeves)
        assert "over 100%" in src
        assert "422" in src.split("over 100%", 1)[0][-400:], "refuse, don't renormalize"

    def test_an_unpriced_benchmark_is_refused(self):
        """It would be weighted into the book and contribute NO return, so the
        aggregate would quietly be over the priced part only — a real-looking
        number for a portfolio that isn't."""
        from routers import scheduled_strategies as ss

        src = inspect.getsource(ss.set_strategy_sleeves)
        assert "no price history" in src


class TestOneWriter:
    """The rebalance, the daily re-price and the hand edit must weight the book
    the same way, or the live portfolio disagrees with the config that describes
    it."""

    def test_the_rebalance_delegates_to_the_shared_writer(self):
        from ingest.phases import momentum

        src = inspect.getsource(momentum._apply_sleeves_to_snapshot)
        assert "from routers._schedule_snapshots import apply_sleeves_to_snapshot" in src

    def test_the_edit_restates_the_REBALANCE_snapshot_before_repricing(self):
        """`compute_and_save_price_update` derives the priced book FROM the
        rebalance snapshot, so re-pricing first would price the old sleeves and
        then be overwritten."""
        from routers import scheduled_strategies as ss

        src = inspect.getsource(ss._write_sleeves)
        assert src.index("apply_sleeves_to_snapshot(") < src.index("compute_and_save_price_update(")

    def test_the_etf_entry_bar_is_the_stock_sleeves_own_anchor(self):
        """⚠ The SPMO +277% incident: `as_of_date` is the nominal grid date and
        can be a FUTURE Monday when the tick fires early, which stamped an ETF
        entry against a bar that did not exist yet."""
        from routers import _schedule_snapshots as snaps

        src = inspect.getsource(snaps.apply_sleeves_to_snapshot)
        assert "stock_entry_dates" in src
        assert "never price entry past the latest real close" in src
