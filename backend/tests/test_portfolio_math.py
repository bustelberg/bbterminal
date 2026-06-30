"""Single-source-of-truth invariants for held-basket returns
(`momentum.portfolio_math`).

The whole point of the module is that a snapshot's return is computed in ONE
place and every surface (the /schedule header MTD, the current-portfolio card,
the run-history rows) DISPLAYS the stored value rather than recomputing — so
they can never disagree. These tests pin the contract the writers
(`compute_and_save_price_update`, `_apply_etf_overlay_to_snapshot`) rely on:

  * a holding's return IS its EUR ratio `exit_eur/entry_eur − 1`;
  * the snapshot's `period_return_pct` IS the weighted mean of those per-row
    returns — exactly what the card sums from the rows.
"""
from __future__ import annotations

from momentum.portfolio_math import holding_eur_return_pct, portfolio_eur_return_pct


class TestHoldingReturn:
    def test_is_the_eur_ratio(self):
        assert abs(holding_eur_return_pct({"entry_price_eur": 100.0, "exit_price_eur": 110.0}) - 10.0) < 1e-9
        assert abs(holding_eur_return_pct({"entry_price_eur": 50.0, "exit_price_eur": 45.0}) - (-10.0)) < 1e-9

    def test_none_without_both_marks(self):
        # An un-repriced ETF (no EUR marks, or stored as 0) has no defined return.
        assert holding_eur_return_pct({"entry_price_eur": 0, "exit_price_eur": 110.0}) is None
        assert holding_eur_return_pct({"entry_price_eur": 100.0}) is None
        assert holding_eur_return_pct({"exit_price_eur": 100.0}) is None
        assert holding_eur_return_pct({}) is None


class TestPortfolioReturn:
    def test_is_weighted_mean_of_forward(self):
        holdings = [
            {"weight": 0.6, "forward_return_pct": 5.0},
            {"weight": 0.4, "forward_return_pct": -2.5},
        ]
        # 0.6·5 + 0.4·(−2.5) = 3 − 1 = 2.0
        assert abs(portfolio_eur_return_pct(holdings) - 2.0) < 1e-9

    def test_skips_unpriced_holdings(self):
        # A holding with no return (un-priced) drops out of both num + denom,
        # so the weighted mean is over the priced subset only.
        holdings = [
            {"weight": 0.6, "forward_return_pct": 5.0},
            {"weight": 0.4, "forward_return_pct": None},
        ]
        assert abs(portfolio_eur_return_pct(holdings) - 5.0) < 1e-9

    def test_none_when_nothing_priced(self):
        assert portfolio_eur_return_pct([{"weight": 1.0, "forward_return_pct": None}]) is None
        assert portfolio_eur_return_pct([]) is None


class TestSnapshotInvariant:
    """The core invariant a consistent snapshot must satisfy — and which the
    writers now produce by construction (they set each `forward_return_pct` to
    the EUR ratio and compute `period_return_pct` via `portfolio_eur_return_pct`)."""

    def test_period_equals_weighted_per_holding_eur_returns(self):
        # Build a basket the way the re-pricer does: forward = the EUR ratio.
        raw = [
            {"weight": 0.5, "entry_price_eur": 100.0, "exit_price_eur": 120.0},  # +20%
            {"weight": 0.3, "entry_price_eur": 50.0, "exit_price_eur": 45.0},    # −10%
            {"weight": 0.2, "entry_price_eur": 200.0, "exit_price_eur": 206.0},  # +3%
        ]
        holdings = [{**h, "forward_return_pct": holding_eur_return_pct(h)} for h in raw]

        # Invariant 1: each stored forward IS the holding's EUR ratio.
        for h in holdings:
            assert abs(h["forward_return_pct"] - holding_eur_return_pct(h)) < 1e-9

        # Invariant 2: the period return IS the weighted mean of those.
        expected = 0.5 * 20.0 + 0.3 * -10.0 + 0.2 * 3.0  # = 7.6
        assert abs(portfolio_eur_return_pct(holdings) - expected) < 1e-9
