"""End-to-end tests for `run_backtest`.

These exercise the full monthly loop with synthetic prices: signal panel
build → per-month selection → forward-return calc → cumulative compounding.
They pin loop semantics so refactors that touch the backtest body
(precompute extraction, panel optimization, etc.) get caught even when
the signal/scoring layers are unchanged.

The synthetic universe uses pure exponential price growth so that every
signal in `PRICE_SIGNAL_DEFS` orders companies the same way (more daily
growth = higher score on every signal). That keeps selection
deterministic without us having to reason about each signal's exact
numerical behaviour.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from momentum.backtest import BacktestConfig, _build_price_index, run_backtest


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

# ~15 months of pre-backtest history — enough for the 200-day MA window
# and the 12-1 momentum signal to compute on the first cutoff.
_PRICE_HISTORY_START = "2023-09-01"
_PRICES_END = "2025-04-15"

_BACKTEST_START = date(2024, 12, 1)
_BACKTEST_END = date(2025, 3, 1)
# `_generate_month_starts(2024-12-01, 2025-03-01)` →
# [Dec, Jan, Feb, Mar]; `run_backtest` iterates `months[:-1]` (last month
# has no forward return) so monthly_records has 3 entries.
_EXPECTED_MONTHS = ["2024-12", "2025-01", "2025-02"]


def _calendar_daily(start: str, end: str) -> pd.DatetimeIndex:
    return pd.date_range(start=start, end=end, freq="D")


def _exp_prices(daily_factor: float, *, dates: pd.DatetimeIndex, start_price: float = 100.0) -> np.ndarray:
    """Pure exponential growth: price[i] = start * factor**i."""
    return start_price * (daily_factor ** np.arange(len(dates)))


def _build_prices_df(companies: dict[int, float], dates: pd.DatetimeIndex) -> pd.DataFrame:
    """Long-format prices DataFrame. `companies` maps company_id → daily growth factor."""
    rows: list[dict] = []
    for cid, factor in companies.items():
        values = _exp_prices(factor, dates=dates)
        for d, v in zip(dates, values):
            rows.append({
                "company_id": cid,
                "target_date": d.date(),
                "price": float(v),
            })
    return pd.DataFrame(rows)


def _build_universe_df(rows: list[tuple[int, str, str | None, str]]) -> pd.DataFrame:
    """rows: list of (company_id, ticker, sector, company_name).
    `sector=None` mirrors the shape produced by `load_universe` for a
    snapshot-based universe (sector comes from `monthly_eligible` instead)."""
    return pd.DataFrame(
        [
            {
                "company_id": cid,
                "gurufocus_ticker": ticker,
                "company_name": name,
                "sector": sector,
                "gurufocus_exchange": "NYSE",
            }
            for (cid, ticker, sector, name) in rows
        ]
    )


def _equal_signal_weights() -> dict[str, float]:
    """Equal weight across every price signal — the synthetic series are
    monotonic, so the relative ordering is the same on every signal."""
    return {
        "mom_12_1": 1,
        "mom_6m": 1,
        "volatility_adjusted_return_6m": 1,
        "drawdown_from_recent_high_pct": 1,
        "above_200ma": 1,
    }


def _expected_forward_return(price_series: pd.Series, entry: pd.Timestamp, exit_: pd.Timestamp) -> float:
    """Re-derive the forward return the same way `_price_on_or_after` does
    in production, so the test asserts loop wiring rather than re-deriving
    growth maths."""
    e = float(price_series[price_series.index >= entry].iloc[0])
    x = float(price_series[price_series.index >= exit_].iloc[0])
    return round((x / e - 1) * 100, 2)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestBasicThreeMonthBacktest:
    """4 companies in 2 sectors, top_n_sectors=2, top_n_per_sector=2 →
    every company is picked every month. Pins the holdings shape, equal
    weights, forward returns, and cumulative compounding."""

    def test_full_loop(self):
        dates = _calendar_daily(_PRICE_HISTORY_START, _PRICES_END)
        companies = {
            10: 1.0010,  # Alpha
            20: 1.0008,  # Alpha
            30: 1.0009,  # Beta
            40: 1.0007,  # Beta
        }
        prices = _build_prices_df(companies, dates)
        universe = _build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (20, "A2", "Alpha", "Alpha-2"),
            (30, "B1", "Beta",  "Beta-1"),
            (40, "B2", "Beta",  "Beta-2"),
        ])

        config = BacktestConfig(
            start_date=_BACKTEST_START,
            end_date=_BACKTEST_END,
            signal_weights=_equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=2,
        )

        result = run_backtest(config, prices, universe)

        assert [r.date for r in result.monthly_records] == _EXPECTED_MONTHS

        # All 4 picked each month, equal weights.
        for rec in result.monthly_records:
            ids = {h.company_id for h in rec.holdings}
            assert ids == {10, 20, 30, 40}, f"month={rec.date} got={ids}"
            assert all(h.weight == pytest.approx(0.25) for h in rec.holdings)
            assert sum(h.weight for h in rec.holdings) == pytest.approx(1.0)

        # Forward returns match what _price_on_or_after derives. Re-derive
        # from the same price index so the test confirms wiring, not maths.
        idx = _build_price_index(prices)
        month_starts = [date(2024, 12, 1), date(2025, 1, 1), date(2025, 2, 1), date(2025, 3, 1)]
        for i, rec in enumerate(result.monthly_records):
            entry_ts = pd.Timestamp(month_starts[i])
            exit_ts = pd.Timestamp(month_starts[i + 1])
            for h in rec.holdings:
                expected = _expected_forward_return(idx[h.company_id], entry_ts, exit_ts)
                assert h.forward_return_pct == pytest.approx(expected), (
                    f"month={rec.date} cid={h.company_id} expected={expected} got={h.forward_return_pct}"
                )

        # Cumulative compounds across the equity curve. All companies have
        # positive daily factors → every monthly portfolio_return_pct is
        # positive → cumulative_return_pct strictly increases.
        cumulatives = [r.cumulative_return_pct for r in result.monthly_records]
        assert cumulatives == sorted(cumulatives)
        # `summary.total_return_pct` equals the final equity-curve value
        # (both are `round(cumulative, 2)` after the last month).
        assert result.summary.total_return_pct == pytest.approx(
            result.monthly_records[-1].cumulative_return_pct
        )
        # Sanity: chain monthly returns and confirm the same final value.
        chained = 1.0
        for r in result.monthly_records:
            if r.portfolio_return_pct is not None:
                chained *= 1 + r.portfolio_return_pct / 100
        assert result.summary.total_return_pct == pytest.approx(round((chained - 1) * 100, 2))

        # `avg_holdings` reflects the 4-per-month constant.
        assert result.summary.avg_holdings == pytest.approx(4.0)
        # No turnover — same 4 names every month.
        assert result.summary.avg_monthly_turnover_pct == pytest.approx(0.0)


class TestTopNPerSectorSelection:
    """6 companies, 3 per sector, top_n_per_sector=2. The weakest in each
    sector should never be picked; the four strongest always are."""

    def test_weakest_dropped_each_month(self):
        dates = _calendar_daily(_PRICE_HISTORY_START, _PRICES_END)
        companies = {
            # Alpha sector (strict ordering: 10 > 11 > 12)
            10: 1.0012,
            11: 1.0010,
            12: 1.0001,  # weakest — should never be picked
            # Beta sector (strict ordering: 20 > 21 > 22)
            20: 1.0011,
            21: 1.0009,
            22: 1.0002,  # weakest — should never be picked
        }
        prices = _build_prices_df(companies, dates)
        universe = _build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (11, "A2", "Alpha", "Alpha-2"),
            (12, "A3", "Alpha", "Alpha-3"),
            (20, "B1", "Beta",  "Beta-1"),
            (21, "B2", "Beta",  "Beta-2"),
            (22, "B3", "Beta",  "Beta-3"),
        ])

        config = BacktestConfig(
            start_date=_BACKTEST_START,
            end_date=_BACKTEST_END,
            signal_weights=_equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=2,
        )
        result = run_backtest(config, prices, universe)

        for rec in result.monthly_records:
            picked = {h.company_id for h in rec.holdings}
            assert picked == {10, 11, 20, 21}, f"month={rec.date} got={picked}"


class TestMonthlyEligibleFilter:
    """`monthly_eligible` (universe_membership snapshot) restricts which
    companies are eligible per month and overrides their sector. The
    backtest must respect the per-month set, including changing membership
    across months and using snapshot-supplied sectors."""

    def test_per_month_universe_changes(self):
        dates = _calendar_daily(_PRICE_HISTORY_START, _PRICES_END)
        # All four grow identically — selection isn't being tested here,
        # only that the per-month universe is applied correctly.
        companies = {10: 1.0010, 11: 1.0010, 12: 1.0010, 13: 1.0010}
        prices = _build_prices_df(companies, dates)
        # Sector is None in the base universe — that's the shape
        # `load_universe` produces when a snapshot is in play.
        universe = _build_universe_df([
            (10, "T10", None, "C10"),
            (11, "T11", None, "C11"),
            (12, "T12", None, "C12"),
            (13, "T13", None, "C13"),
        ])
        # Dec 2024: only 10 + 11 eligible, in two distinct sectors.
        # Jan 2025: all four eligible.
        # Feb 2025: only 12 + 13 eligible.
        monthly_eligible = {
            "2024-12": {10: "X", 11: "Y"},
            "2025-01": {10: "X", 11: "X", 12: "Y", 13: "Y"},
            "2025-02": {12: "X", 13: "Y"},
        }

        config = BacktestConfig(
            start_date=_BACKTEST_START,
            end_date=_BACKTEST_END,
            signal_weights=_equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=2,
        )
        result = run_backtest(
            config, prices, universe, monthly_eligible=monthly_eligible,
        )

        by_month = {r.date: r for r in result.monthly_records}
        assert {h.company_id for h in by_month["2024-12"].holdings} == {10, 11}
        assert {h.company_id for h in by_month["2025-01"].holdings} == {10, 11, 12, 13}
        assert {h.company_id for h in by_month["2025-02"].holdings} == {12, 13}

        # Sector on each holding comes from the snapshot, not the base universe.
        dec_sectors = {h.sector for h in by_month["2024-12"].holdings}
        assert dec_sectors == {"X", "Y"}
        feb_sectors = {h.sector for h in by_month["2025-02"].holdings}
        assert feb_sectors == {"X", "Y"}


class TestEmptyMonthHandling:
    """A month whose universe has zero companies passing screening must
    record `empty_reason` rather than crash, and the equity curve must
    pick up where it left off when later months recover."""

    def test_zero_eligible_month_records_empty_reason(self):
        dates = _calendar_daily(_PRICE_HISTORY_START, _PRICES_END)
        companies = {10: 1.0010, 11: 1.0010}
        prices = _build_prices_df(companies, dates)
        universe = _build_universe_df([
            (10, "T10", None, "C10"),
            (11, "T11", None, "C11"),
        ])
        # Dec is empty, Jan + Feb have eligible companies.
        monthly_eligible = {
            "2024-12": {},
            "2025-01": {10: "X", 11: "Y"},
            "2025-02": {10: "X", 11: "Y"},
        }

        config = BacktestConfig(
            start_date=_BACKTEST_START,
            end_date=_BACKTEST_END,
            signal_weights=_equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=2,
        )
        result = run_backtest(
            config, prices, universe, monthly_eligible=monthly_eligible,
        )

        by_month = {r.date: r for r in result.monthly_records}
        # Dec: zero eligible → empty_reason populated, no holdings, no return.
        dec = by_month["2024-12"]
        assert dec.holdings == []
        assert dec.empty_reason is not None
        assert dec.portfolio_return_pct is None
        assert dec.cumulative_return_pct == 0.0

        # Jan + Feb run normally and start compounding from 0.
        assert {h.company_id for h in by_month["2025-01"].holdings} == {10, 11}
        assert by_month["2025-01"].portfolio_return_pct is not None
        # Cumulative monotonically increases (positive growth → positive returns).
        assert by_month["2025-02"].cumulative_return_pct >= by_month["2025-01"].cumulative_return_pct
