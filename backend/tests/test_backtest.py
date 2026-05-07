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

from momentum.backtest import (
    BacktestConfig, _build_price_index, _generate_rebalance_dates,
    _periods_per_year, run_backtest,
)
from momentum.scoring import score_and_select


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


# ---------------------------------------------------------------------------
# Rebalance frequency variants
# ---------------------------------------------------------------------------

class TestPeriodsPerYear:
    """Pin the per-frequency annualization constants. These feed into the
    Sharpe sqrt-scaling and the annualized-return calculation, so silently
    changing one would shift all reported stats."""

    def test_constants(self):
        assert _periods_per_year("daily") == 252.0
        assert _periods_per_year("weekly") == 52.0
        assert _periods_per_year("monthly") == 12.0
        assert _periods_per_year("every_2_months") == 6.0
        assert _periods_per_year("every_3_months") == 4.0


class TestGenerateRebalanceDates:
    """Cadence: each frequency emits the right stride between start and end.
    Boundary inclusivity matters — the monthly variants emit the end-of-range
    month-start (forward-return needs the *next* date, so the last entry
    is an exit-only sentinel)."""

    def test_monthly(self):
        dates = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 6, 1), "monthly")
        assert dates == [
            date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1),
            date(2024, 4, 1), date(2024, 5, 1), date(2024, 6, 1),
        ]

    def test_every_2_months_strides_by_two(self):
        dates = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 7, 1), "every_2_months")
        assert dates == [date(2024, 1, 1), date(2024, 3, 1), date(2024, 5, 1), date(2024, 7, 1)]

    def test_every_3_months_strides_by_three(self):
        dates = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 12, 1), "every_3_months")
        assert dates == [date(2024, 1, 1), date(2024, 4, 1), date(2024, 7, 1), date(2024, 10, 1)]

    def test_weekly_emits_mondays(self):
        # 2024-01-01 IS a Monday — first emitted date should equal start.
        dates = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 1, 22), "weekly")
        assert dates == [
            date(2024, 1, 1), date(2024, 1, 8), date(2024, 1, 15), date(2024, 1, 22),
        ]
        assert all(d.weekday() == 0 for d in dates)

    def test_weekly_skips_to_first_monday_when_start_isnt_monday(self):
        # 2024-01-03 is a Wednesday → first emitted date is the next Monday.
        dates = _generate_rebalance_dates(date(2024, 1, 3), date(2024, 1, 22), "weekly")
        assert dates[0] == date(2024, 1, 8)

    def test_daily_uses_prices_df_trading_days(self):
        # Synthetic prices with explicit gaps — generator must not invent
        # trading days that aren't in the data.
        rows = [
            {"company_id": 1, "target_date": d, "price": 100.0}
            for d in [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 5), date(2024, 1, 8)]
        ]
        prices = pd.DataFrame(rows)
        dates = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 1, 10), "daily", prices)
        assert dates == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 5), date(2024, 1, 8)]

    def test_daily_requires_prices(self):
        with pytest.raises(ValueError, match="daily frequency requires prices_df"):
            _generate_rebalance_dates(date(2024, 1, 1), date(2024, 1, 10), "daily", None)


class TestEvery2MonthsBacktest:
    """End-to-end: same universe, every_2_months cadence emits half as many
    rebalance periods as monthly across the same date range. Selection +
    forward-return wiring must still work with the wider stride."""

    def test_full_loop(self):
        # 6-month window so we get 3 every-2-month entries (Jan, Mar, May)
        # plus a May→Jul exit pin (May has no forward → 2 records: Jan, Mar).
        history_start = "2023-09-01"
        prices_end = "2024-08-15"
        bt_start = date(2024, 1, 1)
        bt_end = date(2024, 7, 1)

        dates = _calendar_daily(history_start, prices_end)
        companies = {10: 1.0012, 11: 1.0010, 20: 1.0011, 21: 1.0009}
        prices = _build_prices_df(companies, dates)
        universe = _build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (11, "A2", "Alpha", "Alpha-2"),
            (20, "B1", "Beta",  "Beta-1"),
            (21, "B2", "Beta",  "Beta-2"),
        ])

        config = BacktestConfig(
            start_date=bt_start,
            end_date=bt_end,
            signal_weights=_equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=2,
            rebalance_frequency="every_2_months",
        )
        result = run_backtest(config, prices, universe)

        # Periods: [Jan, Mar, May, Jul]; iterates [:-1] → 3 records (Jan, Mar, May).
        assert [r.date for r in result.monthly_records] == ["2024-01", "2024-03", "2024-05"]

        # All 4 picked each period (top_n_sectors=2 × top_n_per_sector=2 = 4).
        for rec in result.monthly_records:
            assert {h.company_id for h in rec.holdings} == {10, 11, 20, 21}

        # Forward returns derived against entry/exit pairs from periods[i] → periods[i+1].
        idx = _build_price_index(prices)
        period_starts = [date(2024, 1, 1), date(2024, 3, 1), date(2024, 5, 1), date(2024, 7, 1)]
        for i, rec in enumerate(result.monthly_records):
            entry_ts = pd.Timestamp(period_starts[i])
            exit_ts = pd.Timestamp(period_starts[i + 1])
            for h in rec.holdings:
                expected = _expected_forward_return(idx[h.company_id], entry_ts, exit_ts)
                assert h.forward_return_pct == pytest.approx(expected)


class TestWeeklyBacktest:
    """Weekly cadence inside a 6-week window. Same universe shape; the
    rebalance period is now 7 days, and entry/exit prices come from the
    next available trading day on/after each Monday."""

    def test_full_loop(self):
        history_start = "2023-09-01"
        prices_end = "2024-03-15"
        bt_start = date(2024, 1, 1)  # Monday
        bt_end = date(2024, 2, 12)   # Monday — 6 weekly entries inclusive

        dates = _calendar_daily(history_start, prices_end)
        companies = {10: 1.0010, 20: 1.0008}
        prices = _build_prices_df(companies, dates)
        universe = _build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (20, "B1", "Beta",  "Beta-1"),
        ])

        config = BacktestConfig(
            start_date=bt_start,
            end_date=bt_end,
            signal_weights=_equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=1,
            rebalance_frequency="weekly",
        )
        result = run_backtest(config, prices, universe)

        # Mondays from Jan 1 to Feb 12 inclusive: 7 dates → 6 records (last is exit).
        assert len(result.monthly_records) == 6
        # Sub-monthly variant emits full YYYY-MM-DD.
        assert all(len(r.date) == 10 and r.date[4] == "-" and r.date[7] == "-"
                   for r in result.monthly_records)
        assert result.monthly_records[0].date == "2024-01-01"
        assert result.monthly_records[-1].date == "2024-02-05"


class TestSharpeFromDailyCurve:
    """Sharpe is now derived from the daily equity curve × √252 regardless
    of rebalance frequency — period-level returns under-sample intra-period
    volatility, so a monthly strategy that's flat at month-end after a
    -15% mid-month dip used to report a wildly inflated Sharpe. Each test
    re-derives the expected value from the result's daily_records to
    confirm the formula stayed the same across frequencies.
    """

    def _build_window(self, *, freq, bt_start, bt_end, prices_end):
        history_start = "2018-01-01"
        dates = _calendar_daily(history_start, prices_end)
        companies = {10: 1.0008, 20: 1.0007, 30: 1.0006, 40: 1.0005}
        prices = _build_prices_df(companies, dates)
        universe = _build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (20, "A2", "Alpha", "Alpha-2"),
            (30, "B1", "Beta",  "Beta-1"),
            (40, "B2", "Beta",  "Beta-2"),
        ])
        config = BacktestConfig(
            start_date=bt_start,
            end_date=bt_end,
            signal_weights=_equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=2,
            rebalance_frequency=freq,
        )
        return run_backtest(config, prices, universe)

    @staticmethod
    def _expected_sharpe(result):
        """Reconstruct daily returns from result.daily_records and apply
        the √252 annualization the backtest uses internally."""
        factors = [1.0 + cum / 100 for _, cum in result.daily_records]
        rets = [
            factors[i] / factors[i - 1] - 1
            for i in range(1, len(factors))
            if factors[i - 1] > 0
        ]
        arr = np.array(rets)
        return round((arr.mean() / arr.std()) * (252 ** 0.5), 2)

    def test_monthly_sharpe_uses_daily_returns_x_sqrt_252(self):
        result = self._build_window(
            freq="monthly",
            bt_start=date(2020, 1, 1),
            bt_end=date(2024, 1, 1),
            prices_end="2024-03-01",
        )
        assert result.summary.sharpe_ratio is not None
        assert len(result.daily_records) >= 21
        assert result.summary.sharpe_ratio == pytest.approx(
            self._expected_sharpe(result)
        )

    def test_every_2_months_sharpe_uses_daily_returns_x_sqrt_252(self):
        result = self._build_window(
            freq="every_2_months",
            bt_start=date(2020, 1, 1),
            bt_end=date(2024, 1, 1),
            prices_end="2024-03-01",
        )
        assert result.summary.sharpe_ratio is not None
        assert len(result.daily_records) >= 21
        assert result.summary.sharpe_ratio == pytest.approx(
            self._expected_sharpe(result)
        )


# ---------------------------------------------------------------------------
# Long-short variants
# ---------------------------------------------------------------------------

def _build_six_company_signals_df() -> pd.DataFrame:
    """Build a synthetic signals DataFrame with strict ordering across two
    sectors. Each company has scalar signal values that the scoring engine
    will normalize, but the relative ordering is fixed: the higher the
    integer, the higher the rank.
    """
    return pd.DataFrame([
        {"company_id": 10, "sector": "Alpha", "gurufocus_ticker": "A1", "company_name": "Alpha-1",
         "mom_12_1": 5, "mom_6m": 5, "volatility_adjusted_return_6m": 5,
         "drawdown_from_recent_high_pct": 5, "above_200ma": 5},
        {"company_id": 11, "sector": "Alpha", "gurufocus_ticker": "A2", "company_name": "Alpha-2",
         "mom_12_1": 3, "mom_6m": 3, "volatility_adjusted_return_6m": 3,
         "drawdown_from_recent_high_pct": 3, "above_200ma": 3},
        {"company_id": 12, "sector": "Alpha", "gurufocus_ticker": "A3", "company_name": "Alpha-3",
         "mom_12_1": 1, "mom_6m": 1, "volatility_adjusted_return_6m": 1,
         "drawdown_from_recent_high_pct": 1, "above_200ma": 1},
        {"company_id": 20, "sector": "Beta", "gurufocus_ticker": "B1", "company_name": "Beta-1",
         "mom_12_1": 4, "mom_6m": 4, "volatility_adjusted_return_6m": 4,
         "drawdown_from_recent_high_pct": 4, "above_200ma": 4},
        {"company_id": 21, "sector": "Beta", "gurufocus_ticker": "B2", "company_name": "Beta-2",
         "mom_12_1": 2, "mom_6m": 2, "volatility_adjusted_return_6m": 2,
         "drawdown_from_recent_high_pct": 2, "above_200ma": 2},
        {"company_id": 22, "sector": "Beta", "gurufocus_ticker": "B3", "company_name": "Beta-3",
         "mom_12_1": 0, "mom_6m": 0, "volatility_adjusted_return_6m": 0,
         "drawdown_from_recent_high_pct": 0, "above_200ma": 0},
    ])


class TestScoreAndSelectDirection:
    """`direction="bottom"` mirrors the top-side ranking: worst sectors,
    worst names within each. Same shape, opposite end."""

    def test_bottom_picks_worst_sector_and_worst_names(self):
        df = _build_six_company_signals_df()
        # Both sectors have 3 names; with top_n_sectors=1 the picker chooses
        # one sector — Alpha (10,11,12) outranks Beta on every signal here
        # (since alpha values include 5,3,1 vs beta's 4,2,0; mean is higher).
        # direction="top" picks Alpha; direction="bottom" picks Beta.
        top = score_and_select(df, _equal_signal_weights(),
                               top_n_sectors=1, top_n_per_sector=2, direction="top")
        bottom = score_and_select(df, _equal_signal_weights(),
                                  top_n_sectors=1, top_n_per_sector=2, direction="bottom")

        assert set(top["sector"].unique()) == {"Alpha"}
        assert set(bottom["sector"].unique()) == {"Beta"}
        # Top: best 2 in Alpha → ids 10, 11.
        assert sorted(top["company_id"].tolist()) == [10, 11]
        # Bottom: worst 2 in Beta → ids 21, 22.
        assert sorted(bottom["company_id"].tolist()) == [21, 22]


class TestLongShortBacktest:
    """End-to-end long-short: select top + bottom, confirm holdings carry
    side flags, and confirm portfolio_return_pct = mean(long) − mean(short)."""

    def test_long_short_full_loop(self):
        dates = _calendar_daily(_PRICE_HISTORY_START, _PRICES_END)
        # Two sectors of three each. Within each sector, ranks are:
        # Alpha: 10 > 11 > 12 (10 strongest, 12 weakest)
        # Beta:  20 > 21 > 22
        companies = {
            10: 1.0012, 11: 1.0010, 12: 1.0001,
            20: 1.0011, 21: 1.0009, 22: 1.0002,
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
            top_n_sectors=2,        # both sectors land on each side
            top_n_per_sector=1,     # top 1 per sector → 2 longs; bottom 1 → 2 shorts
            strategy_type="long_short",
        )
        result = run_backtest(config, prices, universe)

        assert [r.date for r in result.monthly_records] == _EXPECTED_MONTHS

        # Each period: 2 longs (best per sector) + 2 shorts (worst per sector).
        for rec in result.monthly_records:
            longs = [h for h in rec.holdings if h.side == "long"]
            shorts = [h for h in rec.holdings if h.side == "short"]
            assert {h.company_id for h in longs} == {10, 20}
            assert {h.company_id for h in shorts} == {12, 22}
            # Long weight = 1/2; short weight = 1/2 (each side sums to 1.0
            # → 200% gross, 0% net).
            assert all(h.weight == pytest.approx(0.5) for h in longs)
            assert all(h.weight == pytest.approx(0.5) for h in shorts)

        # Portfolio return per period = mean(long_returns) - mean(short_returns).
        # Re-derive from the same price index so the test pins the math, not
        # the magnitudes.
        idx = _build_price_index(prices)
        month_starts = [date(2024, 12, 1), date(2025, 1, 1), date(2025, 2, 1), date(2025, 3, 1)]
        for i, rec in enumerate(result.monthly_records):
            entry_ts = pd.Timestamp(month_starts[i])
            exit_ts = pd.Timestamp(month_starts[i + 1])
            long_rets = [_expected_forward_return(idx[h.company_id], entry_ts, exit_ts)
                         for h in rec.holdings if h.side == "long"]
            short_rets = [_expected_forward_return(idx[h.company_id], entry_ts, exit_ts)
                          for h in rec.holdings if h.side == "short"]
            expected = round(float(np.mean(long_rets)) - float(np.mean(short_rets)), 2)
            assert rec.portfolio_return_pct == pytest.approx(expected), (
                f"period={rec.date} expected={expected} got={rec.portfolio_return_pct}"
            )

        # Long-short on this synthetic universe should be strictly profitable
        # every period — the longs (best names) outgrow the shorts (weakest
        # names) by construction. So cumulative return is monotonically up.
        cumulatives = [r.cumulative_return_pct for r in result.monthly_records]
        assert cumulatives == sorted(cumulatives)
        # And every period's return should be positive (longs beat shorts).
        for r in result.monthly_records:
            assert r.portfolio_return_pct is not None
            assert r.portfolio_return_pct > 0

    def test_long_short_dedupes_overlap(self):
        """When the same company would land on both books (small universe
        with overlapping sector lists), drop it from both. Here the universe
        has only one sector so top + bottom both pick from it."""
        dates = _calendar_daily(_PRICE_HISTORY_START, _PRICES_END)
        # 3 names in a single sector. With top_n_sectors=1 both top and
        # bottom select from the same sector, and with top_n_per_sector=2
        # the top 2 (10,11) and bottom 2 (11,12) collide on 11.
        companies = {10: 1.0012, 11: 1.0010, 12: 1.0001}
        prices = _build_prices_df(companies, dates)
        universe = _build_universe_df([
            (10, "S1", "Solo", "Solo-1"),
            (11, "S2", "Solo", "Solo-2"),
            (12, "S3", "Solo", "Solo-3"),
        ])
        config = BacktestConfig(
            start_date=_BACKTEST_START,
            end_date=_BACKTEST_END,
            signal_weights=_equal_signal_weights(),
            top_n_sectors=1,
            top_n_per_sector=2,
            strategy_type="long_short",
        )
        result = run_backtest(config, prices, universe)

        for rec in result.monthly_records:
            ids = [h.company_id for h in rec.holdings]
            # 11 collided → dropped from both sides. 10 stays long, 12 stays short.
            assert 11 not in ids
            longs = {h.company_id for h in rec.holdings if h.side == "long"}
            shorts = {h.company_id for h in rec.holdings if h.side == "short"}
            assert longs == {10}
            assert shorts == {12}

    def test_long_short_forbids_random(self):
        """Random + long-short produces meaningless results, so the engine
        rejects the combination at config validation."""
        dates = _calendar_daily(_PRICE_HISTORY_START, _PRICES_END)
        prices = _build_prices_df({10: 1.0010, 20: 1.0010}, dates)
        universe = _build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (20, "B1", "Beta",  "Beta-1"),
        ])
        config = BacktestConfig(
            start_date=_BACKTEST_START,
            end_date=_BACKTEST_END,
            signal_weights=_equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=1,
            selection_mode="random",
            random_seed=42,
            strategy_type="long_short",
        )
        with pytest.raises(ValueError, match="long_short"):
            run_backtest(config, prices, universe)

    def test_long_only_default_unchanged(self):
        """Smoke check: omitting strategy_type produces side="long" on every
        holding and the same return math as before this change shipped."""
        dates = _calendar_daily(_PRICE_HISTORY_START, _PRICES_END)
        companies = {10: 1.0010, 20: 1.0008}
        prices = _build_prices_df(companies, dates)
        universe = _build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (20, "B1", "Beta",  "Beta-1"),
        ])
        config = BacktestConfig(
            start_date=_BACKTEST_START,
            end_date=_BACKTEST_END,
            signal_weights=_equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=1,
        )
        result = run_backtest(config, prices, universe)
        for rec in result.monthly_records:
            assert all(h.side == "long" for h in rec.holdings)
