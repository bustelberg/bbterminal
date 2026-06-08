"""Per-month universe filter + empty-month handling.

`monthly_eligible` (universe_membership snapshot) restricts which
companies are eligible per month and overrides their sector. The
backtest must respect the per-month set and skip months cleanly when
the eligible set is empty."""
from __future__ import annotations

from momentum.backtest import BacktestConfig, run_backtest

from tests._backtest_helpers import (
    BACKTEST_END,
    BACKTEST_START,
    PRICE_HISTORY_START,
    PRICES_END,
    build_prices_df,
    build_universe_df,
    calendar_daily,
    equal_signal_weights,
)


class TestMonthlyEligibleFilter:
    """`monthly_eligible` (universe_membership snapshot) restricts which
    companies are eligible per month and overrides their sector. The
    backtest must respect the per-month set, including changing membership
    across months and using snapshot-supplied sectors."""

    def test_per_month_universe_changes(self):
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        # All four grow identically — selection isn't being tested here,
        # only that the per-month universe is applied correctly.
        companies = {10: 1.0010, 11: 1.0010, 12: 1.0010, 13: 1.0010}
        prices = build_prices_df(companies, dates)
        # Sector is None in the base universe — that's the shape
        # `load_universe` produces when a snapshot is in play.
        universe = build_universe_df([
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
            start_date=BACKTEST_START,
            end_date=BACKTEST_END,
            signal_weights=equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=2,
        )
        result = run_backtest(
            config, prices, universe, monthly_eligible=monthly_eligible,
        )

        # Bucket by YYYY-MM — record dates are now full Mondays.
        by_month = {r.date[:7]: r for r in result.monthly_records}
        assert {h.company_id for h in by_month["2024-12"].holdings} == {10, 11}
        assert {h.company_id for h in by_month["2025-01"].holdings} == {10, 11, 12, 13}
        assert {h.company_id for h in by_month["2025-02"].holdings} == {12, 13}

        # Sector on each holding comes from the snapshot, not the base universe.
        dec_sectors = {h.sector for h in by_month["2024-12"].holdings}
        assert dec_sectors == {"X", "Y"}
        feb_sectors = {h.sector for h in by_month["2025-02"].holdings}
        assert feb_sectors == {"X", "Y"}


class TestEmptyMonthHandling:
    """Empty months (zero companies passing screening) must not crash. A
    LEADING empty month — before the strategy can hold anything — is trimmed
    so the table/curve doesn't open with a spurious 0% row; a MID-backtest
    empty month is kept (with `empty_reason`) as a genuine flat period."""

    def test_leading_empty_month_is_trimmed(self):
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        companies = {10: 1.0010, 11: 1.0010}
        prices = build_prices_df(companies, dates)
        universe = build_universe_df([
            (10, "T10", None, "C10"),
            (11, "T11", None, "C11"),
        ])
        # Dec is empty (leading), Jan + Feb have eligible companies.
        monthly_eligible = {
            "2024-12": {},
            "2025-01": {10: "X", 11: "Y"},
            "2025-02": {10: "X", 11: "Y"},
        }

        config = BacktestConfig(
            start_date=BACKTEST_START,
            end_date=BACKTEST_END,
            signal_weights=equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=2,
        )
        result = run_backtest(
            config, prices, universe, monthly_eligible=monthly_eligible,
        )

        by_month = {r.date[:7]: r for r in result.monthly_records}
        # The leading empty Dec is trimmed — no spurious "2024-12 · 0%" row.
        assert "2024-12" not in by_month
        # Jan is now the first record, holds, and compounds from 0.
        assert result.monthly_records[0].date[:7] == "2025-01"
        assert {h.company_id for h in by_month["2025-01"].holdings} == {10, 11}
        assert by_month["2025-01"].portfolio_return_pct is not None
        assert by_month["2025-02"].cumulative_return_pct >= by_month["2025-01"].cumulative_return_pct

    def test_mid_backtest_empty_month_is_kept(self):
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        companies = {10: 1.0010, 11: 1.0010}
        prices = build_prices_df(companies, dates)
        universe = build_universe_df([
            (10, "T10", None, "C10"),
            (11, "T11", None, "C11"),
        ])
        # Dec holds, Jan is empty (mid-backtest), Feb holds again.
        monthly_eligible = {
            "2024-12": {10: "X", 11: "Y"},
            "2025-01": {},
            "2025-02": {10: "X", 11: "Y"},
        }

        config = BacktestConfig(
            start_date=BACKTEST_START,
            end_date=BACKTEST_END,
            signal_weights=equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=2,
        )
        result = run_backtest(
            config, prices, universe, monthly_eligible=monthly_eligible,
        )

        by_month = {r.date[:7]: r for r in result.monthly_records}
        # Dec is the first held period (kept — not leading-empty).
        assert {h.company_id for h in by_month["2024-12"].holdings} == {10, 11}
        # Jan: mid-backtest empty → kept, with a reason, flat cumulative.
        jan = by_month["2025-01"]
        assert jan.holdings == []
        assert jan.empty_reason is not None
        assert jan.portfolio_return_pct is None
        assert jan.cumulative_return_pct == by_month["2024-12"].cumulative_return_pct
        # Feb recovers and holds again.
        assert {h.company_id for h in by_month["2025-02"].holdings} == {10, 11}
