"""A rebalance date beyond the latest available close must not strand the
prior period with a null return.

Regression: with a first-Wednesday grid evaluated before that Wednesday's
close exists in the data (e.g. today is the first Wednesday but only the
prior Tuesday's close is loaded), the future rebalance has no entry price.
The engine must drop it and treat the PRIOR period as the open period —
valued through the latest available close — rather than anchoring the
prior period's exit to a date with no data (which yielded a blank '—'
return in the portfolio table).
"""
from __future__ import annotations

from datetime import date

from momentum.backtest import BacktestConfig, run_backtest

from tests._backtest_helpers import (
    build_prices_df,
    build_universe_df,
    calendar_daily,
    equal_signal_weights,
)


def test_future_rebalance_dropped_prior_period_is_open():
    # Daily closes end 2026-06-02 (a Tuesday). The first Wednesday of June
    # is the 3rd — beyond the data.
    dates = calendar_daily("2025-01-01", "2026-06-02")
    companies = {10: 1.0010, 20: 1.0008, 30: 1.0009, 40: 1.0007}
    prices = build_prices_df(companies, dates)
    universe = build_universe_df([
        (10, "A1", "Alpha", "Alpha-1"),
        (20, "A2", "Alpha", "Alpha-2"),
        (30, "B1", "Beta", "Beta-1"),
        (40, "B2", "Beta", "Beta-2"),
    ])

    config = BacktestConfig(
        start_date=date(2025, 1, 1),
        end_date=date(2026, 6, 3),  # generates the 2026-06-03 Wednesday rebalance
        signal_weights=equal_signal_weights(),
        top_n_sectors=2,
        top_n_per_sector=2,
        rebalance_frequency="monthly",
        rebalance_weekday=2,  # Wednesday
        include_open_period=True,
    )

    result = run_backtest(config, prices, universe)
    rec_dates = [r.date for r in result.monthly_records]

    # The future (un-priced) rebalance must not appear as a period.
    assert "2026-06-03" not in rec_dates

    # The last period is the prior rebalance (first Wed of May = 2026-05-06),
    # marked open and carrying a real return through the latest close.
    last = result.monthly_records[-1]
    assert last.date == "2026-05-06"
    assert last.is_open is True
    assert last.portfolio_return_pct is not None
    assert len(last.holdings) == 4
