"""`run_current_portfolio` rebalance-weekday selection.

The current-picks "as of" date is the effective rebalance date — the
chosen weekday's first occurrence in the current month, falling back to
the prior month when today is before this month's rebalance. Pins that
date math so a first-Wednesday strategy stamps the right entry date
instead of the literal 1st-of-month it used before.
"""
from __future__ import annotations

from datetime import date

from momentum.backtest import BacktestConfig, run_current_portfolio

from tests._backtest_helpers import (
    build_prices_df,
    build_universe_df,
    calendar_daily,
    equal_signal_weights,
)


def _cfg(weekday: int) -> BacktestConfig:
    return BacktestConfig(
        start_date=date(2025, 1, 1),
        end_date=date(2026, 12, 31),
        signal_weights=equal_signal_weights(),
        top_n_sectors=2,
        top_n_per_sector=2,
        rebalance_weekday=weekday,
    )


def _data():
    # ~18 months of daily prices so the momentum signals have a full
    # lookback before June 2026.
    dates = calendar_daily("2025-01-01", "2026-06-30")
    prices = build_prices_df({10: 1.001, 11: 1.0008, 12: 1.0006, 13: 1.0004}, dates)
    universe = build_universe_df([
        (10, "A1", "Alpha", "Alpha-1"),
        (11, "A2", "Alpha", "Alpha-2"),
        (12, "B1", "Beta", "Beta-1"),
        (13, "B2", "Beta", "Beta-2"),
    ])
    return prices, universe


def test_first_wednesday_is_the_as_of_date():
    prices, universe = _data()
    # June 2026: Mon 1, Tue 2, Wed 3 → first Wednesday is the 3rd.
    cp = run_current_portfolio(
        _cfg(weekday=2), prices, universe, today=date(2026, 6, 15),
    )
    assert cp.as_of_date == "2026-06-03"


def test_default_monday_is_the_as_of_date():
    prices, universe = _data()
    # weekday=0 → first Monday of June 2026 is the 1st.
    cp = run_current_portfolio(
        _cfg(weekday=0), prices, universe, today=date(2026, 6, 15),
    )
    assert cp.as_of_date == "2026-06-01"


def test_before_deciding_bar_falls_back_to_prior_month():
    prices, universe = _data()
    # today = Jun 1 (Mon), BEFORE the first Wednesday's deciding bar (Tue Jun 2's
    # close isn't settled yet) → still holds May's pick. May 2026: Fri 1 → first
    # Wed = May 6. With min(today, latest_data) the Jun-3 rebalance isn't yet
    # decidable, so we step back.
    cp = run_current_portfolio(
        _cfg(weekday=2), prices, universe, today=date(2026, 6, 1),
    )
    assert cp.as_of_date == "2026-05-06"


def test_fires_early_once_deciding_bar_is_in():
    prices, universe = _data()
    # The Saturday-rebalance case: today = Sat May 30, BEFORE the first Monday
    # of June (Jun 1), but its deciding bar — Fri May 29's close — is already in.
    # So we look ahead and produce the upcoming Jun 1 rebalance early, with picks
    # identical to running it on the Monday.
    cp = run_current_portfolio(
        _cfg(weekday=0), prices, universe, today=date(2026, 5, 30),
    )
    assert cp.as_of_date == "2026-06-01"


def test_saturday_run_reproduces_the_monday_run():
    # The core guarantee: firing on Saturday (before the Jun 1 first Monday)
    # produces the SAME picks + cost basis as running it on the Monday — the
    # rebalance is fully determined by Friday's (May 29) close, which both runs
    # see. Compare holdings identity + entry date/price (the cost basis).
    prices, universe = _data()
    sat = run_current_portfolio(_cfg(weekday=0), prices, universe, today=date(2026, 5, 30))
    mon = run_current_portfolio(_cfg(weekday=0), prices, universe, today=date(2026, 6, 1))
    assert sat.as_of_date == mon.as_of_date == "2026-06-01"

    def _key(cp):
        return sorted(
            (h.company_id, h.entry_date, h.entry_price_eur, h.entry_price_local)
            for h in cp.holdings
        )
    assert sat.holdings and _key(sat) == _key(mon)
    # Entry is the prior trading day's close — STRICTLY before the Jun 1
    # rebalance (real weekday-only data → Fri May 29; this synthetic helper is
    # calendar-daily so it's May 31). The point: not the rebalance-day close.
    assert all(h.entry_date is not None and h.entry_date < "2026-06-01" for h in sat.holdings)
