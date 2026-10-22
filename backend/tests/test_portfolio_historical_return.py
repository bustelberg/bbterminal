from datetime import date

import pytest

from routers._portfolio_historical_return import chain_airs_history


def _row(day, cumulative, capital=100):
    return {"periode": day, "cumulatief_rendement": cumulative, "beginvermogen": capital}


def test_chains_year_resets_and_removes_opening_ytd_gain():
    rows = [_row("2021-10-31", 5), _row("2021-12-31", 10)]
    rows += [_row(f"{year}-12-31", 10) for year in range(2022, 2026)]
    rows += [_row("2026-10-20", 5)]
    result = chain_airs_history(rows, "2021-10-20", "2026-10-20")
    growth = (1.1 / 1.05) * 1.1 ** 4 * 1.05
    elapsed = (date(2026, 10, 20) - date(2021, 10, 31)).days / 365.25
    assert result["available"]
    assert result["start_date"] == "2021-10-31"
    assert result["total_return_pct"] == pytest.approx((growth - 1) * 100)
    assert result["portfolio_cagr_pct"] == pytest.approx((growth ** (1 / elapsed) - 1) * 100)


def test_missing_year_and_unfunded_history_are_not_zero_returns():
    rows = [_row("2021-10-31", 5), _row("2021-12-31", 10), _row("2026-10-20", 5)]
    assert not chain_airs_history(rows, "2021-10-20", "2026-10-20")["available"]
    rows += [_row(f"{year}-12-31", 0, 0) for year in range(2022, 2026)]
    assert not chain_airs_history(rows, "2021-10-20", "2026-10-20")["available"]


def test_current_year_only_cannot_be_called_five_years():
    result = chain_airs_history([_row("2026-10-20", 12)], "2021-10-20", "2026-10-20")
    assert not result["available"]
