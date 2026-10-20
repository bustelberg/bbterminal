"""Direct ETF comparison uses a pair-specific shared monthly window."""
from __future__ import annotations

import pytest

from routers.diversifier import (
    _daily_returns_from_curve,
    _daily_returns_from_prices,
    _etf_pair_response,
    _strategy_monthly_returns,
    _aligned_daily_returns,
)


def test_etf_pair_returns_are_aligned_and_compounded_by_calendar_year():
    result = _etf_pair_response(
        {"benchmark_id": 1, "ticker": "AAA", "name": "First ETF"},
        {"2024-01": 0.10, "2024-02": -0.05, "2025-01": 0.02},
        {"benchmark_id": 2, "ticker": "BBB", "name": "Second ETF"},
        {"2024-02": 0.20, "2025-01": -0.10, "2025-02": 0.03},
    )

    assert result.overlap_months == 1
    assert result.overlap_from == "2025-01"
    assert result.overlap_to == "2025-01"
    assert [(r.month, r.first_return, r.second_return) for r in result.monthly] == [
        ("2025-01", 0.02, -0.10),
    ]
    assert [(r.year, round(r.first_return, 4), round(r.second_return, 4)) for r in result.annual] == [
        (2025, 0.02, -0.10),
    ]


def test_pair_drops_partial_start_from_daily_metrics_but_keeps_partial_end():
    months = {"2015-11": 0.9, "2015-12": -0.4, "2016-01": 0.1, "2016-02": 0.2,
              "2026-01": 0.03, "2026-09": 0.04}
    first_daily = {"2015-12-31": -0.8, "2016-01-04": 0.01, "2016-01-05": 0.02,
                   "2026-10-08": 0.03}
    second_daily = {**first_daily, "2015-12-31": 0.9}
    result = _etf_pair_response(
        {"ticker": "A", "name": "A"}, months,
        {"ticker": "B", "name": "B"}, months, first_daily, second_daily,
    )
    assert result.overlap_from == "2016-01"
    assert result.overlap_to == "2026-09"
    assert [row.year for row in result.annual] == [2016, 2026]
    assert [row.date for row in result.daily] == ["2016-01-04", "2016-01-05", "2026-10-08"]
    assert result.correlation == pytest.approx(1)


def test_pair_with_only_an_incomplete_starting_year_has_no_comparison_data():
    result = _etf_pair_response(
        {"ticker": "A", "name": "A"}, {"2026-11": 0.1},
        {"ticker": "B", "name": "B"}, {"2026-11": 0.1},
        {"2026-11-02": 0.1}, {"2026-11-02": 0.1},
    )
    assert result.monthly == result.daily == result.annual == []
    assert result.correlation is None


def test_pair_daily_returns_keep_only_shared_trading_dates():
    result = _etf_pair_response(
        {"benchmark_id": 1, "ticker": "AAA", "name": "First ETF"}, {"2024-01": 0.01, "2024-02": 0.01},
        {"benchmark_id": 2, "ticker": "BBB", "name": "Second ETF"}, {"2024-01": 0.01, "2024-02": 0.01},
        {"2024-01-02": 0.01, "2024-01-03": -0.02},
        {"2024-01-03": 0.03, "2024-01-04": 0.04},
    )

    assert [(r.date, r.first_return, r.second_return) for r in result.daily] == [
        ("2024-01-03", -0.02, 0.03),
    ]
    assert result.correlation is None  # One daily observation is insufficient.


def test_daily_alignment_compares_the_same_interval_across_a_market_holiday():
    first, second = _aligned_daily_returns(
        [("2024-01-02", 100), ("2024-01-03", 80), ("2024-01-04", 90), ("2024-01-05", 99)],
        [("2024-01-02", 200), ("2024-01-04", 180), ("2024-01-05", 198)],
    )
    assert first == pytest.approx({"2024-01-04": -0.1, "2024-01-05": 0.1})
    assert second == pytest.approx(first)
    result = _etf_pair_response(
        {"ticker": "A", "name": "A"}, {"2024-01": 0.1, "2024-02": -0.1},
        {"ticker": "B", "name": "B"}, {"2024-01": -0.1, "2024-02": 0.1},
        first, second,
    )
    assert result.correlation == pytest.approx(1)  # Monthly correlation would be -1.


def test_daily_return_helpers_convert_prices_and_cumulative_curve():
    assert _daily_returns_from_prices([
        ("2024-01-02", 100), ("2024-01-03", 105),
    ]) == pytest.approx({"2024-01-03": 0.05})
    assert _daily_returns_from_curve([
        {"date": "2024-01-02", "cumulative_return_pct": 0},
        {"date": "2024-01-03", "cumulative_return_pct": 5},
    ]) == pytest.approx({"2024-01-03": 0.05})


def test_strategy_monthly_returns_follow_daily_curve_not_rebalance_records():
    """Diversifier periods must agree with the equity-curve yearly table."""
    result = {
        "daily_records": [
            {"date": "2024-01-02", "cumulative_return_pct": 0},
            {"date": "2024-01-31", "cumulative_return_pct": 10},
            {"date": "2024-02-29", "cumulative_return_pct": 32},
            {"date": "2024-03-28", "cumulative_return_pct": 18.8},
        ],
    }
    # These deliberately represent a divergent legacy/rebalance-period chain.
    monthly_records = [
        {"date": "2024-01-01", "cumulative_return_pct": 0},
        {"date": "2024-02-01", "cumulative_return_pct": 5},
        {"date": "2024-03-01", "cumulative_return_pct": 8},
    ]

    assert _strategy_monthly_returns(result, None, monthly_records) == pytest.approx({
        "2024-02": 0.20,
        "2024-03": -0.10,
    })


def test_strategy_monthly_returns_drops_open_daily_month_and_falls_back_for_legacy_runs():
    records = [
        {"date": "2024-01-01", "cumulative_return_pct": 5},
        {"date": "2024-02-01", "cumulative_return_pct": 10},
    ]
    assert _strategy_monthly_returns({}, None, records) == pytest.approx({
        "2024-01": 0.05,
        "2024-02": 5 / 105,
    })

    result = {"daily_records": [
        {"date": "2024-01-31", "cumulative_return_pct": 0},
        {"date": "2024-02-29", "cumulative_return_pct": 10},
        {"date": "2024-03-15", "cumulative_return_pct": 20},
    ]}
    with_open = [
        {"date": "2024-03-01", "cumulative_return_pct": 20, "is_open": True},
    ]
    assert _strategy_monthly_returns(result, None, with_open) == pytest.approx({"2024-02": 0.10})
