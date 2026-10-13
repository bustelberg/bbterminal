"""Direct ETF comparison uses a pair-specific shared monthly window."""
from __future__ import annotations

import pytest

from routers.diversifier import _daily_returns_from_curve, _daily_returns_from_prices, _etf_pair_response


def test_etf_pair_returns_are_aligned_and_compounded_by_calendar_year():
    result = _etf_pair_response(
        {"benchmark_id": 1, "ticker": "AAA", "name": "First ETF"},
        {"2024-01": 0.10, "2024-02": -0.05, "2025-01": 0.02},
        {"benchmark_id": 2, "ticker": "BBB", "name": "Second ETF"},
        {"2024-02": 0.20, "2025-01": -0.10, "2025-02": 0.03},
    )

    assert result.overlap_months == 2
    assert result.overlap_from == "2024-02"
    assert result.overlap_to == "2025-01"
    assert [(r.month, r.first_return, r.second_return) for r in result.monthly] == [
        ("2024-02", -0.05, 0.20), ("2025-01", 0.02, -0.10),
    ]
    assert [(r.year, round(r.first_return, 4), round(r.second_return, 4)) for r in result.annual] == [
        (2024, -0.05, 0.20), (2025, 0.02, -0.10),
    ]


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


def test_daily_return_helpers_convert_prices_and_cumulative_curve():
    assert _daily_returns_from_prices([
        ("2024-01-02", 100), ("2024-01-03", 105),
    ]) == pytest.approx({"2024-01-03": 0.05})
    assert _daily_returns_from_curve([
        {"date": "2024-01-02", "cumulative_return_pct": 0},
        {"date": "2024-01-03", "cumulative_return_pct": 5},
    ]) == pytest.approx({"2024-01-03": 0.05})
