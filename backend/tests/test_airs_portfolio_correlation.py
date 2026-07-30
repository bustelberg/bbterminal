"""Correlation matrix — the properties that must hold whatever the data is.

The matrix is read off the SAME buy-and-hold curve as the /portfolios YTD column (`_index`),
turned into a dated return series and correlated pairwise-complete. These test the two pure
pieces — the curve→returns step and the pairwise-correlation step — without a DB.
"""
from __future__ import annotations

import math

from routers._airs_portfolio_correlation import (
    MIN_OVERLAP_DAYS,
    _matrix,
    _returns_from_curve,
)


def _series(start: str, rets: list[float]) -> dict[str, float]:
    """A dated return series on consecutive fake dates, for feeding `_matrix` directly."""
    from datetime import date, timedelta
    d0 = date.fromisoformat(start)
    return {(d0 + timedelta(days=i)).isoformat(): r for i, r in enumerate(rets)}


class TestMatrix:
    def test_perfectly_correlated_is_one_anticorrelated_is_minus_one(self):
        n = MIN_OVERLAP_DAYS + 10
        a = _series("2026-01-05", [0.01 * ((i % 5) - 2) for i in range(n)])
        same = dict(a)                                    # identical to a → +1
        down = {d: -v for d, v in a.items()}              # negated a → −1
        m, _ = _matrix({1: a, 2: same, 3: down}, [1, 2, 3])
        assert math.isclose(m[0][1], 1.0, abs_tol=1e-9)   # a vs same
        assert math.isclose(m[0][2], -1.0, abs_tol=1e-9)  # a vs −a
        assert math.isclose(m[2][0], -1.0, abs_tol=1e-9)  # symmetric

    def test_symmetric_with_unit_diagonal(self):
        n = MIN_OVERLAP_DAYS + 5
        s1 = _series("2026-01-05", [0.01 * math.sin(i) for i in range(n)])
        s2 = _series("2026-01-05", [0.01 * math.cos(i) for i in range(n)])
        m, obs = _matrix({1: s1, 2: s2}, [1, 2])
        assert obs == [n, n]
        assert math.isclose(m[0][0], 1.0, abs_tol=1e-9)
        assert math.isclose(m[1][1], 1.0, abs_tol=1e-9)
        assert m[0][1] is not None and math.isclose(m[0][1], m[1][0], abs_tol=1e-12)
        assert -1.0 - 1e-9 <= m[0][1] <= 1.0 + 1e-9

    def test_insufficient_overlap_is_null_not_a_number(self):
        # Two series that overlap on FEWER than MIN_OVERLAP_DAYS common dates → null, not a
        # confident correlation off a handful of points.
        long = _series("2026-01-05", [0.01 * ((i % 3) - 1) for i in range(MIN_OVERLAP_DAYS + 20)])
        short = _series("2026-06-01", [0.01, -0.02, 0.015, -0.01, 0.02])  # 5 days, no overlap window
        m, _ = _matrix({1: long, 2: short}, [1, 2])
        assert m[0][1] is None
        assert m[1][0] is None
        # ...and a series too short even with itself has a null diagonal (min_periods).
        assert m[1][1] is None

    def test_missing_series_is_all_null(self):
        s = _series("2026-01-05", [0.01] * (MIN_OVERLAP_DAYS + 3))
        m, obs = _matrix({1: s, 2: None}, [1, 2])
        assert obs[1] == 0
        assert m[0][1] is None and m[1][0] is None and m[1][1] is None


class TestReturnsFromCurve:
    def test_single_leg_returns_track_price_moves(self):
        # One 100%-weight holding priced daily. Its EUR curve IS the price series (rebased), so
        # the daily returns are the price's daily returns.
        series = [("2026-01-02", 100.0), ("2026-01-03", 110.0), ("2026-01-04", 99.0)]
        rets = _returns_from_curve([(1.0, series)], "2026-01-02", total_w=1.0)
        assert rets is not None
        assert math.isclose(rets["2026-01-03"], 0.10, abs_tol=1e-9)
        assert math.isclose(rets["2026-01-04"], 99.0 / 110.0 - 1.0, abs_tol=1e-9)

    def test_under_coverage_returns_none(self):
        # A priceable leg worth 40% of a 100% book — below MIN_COVERAGE — yields no series
        # rather than a renormalised invention.
        series = [("2026-01-02", 100.0), ("2026-01-03", 101.0)]
        rets = _returns_from_curve([(0.4, series)], "2026-01-02", total_w=1.0)
        assert rets is None
