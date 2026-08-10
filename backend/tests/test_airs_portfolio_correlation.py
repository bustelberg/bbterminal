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
    _series_block,
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


class TestSeriesBlock:
    """The encoding the instrument table charts from — one shared date axis, per-key columns.

    ⚠ THE ENCODING IS A MEASUREMENT, NOT A STYLE CHOICE. See `_series_block`: the obvious
    `[[date, value], …]` per instrument costs 1,270 KB raw against this shape's 452 KB on the
    real book, because it repeats a 10-byte date string once per instrument per trading day.
    """

    def test_one_axis_is_the_union_and_a_gap_is_null_not_zero(self):
        # ⚠ THE ASSERTION THIS CLASS EXISTS FOR. Two venues on different calendars: A trades on
        # the 6th, B does not. B's column must carry None there — a 0.0 would be a price of zero,
        # which the sparkline draws as a crash to the floor and back on every foreign holiday.
        eur = {
            1: [("2026-01-02", 10.0), ("2026-01-06", 12.0)],
            2: [("2026-01-02", 100.0), ("2026-01-05", 101.0)],
        }
        inst = {
            "A": {"series_key": "a:1", "state": "direct"},
            "B": {"series_key": "a:2", "state": "direct"},
        }
        block = _series_block(inst, eur, {}, "2026-01-01")

        assert block["dates"] == ["2026-01-02", "2026-01-05", "2026-01-06"]
        assert block["values"]["a:1"] == [10.0, None, 12.0]
        assert block["values"]["a:2"] == [100.0, 101.0, None]

    def test_it_trims_to_the_window_start(self):
        eur = {1: [("2025-06-01", 5.0), ("2026-01-02", 10.0)]}
        block = _series_block({"A": {"series_key": "a:1"}}, eur, {}, "2026-01-01")
        assert block["dates"] == ["2026-01-02"]
        assert block["values"]["a:1"] == [10.0]

    def test_a_lookthrough_key_reads_the_wrapped_models_curve(self):
        # ⚠ `p:` NOT `a:` — a certificate has no asset series of its own. The two id spaces are
        # disjoint sets of small integers, so keying them both as bare numbers would collide
        # analysis_id 7 with portfolio 7 and chart one as the other.
        eur = {7: [("2026-01-02", 50.0)]}
        look = {7: [("2026-01-02", 100.0), ("2026-01-03", 103.0)]}
        block = _series_block({"C": {"series_key": "p:7"}}, eur, look, "2026-01-01")
        assert block["values"] == {"p:7": [100.0, 103.0]}

    def test_an_unpriced_instrument_contributes_no_column(self):
        # series_key is None; it must not appear as an empty column, which would chart as a
        # flat line where the honest answer is "there is no series".
        block = _series_block({"D": {"series_key": None, "state": "unpriced"}}, {}, {}, "2026-01-01")
        assert block == {"dates": [], "values": {}}

    def test_two_instruments_sharing_one_listing_share_one_column(self):
        # Deduped on the key, so a listing held under two ISINs is not stored (or shipped) twice.
        eur = {1: [("2026-01-02", 10.0)]}
        inst = {"A": {"series_key": "a:1"}, "B": {"series_key": "a:1"}}
        block = _series_block(inst, eur, {}, "2026-01-01")
        assert list(block["values"]) == ["a:1"]
