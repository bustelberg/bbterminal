"""Unit tests for the diversifier correlation + blend math."""
from __future__ import annotations

import math

import numpy as np

from momentum.diversification import (
    align,
    annual_breakdown,
    analyze_pair,
    annualized_stats,
    monthly_records_to_returns,
    optimal_blend,
    optimize_portfolio,
    pearson,
    prices_to_monthly_returns,
    top_drawdowns,
)


class TestPricesToMonthlyReturns:
    def test_month_end_resample(self):
        prices = [
            ("2024-01-10", 100.0),
            ("2024-01-31", 110.0),   # Jan close
            ("2024-02-15", 121.0),   # Feb close (last in month)
            ("2024-03-29", 121.0),   # Mar close, flat
        ]
        r = prices_to_monthly_returns(prices)
        # First month (Jan) has no prior anchor -> dropped.
        assert "2024-01" not in r
        assert math.isclose(r["2024-02"], 0.10, abs_tol=1e-9)   # 121/110 - 1
        assert math.isclose(r["2024-03"], 0.0, abs_tol=1e-9)

    def test_accepts_date_objects_and_skips_bad(self):
        from datetime import date
        prices = [
            (date(2024, 1, 31), 100.0),
            (date(2024, 2, 29), 50.0),
            (date(2024, 3, 29), None),   # skipped
            (date(2024, 4, 30), -5.0),   # skipped (<=0)
        ]
        r = prices_to_monthly_returns(prices)
        assert math.isclose(r["2024-02"], -0.5, abs_tol=1e-9)
        assert "2024-03" not in r and "2024-04" not in r


class TestMonthlyRecordsToReturns:
    def test_derived_from_cumulative(self):
        records = [
            {"date": "2024-01", "cumulative_return_pct": 10.0},   # +10%
            {"date": "2024-02", "cumulative_return_pct": 21.0},   # (1.21/1.10)-1 = +10%
            {"date": "2024-03", "cumulative_return_pct": 21.0},   # flat
        ]
        r = monthly_records_to_returns(records)
        assert math.isclose(r["2024-01"], 0.10, abs_tol=1e-9)
        assert math.isclose(r["2024-02"], 0.10, abs_tol=1e-9)
        assert math.isclose(r["2024-03"], 0.0, abs_tol=1e-9)

    def test_skips_open_period(self):
        records = [
            {"date": "2024-01", "cumulative_return_pct": 5.0},
            {"date": "2024-02", "cumulative_return_pct": 8.0, "is_open": True},
        ]
        r = monthly_records_to_returns(records)
        assert set(r) == {"2024-01"}


class TestAlignAndPearson:
    def test_align_common_months(self):
        a = {"2024-01": 0.1, "2024-02": 0.2, "2024-03": 0.3}
        b = {"2024-02": 1.0, "2024-03": 2.0, "2024-04": 3.0}
        av, bv, months = align(a, b)
        assert months == ["2024-02", "2024-03"]
        assert av == [0.2, 0.3] and bv == [1.0, 2.0]

    def test_perfect_correlation(self):
        a = [0.01, 0.02, -0.01, 0.03]
        b = [0.02, 0.04, -0.02, 0.06]   # exactly 2x
        assert math.isclose(pearson(a, b), 1.0, abs_tol=1e-9)

    def test_anticorrelation(self):
        a = [0.01, 0.02, -0.01, 0.03]
        b = [-0.01, -0.02, 0.01, -0.03]
        assert math.isclose(pearson(a, b), -1.0, abs_tol=1e-9)

    def test_undefined_when_flat_or_short(self):
        assert pearson([0.1, 0.1, 0.1], [0.1, 0.2, 0.3]) is None   # zero variance
        assert pearson([0.1], [0.2]) is None                       # too short


class TestAnnualizedStats:
    def test_known_values(self):
        # 12 months of +1% each -> ann_return = 1.01^12 - 1
        r = [0.01] * 12
        st = annualized_stats(r)
        assert math.isclose(st.ann_return, 1.01 ** 12 - 1, rel_tol=1e-9)
        # Zero vol -> Sharpe/Sortino undefined (no risk).
        assert st.ann_vol == 0.0
        assert st.sharpe is None
        assert st.sortino is None

    def test_sharpe_sign_and_sortino_present(self):
        r = [0.03, -0.02, 0.04, -0.01, 0.02, 0.01]
        st = annualized_stats(r, rf_annual=0.0)
        assert st.sharpe is not None and st.sortino is not None
        # Net positive series -> positive Sharpe.
        assert st.sharpe > 0
        # 6 months, 4 positive -> win rate 4/6; median of the sorted returns.
        assert math.isclose(st.win_rate, 4 / 6, abs_tol=1e-9)
        assert math.isclose(st.median_month, 0.015, abs_tol=1e-9)


class TestOptimalBlend:
    def test_never_below_baseline(self):
        # A pure-noise ETF can't beat a strong strategy -> weight should
        # land at 0 (the baseline is always in the grid).
        rng = np.random.default_rng(0)
        strat = (0.02 + 0.005 * rng.standard_normal(60)).tolist()
        etf = (0.05 * rng.standard_normal(60)).tolist()   # zero-mean, high vol
        b = optimal_blend(strat, etf, w_max=0.5, step=0.05)
        base = annualized_stats(strat)
        assert b.weight == 0.0
        assert math.isclose(b.sharpe, base.sharpe, rel_tol=1e-9)

    def test_diversifier_lifts_sharpe(self):
        # Two positive-drift, NEGATIVELY-correlated series: blending must
        # raise Sharpe, so the optimizer picks a non-zero weight.
        n = 60
        base = [0.02 if i % 2 == 0 else -0.005 for i in range(n)]
        anti = [-0.005 if i % 2 == 0 else 0.02 for i in range(n)]
        b = optimal_blend(base, anti, w_max=0.5, step=0.05)
        assert b.weight > 0.0
        assert b.sharpe > annualized_stats(base).sharpe

    def test_objective_returns_grid_max_for_its_metric(self):
        # The core contract: whichever objective is chosen, the returned weight
        # is the grid point that maximizes THAT metric. Brute-force the same
        # grid and confirm no weight beats the returned one.
        rng = np.random.default_rng(3)
        strat = (0.012 + 0.04 * rng.standard_normal(72)).tolist()
        etf = (0.004 + 0.05 * rng.standard_normal(72)).tolist()
        s, e = np.asarray(strat), np.asarray(etf)

        def metric_at(w: float, obj: str):
            st = annualized_stats(((1 - w) * s + w * e).tolist())
            return st.sortino if obj == "sortino" else st.sharpe

        for obj in ("sharpe", "sortino"):
            best = optimal_blend(strat, etf, w_max=0.5, step=0.05, objective=obj)
            best_metric = metric_at(best.weight, obj)
            assert best_metric is not None
            for k in range(11):  # 0.00 .. 0.50
                m = metric_at(round(k * 0.05, 6), obj)
                if m is not None:
                    assert best_metric >= m - 1e-9

    def test_objective_still_reports_both_stats(self):
        b = optimal_blend([0.01] * 12, [0.02] * 12, objective="sortino")
        assert hasattr(b, "sharpe") and hasattr(b, "sortino")

    def test_grid_is_inclusive_and_clean(self):
        b = optimal_blend([0.01] * 12, [0.02] * 12, w_max=0.5, step=0.05)
        # Weight is a clean multiple of step within [0, 0.5].
        assert 0.0 <= b.weight <= 0.5
        assert math.isclose((b.weight / 0.05) % 1.0, 0.0, abs_tol=1e-9)


class TestOptimizePortfolio:
    def _months(self, vals: list[float], start_idx: int = 0) -> dict[str, float]:
        # Build a YYYY-MM keyed series of len(vals) months from 2010-01.
        out = {}
        for i, v in enumerate(vals):
            idx = start_idx + i
            y, m = 2010 + idx // 12, idx % 12 + 1
            out[f"{y:04d}-{m:02d}"] = v
        return out

    def test_anticorrelated_diversifier_gets_weight(self):
        n = 60
        strat = self._months([(0.03 if i % 2 == 0 else -0.005) for i in range(n)])
        anti = self._months([(-0.005 if i % 2 == 0 else 0.03) for i in range(n)])
        opt = optimize_portfolio(strat, [("ANTI", anti)], objective="sharpe", max_total_etf=0.5)
        assert opt.assets == ["Strategy", "ANTI"]
        assert abs(sum(opt.weights) - 1.0) < 1e-6
        # The anti-correlated sleeve cuts vol hard → it takes meaningful weight
        # (up to the cap) and Sharpe rises vs strategy-alone.
        assert opt.weights[1] > 0.1
        assert opt.after.sharpe > opt.before.sharpe

    def test_useless_etf_stays_near_zero(self):
        rng = np.random.default_rng(1)
        n = 60
        strat = self._months((0.02 + 0.01 * rng.standard_normal(n)).tolist())
        junk = self._months((0.06 * rng.standard_normal(n)).tolist())  # zero-mean noise
        opt = optimize_portfolio(strat, [("JUNK", junk)], objective="sharpe", max_total_etf=0.5)
        assert opt.weights[1] < 0.1
        assert opt.after.sharpe >= opt.before.sharpe - 1e-9   # never worse than baseline

    def test_etf_sleeve_cap_respected(self):
        n = 60
        strat = self._months([(0.03 if i % 2 == 0 else -0.005) for i in range(n)])
        anti = self._months([(-0.005 if i % 2 == 0 else 0.03) for i in range(n)])
        opt = optimize_portfolio(strat, [("ANTI", anti)], max_total_etf=0.2)
        assert sum(opt.weights[1:]) <= 0.2 + 1e-6

    def test_common_window_and_limited_by(self):
        # Strategy spans 2010-01.., ETF starts a year later -> window starts at
        # the ETF's first month and `limited_by` names it.
        strat = self._months([0.01] * 36, start_idx=0)            # 2010-01 .. 2012-12
        etf = self._months([0.02] * 24, start_idx=12)             # 2011-01 .. 2012-12
        opt = optimize_portfolio(strat, [("LATE", etf)], max_total_etf=0.5)
        assert opt.period_from == "2011-01"
        assert opt.months == 24
        assert opt.limited_by == "LATE"

    def test_no_etfs_is_strategy_only(self):
        strat = self._months([0.01, 0.02, -0.01, 0.03])
        opt = optimize_portfolio(strat, [], max_total_etf=0.5)
        assert opt.weights == [1.0]
        assert opt.after.sharpe == opt.before.sharpe


class TestAnnualBreakdown:
    def test_per_year_return_and_vol(self):
        # 2023: two +10% months. 2024: +10%, -5%.
        months = ["2023-11", "2023-12", "2024-01", "2024-02"]
        before = [0.10, 0.10, 0.10, -0.05]
        after = [0.05, 0.05, 0.02, 0.02]
        rows = annual_breakdown(months, before, after)
        assert [r.year for r in rows] == [2023, 2024]
        # 2023 before: 1.1*1.1 - 1 = 0.21
        assert math.isclose(rows[0].return_before, 0.21, abs_tol=1e-9)
        # 2024 before: 1.10*0.95 - 1 = 0.045
        assert math.isclose(rows[1].return_before, 0.045, abs_tol=1e-9)
        # Vol is defined (each year has 2 months).
        assert rows[0].vol_before is not None and rows[1].vol_after is not None
        # Each year carries its per-month rows.
        assert [m.month for m in rows[0].months] == ["2023-11", "2023-12"]
        assert math.isclose(rows[1].months[1].return_before, -0.05, abs_tol=1e-9)

    def test_single_month_year_has_no_vol(self):
        rows = annual_breakdown(["2025-06"], [0.03], [0.01])
        assert rows[0].vol_before is None
        assert math.isclose(rows[0].return_before, 0.03, abs_tol=1e-9)

    def test_empty(self):
        assert annual_breakdown([], [], []) == []


class TestTopDrawdowns:
    def test_single_drawdown_with_recovery(self):
        # +10%, then -20%, -20% (trough), then back up to recover.
        months = ["2020-01", "2020-02", "2020-03", "2020-04", "2020-05"]
        rets = [0.10, -0.20, -0.20, 0.30, 0.40]
        dds = top_drawdowns(months, rets, top_n=10)
        assert len(dds) == 1
        d = dds[0]
        # Peak after Jan (+10%), trough at Mar after two -20% months.
        assert d.peak_date == "2020-01"
        assert d.trough_date == "2020-03"
        assert d.recovery_date is not None
        assert d.depth_pct < -30   # 0.8*0.8 - 1 = -36%

    def test_ongoing_drawdown_has_no_recovery(self):
        months = ["2020-01", "2020-02", "2020-03"]
        rets = [0.05, -0.10, -0.10]   # never recovers
        dds = top_drawdowns(months, rets)
        assert len(dds) == 1
        assert dds[0].recovery_date is None

    def test_ranked_and_capped(self):
        # Several separate crashes; ensure sorted worst-first and capped.
        months = [f"20{y:02d}-01" for y in range(20, 32)]
        rets = [0.1, -0.3, 0.5, -0.1, 0.3, -0.5, 0.8, -0.05, 0.2, -0.4, 0.6, -0.02]
        dds = top_drawdowns(months, rets, top_n=2)
        assert len(dds) == 2
        assert dds[0].depth_pct <= dds[1].depth_pct   # worst first

    def test_no_drawdown_when_monotonic(self):
        assert top_drawdowns(["a", "b", "c"], [0.01, 0.02, 0.03]) == []


class TestAnalyzePair:
    def test_end_to_end_shape(self):
        strat = {f"2024-{m:02d}": 0.01 for m in range(1, 13)}
        etf = {f"2024-{m:02d}": (0.02 if m % 2 else -0.01) for m in range(1, 13)}
        out = analyze_pair(strat, etf, rf_annual=0.0)
        assert out["overlap_months"] == 12
        assert out["overlap_from"] == "2024-01" and out["overlap_to"] == "2024-12"
        assert "correlation" in out and "blend_weight" in out
        assert "sharpe_lift" in out and "sortino_lift" in out

    def test_thin_overlap(self):
        strat = {"2024-01": 0.01, "2024-02": 0.02}
        etf = {"2024-02": 0.03, "2024-03": 0.04}
        out = analyze_pair(strat, etf)
        assert out["overlap_months"] == 1
        # Single-point overlap -> correlation undefined, stats degrade gracefully.
        assert out["correlation"] is None
