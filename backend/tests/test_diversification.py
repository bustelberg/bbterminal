"""Unit tests for the diversifier correlation + blend math."""
from __future__ import annotations

import math

import numpy as np

from momentum.diversification import (
    _simulate_rebalance,
    align,
    annual_breakdown,
    analyze_pair,
    annualized_stats,
    blended_calendar_returns,
    component_return_since,
    monthly_records_to_returns,
    optimal_blend,
    optimize_portfolio,
    pearson,
    portfolio_current_state,
    prices_to_monthly_returns,
    rets_from_cum,
    simulate_portfolio,
    top_drawdowns,
)


class TestCalendarReturns:
    MONTHS = ["2025-12", "2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06"]
    RETS = [0.05, 0.10, 0.05, 0.03, 0.04, 0.02, -0.0098]

    def test_mtd_is_latest_month(self):
        cal = blended_calendar_returns(self.MONTHS, self.RETS, "2026-05-30")
        assert abs(cal.mtd_pct - (-0.98)) < 0.01

    def test_since_inception_excludes_partial_start_month(self):
        # Go-live 2026-05-30 → May's bar (mostly pre-inception) is excluded, so
        # since-inception == the June return == MTD.
        cal = blended_calendar_returns(self.MONTHS, self.RETS, "2026-05-30")
        assert abs(cal.since_inception_pct - cal.mtd_pct) < 1e-9

    def test_inception_at_month_start_includes_that_month(self):
        cal = blended_calendar_returns(self.MONTHS, self.RETS, "2026-05-01")
        # May (+2%) and June (-0.98%) compound: 1.02 * 0.9902 - 1.
        assert abs(cal.since_inception_pct - ((1.02 * 0.9902 - 1) * 100)) < 0.01

    def test_ytd_compounds_calendar_year_only(self):
        cal = blended_calendar_returns(self.MONTHS, self.RETS, "2026-05-30")
        eq = 1.0
        for r in self.RETS[1:]:   # Jan..Jun (Dec 2025 excluded)
            eq *= 1 + r
        assert abs(cal.ytd_pct - (eq - 1) * 100) < 0.01

    def test_empty(self):
        cal = blended_calendar_returns([], [], "2026-01-01")
        assert cal.mtd_pct is None and cal.ytd_pct is None and cal.since_inception_pct is None


class TestComponentReturnSince:
    def test_compounds_only_since_inception(self):
        series = {"2026-04": 0.10, "2026-05": 0.02, "2026-06": -0.0098}
        months = ["2026-04", "2026-05", "2026-06"]
        # Inception end-of-May → only June counts.
        assert abs(component_return_since(series, months, "2026-05-30") - (-0.98)) < 0.01
        # No inception → all three compound.
        full = (1.10 * 1.02 * 0.9902 - 1) * 100
        assert abs(component_return_since(series, months, "") - full) < 0.01


class TestRetsFromCum:
    def test_inverse_of_cum_curve(self):
        rets = [0.05, -0.02, 0.03]
        cum = []
        eq = 1.0
        for r in rets:
            eq *= 1 + r
            cum.append(round((eq - 1) * 100, 4))
        back = rets_from_cum(cum)
        assert all(abs(a - b) < 1e-6 for a, b in zip(back, rets))


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

    def test_strategy_pinned_and_sleeve_fills_rest(self):
        n = 60
        strat = self._months([(0.03 if i % 2 == 0 else -0.005) for i in range(n)])
        anti = self._months([(-0.005 if i % 2 == 0 else 0.03) for i in range(n)])
        opt = optimize_portfolio(strat, [("ANTI", anti)], core_pct=0.6)
        assert opt.assets == ["Strategy", "ANTI"]
        # No bonds: strategy = core (0.6); the ETF sleeve gets the rest (0.4).
        assert math.isclose(opt.weights[0], 0.6, abs_tol=1e-6)
        assert math.isclose(sum(opt.weights[1:]), 0.4, abs_tol=1e-6)

    def test_optimizer_prefers_better_sleeve_etf(self):
        # ANTI diversifies (anti-correlated); SAME is a clone of the strategy
        # (no benefit). The sleeve should tilt toward ANTI.
        n = 60
        strat = self._months([(0.03 if i % 2 == 0 else -0.005) for i in range(n)])
        anti = self._months([(-0.005 if i % 2 == 0 else 0.03) for i in range(n)])
        same = self._months([(0.03 if i % 2 == 0 else -0.005) for i in range(n)])
        opt = optimize_portfolio(strat, [("ANTI", anti), ("SAME", same)], core_pct=0.4)
        wmap = dict(zip(opt.assets, opt.weights))
        assert wmap["ANTI"] > wmap["SAME"]

    def test_bonds_share_the_core(self):
        # Strategy + BOND form the 0.6 core; the ETF sleeve is 0.4.
        n = 60
        strat = self._months([(0.03 if i % 2 == 0 else -0.005) for i in range(n)])
        bond = self._months([0.004] * n)
        etf = self._months([(0.02 if i % 3 == 0 else -0.003) for i in range(n)])
        opt = optimize_portfolio(strat, [("ETF", etf)], bonds=[("BOND", bond)], core_pct=0.6)
        assert opt.assets == ["Strategy", "BOND", "ETF"]
        # Core (strategy + bond) sums to 0.6; the diversifier sleeve to 0.4.
        assert math.isclose(opt.weights[0] + opt.weights[1], 0.6, abs_tol=1e-6)
        assert math.isclose(opt.weights[2], 0.4, abs_tol=1e-6)

    def test_all_bond_when_strategy_is_bad(self):
        # A high-vol, zero-mean strategy vs a steady positive bond: for Sharpe
        # the core should tilt heavily to the bond (strategy share near 0).
        rng = np.random.default_rng(2)
        n = 60
        strat = self._months((0.08 * rng.standard_normal(n)).tolist())   # junk
        bond = self._months([0.005] * n)                                  # steady
        etf = self._months([0.004] * n)
        opt = optimize_portfolio(strat, [("ETF", etf)], bonds=[("BOND", bond)], core_pct=0.6)
        assert opt.weights[0] < opt.weights[1]   # bond > strategy within the core

    def test_after_is_the_monthly_rebalanced_static_blend(self):
        # The optimizer's portfolio is rebalanced back to target EVERY month, so
        # `after` is the fixed-weight (static) blend of the chosen weights — no
        # drift/band events — and it equals exactly the objective the optimizer
        # maximized.
        from momentum.diversification import annualized_stats

        n = 48
        strat = self._months([(0.04 if i % 2 == 0 else -0.01) for i in range(n)])
        div = self._months([(0.01 if i % 3 == 0 else -0.002) for i in range(n)])
        opt = optimize_portfolio(strat, [("DIV", div)], core_pct=0.6)
        assert opt.rebalance_count == 0      # monthly rebalance → no band events
        assert opt.rebalance_dates == []
        # `after` is the static blend of opt.weights, reconstructed independently.
        ms = sorted(set(strat) & set(div))
        R = np.array([[strat[m], div[m]] for m in ms])
        st = annualized_stats((R @ np.array(opt.weights)).tolist(), 0.0)
        assert math.isclose(opt.after.sharpe, st.sharpe, rel_tol=1e-9)
        assert math.isclose(opt.after.sortino, st.sortino, rel_tol=1e-9)

    def test_core_100_is_strategy_alone(self):
        n = 24
        strat = self._months([(0.03 if i % 2 == 0 else -0.01) for i in range(n)])
        etf = self._months([0.01] * n)
        opt = optimize_portfolio(strat, [("X", etf)], core_pct=1.0)
        assert math.isclose(opt.weights[0], 1.0, abs_tol=1e-9)
        assert opt.rebalance_count == 0

    def test_common_window_and_limited_by(self):
        # Strategy spans 2010-01.., ETF starts a year later -> window starts at
        # the ETF's first month and `limited_by` names it.
        strat = self._months([0.01] * 36, start_idx=0)            # 2010-01 .. 2012-12
        etf = self._months([0.02] * 24, start_idx=12)             # 2011-01 .. 2012-12
        opt = optimize_portfolio(strat, [("LATE", etf)], core_pct=0.6)
        assert opt.period_from == "2011-01"
        assert opt.months == 24
        assert opt.limited_by == "LATE"

    def test_no_etfs_is_strategy_only(self):
        strat = self._months([0.01, 0.02, -0.01, 0.03])
        opt = optimize_portfolio(strat, [], core_pct=0.6)
        assert opt.weights == [1.0]
        assert opt.rebalance_count == 0
        assert opt.after.sharpe == opt.before.sharpe

    def test_weights_are_on_the_2p5_grid(self):
        # Every weight must be a multiple of 2.5% (discrete search), and sum to 1.
        n = 60
        strat = self._months([(0.03 if i % 2 == 0 else -0.005) for i in range(n)])
        anti = self._months([(-0.005 if i % 2 == 0 else 0.03) for i in range(n)])
        gold = self._months([(0.01 if i % 3 == 0 else -0.002) for i in range(n)])
        opt = optimize_portfolio(strat, [("ANTI", anti), ("GOLD", gold)], core_min=0.4, core_max=0.6)
        for w in opt.weights:
            assert abs(round(w / 0.025) - w / 0.025) < 1e-9, f"{w} not on the 2.5% grid"
        assert math.isclose(sum(opt.weights), 1.0, abs_tol=1e-9)

    def test_core_range_is_respected(self):
        # With no bonds the core == the strategy weight; it must land inside
        # [core_min, core_max] (on the grid), not be pinned to a single value.
        n = 60
        strat = self._months([(0.03 if i % 2 == 0 else -0.005) for i in range(n)])
        anti = self._months([(-0.005 if i % 2 == 0 else 0.03) for i in range(n)])
        opt = optimize_portfolio(strat, [("ANTI", anti)], core_min=0.30, core_max=0.50)
        assert 0.30 - 1e-9 <= opt.weights[0] <= 0.50 + 1e-9

    def test_optimizer_is_deterministic(self):
        # Same inputs + settings ⇒ byte-identical weights, every run (fixed seed,
        # RNG reseeded per core-weight). Repeatability guarantee.
        n = 90
        strat = self._months([(0.03 if i % 2 == 0 else -0.01) for i in range(n)])
        e1 = self._months([(0.01 if i % 3 == 0 else -0.003) for i in range(n)])
        e2 = self._months([(-0.004 if i % 2 == 0 else 0.02) for i in range(n)])
        etfs = [("E1", e1), ("E2", e2)]
        a = optimize_portfolio(strat, etfs, core_min=0.0, core_max=1.0, objective="sortino")
        b = optimize_portfolio(strat, etfs, core_min=0.0, core_max=1.0, objective="sortino")
        assert a.weights == b.weights

    def test_more_restarts_never_worse(self):
        # search_restarts is configurable and MONOTONIC: each restart adds a
        # seeded start and we keep the best, so more restarts can only match or
        # beat fewer (never worse) — lower chance of missing the global optimum.
        from momentum.diversification import annualized_stats

        rng = np.random.default_rng(7)
        nn = 72
        strat = self._months((0.012 + 0.05 * rng.standard_normal(nn)).tolist())
        etfs = [(f"E{k}", self._months((0.003 + 0.03 * rng.standard_normal(nn)).tolist())) for k in range(6)]
        ms = sorted(set.intersection(set(strat), *[set(e[1]) for e in etfs]))
        R = np.array([[strat[m]] + [e[1][m] for e in etfs] for m in ms])

        def obj(opt):
            return annualized_stats((R @ np.array(opt.weights)).tolist(), 0.0).sortino

        low = optimize_portfolio(strat, etfs, core_min=0.0, core_max=1.0, objective="sortino", search_restarts=1)
        high = optimize_portfolio(strat, etfs, core_min=0.0, core_max=1.0, objective="sortino", search_restarts=8)
        assert obj(high) >= obj(low) - 1e-9

    def test_core_pct_back_compat_pins_the_core(self):
        # The legacy single `core_pct` still pins the core (min == max).
        n = 60
        strat = self._months([(0.03 if i % 2 == 0 else -0.005) for i in range(n)])
        anti = self._months([(-0.005 if i % 2 == 0 else 0.03) for i in range(n)])
        opt = optimize_portfolio(strat, [("ANTI", anti)], core_pct=0.575)
        assert math.isclose(opt.weights[0], 0.575, abs_tol=1e-9)  # 0.575 = 23×2.5%


class TestSimulatePortfolio:
    def _months(self, vals, start_idx=0):
        out = {}
        for i, v in enumerate(vals):
            idx = start_idx + i
            y, m = 2010 + idx // 12, idx % 12 + 1
            out[f"{y:04d}-{m:02d}"] = v
        return out

    def test_normalizes_and_holds_target(self):
        n = 36
        strat = self._months([0.01] * n)
        gld = self._months([0.0] * n)
        # 60/20/20 but passed as 6/2/2 — should normalize. Flat returns → no drift.
        opt = simulate_portfolio([
            ("Strategy", strat, 6, 0.10),
            ("GLD", gld, 2, 0.10),
            ("UUP", self._months([0.0] * n), 2, 0.10),
        ])
        assert opt.assets == ["Strategy", "GLD", "UUP"]
        assert math.isclose(opt.weights[0], 0.6, abs_tol=1e-6)
        assert math.isclose(opt.weights[1], 0.2, abs_tol=1e-6)
        assert opt.rebalance_count == 0   # nothing drifts

    def test_rebalances_on_band_breach(self):
        # Strategy compounds fast vs flat funds → its weight breaches the band.
        n = 48
        strat = self._months([0.08] * n)
        flat = self._months([0.0] * n)
        opt = simulate_portfolio([
            ("Strategy", strat, 60, 0.10),
            ("GLD", flat, 40, 0.10),
        ])
        assert opt.rebalance_count > 0
        # `after` is the rebalanced portfolio; `before` is strategy alone.
        assert opt.before.ann_return is not None and opt.after.ann_return is not None

    def test_zero_band_never_triggers(self):
        n = 24
        strat = self._months([0.05] * n)
        flat = self._months([0.0] * n)
        opt = simulate_portfolio([("Strategy", strat, 60, 0.0), ("X", flat, 40, 0.0)])
        assert opt.rebalance_count == 0


class TestPortfolioCurrentState:
    def _months(self, vals, start_idx=0):
        out = {}
        for i, v in enumerate(vals):
            idx = start_idx + i
            y, m = 2010 + idx // 12, idx % 12 + 1
            out[f"{y:04d}-{m:02d}"] = v
        return out

    def test_no_drift_no_rebalance(self):
        n = 12
        st = portfolio_current_state([
            ("Strategy", self._months([0.0] * n), 60, 0.10),
            ("GLD", self._months([0.0] * n), 40, 0.10),
        ])
        assert st.enough_data and not st.rebalance_needed
        assert math.isclose(st.holdings[0].current, 0.6, abs_tol=1e-6)
        assert st.as_of == self._months([0.0] * n).popitem()[0] or st.as_of is not None

    def test_drift_breaches_and_flags_rebalance(self):
        # Strategy keeps outgrowing the flat fund → on the final month it's drifted
        # past its band, so a rebalance is flagged.
        n = 6
        st = portfolio_current_state([
            ("Strategy", self._months([0.2] * n), 60, 0.10),
            ("GLD", self._months([0.0] * n), 40, 0.10),
        ])
        assert st.enough_data
        # Strategy is the breached one (drifted above 70%).
        assert st.holdings[0].current > 0.70
        assert st.holdings[0].breached and st.rebalance_needed

    def test_empty_when_no_overlap(self):
        st = portfolio_current_state([
            ("Strategy", {"2010-01": 0.01}, 60, 0.10),
            ("GLD", {"2020-01": 0.01}, 40, 0.10),
        ])
        assert not st.enough_data


class TestSimulateRebalance:
    CORE = np.array([0])   # core = strategy only (no bonds) in these tests

    def test_triggers_when_core_grows_above_band(self):
        months = [f"2020-{m:02d}" for m in range(1, 13)]
        R = np.array([[0.2, 0.0]] * 12)   # strategy +20%/mo, ETF flat → core drifts up
        rets, rebals = _simulate_rebalance(months, R, np.array([0.6, 0.4]), self.CORE, 0.6, 0.1)
        assert len(rets) == 12
        assert len(rebals) >= 1

    def test_triggers_when_core_drops_below_band(self):
        months = [f"2020-{m:02d}" for m in range(1, 13)]
        R = np.array([[-0.2, 0.0]] * 12)  # strategy -20%/mo, ETF flat → core drifts down
        _rets, rebals = _simulate_rebalance(months, R, np.array([0.6, 0.4]), self.CORE, 0.6, 0.1)
        assert len(rebals) >= 1

    def test_no_trigger_inside_band(self):
        months = ["2020-01", "2020-02"]
        R = np.array([[0.0, 0.0], [0.0, 0.0]])   # no drift → stays at center
        rets, rebals = _simulate_rebalance(months, R, np.array([0.6, 0.4]), self.CORE, 0.6, 0.1)
        assert rebals == []
        assert all(abs(x) < 1e-12 for x in rets)

    def test_core_spans_strategy_plus_bonds(self):
        # Core = strategy + bond (indices 0,1); ETF index 2 is the sleeve.
        # Strategy+bond grow, ETF flat → the COMBINED core drifts above the band.
        months = [f"2020-{m:02d}" for m in range(1, 13)]
        R = np.array([[0.2, 0.2, 0.0]] * 12)
        _rets, rebals = _simulate_rebalance(
            months, R, np.array([0.3, 0.3, 0.4]), np.array([0, 1]), 0.6, 0.1
        )
        assert len(rebals) >= 1


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
