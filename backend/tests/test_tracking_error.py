"""TE = √(Σ(aₜ−ā)²/(T−1)) · √f, and every test is a closed-form identity rather than a fixture.

 THE TWO CASES THAT SEPARATE TE FROM THE THING IT IS CONSTANTLY CONFUSED WITH are the first two:
a book identical to its index has zero TE, and a book beating its index by exactly the same amount
every single period ALSO has zero TE while having a large active return. Tracking error is the
SPREAD of the active return, never the active return.

 AND THE DEFINITION IS PINNED, NOT ASSUMED. `ā` subtracted, divisor `T−1`. The other convention
(√(Σaₜ²/T)) is also called tracking error and reads higher; the test asserts which one this is, so
a future "simplification" to `np.std(a)` fails here rather than in somebody's risk report.
"""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pytest

import routers._tracking_error as T

BENCH = "IE00B6R52259"      # the ACWI tracker `_BENCHMARK_RISK_ETF` maps to
BOND_BENCH = "IE00BDBRDM35" # the EUR-hedged AGGH tracker used by risk profiles
ONE = "US0000000001"
BOND = "IE0000000002"


def _weekdays(n: int) -> list[str]:
    out, d = [], date.today()
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d -= timedelta(days=1)
    return sorted(out)


def _prices(returns: list[float], dates: list[str], start: float = 100.0):
    v, out = start, []
    for d, r in zip(dates, [0.0, *returns]):
        v *= 1.0 + r
        out.append((d, v))
    return out


@pytest.fixture
def wire(monkeypatch):
    """Patch the price load so one holding IS the portfolio and one ISIN is the tracker."""
    def _wire(port: list[float], bench: list[float], dates: list[str]):
        import routers._airs_portfolio_analysis as PA
        monkeypatch.setattr(PA, "_daily_eur", lambda isins, years: {
            ONE: _prices(port, dates), BENCH: _prices(bench, dates)})
    return _wire


def _run(freq: str = "daily", holdings=None, *, require_complete_return: bool = False):
    return T.compute_tracking_error(
        holdings if holdings is not None
        else [{"isin": ONE, "name": "One", "weight_pct": 100.0, "is_fund": False}],
        "ACWI", frequency=freq, require_complete_return=require_complete_return)


class TestBenchmarkHistory:
    def test_new_holding_joins_after_two_prices_and_old_weights_are_renormalised(self):
        days = ["2021-10-19", "2024-10-19", "2026-10-19"]
        built = {
            "benchmark_component_weights": {"ACWI": 100},
            "benchmark_component_isins": {"ACWI": BENCH},
            "weight": {ONE: .6, BOND: .4}, "names": {ONE: "Old", BOND: "New"},
            "series": {
                BENCH: list(zip(days, [100, 100, 100])),
                ONE: list(zip(days, [100, 110, 121])),
                BOND: list(zip(days[1:], [100, 120])),
            },
        }
        got = T._current_weights_history(built, 5)
        assert got["available"]
        # First period earns the old holding's 10% at 100% weight. Second earns 60/40 of 10/20%.
        assert got["total_return_pct"] == pytest.approx((1.1 * 1.14 - 1) * 100)
        assert got["min_weight_covered_pct"] == pytest.approx(60)
        assert got["avg_weight_covered_pct"] == pytest.approx(80)
        assert got["coverage_adjusted"]
        assert got["holdings_history"][1]["first_used"] == days[1]
        assert got["active_return_pp"] == pytest.approx(got["portfolio_cagr_pct"])

    def test_no_priced_holdings_for_a_period_is_not_a_flat_return(self):
        built = {
            "benchmark_component_weights": {"ACWI": 100},
            "benchmark_component_isins": {"ACWI": BENCH},
            "weight": {ONE: 1}, "names": {},
            "series": {BENCH: [("2021-10-19", 100), ("2026-10-19", 120)], ONE: []},
        }
        got = T._current_weights_history(built, 5)
        assert not got["available"]
        assert "No holdings" in got["reason"]

    def test_current_weights_five_year_comparison_uses_matching_benchmark_dates(self, wire):
        dates = _weekdays(1600)
        returns = [0.0001] * (len(dates) - 1)
        wire(returns, returns, dates)
        result = _run("monthly", require_complete_return=True)
        comparison = result["historical_comparison"]
        assert comparison["available"]
        assert comparison["portfolio_cagr_pct"] == pytest.approx(comparison["benchmark_cagr_pct"])
        assert comparison["active_return_pp"] == pytest.approx(0)
        assert comparison["start_date"] == result["benchmark_history"]["start_date"]
        assert comparison["end_date"] == result["benchmark_history"]["end_date"]

    def test_young_portfolio_does_not_shorten_five_year_benchmark(self):
        built = {
            "keys": ["2026-09-10", "2026-10-19"],
            "benchmark_component_weights": {"ACWI": 60, "BONDS": 40},
            "benchmark_component_isins": {"ACWI": BENCH, "BONDS": BOND_BENCH},
            "series": {
                BENCH: [("2021-10-19", 100), ("2026-09-10", 150), ("2026-10-19", 160)],
                BOND_BENCH: [("2021-10-19", 5), ("2026-09-10", 4.8), ("2026-10-19", 4.9)],
            },
        }
        got = T._benchmark_history(built, 5)
        assert got["start_date"] == "2021-10-19"
        assert got["end_date"] == "2026-10-19"
        assert got["complete"] is True
        stock = got["components"][0]
        assert stock["start_price_eur"] == 100
        assert stock["end_price_eur"] == 160
        assert stock["window_return_pct"] == pytest.approx(60)
        elapsed = (date.fromisoformat("2026-10-19") - date.fromisoformat("2021-10-19")).days / 365.25
        wealth = (1 + .6 * .5 + .4 * (-.04)) * (1 + .6 * (160 / 150 - 1) + .4 * (4.9 / 4.8 - 1))
        assert got["cagr_pct"] == pytest.approx((wealth ** (1 / elapsed) - 1) * 100)


class TestTheDefinition:
    def test_equity_only_policy_preserves_book_weights_without_bond_prices(self, wire):
        dates = _weekdays(400)
        T._risk_prices.cache_clear()
        wire([.02] * 399, [.03] * 399, dates)
        built = T.build_paired_series([
            {"isin": ONE, "name": "Stock", "weight_pct": 45, "bucket": "Equity"},
            {"isin": BOND, "name": "Bond", "weight_pct": 40, "bucket": "Bonds"},
        ], "ACWI", "daily", 1, benchmark_weights={"Stocks": 60, "Bonds": 40, "EquityOnly": 1})
        assert built["portfolio"][0] == pytest.approx(.45 * .02)
        assert built["benchmark"][0] == pytest.approx(.6 * .03)
        assert built["benchmark_label"] == "60% ACWI"
        assert built["benchmark_component_isins"] == {"ACWI": BENCH}
        assert built["total"] == 1

    def test_risk_profile_uses_bonds_in_the_portfolio_and_fixed_policy_benchmark(self, monkeypatch):
        """A Neutral profile is a 60/40 portfolio, never a stocks-only ACWI comparison."""
        dates = _weekdays(400)
        stock_returns = [0.02] * 399
        bond_returns = [0.01] * 399
        acwi_returns = [0.03] * 399
        aggh_returns = [0.005] * 399
        import routers._airs_portfolio_analysis as PA
        T._risk_prices.cache_clear()
        monkeypatch.setattr(PA, "_daily_eur", lambda isins, years: {
            ONE: _prices(stock_returns, dates), BOND: _prices(bond_returns, dates),
            BENCH: _prices(acwi_returns, dates), BOND_BENCH: _prices(aggh_returns, dates),
        })

        built = T.build_paired_series([
            {"isin": ONE, "name": "Stock", "weight_pct": 60.0, "bucket": "Equity"},
            {"isin": BOND, "name": "Bond", "weight_pct": 40.0, "bucket": "Bonds"},
            {"name": "Cash", "weight_pct": 10.0, "bucket": "Cash"},
        ], "ACWI", "daily", 5, benchmark_weights={"Stocks": 60, "Bonds": 40})

        assert built["portfolio"][0] == pytest.approx(0.016)
        assert built["benchmark"][0] == pytest.approx(0.020)
        assert built["benchmark_label"] == "60% ACWI + 40% AGGH.AS"
        assert built["total"] == 2

    def test_missing_aggh_history_names_the_broken_policy_tracker(self, monkeypatch):
        dates = _weekdays(400)
        import routers._airs_portfolio_analysis as PA
        T._risk_prices.cache_clear()
        monkeypatch.setattr(PA, "_daily_eur", lambda _isins, _years: {
            ONE: _prices([0.01] * 399, dates),
            BOND: _prices([0.0] * 399, dates),
            BENCH: _prices([0.01] * 399, dates),
        })
        with pytest.raises(T.SeriesError, match=r"AGGH\.AS"):
            T.build_paired_series([
                {"isin": ONE, "name": "Stock", "weight_pct": 60.0, "bucket": "Equity"},
                {"isin": BOND, "name": "Bond", "weight_pct": 40.0, "bucket": "Bonds"},
            ], "ACWI", "daily", 5, benchmark_weights={"Stocks": 60, "Bonds": 40})

    def test_non_stock_positions_stay_in_the_airs_denominator(self, wire):
        dates = _weekdays(400)
        wire([0.01] * 399, [0.0] * 399, dates)
        holdings = [
            {"isin": ONE, "name": "One", "weight_pct": 25.0, "is_fund": False},
            {"isin": "IE1", "name": "Fund", "weight_pct": 75.0, "is_fund": True},
        ]

        built = T.build_paired_series(holdings, "ACWI", "daily", 5)

        assert built["portfolio"][0] == pytest.approx(0.0025)
        assert built["weight_used"][0] == pytest.approx(0.25)

    def test_a_book_that_IS_the_index_has_no_tracking_error(self, wire):
        dates = _weekdays(400)
        b = list(np.random.default_rng(1).normal(0.0004, 0.01, 399))
        wire(b, b, dates)
        got = _run()
        assert got["available"]
        assert got["tracking_error_pct"] == pytest.approx(0.0, abs=1e-9)

    def test_a_CONSTANT_excess_is_a_big_active_return_and_zero_tracking_error(self, wire):
        """ THE CASE THAT DEFINES THE MEASURE. Beating the index by the same amount every period
        is not divergence — there is nothing to be volatile. A panel that showed only one of these
        two numbers would call this book either riskless or identical to its index."""
        dates = _weekdays(400)
        b = list(np.random.default_rng(2).normal(0.0004, 0.01, 399))
        wire([x + 0.0005 for x in b], b, dates)
        got = _run()
        assert got["tracking_error_pct"] == pytest.approx(0.0, abs=1e-6)
        assert got["mean_active_per_period_pct"] == pytest.approx(0.05, abs=1e-6)

    def test_it_is_the_bessel_corrected_annualised_sd_of_the_active_return(self, wire):
        dates = _weekdays(400)
        rng = np.random.default_rng(3)
        b = rng.normal(0.0004, 0.01, 399)
        p = b + rng.normal(0.0, 0.004, 399)
        wire(list(p), list(b), dates)
        got = _run()

        want = float(np.std(p - b, ddof=1) * np.sqrt(252) * 100)
        assert got["tracking_error_pct"] == pytest.approx(want, abs=0.05)
        #  ddof=1, NOT 0 — asserted against the population sd it must NOT equal.
        population = float(np.std(p - b, ddof=0) * np.sqrt(252) * 100)
        assert abs(got["tracking_error_pct"] - want) < abs(got["tracking_error_pct"] - population)

    def test_the_annualisation_constant_follows_the_cadence(self, wire):
        dates = _weekdays(700)
        rng = np.random.default_rng(4)
        b = rng.normal(0.0004, 0.01, 699)
        wire(list(b + rng.normal(0, 0.004, 699)), list(b), dates)
        assert _run("daily")["periods_per_year"] == 252.0
        assert _run("weekly")["periods_per_year"] == 52.0
        assert _run("monthly")["periods_per_year"] == 12.0

    def test_the_information_ratio_is_active_return_over_tracking_error(self, wire):
        dates = _weekdays(400)
        rng = np.random.default_rng(5)
        b = rng.normal(0.0004, 0.01, 399)
        p = b + rng.normal(0.0002, 0.004, 399)
        wire(list(p), list(b), dates)
        got = _run()
        assert got["information_ratio"] == pytest.approx(
            got["active_return_ann_pct"] / got["tracking_error_pct"], abs=1e-9)


class TestWhatItRefuses:
    def test_no_common_history_repairs_once_then_retries(self, monkeypatch):
        dates = _weekdays(500)
        prices = _prices([0.001] * (len(dates) - 1), dates)
        repaired = False
        import routers._airs_portfolio_analysis as PA

        def _load(_isins, _years):
            return {ONE: prices if repaired else [], BENCH: prices}

        def _repair(isins, years):
            nonlocal repaired
            assert {ONE, BENCH} <= isins
            assert years == 5
            repaired = True
            T._risk_prices.cache_clear()

        monkeypatch.setattr(PA, "_daily_eur", _load)
        monkeypatch.setattr(T, "_repair_risk_inputs", _repair)
        T._risk_prices.cache_clear()

        got = _run("monthly", require_complete_return=True)

        assert repaired is True
        assert got["available"] is True

    def test_no_common_history_names_unpriced_holdings_after_repair(self, monkeypatch):
        import routers._airs_portfolio_analysis as PA
        monkeypatch.setattr(PA, "_daily_eur", lambda _isins, _years: {ONE: [], BENCH: []})
        monkeypatch.setattr(T, "_repair_risk_inputs", lambda _isins, _years: None)
        T._risk_prices.cache_clear()

        got = _run("monthly", require_complete_return=True)

        assert got["available"] is False
        assert "Automatic price and FX repair" in got["reason"]
        assert "One" in got["reason"]

    def test_quality_gate_reports_ready_policy_inputs(self, monkeypatch):
        dates = _weekdays(1_600)
        prices = _prices([0.001] * (len(dates) - 1), dates)
        import routers._airs_portfolio_analysis as PA
        monkeypatch.setattr(PA, "_daily_eur", lambda isins, years: {
            ONE: prices, BOND: prices, BENCH: prices, BOND_BENCH: prices,
        })

        got = T.assess_risk_data_quality([
            {"isin": ONE, "name": "Stock", "weight_pct": 60.0, "bucket": "Equity"},
            {"isin": BOND, "name": "Bond", "weight_pct": 40.0, "bucket": "Bonds"},
        ], "ACWI", benchmark_weights={"Stocks": 60, "Bonds": 40})

        assert got["available"] is True
        assert got["status"] == "ready"
        assert got["benchmark"] == "60% ACWI + 40% AGGH.AS"

    @pytest.mark.parametrize("metric", ["correlation", "volatility", "drawdown"])
    def test_every_headline_risk_metric_uses_the_latest_complete_window(self, metric, monkeypatch):
        """All headline metrics retain the exact sleeve, even when its window is shorter."""
        dates = _weekdays(1_600)
        prices = _prices([0.001] * (len(dates) - 1), dates)
        import routers._airs_portfolio_analysis as PA
        monkeypatch.setattr(PA, "_daily_eur", lambda isins, years: {
            ONE: [(d, value) for d, value in prices if not d.startswith("2024-06")],
            BENCH: prices,
        })
        holdings = [{"isin": ONE, "name": "One", "weight_pct": 100.0, "is_fund": False}]
        if metric == "correlation":
            from routers._portfolio_correlation_risk import compute_risk_correlation
            got = compute_risk_correlation(holdings, "ACWI", frequency="monthly",
                                           require_complete_history=True)
        elif metric == "volatility":
            from routers._portfolio_volatility import compute_volatility
            got = compute_volatility(holdings, "ACWI", frequency="monthly",
                                     require_complete_history=True)
        else:
            from routers._portfolio_drawdown import compute_drawdown
            got = compute_drawdown(holdings, "ACWI", frequency="daily",
                                   require_complete_history=True)

        assert got["available"] is True
        if metric != "drawdown":
            assert got["window_from"] >= "2024-07"

    def test_monthly_return_uses_a_benchmark_shorter_than_five_years(self, wire, monkeypatch):
        """A young benchmark produces an honestly labelled common window, not a refusal."""
        dates = _weekdays(1_600)
        returns = [0.001] * (len(dates) - 1)
        prices = _prices(returns, dates)
        import routers._airs_portfolio_analysis as PA
        monkeypatch.setattr(PA, "_daily_eur", lambda isins, years: {
            ONE: prices,
            # Enough monthly returns to calculate a statistic, but no opening mark five years
            # before the final close. This must not be annualised and labelled as five years.
            BENCH: prices[-700:],
        })

        got = _run("monthly", require_complete_return=True)

        assert got["available"] is True
        assert got["history_limited"] is True
        assert got["window_from"] >= prices[-700][0][:7]

    def test_monthly_return_uses_the_latest_continuous_window_after_a_gap(self, wire, monkeypatch):
        """A production FX-page cap once made a holding vanish from the window's early months.

        The return stays exact: it starts after the gap rather than silently dropping the holding
        from early months or presenting the result as a five-year number.
        """
        dates = _weekdays(1_600)
        returns = [0.001] * (len(dates) - 1)
        import routers._airs_portfolio_analysis as PA
        prices = _prices(returns, dates)
        monkeypatch.setattr(PA, "_daily_eur", lambda isins, years: {
            # The holding has enough prices to look priceable, but one historical month was lost
            # — precisely the shape a truncated FX read can produce.
            ONE: [(d, value) for d, value in prices if not d.startswith("2024-06")],
            BENCH: prices,
        })

        got = _run("monthly", require_complete_return=True)

        assert got["available"] is True
        assert got["history_limited"] is True
        assert got["window_from"] >= "2024-07"

    def test_too_few_observations(self, wire):
        dates = _weekdays(40)
        rng = np.random.default_rng(6)
        wire(list(rng.normal(0, 0.01, 39)), list(rng.normal(0, 0.01, 39)), dates)
        got = _run()
        assert got["available"] is False
        #  The floor is named. "Not enough data" sends the reader to guess whether it is a bug.
        assert str(T.MIN_OBS["daily"]) in got["reason"]

    def test_monthly_short_history_uses_an_exact_preliminary_daily_measure(self, monkeypatch):
        """New funds have real daily closes well before they have 12 monthly returns."""
        dates = _weekdays(28)
        prices = _prices([0.001] * (len(dates) - 1), dates)
        import routers._airs_portfolio_analysis as PA
        monkeypatch.setattr(PA, "_daily_eur", lambda isins, years: {ONE: prices, BENCH: prices})
        T._risk_prices.cache_clear()

        got = _run("monthly", require_complete_return=True)

        assert got["available"] is True
        assert got["preliminary"] is True
        assert got["frequency"] == "daily"
        assert got["observations"] == len(dates) - 1
        # The UI's audit cards receive the actual compounded window return and exponent, rather
        # than having to reverse-engineer rounded annualised figures.
        assert got["portfolio_return_total_pct"] is not None
        assert got["benchmark_return_total_pct"] is not None
        assert got["annualization_factor"] == pytest.approx(252 / (len(dates) - 1))
        assert got["active_return_std_per_period_pct"] is not None
        component = got["benchmark_components"]
        assert len(component) == 1
        assert component[0]["name"] == "ACWI"
        assert component[0]["isin"] == BENCH
        assert component[0]["weight_pct"] == 100.0
        assert component[0]["window_return_pct"] == pytest.approx(got["benchmark_return_total_pct"])
        assert component[0]["annualized_return_pct"] == pytest.approx(got["benchmark_return_ann_pct"])
        assert component[0]["start_price_eur"] == pytest.approx(prices[0][1])
        assert component[0]["end_price_eur"] == pytest.approx(prices[-1][1])
        assert component[0]["start_date"] == prices[0][0]
        assert component[0]["end_date"] == prices[-1][0]

    def test_a_benchmark_with_no_investable_tracker(self, wire):
        dates = _weekdays(400)
        b = list(np.random.default_rng(7).normal(0, 0.01, 399))
        wire(b, b, dates)
        got = T.compute_tracking_error(
            [{"isin": ONE, "name": "One", "weight_pct": 100.0, "is_fund": False}], "NASDAQ")
        assert got["available"] is False
        #  And it names the ones that do work, so the reader can pick one rather than conclude
        # the panel is broken.
        assert "ACWI" in got["reason"]

    def test_a_book_of_only_funds(self, wire):
        dates = _weekdays(400)
        b = list(np.random.default_rng(8).normal(0, 0.01, 399))
        wire(b, b, dates)
        got = _run(holdings=[{"isin": "IE1", "name": "iShares", "weight_pct": 100.0,
                              "is_fund": True}])
        assert got["available"] is False
        assert "no individual stocks" in got["reason"]


class TestTheCadenceCaveat:
    def test_daily_says_it_is_inflated_and_weekly_says_nothing(self, wire):
        """ THE BIAS RIDES WITH THE NUMBER. Non-synchronous closes (tracker 16:30 London, US
        holding 21:00) lower the measured covariance, and `var(a) = var(p) + var(b) − 2cov(p,b)`,
        so a daily TE reads HIGH. The opposite direction from beta's bias, same cause."""
        dates = _weekdays(700)
        rng = np.random.default_rng(9)
        b = rng.normal(0.0004, 0.01, 699)
        wire(list(b + rng.normal(0, 0.004, 699)), list(b), dates)
        assert _run("daily")["cadence_note"]
        assert _run("weekly")["cadence_note"] is None
