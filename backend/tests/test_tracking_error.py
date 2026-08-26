"""TE = √(Σ(aₜ−ā)²/(T−1)) · √f, and every test is a closed-form identity rather than a fixture.

⚠⚠ THE TWO CASES THAT SEPARATE TE FROM THE THING IT IS CONSTANTLY CONFUSED WITH are the first two:
a book identical to its index has zero TE, and a book beating its index by exactly the same amount
every single period ALSO has zero TE while having a large active return. Tracking error is the
SPREAD of the active return, never the active return.

⚠ AND THE DEFINITION IS PINNED, NOT ASSUMED. `ā` subtracted, divisor `T−1`. The other convention
(√(Σaₜ²/T)) is also called tracking error and reads higher; the test asserts which one this is, so
a future "simplification" to `np.std(a)` fails here rather than in somebody's risk report.
"""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pytest

import routers._tracking_error as T

BENCH = "IE00B6R52259"      # the ACWI tracker `_BENCHMARK_RISK_ETF` maps to
ONE = "US0000000001"


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


def _run(freq: str = "daily", holdings=None):
    return T.compute_tracking_error(
        holdings if holdings is not None
        else [{"isin": ONE, "name": "One", "weight_pct": 100.0, "is_fund": False}],
        "ACWI", frequency=freq)


class TestTheDefinition:
    def test_a_book_that_IS_the_index_has_no_tracking_error(self, wire):
        dates = _weekdays(400)
        b = list(np.random.default_rng(1).normal(0.0004, 0.01, 399))
        wire(b, b, dates)
        got = _run()
        assert got["available"]
        assert got["tracking_error_pct"] == pytest.approx(0.0, abs=1e-9)

    def test_a_CONSTANT_excess_is_a_big_active_return_and_zero_tracking_error(self, wire):
        """⚠⚠ THE CASE THAT DEFINES THE MEASURE. Beating the index by the same amount every period
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
        # ⚠ ddof=1, NOT 0 — asserted against the population sd it must NOT equal.
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
    def test_too_few_observations(self, wire):
        dates = _weekdays(40)
        rng = np.random.default_rng(6)
        wire(list(rng.normal(0, 0.01, 39)), list(rng.normal(0, 0.01, 39)), dates)
        got = _run()
        assert got["available"] is False
        # ⚠ THE FLOOR IS NAMED. "Not enough data" sends the reader to guess whether it is a bug.
        assert str(T.MIN_OBS["daily"]) in got["reason"]

    def test_a_benchmark_with_no_investable_tracker(self, wire):
        dates = _weekdays(400)
        b = list(np.random.default_rng(7).normal(0, 0.01, 399))
        wire(b, b, dates)
        got = T.compute_tracking_error(
            [{"isin": ONE, "name": "One", "weight_pct": 100.0, "is_fund": False}], "NASDAQ")
        assert got["available"] is False
        # ⚠ AND IT NAMES THE ONES THAT DO WORK, so the reader can pick one rather than conclude
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
        """⚠ THE BIAS RIDES WITH THE NUMBER. Non-synchronous closes (tracker 16:30 London, US
        holding 21:00) lower the measured covariance, and `var(a) = var(p) + var(b) − 2cov(p,b)`,
        so a daily TE reads HIGH. The opposite direction from beta's bias, same cause."""
        dates = _weekdays(700)
        rng = np.random.default_rng(9)
        b = rng.normal(0.0004, 0.01, 699)
        wire(list(b + rng.normal(0, 0.004, 699)), list(b), dates)
        assert _run("daily")["cadence_note"]
        assert _run("weekly")["cadence_note"] is None
