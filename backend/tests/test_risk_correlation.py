"""ρ as a risk measure, and the identity that ties it to the tracking error.

 THE TEST THAT MATTERS IS `σₐ² = σₚ² + σᵦ² − 2ρσₚσᵦ`. The Risk panel shows the active volatility
measured directly from the active returns, and beside it the same figure rebuilt from ρ. That is a
claim the screen makes, so it is a claim a test has to hold to — and it only holds while both views
read the SAME series, which is why `build_paired_series` is shared rather than duplicated.

 NOTHING HERE TOUCHES ATTRIBUTION, deliberately. Correlation does not appear in a Brinson
decomposition and does not sum to the active return; the two are separate panels precisely so that
no test ever needs to reconcile them.
"""
from __future__ import annotations

import asyncio
from datetime import date, timedelta

import numpy as np
import pytest

import routers._portfolio_correlation_risk as C
import routers._tracking_error as T

BENCH = "IE00B6R52259"


def _weekdays(n: int) -> list[str]:
    out, d = [], date.today()
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d -= timedelta(days=1)
    return sorted(out)


def _prices(returns, dates, start: float = 100.0):
    v, out = start, []
    for d, r in zip(dates, [0.0, *returns]):
        v *= 1.0 + r
        out.append((d, v))
    return out


@pytest.fixture
def book(monkeypatch):
    """A three-name book with varying market exposure, plus its tracker."""
    dates = _weekdays(900)
    rng = np.random.default_rng(11)
    n = len(dates) - 1
    b = rng.normal(0.0004, 0.010, n)
    cols = {
        "US0000000001": 1.1 * b + rng.normal(0, 0.008, n),
        "US0000000002": 0.8 * b + rng.normal(0, 0.012, n),
        "NL0000000003": 0.3 * b + rng.normal(0, 0.020, n),
    }
    payload = {i: _prices(list(r), dates) for i, r in cols.items()}
    payload[BENCH] = _prices(list(b), dates)

    import routers._airs_portfolio_analysis as PA
    monkeypatch.setattr(PA, "_daily_eur", lambda isins, years: payload)
    return [{"isin": i, "name": f"N{k}", "weight_pct": 100.0 / len(cols), "is_fund": False}
            for k, i in enumerate(cols)]


class TestTheIdentityThatLinksItToTrackingError:
    @pytest.mark.parametrize("freq", ["daily", "weekly", "monthly"])
    def test_the_variance_identity_closes(self, book, freq):
        got = C.compute_risk_correlation(book, "ACWI", frequency=freq)
        assert got["available"], got.get("reason")
        #  Floating-point noise or nothing. Anything a reader could see on screen means the two
        # series stopped being the same two series — which is a bug, not a market fact, and the
        # panel says so in those words.
        assert got["identity_gap_pp"] == pytest.approx(0.0, abs=1e-9)

    @pytest.mark.parametrize("freq", ["daily", "weekly", "monthly"])
    def test_the_active_vol_IS_the_tracking_error_view_figure(self, book, freq):
        """ THE TWO VIEWS SIT ONE CLICK APART. A reader who switches and finds 14.09% become
        14.11% has learned that one of them is wrong and no way to tell which."""
        corr = C.compute_risk_correlation(book, "ACWI", frequency=freq)
        te = T.compute_tracking_error(book, "ACWI", frequency=freq)
        assert corr["active_vol_pct"] == pytest.approx(te["tracking_error_pct"], abs=1e-9)

    def test_r_squared_is_the_square_of_the_rho_on_screen(self, book):
        """ ρ IS RETURNED UNROUNDED for exactly this reason — rounding it in the payload while
        squaring the full-precision value puts a ρ on screen that does not square to its own R²."""
        got = C.compute_risk_correlation(book, "ACWI")
        assert got["r_squared"] == pytest.approx(got["benchmark_corr"] ** 2, abs=1e-12)

    def test_a_clone_of_the_index_correlates_perfectly_and_does_not_diverge(self, monkeypatch):
        dates = _weekdays(900)
        b = list(np.random.default_rng(3).normal(0.0004, 0.01, len(dates) - 1))
        import routers._airs_portfolio_analysis as PA
        monkeypatch.setattr(PA, "_daily_eur", lambda isins, years: {
            "US0000000001": _prices(b, dates), BENCH: _prices(b, dates)})
        got = C.compute_risk_correlation(
            [{"isin": "US0000000001", "name": "Clone", "weight_pct": 100.0, "is_fund": False}],
            "ACWI")
        assert got["benchmark_corr"] == pytest.approx(1.0, abs=1e-9)
        assert got["active_vol_pct"] == pytest.approx(0.0, abs=1e-9)


def test_risk_correlation_endpoint_falls_back_to_preliminary_daily_sample(monkeypatch):
    """One monthly return is not a correlation, but ten real daily returns are informative."""
    import routers.airs as airs

    calls = []

    def fake_compute(_holdings, _benchmark, frequency, _years, **kwargs):
        calls.append((frequency, kwargs))
        if frequency == "monthly":
            return {"available": False, "benchmark": "ACWI", "frequency": "monthly",
                    "reason": "Only 1 monthly observations overlap with ACWI; 12 is the floor."}
        return {"available": True, "benchmark": "ACWI", "frequency": "daily",
                "preliminary": kwargs["preliminary"], "observations": 10}

    import routers._portfolio_correlation_risk as risk
    monkeypatch.setattr(risk, "compute_risk_correlation", fake_compute)

    result = asyncio.run(airs.airs_portfolio_risk_correlation(
        airs.ActiveShareRequest(holdings=[]), "ACWI", "monthly", 5))

    assert result["available"]
    assert result["frequency"] == "daily"
    assert result["preliminary"] is True
    assert calls == [
        ("monthly", {"benchmark_weights": None, "require_complete_history": True}),
        ("daily", {"benchmark_weights": None, "require_complete_history": True,
                   "minimum_observations": 10, "preliminary": True}),
    ]


class TestTheMatrix:
    def test_preliminary_daily_matrix_uses_its_explicit_ten_day_floor(self, monkeypatch):
        """A preliminary 10-day answer must not ship with a completely empty matrix."""
        dates = _weekdays(16)
        rng = np.random.default_rng(7)
        benchmark_returns = rng.normal(0.0004, 0.01, len(dates) - 1)
        payload = {
            BENCH: _prices(benchmark_returns, dates),
            "US0000000001": _prices(0.9 * benchmark_returns + rng.normal(0, 0.004, len(benchmark_returns)), dates),
            "US0000000002": _prices(0.6 * benchmark_returns + rng.normal(0, 0.007, len(benchmark_returns)), dates),
        }
        import routers._airs_portfolio_analysis as PA
        monkeypatch.setattr(PA, "_daily_eur", lambda isins, years: payload)
        got = C.compute_risk_correlation([
            {"isin": "US0000000001", "name": "One", "weight_pct": 50.0, "is_fund": False},
            {"isin": "US0000000002", "name": "Two", "weight_pct": 50.0, "is_fund": False},
        ], "ACWI", frequency="daily", require_complete_history=True,
           minimum_observations=10, preliminary=True)
        assert got["available"], got.get("reason")
        assert got["min_pair_observations"] == 10
        assert got["pairs_measured"] == 1
        assert got["matrix"][0][1] is not None

    def test_it_is_square_symmetric_and_unit_on_the_diagonal(self, book):
        got = C.compute_risk_correlation(book, "ACWI")
        m, labels = got["matrix"], got["labels"]
        assert len(m) == len(labels) == 3
        for i in range(3):
            assert m[i][i] == pytest.approx(1.0, abs=1e-9)
            for j in range(3):
                assert m[i][j] == pytest.approx(m[j][i], abs=1e-9)

    def test_it_is_ordered_by_weight_not_by_isin(self, book):
        """ A MATRIX ORDERED BY IDENTIFIER PUTS THE TWO POSITIONS THAT MATTER AT OPPOSITE CORNERS.
        The reader is looking for concentration, so the largest holdings lead."""
        heavy = [dict(h, weight_pct=w) for h, w in zip(book, [10.0, 70.0, 20.0])]
        assert C.compute_risk_correlation(heavy, "ACWI")["labels"] == ["N1", "N2", "N0"]

    def test_the_mean_pair_correlation_is_the_off_diagonal_mean(self, book):
        got = C.compute_risk_correlation(book, "ACWI")
        m = got["matrix"]
        off = [m[0][1], m[0][2], m[1][2]]
        assert got["mean_pair_corr"] == pytest.approx(sum(off) / 3, abs=1e-3)
        assert got["pairs_measured"] == 3

    def test_least_and_most_correlated_are_the_ends_of_one_ordering(self, book):
        got = C.compute_risk_correlation(book, "ACWI")
        assert got["least_correlated"][0]["rho"] <= got["most_correlated"][0]["rho"]

    def test_a_thin_pair_is_null_rather_than_faintly_coloured(self):
        """ OVER TEN WEEKS A CORRELATION IS NOISE WITH A SIGN — its standard error is ~1/√n, so
        0.30 at n=10 is indistinguishable from 0.0 and from 0.6 alike. Rendered as a tinted cell it
        looks exactly as authoritative as one measured over five years."""
        assert C.MIN_PAIR_OBS >= 30
