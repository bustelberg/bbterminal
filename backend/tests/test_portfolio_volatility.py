"""σ_p = √(Σ(Rₜ−R̄)²/(T−1))·√f, and the two things that make it trustworthy rather than merely present.

⚠⚠ FIRST: IT IS THE SAME σₚ THE CORRELATION VIEW USES. That view prints `σₐ² = σₚ² + σᵦ² − 2ρσₚσᵦ`
and invites the reader to check it; if the Volatility view one click away showed a different σₚ, the
reader would have learned that one of them is wrong and nothing about which. Both come from
`build_paired_series`, and this asserts they still do.

⚠⚠ SECOND: NO CASH-FLOW CONTAMINATION. Computing risk off an ACCOUNT VALUE makes a deposit look like
a huge gain and a withdrawal like a crash, so a book that merely received money reads as turbulent —
which is what time-weighted returns exist to fix. This series never has flows in it: it is a
weighted basket of instrument price returns, so scaling every holding's VALUE changes nothing. That
is what the last test here pins.
"""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pytest

import routers._portfolio_correlation_risk as C
import routers._portfolio_volatility as V

BENCH = "IE00B6R52259"


def _weekdays(n: int) -> list[str]:
    out, d = [], date.today()
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d -= timedelta(days=1)
    return sorted(out)


def _prices(rs, dates, start: float = 100.0):
    v, out = start, []
    for d, r in zip(dates, [0.0, *rs]):
        v *= 1.0 + r
        out.append((d, v))
    return out


@pytest.fixture
def book(monkeypatch):
    dates = _weekdays(900)
    rng = np.random.default_rng(21)
    n = len(dates) - 1
    b = rng.normal(0.0004, 0.010, n)
    cols = {
        "US0000000001": 1.1 * b + rng.normal(0, 0.008, n),
        "NL0000000003": 0.3 * b + rng.normal(0, 0.020, n),
    }
    payload = {i: _prices(list(r), dates) for i, r in cols.items()}
    payload[BENCH] = _prices(list(b), dates)
    import routers._airs_portfolio_analysis as PA
    monkeypatch.setattr(PA, "_daily_eur", lambda isins, years: payload)
    return [{"isin": i, "name": f"N{k}", "weight_pct": 50.0, "is_fund": False}
            for k, i in enumerate(cols)]


class TestTheFormula:
    def test_it_is_the_bessel_corrected_annualised_sd(self, book):
        from routers._tracking_error import build_paired_series

        got = V.compute_volatility(book, "ACWI", frequency="weekly")
        p = np.asarray(build_paired_series(book, "ACWI", "weekly", 5)["portfolio"])
        want = float(np.std(p, ddof=1) * np.sqrt(got["periods_per_year"]) * 100)
        assert got["volatility_pct"] == pytest.approx(want, abs=1e-9)
        # ⚠ ddof=1 — asserted against the population sd it must NOT equal.
        population = float(np.std(p, ddof=0) * np.sqrt(got["periods_per_year"]) * 100)
        assert abs(got["volatility_pct"] - want) < abs(got["volatility_pct"] - population)

    @pytest.mark.parametrize("freq", ["daily", "weekly", "monthly"])
    def test_sigma_p_matches_the_correlation_view_exactly(self, book, freq):
        vol = V.compute_volatility(book, "ACWI", frequency=freq)
        corr = C.compute_risk_correlation(book, "ACWI", frequency=freq)
        assert vol["volatility_pct"] == pytest.approx(corr["portfolio_vol_pct"], abs=1e-9)
        assert vol["benchmark_volatility_pct"] == pytest.approx(
            corr["benchmark_vol_pct"], abs=1e-9)

    def test_downside_deviation_is_sortinos_convention(self, book):
        """⚠ DIVIDED BY ALL n, AGAINST A TARGET OF 0 — not the semi-deviation (below-MEAN only,
        divided by how many there are), which is also called downside deviation and reads higher.
        This is the one `sortino` is built on, so the ratio equals its own parts."""
        from routers._tracking_error import build_paired_series

        got = V.compute_volatility(book, "ACWI", frequency="weekly")
        p = np.asarray(build_paired_series(book, "ACWI", "weekly", 5)["portfolio"])
        want = float(np.sqrt(np.mean(np.minimum(p, 0.0) ** 2))
                     * np.sqrt(got["periods_per_year"]) * 100)
        assert got["downside_dev_pct"] == pytest.approx(want, abs=1e-9)
        assert got["downside_dev_pct"] < got["volatility_pct"]

    def test_the_ratios_equal_their_own_parts(self, book):
        got = V.compute_volatility(book, "ACWI", frequency="weekly")
        assert got["sharpe"] == pytest.approx(
            got["return_ann_pct"] / got["volatility_pct"], abs=1e-9)
        assert got["sortino"] == pytest.approx(
            got["return_ann_pct"] / got["downside_dev_pct"], abs=1e-9)

    def test_a_series_that_never_falls_has_zero_downside_and_no_sortino(self, monkeypatch):
        """⚠ 0.0 AND None MEAN DIFFERENT THINGS. Zero downside deviation is a measurement — nothing
        ever fell short. A null Sortino is "a ratio over zero has no value"."""
        dates = _weekdays(401)
        up = [0.001] * 400
        import routers._airs_portfolio_analysis as PA
        monkeypatch.setattr(PA, "_daily_eur", lambda isins, years: {
            "US0000000001": _prices(up, dates), BENCH: _prices(up, dates)})
        got = V.compute_volatility(
            [{"isin": "US0000000001", "name": "Up", "weight_pct": 100.0, "is_fund": False}],
            "ACWI", frequency="daily")
        assert got["downside_dev_pct"] == 0.0
        assert got["sortino"] is None
        assert got["negative_periods_pct"] == 0.0


class TestFlowsCannotReachIt:
    def test_scaling_every_holding_changes_nothing(self, monkeypatch):
        """⚠⚠ THE PROPERTY TWR EXISTS TO PRODUCE, HERE BY CONSTRUCTION. A deposit that doubles the
        book is, in a value series, a +100% period; in a series of weighted instrument RETURNS it
        does not appear at all. Doubling every price level leaves the returns identical, so the
        volatility must be bit-identical too."""
        dates = _weekdays(600)
        rng = np.random.default_rng(31)
        n = len(dates) - 1
        b = rng.normal(0.0004, 0.010, n)
        r1 = list(1.1 * b + rng.normal(0, 0.008, n))
        h = [{"isin": "US0000000001", "name": "One", "weight_pct": 100.0, "is_fund": False}]

        import routers._airs_portfolio_analysis as PA
        monkeypatch.setattr(PA, "_daily_eur", lambda isins, years: {
            "US0000000001": _prices(r1, dates, start=100.0), BENCH: _prices(list(b), dates)})
        small = V.compute_volatility(h, "ACWI", frequency="weekly")

        # The identical book, ten times the money.
        monkeypatch.setattr(PA, "_daily_eur", lambda isins, years: {
            "US0000000001": _prices(r1, dates, start=1000.0), BENCH: _prices(list(b), dates)})
        large = V.compute_volatility(h, "ACWI", frequency="weekly")

        assert large["volatility_pct"] == pytest.approx(small["volatility_pct"], abs=1e-12)
        assert large["downside_dev_pct"] == pytest.approx(small["downside_dev_pct"], abs=1e-12)
        assert large["return_ann_pct"] == pytest.approx(small["return_ann_pct"], abs=1e-12)
