"""Volatility — the spread of the book's OWN returns, annualised.

    σ_p = √( 1/(T−1) · Σ (Rₜ − R̄)² ) · √f

Same construction as the tracking error, on `Rₜᵖ` instead of `Rₜᵖ − Rₜᵇ`. Same `annualized_stats`,
so σ_p here, σ_p in the correlation view's identity, and the vol column on every holding row are one
function — not three.

⚠⚠ CASH FLOWS CANNOT CONTAMINATE THIS, AND NOT FOR THE REASON YOU MIGHT EXPECT. The usual hazard is
computing risk off an ACCOUNT VALUE series: a deposit looks like a huge positive return and a
withdrawal like a crash, so the volatility of a book that merely received money reads as turbulence.
The standard fix is time-weighted (chain-linked) returns, which strip the flows out.

This series never has flows in it to strip. It is not a value series at all — it is a weighted basket
of INSTRUMENT price returns (`build_paired_series`), so a period's return is `Σwᵢrᵢ / Σwᵢ` over the
holdings, and money moving into or out of the account changes nothing in it. That is the property
TWR exists to produce, arrived at by never introducing the problem.

⚠ THE PRICE OF THAT IS THE OTHER CAVEAT, AND IT IS REAL: the weights are TODAY'S, carried backwards.
So this is the volatility of the portfolio AS IT STANDS, not the volatility the client actually
experienced — a name bought in March contributes its January return. The book's realised, chain-
linked TWR exists (`_airs_portfolio_perf`) but describes the WHOLE book including funds and cash,
which is a different portfolio from the stock sleeve every other view in this panel measures. One
portfolio per panel beats one more number.

⚠⚠ DOWNSIDE DEVIATION IS SORTINO'S, NOT THE SEMI-DEVIATION. It divides by ALL n observations and
measures shortfall against the risk-free target (0 here), which is what `annualized_stats.sortino`
is already built on. The other convention — only the below-MEAN observations, divided by how many
there are, measured against the mean — is also called downside deviation and reads higher. Picking
the one the existing Sortino uses is what lets the ratio on screen equal its own parts.
"""
from __future__ import annotations

from routers._tracking_error import PERIODS, SeriesError, build_paired_series


def compute_volatility(holdings: list[dict], benchmark: str,
                       frequency: str = "weekly", years: int = 5) -> dict:
    """σ of the stock sleeve's own returns, its downside half, and the benchmark's for scale."""
    import numpy as np  # noqa: PLC0415

    from momentum.diversification import annualized_stats  # noqa: PLC0415

    freq = frequency if frequency in PERIODS else "weekly"
    try:
        built = build_paired_series(holdings, benchmark, freq, years)
    except SeriesError as e:
        return {"available": False, "benchmark": benchmark, "frequency": freq, "reason": e.reason}

    ppy = built["periods_per_year"]
    p = np.asarray(built["portfolio"], dtype=float)
    b = np.asarray(built["benchmark"], dtype=float)

    sp = annualized_stats(p.tolist(), periods_per_year=ppy)
    sb = annualized_stats(b.tolist(), periods_per_year=ppy)

    # ⚠ THE WORST AND BEST SINGLE PERIOD, because "18% annualised volatility" is not a thing anybody
    # has felt. A client experiences the worst week, not the second moment of the distribution —
    # and the two can be far apart for a book with fat tails, which is exactly when σ misleads.
    worst = float(p.min()) if p.size else None
    best = float(p.max()) if p.size else None
    negative = float(np.mean(p < 0)) if p.size else None

    return {
        "available": True,
        "benchmark": benchmark,
        "frequency": freq,
        "periods_per_year": ppy,
        "observations": int(p.size),
        "years": years,

        "volatility_pct": None if sp.ann_vol is None else sp.ann_vol * 100.0,
        # ⚠ THE SAME FUNCTION AND THE SAME SERIES AS THE BOOK'S — printed for scale, never as a
        # verdict. A sleeve more volatile than its index is not by itself worse; it is what the
        # active share and tracking error views are about.
        "benchmark_volatility_pct": None if sb.ann_vol is None else sb.ann_vol * 100.0,
        "downside_dev_pct": None if sp.downside_dev is None else sp.downside_dev * 100.0,
        "benchmark_downside_dev_pct": (None if sb.downside_dev is None
                                       else sb.downside_dev * 100.0),
        "return_ann_pct": None if sp.ann_return is None else sp.ann_return * 100.0,
        "benchmark_return_ann_pct": None if sb.ann_return is None else sb.ann_return * 100.0,
        # ⚠ AT rf = 0, STATED. A Sharpe quoted without its risk-free rate is not comparable with
        # anybody else's, and at today's rates the difference is not cosmetic.
        "sharpe": sp.sharpe,
        "sortino": sp.sortino,
        "risk_free_pct": 0.0,

        "worst_period_pct": None if worst is None else worst * 100.0,
        "best_period_pct": None if best is None else best * 100.0,
        "negative_periods_pct": None if negative is None else negative * 100.0,

        "priced_holdings": built["priced"],
        "total_holdings": built["total"],
        "cadence_note": (
            "Daily volatility is the standard basis and is NOT distorted by the closing-time gap "
            "that affects beta, correlation and tracking error — those compare two series, and a "
            "volatility compares a series with itself."
            if freq == "daily" else None),
    }
