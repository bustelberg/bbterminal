"""Correlation — a RISK measure, and deliberately not an attribution.

    ρ(X,Y) = Cov(X,Y) / (σ_X · σ_Y)

Two uses, both here and both about risk:

  * ρ between the POSITIONS — the diversification check. Are the twenty names twenty bets, or one
    bet held twenty times?
  * ρ between the PORTFOLIO and the BENCHMARK — which is the same fact the tracking-error view
    reports, seen from the other side:

        σ_a² = σ_p² + σ_b² − 2ρ σ_p σ_b

    Lower correlation with the index ⇒ higher tracking error, mechanically. The panel prints both
    sides of that identity so it can be checked rather than asserted.

⚠⚠ THIS IS NOT ATTRIBUTION AND MUST NEVER BE MERGED WITH IT. Attribution (Brinson-Fachler) is a
DECOMPOSITION of the active return — allocation `(wᵢᵖ − wᵢᵇ)(Rᵢᵇ − Rᵇ)`, selection
`wᵢᵇ(Rᵢᵖ − Rᵢᵇ)`, and their interaction — whose terms sum EXACTLY to the active return. Correlation
appears nowhere in it, sums to nothing, and answers a different question: attribution says where the
excess came FROM, correlation says how much the book can diverge AT ALL. They are separate panels in
this app on purpose (`AttributionPanel` is its own dialog), and a combined view would imply the two
reconcile. They do not; they are not that kind of number.

⚠ SAME SERIES AS THE TRACKING ERROR, from `build_paired_series`. The identity above is only
checkable if both views measure the same periods, the same sleeve and the same renormalisation.
"""
from __future__ import annotations

from routers._tracking_error import (
    MIN_OBS,
    PERIODS,
    SeriesError,
    build_paired_series,
)

#: Fewest overlapping observations a PAIR of holdings needs before its ρ is reported.
#:
#: ⚠ A CORRELATION OVER TEN WEEKS IS NOISE WITH A SIGN. Its standard error is ~1/√n, so at n=10 a
#: reading of 0.30 is indistinguishable from 0.0 and from 0.6 alike — and rendered as a coloured
#: cell it looks exactly as authoritative as one measured over five years. Below this the cell is
#: null (drawn as the grid's own "no number here" mark), never a faint colour.
MIN_PAIR_OBS = 30

#: How many of the most- and least-correlated pairs to name.
_TOP_PAIRS = 8


def compute_risk_correlation(holdings: list[dict], benchmark: str,
                             frequency: str = "weekly", years: int = 5) -> dict:
    """ρ to the benchmark, the variance identity behind it, and the position-by-position matrix."""
    import numpy as np  # noqa: PLC0415
    import pandas as pd  # noqa: PLC0415

    freq = frequency if frequency in PERIODS else "weekly"
    try:
        built = build_paired_series(holdings, benchmark, freq, years)
    except SeriesError as e:
        return {"available": False, "benchmark": benchmark, "frequency": freq, "reason": e.reason}

    ppy = built["periods_per_year"]
    rt = np.sqrt(ppy)
    p = np.asarray(built["portfolio"], dtype=float)
    b = np.asarray(built["benchmark"], dtype=float)
    a = np.asarray(built["active"], dtype=float)

    sd_p = float(np.std(p, ddof=1))
    sd_b = float(np.std(b, ddof=1))
    sd_a = float(np.std(a, ddof=1))
    # ⚠ ddof=1 EVERYWHERE, matching `annualized_stats`, or the identity below fails by a factor of
    # (T−1)/T — small, constant, and exactly the kind of discrepancy that reads as a real finding.
    rho = (float(np.corrcoef(p, b)[0][1]) if sd_p > 0 and sd_b > 0 else None)

    # ⚠ THE IDENTITY, RECOMPUTED FROM ρ RATHER THAN ASSUMED. `σ_a² = σ_p² + σ_b² − 2ρσ_pσ_b` is the
    # claim the panel makes; returning both sides lets the reader see it hold instead of trusting
    # that it does. They agree to floating-point noise when everything is right, and visibly
    # diverge if either series is ever built differently — which is the point.
    implied = (None if rho is None
               else float(np.sqrt(max(0.0, sd_p ** 2 + sd_b ** 2 - 2 * rho * sd_p * sd_b))))

    # ── the position matrix ────────────────────────────────────────────────────────────────────
    per = built["per_holding"]
    names = built["names"]
    weight = built["weight"]
    # ⚠ PAIRWISE-COMPLETE, NOT LISTWISE. A single holding that listed two years ago would, under
    # listwise deletion, truncate EVERY pair in the book to its own short history — one late
    # arrival silently rewriting the whole matrix. `DataFrame.corr` pairs each column pair on the
    # periods both of them have; `min_periods` then nulls the pairs that are still too thin.
    frame = pd.DataFrame({i: per[i] for i in sorted(per)})
    corr = frame.corr(min_periods=MIN_PAIR_OBS)

    # ⚠ ORDERED BY WEIGHT, DESCENDING — a matrix ordered alphabetically or by ISIN puts the two
    # positions that actually matter at opposite corners. The reader is looking for concentration.
    order = sorted(corr.columns, key=lambda i: -weight.get(i, 0.0))
    labels = [names.get(i) or i for i in order]
    grid: list[list[float | None]] = []
    for i in order:
        row: list[float | None] = []
        for j in order:
            v = corr.at[i, j] if i in corr.index and j in corr.columns else None
            row.append(None if v is None or pd.isna(v) else round(float(v), 3))
        grid.append(row)

    # ── the diversification summary ────────────────────────────────────────────────────────────
    pairs: list[dict] = []
    for x in range(len(order)):
        for y in range(x + 1, len(order)):
            v = grid[x][y]
            if v is not None:
                pairs.append({"a": labels[x], "b": labels[y], "rho": v})
    by_rho = sorted(pairs, key=lambda r: r["rho"])
    # ⚠ THE MEAN OFF-DIAGONAL ρ IS THE ONE NUMBER THAT SUMMARISES A MATRIX, and it is an UNWEIGHTED
    # mean of the pairs on purpose: it answers "are these names alike?", a question about the
    # selection, not about the sizing. Weighting it by position size would answer a different
    # question and quietly make a concentrated book look better diversified than its names are.
    mean_rho = (sum(r["rho"] for r in pairs) / len(pairs)) if pairs else None

    return {
        "available": True,
        "benchmark": benchmark,
        "frequency": freq,
        "periods_per_year": ppy,
        "observations": len(a),
        "years": years,

        # ── portfolio vs benchmark ──
        # ⚠ NOT ROUNDED HERE. `r_squared` below is ρ², and rounding ρ in the payload while squaring
        # the full-precision value would put a ρ on screen that does not square to the R² beside it.
        # Formatting belongs to the view, which shows both to two decimals; the reconciliation has
        # to survive being checked at whatever precision is displayed.
        "benchmark_corr": rho,
        "portfolio_vol_pct": sd_p * rt * 100.0,
        "benchmark_vol_pct": sd_b * rt * 100.0,
        # The SAME quantity the tracking-error view reports, from the same series.
        "active_vol_pct": sd_a * rt * 100.0,
        "implied_active_vol_pct": None if implied is None else implied * rt * 100.0,
        # ⚠ HOW FAR THE IDENTITY MISSES, IN PERCENTAGE POINTS. Rounding noise is ~1e-13; anything a
        # reader could see means the two series stopped being the same two series.
        "identity_gap_pp": (None if implied is None else abs(implied - sd_a) * rt * 100.0),
        # ρ² — the share of the book's variance the index explains. Stated because "correlation
        # 0.9" and "81% of the movement" land very differently on the same fact.
        "r_squared": None if rho is None else rho ** 2,

        # ── the positions ──
        "labels": labels,
        "matrix": grid,
        "mean_pair_corr": None if mean_rho is None else round(mean_rho, 4),
        "pairs_measured": len(pairs),
        "min_pair_observations": MIN_PAIR_OBS,
        "least_correlated": by_rho[:_TOP_PAIRS],
        "most_correlated": list(reversed(by_rho[-_TOP_PAIRS:])),
        "priced_holdings": built["priced"],
        "total_holdings": built["total"],
        "min_observations": MIN_OBS[freq],
        "cadence_note": (
            "Daily closes are not synchronous — the tracker closes at 16:30 London, a US holding at "
            "21:00 — which mechanically LOWERS every correlation measured against it. Weekly spans "
            "the gap: measured on this book, Microsoft vs ACWI reads 0.38 daily against 0.50 weekly."
            if freq == "daily" else None),
    }
