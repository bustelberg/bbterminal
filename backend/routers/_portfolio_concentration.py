"""Concentration — how much of the book sits in how few names.

    C₁₀ = Σᵢ₌₁¹⁰ w₍ᵢ₎        HHI = Σ wᵢ²        N_eff = 1 / HHI

⚠⚠ ON ISSUERS, NOT ON LINES — the same folding active share uses (`build_issuer_weights`). A book
holding Alphabet A and Alphabet C holds ONE position in Alphabet, and counting them as two lines
understates concentration exactly where it matters: at the top, where the ten largest are decided.
Two views of the same book that disagreed about how many positions it has would be worse than
either.

⚠⚠ CASH AND FUNDS ARE OUT OF THE DENOMINATOR, AND BOTH ANSWERS ARE RETURNED because the choice
genuinely changes the number. The panel's convention throughout is "the individual stocks are 100%",
which is what makes C₁₀ comparable with another book's — but a book that is 30% cash really is less
concentrated in absolute terms, and hiding that would be its own distortion. So `top10_pct` is of
the sleeve and `top10_of_book_pct` is of everything, side by side, each labelled.

⚠ HHI IS THE BETTER NUMBER AND C₁₀ IS THE ONE PEOPLE ASK FOR. A cut at exactly ten is arbitrary:
two books with the same C₁₀ can be an even ten-name portfolio and one dominated by its top three.
HHI has no cut-off, and `N_eff = 1/HHI` puts it in units anybody can hold in their head — "forty
names, but effectively twelve".

⚠ WEIGHTS ARE FRACTIONS IN THE HHI, NOT PERCENTAGES. `Σwᵢ²` on percentages is 10,000× larger (the
antitrust convention), and `N_eff = 1/HHI` only inverts cleanly on fractions. Getting this wrong
gives an effective position count of 0.0001, which at least fails loudly — the dangerous version is
mixing the two conventions between the book and the benchmark.
"""
from __future__ import annotations

from routers._active_share import IssuerError, build_issuer_weights

#: The cut-offs reported. ⚠ SEVERAL, because one is arbitrary: C₁ says whether a single name
#: dominates, C₁₀ is the convention, and the shape between them is the actual finding.
_CUTS = (1, 3, 5, 10, 20)


def _profile(weights_pct: list[float]) -> dict:
    """C-cuts, HHI and N_eff for one weight vector (in PERCENT, summing to ~100)."""
    if not weights_pct:
        return {"cuts": {}, "hhi": None, "effective_positions": None, "n": 0}
    ordered = sorted(weights_pct, reverse=True)
    total = sum(ordered)
    if total <= 0:
        return {"cuts": {}, "hhi": None, "effective_positions": None, "n": len(ordered)}
    # ⚠ FRACTIONS FOR THE HHI — see the module header.
    fr = [w / total for w in ordered]
    hhi = sum(f * f for f in fr)
    return {
        # ⚠ `min(c, len)` IS NOT APPLIED: C₂₀ of a fifteen-name book is simply 100%, which is the
        # true answer and a useful one. Capping the label to the count instead would print "C₁₅"
        # and quietly stop being comparable with the next book.
        "cuts": {c: sum(ordered[:c]) for c in _CUTS},
        "hhi": hhi,
        # ⚠ 1/HHI, so an equal-weight book of N names returns exactly N. That identity is what makes
        # the number readable, and it is the first thing a test should check.
        "effective_positions": (1.0 / hhi) if hhi > 0 else None,
        "n": len(ordered),
    }


def compute_concentration(holdings: list[dict], benchmark: str) -> dict:
    """C₁₀ / HHI / N_eff for the book's stock sleeve, beside the benchmark's own."""
    try:
        built = build_issuer_weights(holdings, benchmark)
    except IssuerError as e:
        return {"available": False, "reason": e.reason, "benchmark": benchmark}

    port, bench = built["port"], built["bench"]
    stocks_w, total_all = built["stocks_w"], built["total_all"]

    rows = sorted(port.values(), key=lambda r: -r["weight_pct"])
    p = _profile([r["weight_pct"] for r in rows])
    b = _profile([r["weight_pct"] for r in bench.values()])

    # The sleeve as a share of everything — the scale factor between the two denominators.
    sleeve_share = (stocks_w / total_all) if total_all > 0 else None

    cumulative = 0.0
    top: list[dict] = []
    for i, r in enumerate(rows[:20], 1):
        cumulative += r["weight_pct"]
        top.append({
            "rank": i,
            "name": r["name"],
            "weight_pct": r["weight_pct"],
            "cumulative_pct": cumulative,
            # ⚠ THE INDEX'S WEIGHT IN THE SAME ISSUER, so a big position can be read as a big BET
            # or merely as a big company. Apple at 6% is not concentration if the index holds 5%.
            "benchmark_pct": bench.get(r["key"], {}).get("weight_pct", 0.0),
        })

    return {
        "available": True,
        "benchmark": benchmark,
        "issuers": p["n"],
        "benchmark_issuers": b["n"],

        # ⚠ OF THE SLEEVE. The panel's convention, and the one that compares across books.
        "top1_pct": p["cuts"].get(1),
        "top3_pct": p["cuts"].get(3),
        "top5_pct": p["cuts"].get(5),
        "top10_pct": p["cuts"].get(10),
        "top20_pct": p["cuts"].get(20),
        # ⚠ OF THE WHOLE BOOK, cash and funds included — the other honest answer to the same
        # question. Returned rather than chosen between; see the module header.
        "top10_of_book_pct": (None if p["cuts"].get(10) is None or sleeve_share is None
                              else p["cuts"][10] * sleeve_share),
        "stocks_pct": None if sleeve_share is None else sleeve_share * 100.0,

        "hhi": p["hhi"],
        "effective_positions": p["effective_positions"],
        "benchmark_top10_pct": b["cuts"].get(10),
        "benchmark_hhi": b["hhi"],
        "benchmark_effective_positions": b["effective_positions"],

        "top": top,
        "benchmark_covered_pct": built["coverage"].get("covered_pct"),
        # ⚠ WHEN THE INDEX SIDE WAS MEASURED. This view weights the benchmark by market cap,
        # exactly as active share does, so it owes the reader the same date range — a cap read
        # three weeks ago is a three-week-old weight, and N_eff computed off it is that old too.
        "benchmark_caps_from": built["coverage"].get("caps_from"),
        "benchmark_caps_to": built["coverage"].get("caps_to"),
        "benchmark_caps_unstamped": built["coverage"].get("caps_unstamped"),
        "unresolved": len(built["unresolved"]),
    }
