"""ONE definition of "what the portfolio weighs", shared by the composition charts and Brinson.

WHY THIS MODULE EXISTS
    The composition chart and the attribution table both answer "how much Technology do we hold",
    and they used to answer it differently — 36% against 39.1% on Bustelberg Offensief, with ASML
    reading 7.30% on one and 5.75% on the other. Both were correct: the chart divided TODAY's value
    by the whole equity sleeve, attribution divided the value at the window's OPEN by the
    attributable holdings alone. Two right answers to one question is indistinguishable from one
    wrong answer, and the reader has no way to tell which.

    The decision (2026-07-31) was to make them identical: the composition adopts attribution's
    basis outright. The only way that holds is for both to read the SAME function — a second
    implementation of "attributable" is a second definition of it, and it will drift on the first
    edit that touches one and not the other.

⚠ THE BASIS IS THE BEGINWAARDE, AND THAT COSTS SOMETHING REAL — SAY SO, NEVER HIDE IT.
    Two classes of holding cannot be expressed on this basis at all:

      bought mid-window   no Beginwaarde ⇒ weight 0 ⇒ absent. Correct for a 1 Jan-anchored
                          attribution (it earned nothing over a window it was not held for) and
                          WRONG as a statement of what is held today. A stock bought in March and
                          standing at 8% of the book does not appear.
      unpriceable         a Leonteq structured product or an in-house fund has no series, so there
                          is no return to attribute. Its sector then reads as UNOWNED — measured,
                          a model holding 6% Healthcare was credited +1.73pp of allocation for
                          "avoiding" Healthcare. A false finding, not a missing one.

    Neither can be fixed by arithmetic; both are properties of the basis. So every caller gets
    `excluded` and `attributable_pct` back and is expected to PUT THEM ON SCREEN. Weight that
    silently leaves a percentage is the failure this codebase already has coverage floors for
    (TOPS_OFF_BEH once read "+0.00%", which was its 1% cash line alone).
"""
from __future__ import annotations

from datetime import date

from deps import supabase

# Bucket sentinels + the classifier live in the analysis module; importing them here is safe
# because that module reaches this one only through a function-level import (no cycle).
from ._airs_portfolio_analysis import (
    CASH_BUCKET,
    UNKNOWN_BUCKET,
    _buckets,
    _country_by_code,
    _grid,
)
from ._airs_portfolio_perf import compute_holding_marks, ytd_anchor_for

AXIS_IDX = {"sector": 0, "region": 1, "currency": 2}


def window_start(source: str, window: str, effective: str | None) -> str | None:
    """The date the weights are taken at. ⚠ AIRS reports the book over the CALENDAR year only, so
    a book-sourced window always opens 1 January — 'since inception' has no book equivalent and
    must not silently borrow the model's."""
    if source == "book":
        return f"{date.today().year}-01-01"
    return effective if window == "since" else ytd_anchor_for(effective)


def model_legs(portfolio_id: int, eff: str | None, start: str) -> list[dict]:
    """The model's NOMINAL composition as legs: weight = the design percentage, return = the
    yfinance EUR return over `start`.

    ⚠ A MODEL HAS NO BEGINWAARDE — it is a set of intended percentages, not a book with a value on
    a date. So the basis switch does not reach this path: the weight is the stated percentage
    either way, and only the exclusions below apply.
    """
    pos = (supabase.table("airs_model_portfolio_position")
           .select("isin,fonds,percentage,datum")
           .eq("portfolio_id", portfolio_id).execute().data or [])
    if eff:
        pos = [r for r in pos if r.get("datum") == eff]

    # ⚠ THE SAME EXPANSION THE COMPOSITION CHART USES, from the same function. A certificate has
    # no price series, so unexpanded it lands in `unpriced` — and an unpriced EQUITY is the
    # dangerous exclusion above: its sectors read as UNOWNED.
    from ._airs_lookthrough import expand_positions  # noqa: PLC0415

    pos, _lt = expand_positions(portfolio_id, eff, pos)
    held = sorted({r["isin"] for r in pos if r.get("isin")})
    marks = compute_holding_marks(held, start)
    return [{
        "isin": r.get("isin"),
        "weight_pct": float(r.get("percentage") or 0),
        "return_pct": None if not r.get("isin") else (marks.get(r["isin"]) or {}).get("return_pct"),
        "airs_name": r.get("fonds"),
        "is_cash": not r.get("isin"),
        "via_names": r.get("via_names") or [],
    } for r in pos]


def book_legs(portfolio_id: int) -> list[dict] | None:
    """The paired AIRS BOOK as legs: weight = the START-of-window EUR value as a % of the book,
    return = the VOLK start→current EUR TOTAL return. None when no book is paired.

    ⚠ TOTAL, NOT PRICE — AND THE REASON IS THAT THE ROW BEHIND THIS MODAL ALREADY SAYS TOTAL.
    Expanding a portfolio on /management-dashboard shows a `Return` column computed as
    `(current + gross dividend + dividend_tax) ÷ Beginwaarde − 1` (`startWeights.holdingTotalReturn`
    — the tax is ADDED because AIRS books withholding as a negative, so that sum IS current + NET
    income). This function used to compute `current ÷ Beginwaarde − 1` and drop the income leg
    entirely, so the same holding of the same book showed two different returns on the same page:
    the row's, and the modal's. Both were AIRS-sourced and neither was wrong — they were answers to
    different questions, presented identically.

    So the numerator is now the SAME one, and the income comes from the SAME loader the row uses
    (`_direct_result` over the Mutaties journal, keyed on `holding_name`), rather than a second
    reading of the same journal that could drift from it.

    ⚠ WHICH MAKES THE PORTFOLIO SIDE A TOTAL RETURN AGAINST A PRICE-RETURN BENCHMARK, and that is
    a real asymmetry, not a rounding one. `_asset_benchmark` rebuilds the index on FULL-cap PRICE
    returns — dividends are not in it (validated against ISAC: ~1.1pp/yr of the gap is exactly
    this). Attributed naively, every dividend a holding pays reads as selection skill. It is
    surfaced (`return_basis` on each leg) rather than silently absorbed, because the alternative —
    keeping the modal on a price return so the benchmark comparison stays clean — puts two
    different numbers for one holding on one screen, which is the worse of the two lies.

    ⚠ THE WEIGHT IS THE BEGINWAARDE, NOT THE HUIDIGE WAARDE. Weighting a window's return by the
    CURRENT value overweights the winners (a holding that doubled carries ~2× the share it started
    with), which retroactively inflates the portfolio return — the same look-ahead bias the
    benchmark avoids with start-of-window cap weights. Measured on AITopSelectie: current-weighting
    read +58.75% against the book's true +44.99%. Start-weighting reproduces the realised return
    exactly (Σstartᵢ·retᵢ / Σstartᵢ = (Σcur − Σstart) / Σstart).

    ⚠ A HOLDING WITH NO BEGINWAARDE GETS WEIGHT 0 AND DROPS OUT. It was bought during the window.
    That is right for attribution and is the sharp edge of using this basis for composition too —
    see the module header.
    """
    from routers._airs_account_links import list_account_links  # noqa: PLC0415
    from routers._airs_holding_isin import resolve_account_isins  # noqa: PLC0415
    from ._airs_portfolio_analysis import _expand_book_rows  # noqa: PLC0415

    from ._airs_accounts import _direct_result  # noqa: PLC0415

    link = next((a for a in list_account_links()["accounts"]
                 if a.get("model_portfolio_id") == portfolio_id), None)
    if not link:
        return None
    rows = _expand_book_rows(
        resolve_account_isins(link["portefeuille"], freshen=False).get("rows") or [])
    # THE ROW'S OWN INCOME LOADER, keyed on `holding_name` exactly as `account_holdings` keys it.
    # A second pass over the Mutaties journal here would be a second answer to "what did this
    # holding pay", free to drift from the column the reader is comparing against.
    income, _sold = _direct_result(link["portefeuille"],
                                   {r.get("holding_name") for r in rows if r.get("holding_name")})
    total = sum(float(r.get("start_value_eur") or 0) for r in rows) or 1.0
    out: list[dict] = []
    for r in rows:
        start_val = float(r.get("start_value_eur") or 0)
        cur = float(r.get("current_value_eur") or 0)
        is_cash = r.get("asset_class") == "Cash" or not r.get("isin")
        # ⚠ THE TAX IS ADDED, NOT SUBTRACTED, AND IT IS NOT A TYPO. `tax_eur` is already negative
        # (AIRS books withholding as a debit), so `gross + tax` IS the net. Writing the intuitive
        # `- tax` adds the withholding back and overstates every foreign holding by twice it —
        # silently, because the result is still a plausible number. Same note as `valueWithIncome`.
        d = income.get(r.get("holding_name"))
        net_income = ((d.gross_eur or 0.0) + (d.tax_eur or 0.0)) if d else 0.0
        out.append({
            "isin": r.get("isin"),
            "weight_pct": start_val / total * 100.0,
            "return_pct": (((cur + net_income) / start_val - 1.0) * 100.0)
            if (not is_cash and start_val > 0) else None,
            "airs_name": r.get("holding_name"),
            "is_cash": is_cash,
            "via_names": r.get("via_names") or [],
            "asset_class": r.get("bucket"),
            # What this return INCLUDES, carried so the benchmark comparison can say that its own
            # side does not. `None` income is a book whose journal we have not read; 0.0 is a
            # holding that genuinely paid nothing — kept apart for the same reason the row does.
            "income_eur": (net_income if d else None),
            "return_basis": "total",
        })
    return out


def portfolio_legs(source: str, portfolio_id: int, eff: str | None,
                   start: str) -> list[dict] | None:
    """Legs from the chosen source. None only when `source=book` and no book is paired."""
    return book_legs(portfolio_id) if source == "book" else model_legs(portfolio_id, eff, start)


def split_legs(legs: list[dict], idx: int, grid: dict | None = None,
               codes: dict | None = None) -> tuple[list[dict], list[dict], float]:
    """Split legs into (attributable, excluded, total_weight) on ONE axis.

    ⚠ THIS LADDER IS THE DEFINITION OF "ATTRIBUTABLE" AND IT LIVES HERE ONCE. Both the composition
    axes and the Brinson rows are built from its output, which is the only reason they agree.

    ⚠ TWO KINDS OF EXCLUSION, AND THEY ARE NOT THE SAME FACT.

      fund / cash  genuinely NOT a sector bet. An ETF has no sector; the benchmark's weight in the
                   fund bucket is zero, so Brinson would score holding a world tracker as a sector
                   call. Excluding it is right and costs nothing.
      UNPRICED     a real equity, in a real sector, that we cannot price. Dropping it makes its
                   sector read as UNOWNED — a false finding, not a missing one. It still has to go
                   (there is no return to attribute), so it is flagged LOUDLY instead.

    ⚠ THE LADDER IS PER AXIS. A bond with a known domicile is attributable on `region` and not on
    `sector`; a fund is out on all three. `idx` picks which of `_buckets`'s three answers decides.

    ⚠ NO ASSET-CLASS FILTER HERE, DELIBERATELY. The composition's sector axis used to restrict to
    the {Equity, Equity ETF} sleeve and rely on that to keep bonds out. Two overlapping rules for
    one question is how the panels diverged in the first place — the ladder alone decides, so the
    two sides match BY CONSTRUCTION rather than by the two rules happening to agree.
    """
    grid = _grid(sorted({leg["isin"] for leg in legs if leg.get("isin")})) if grid is None else grid
    codes = _country_by_code() if codes is None else codes

    attributable: list[dict] = []
    excluded: list[dict] = []
    total_w = 0.0
    for h in legs:
        w = h["weight_pct"]
        if w <= 0:
            # ⚠ NOT AN EXCLUSION TO REPORT — a zero-weight leg is a holding bought during the
            # window (no Beginwaarde). It carries no weight to account for, so listing it among
            # the excluded would imply a percentage was taken away from the reader.
            continue
        total_w += w
        isin = h.get("isin")
        is_cash = h.get("is_cash", not isin)
        row = grid.get(isin) if isin else None
        bucket = _buckets(row, is_cash=is_cash, isin=isin, codes=codes)[idx]
        # Cash returns a flat 0% — its drag is a FACT, so it is carried, not invented.
        ret = 0.0 if is_cash else h.get("return_pct")
        reason = ("cash" if bucket == CASH_BUCKET
                  else "unpriced" if ret is None
                  else "unclassified" if bucket == UNKNOWN_BUCKET   # funds fold in here
                  else None)
        item = {**h, "return_pct": ret, "bucket": bucket, "reason": reason, "grid_row": row}
        (excluded if reason else attributable).append(item)
    return attributable, excluded, total_w


def renormalise(attributable: list[dict]) -> float:
    """Σ of the attributable weights — the denominator both sides divide by.

    ⚠ RETURNED RATHER THAN APPLIED, because the callers rebase different shapes (Brinson rebases
    (weight, return) pairs; the composition rebases holding dicts) and both must divide by this
    same number. Zero when nothing is attributable, which the caller must treat as "no answer"
    rather than dividing by it.
    """
    return sum(i["weight_pct"] for i in attributable)
