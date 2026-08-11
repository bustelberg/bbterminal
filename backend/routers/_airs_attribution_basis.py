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

from routers._airs_ref import positions_for as ref_positions_for

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
    pos = ref_positions_for(portfolio_id)       # one shared read — see `_airs_ref`
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


def book_legs(portfolio_id: int, start: str) -> list[dict] | None:
    """The paired AIRS BOOK as legs: weight = the START-of-window EUR value as a % of the book,
    return = the INSTRUMENT's own EUR price return over `start`. None when no book is paired.

    ⚠⚠ THE RETURN IS PRICED FROM `asset_price`, NOT FROM AIRS, AND THAT IS THE WHOLE POINT OF THIS
    FUNCTION'S EXISTENCE IN A BRINSON PANEL. Selection effect is `w_b × (R_p,bucket − R_b,bucket)`:
    it subtracts the portfolio's return from the BENCHMARK's for the same names. The benchmark is
    rebuilt from `asset_price` (`_asset_benchmark`), so pricing our side off AIRS put the SAME
    instrument on the two sides of that subtraction at two different numbers, and Brinson booked
    the difference as skill. Measured 2026-08-05 on direct holdings only, over one window:

        ASML Holding              AIRS +49.68%   yfinance +63.70%   -14.03pp
        Lam Research              AIRS +74.16%   yfinance +89.90%   -15.74pp
        Taiwan Semiconductor ADR  AIRS +32.55%   yfinance +46.77%   -14.21pp

        AITopSelectie   median |gap| 3.39pp, 18 of 20 legs over 1pp
        BUS_Offensief   median |gap| 2.20pp, 20 of 25 legs over 1pp

    Hold ASML at exactly its index weight and the old basis still reported a 14pp selection effect
    on it. The gaps are also ONE-DIRECTIONAL (AIRS below yfinance — the holdings snapshot is a day
    or more behind the latest close), so they bias rather than cancel. CLAUDE.md already carried
    the rule — "THE BENCHMARK MUST BE PRICED IN THE SAME WORLD AS THE PORTFOLIO" — and it had been
    applied to the benchmark and never back-applied here.

    ⚠⚠ AND IT FIXES A SECOND FAULT THAT WAS WORSE. `_expand_book_rows` splits a certificate's start
    AND current value by each holding's share, so every instrument inside one came out with the
    WRAPPER's return: BUS_Offensief's 50 legs carried 31 distinct returns, and its 23 wrapped legs
    carried FOUR between them. "Selection" on those was measuring the certificate. This is the same
    defect `_airs_portfolio_analysis` documents and fixed for its holdings table (NVIDIA read
    +0.08% against its own +2.82%); `book_legs` fed Brinson and never got the fix. Pricing the
    INSTRUMENT rather than the book's slice of it closes both faults with one source.

    ⚠ THE PRICE THIS PAYS, AND IT IS REAL: these legs no longer reproduce AIRS's own
    `cumulatief_rendement`. They are not AIRS's numbers any more. That is correct for a RELATIVE
    decomposition — a difference between two vendors is not alpha — but it means the panel's
    portfolio return will sit a little away from the book's own. `airs_return_pct` rides along on
    every leg so the gap can be shown rather than discovered, and the headline return elsewhere in
    the modal still comes from AIRS, which remains the system of record for what the book MADE.

    ⚠ IT ALSO RETIRES THE TOTAL-vs-PRICE ASYMMETRY. The old basis was a TOTAL return (income in the
    numerator) against a PRICE-return benchmark, so every dividend a holding paid read as selection
    skill (~1.1pp/yr, validated against ISAC). Both sides are now price returns. The income is
    still loaded and still carried per leg (`income_eur`), because the reader is owed the fact that
    it is NOT in the comparison — it is simply no longer smuggled into one side of it.

    ⚠ THE WEIGHT IS STILL AIRS'S BEGINWAARDE, AND DELIBERATELY SO. A weight does not need the two
    sides to share a vendor — Brinson compares OUR weight against the INDEX's by construction —
    and the Beginwaarde share is what the book actually held. Only the return had to be unified.

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
    # ⚠ THE SAME LOADER `model_legs` USES, OVER THE SAME WINDOW — which is what puts the book path
    # and the model path on one basis as well, not just this side and the benchmark. Priced AFTER
    # the expansion, so a looked-through leg is priced as the INSTRUMENT it is rather than as its
    # certificate's slice.
    marks = compute_holding_marks(sorted({r["isin"] for r in rows if r.get("isin")}), start)
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
        isin = r.get("isin")
        out.append({
            "isin": isin,
            "weight_pct": start_val / total * 100.0,
            # ⚠ THE INSTRUMENT'S OWN EUR PRICE RETURN, from the SAME series the benchmark is built
            # from. None where we cannot price it — which `split_legs` then reports as `unpriced`,
            # the one exclusion that is a genuine gap rather than an answer.
            "return_pct": None if not isin else (marks.get(isin) or {}).get("return_pct"),
            # ⚠ AIRS'S OWN FIGURE, CARRIED BUT NOT USED. It is what the book says this position
            # made, and it is how the difference between this panel and the book's own return can
            # be shown rather than discovered. ⚠ For a leg inside a certificate it is the WRAPPER's
            # rate stamped on this holding — never present it as the instrument's.
            "airs_return_pct": (((cur + net_income) / start_val - 1.0) * 100.0)
            if (not is_cash and start_val > 0) else None,
            "airs_name": r.get("holding_name"),
            "is_cash": is_cash,
            "via_names": r.get("via_names") or [],
            "asset_class": r.get("bucket"),
            # ⚠ CARRIED, AND NO LONGER INSIDE THE RETURN. Both sides of the comparison are price
            # returns now, so a dividend can no longer read as selection skill — but the reader is
            # still owed the fact that income exists and is OUT of the comparison. `None` is a book
            # whose journal we have not read; 0.0 is a holding that genuinely paid nothing.
            "income_eur": (net_income if d else None),
            "return_basis": "price",
        })
    return out


def portfolio_legs(source: str, portfolio_id: int, eff: str | None,
                   start: str) -> list[dict] | None:
    """Legs from the chosen source. None only when `source=book` and no book is paired."""
    # ⚠ BOTH PATHS NOW TAKE `start` AND BOTH PRICE FROM `asset_price`. The book path used to price
    # itself off AIRS and ignore the window entirely, so switching `source` changed the VENDOR as
    # well as the weights — two variables at once, on a control the reader thinks moves one.
    return (book_legs(portfolio_id, start) if source == "book"
            else model_legs(portfolio_id, eff, start))


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
