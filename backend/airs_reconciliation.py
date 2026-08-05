"""Why the book's own YTD is not the YTD its open positions add up to.

THE FACT THIS EXISTS FOR, MEASURED 2026-08-05 OVER 39 ACCOUNTS
    The /portfolios positions table reports a start-weighted return over the positions a book
    HOLDS. AIRS reports `cumulatief_rendement`, its own flow-aware figure for the same year. They
    disagree by more than 1pp on **23 of 39 accounts**:

        AITopSelectie OFF DYN     book +38.73%   open positions +37.84%   -0.89pp
        BUS_FTS_BEPOFF_DYN        book  -4.57%   open positions  -1.30%   +3.27pp
        BUS_BM_AAN_ww_EUR_2026_d  book +13.86%   open positions +11.99%   -1.87pp

    Both numbers are already on screen, a few lines apart, with nothing saying why they differ.
    That is the pair a reader cannot arbitrate — and the bigger one is not the safer one.

⚠ A RECONCILIATION IS DONE IN EUROS, NOT IN PERCENT. Percentages over different denominators do
    not add: the book's return is measured on `beginvermogen` and the table's on the sum of the
    rows' `Beginwaarde`, which are different numbers (see below). Every component here is a euro
    amount that sums exactly; the percentages are shown, never summed.

⚠ THE TWO OPENING CAPITALS DIFFER FOR **TWO** REASONS, WITH OPPOSITE SIGNS, AND THEY CANNOT BE
    SEPARATED WITHOUT THE TRANSACTIONS. Measured on AITopSelectie: `beginvermogen` 1,000,000
    against Σ Beginwaarde 1,006,881 — the ROWS claim MORE opening value than the book had.

      * a position sold outright during the year has opening value in the book and NO ROW LEFT to
        carry it            -> pushes the book's opening ABOVE the rows'   (BUS_FTS_BEPOFF_DYN:
                               1,000,782 against 960,232 — EUR 40,550 of opening value with no row)
      * AIRS RESTATES `Beginwaarde lopend jaar` TO THE CURRENT QUANTITY, so a position bought into
        during the year carries an opening value for shares it did not own in January
                            -> pushes the rows' opening ABOVE the book's   (AITopSelectie)

    Netting to one number, as `start_gap_eur` must, is honest only because it is LABELLED as the
    net of two effects. Reporting it as "closed positions" would be a claim, and on AITopSelectie
    it would be a claim of a NEGATIVE amount of closed positions.

⚠ WHAT IS NOT GUESSED. `sold_income_eur` is measured — the Mutaties journal names the funds that
    paid this book and are no longer held (`airs_mutaties.attach`'s unattached half). Everything
    still unaccounted for after that is reported AS unaccounted for, under its own name, rather
    than distributed across the components that happen to be computable. A residual folded into a
    number that reconciles is how a reconciliation stops being one.

AND WITH THE TRANSACTIONS, IT CLOSES. Measured on AITopSelectie OFF DYN 2026-08-05:

        book's own result (beleggingsresultaat)      387,293.75
        open positions still held                    380,986.94
        realised on sales this year (Res. YtD)         6,306.85
        income from funds no longer held                   0.00
        ----------------------------------------------------------
        residual                                          -0.04     <- rounding, on 387k

    That is the whole year, accounted for by the positions. `total_result_eur` is that sum, and
    `residual_vs_book_eur` is what it fails to explain — asserted every time, never assumed.

⚠⚠ A RESULT DIVIDED BY AN OPENING CAPITAL IS ONLY A RETURN WHEN NOTHING WAS PAID IN OR OUT.
    387,293.75 / 1,000,000 = 38.729375%, which IS `cumulatief_rendement` to the sixth decimal —
    but only because that book has zero `stortingen` and zero `onttrekkingen`. AzTopSelectie_DYN
    opened at ZERO and took a EUR 1,000,000 deposit: the same division is undefined, and any
    figure produced from it would be a fiction where AIRS's own reads -0.12%. So
    `total_return_pct` is computed ONLY on a flow-free book, and `return_basis` says which case
    the reader is in. Reproducing a flow-aware return from positions is not a harder version of
    this arithmetic; it is a different measure, and AIRS already publishes it.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class OpenSide:
    """What the positions still held explain, in euros."""

    start_eur: float = 0.0
    # ⚠ Current value PLUS net income — the same total-return numerator the positions table's
    # Return column uses. A price-only figure here would leave every dividend in the residual and
    # make the reconciliation blame the wrong thing.
    end_eur: float = 0.0
    priced: int = 0
    unpriced: int = 0

    @property
    def result_eur(self) -> float:
        return round(self.end_eur - self.start_eur, 2)

    @property
    def return_pct(self) -> float | None:
        """⚠ None, never 0, when nothing has an opening value. A book whose rows cannot be priced
        has an UNDEFINED return over them, and a 0.00% beside the book's +38% reads as a finding."""
        return None if self.start_eur <= 0 else (self.end_eur / self.start_eur - 1) * 100


@dataclass
class Reconciliation:
    # The book's own, from AIRS's monthly ATT rows. Authoritative — never recomputed here.
    book_return_pct: float | None = None
    book_result_eur: float | None = None
    book_start_eur: float | None = None
    book_end_eur: float | None = None
    deposits_eur: float = 0.0
    withdrawals_eur: float = 0.0
    costs_eur: float = 0.0
    # AIRS's own internal check on the months we stored, passed through rather than re-derived.
    book_reconciles: bool | None = None

    open: OpenSide = field(default_factory=OpenSide)

    # Income paid to this book by funds it no longer holds. MEASURED (the Mutaties journal), so it
    # leaves the residual rather than sitting in it.
    sold_income_eur: float = 0.0
    sold_funds: list[str] = field(default_factory=list)

    # ── What the sales realised this year, from the Transacties sheet. None = no sheet cached, and
    # that is NOT zero: a book whose transactions have never been fetched has an UNKNOWN realised
    # result, and reporting 0 would hand it the open positions' figure as its total.
    realised_ytd_eur: float | None = None
    realised_names: int = 0
    # Why the sheet could not be read, when it could not.
    realised_note: str | None = None

    # The net of the two opposite effects described in the module docstring. Labelled, never
    # called "closed positions".
    start_gap_eur: float | None = None

    # ── The answer the panel exists for: the year, built from the positions.
    total_result_eur: float | None = None
    total_return_pct: float | None = None
    # 'opening_capital' when the division is valid, 'flows' when it is refused and why.
    return_basis: str | None = None
    # ⚠ ASSERTED, NOT ASSUMED. What `total_result_eur` fails to explain of the book's own result.
    residual_vs_book_eur: float | None = None
    reconciles: bool | None = None

    # What nothing on hand explains, BEFORE the sales are counted. Kept beside the residual so the
    # reader can see how much of the gap the transactions actually closed.
    unexplained_eur: float | None = None
    gap_pp: float | None = None

    # How many Transacties rows are cached for this book. None = never fetched.
    transaction_rows: int | None = None
    # Transaction types the parser does not interpret, with counts (e.g. {'D': 1}).
    unknown_transaction_types: dict[str, int] = field(default_factory=dict)


# Below this, the residual is rounding rather than a missing leg. The measured case lands at
# EUR 0.04 on a EUR 387k year; EUR 1 is the same threshold `_year_perf` uses for AIRS's own check.
RECONCILES_EUR = 1.0


def reconcile(book: dict | None, open_side: OpenSide, sold_income_eur: float = 0.0,
              sold_funds: list[str] | None = None,
              transaction_rows: int | None = None,
              realised_ytd_eur: float | None = None, realised_names: int = 0,
              realised_note: str | None = None,
              unknown_transaction_types: dict[str, int] | None = None) -> Reconciliation:
    """Line up the book's own year against the positions — held AND sold.

    `book` is one account's aggregated ATT row (`_airs_accounts._year_perf`). None when AIRS has
    stored no performance for it — in which case there is nothing authoritative to reconcile
    AGAINST, and saying so beats presenting the positions' figure as though it were the book's.

    `realised_ytd_eur` is None when no Transacties sheet is cached. ⚠ That is NOT zero: an
    unfetched sheet leaves the realised result UNKNOWN, and defaulting it to 0 would publish the
    open positions' total as the year's — understating by exactly the amount nobody had looked up.
    """
    r = Reconciliation(open=open_side, sold_income_eur=round(sold_income_eur, 2),
                       sold_funds=sorted(sold_funds or []), transaction_rows=transaction_rows,
                       realised_ytd_eur=(None if realised_ytd_eur is None
                                         else round(realised_ytd_eur, 2)),
                       realised_names=realised_names, realised_note=realised_note,
                       unknown_transaction_types=dict(unknown_transaction_types or {}))

    # ⚠ THE TOTAL NEEDS EVERY LEG, SO IT IS NONE UNTIL IT HAS THEM. Held + realised + income from
    # names no longer held: drop any one and the sum is not the year.
    if r.realised_ytd_eur is not None and open_side.start_eur > 0:
        r.total_result_eur = round(
            open_side.result_eur + r.realised_ytd_eur + r.sold_income_eur, 2)

    if not book:
        return r

    r.book_return_pct = _f(book.get("cumulatief_rendement"))
    # ⚠ `beleggingsresultaat`, NOT `eindvermogen - beginvermogen`. The difference is deposits and
    # withdrawals, and on a book that took EUR 1m mid-year the subtraction reports the deposit as
    # profit (AzTopSelectie: begin 0, end 998,784, and it LOST 1,216).
    r.book_result_eur = _f(book.get("beleggingsresultaat"))
    r.book_start_eur = _f(book.get("beginvermogen"))
    r.book_end_eur = _f(book.get("eindvermogen"))
    r.deposits_eur = _f(book.get("stortingen")) or 0.0
    r.withdrawals_eur = _f(book.get("onttrekkingen")) or 0.0
    r.costs_eur = _f(book.get("kosten")) or 0.0
    r.book_reconciles = book.get("reconciles")

    if r.book_start_eur is not None and open_side.start_eur > 0:
        r.start_gap_eur = round(r.book_start_eur - open_side.start_eur, 2)
    if r.book_result_eur is not None:
        r.unexplained_eur = round(
            r.book_result_eur - open_side.result_eur - r.sold_income_eur, 2)
    # ⚠ pp, NOT %. It is a difference between two percentages measured on different denominators;
    # calling it a percentage invites dividing by it.
    if r.book_return_pct is not None and open_side.return_pct is not None:
        r.gap_pp = round(open_side.return_pct - r.book_return_pct, 4)

    # ⚠ THE CHECK IS THE PRODUCT. A total assembled from three legs that is never set against the
    # book's own figure is an assertion; set against it, it is a reconciliation. Measured residual
    # on AITopSelectie: EUR -0.04 on a EUR 387,293.75 year.
    if r.total_result_eur is not None and r.book_result_eur is not None:
        r.residual_vs_book_eur = round(r.total_result_eur - r.book_result_eur, 2)
        r.reconciles = abs(r.residual_vs_book_eur) < RECONCILES_EUR

    # ⚠⚠ ONLY ON A FLOW-FREE BOOK. `result / opening capital` reproduces `cumulatief_rendement`
    # exactly — verified across the fleet 2026-08-05, on ALL 30 accounts with no flows, to within
    # 0.01pp — and is undefined the moment money moves: AzTopSelectie opened at ZERO and took
    # EUR 1m, where the division is by zero and any number from it would be invented. AIRS's own
    # figure is flow-aware and is the one to read there, so the basis is named, not fudged.
    #
    # ⚠ GROSS, NOT NET. EUR 100k in during January and EUR 100k out in December nets to zero and
    # is emphatically not a flow-free year: the extra capital was invested for eleven months, so
    # `beginvermogen` is no longer the capital the result was earned on. Netting them would let
    # exactly the case that most needs the flow-aware figure pass as safe.
    flows = round(abs(r.deposits_eur) + abs(r.withdrawals_eur), 2)
    if r.total_result_eur is None or not r.book_start_eur:
        r.return_basis = "unavailable"
    elif flows:
        r.return_basis = "flows"
    else:
        r.return_basis = "opening_capital"
        r.total_return_pct = r.total_result_eur / r.book_start_eur * 100
    return r


def open_side_from_rows(rows: list[dict]) -> OpenSide:
    """The open positions' two totals, on the SAME basis the positions table's own Total uses:
    priced rows only, current value plus NET income over opening value.

    ⚠ A ROW WITHOUT AN OPENING VALUE IS NOT A ZERO, IT IS OUT. It was not held when the year
    opened (or is a cash line), so its return is undefined — including it at 0 would drag the
    result toward zero by an amount nothing on screen could account for. Counted as `unpriced`,
    so the reader can see how much of the book the comparison actually spans.

    ⚠ THE TAX IS ADDED, NOT SUBTRACTED. `dividend_tax_eur` is already negative (AIRS books
    withholding as a debit), so `gross + tax` IS the net — the same trap `startWeights` documents,
    where the intuitive minus overstates every foreign holding by twice the withholding.
    """
    side = OpenSide()
    for r in rows:
        start = _f(r.get("start_value_eur")) or 0.0
        end = _f(r.get("current_value_eur"))
        if start == 0 or end is None:
            side.unpriced += 1
            continue
        income = (_f(r.get("dividend_eur")) or 0.0) + (_f(r.get("dividend_tax_eur")) or 0.0)
        side.start_eur = round(side.start_eur + start, 2)
        side.end_eur = round(side.end_eur + end + income, 2)
        side.priced += 1
    return side


def _f(v: object) -> float | None:
    if v is None:
        return None
    try:
        return float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
