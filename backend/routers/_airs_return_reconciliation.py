"""The loader behind /portfolios → expand an account → "Total return".

⚠ IT READS THE FIGURES THE OTHER PANELS SHOW; IT DOES NOT RECOMPUTE THEM. The book's side comes
from `_year_perf` (the same aggregation the account row's YTD column is drawn from) and the open
side from `account_holdings` (the same rows the positions table renders). A reconciliation that
derived either side "the same way" would be a third number, free to disagree with both of the ones
it claims to be reconciling — which is the exact failure this panel exists to expose.
"""
from __future__ import annotations

import logging

from deps import supabase

_log = logging.getLogger(__name__)


def _realised(portefeuille: str):
    """This book's realised result this year, from its CACHED Transacties sheet.

    ⚠ IT DOES NOT FETCH. A reconciliation panel that silently drove a headless AIRS session would
    turn a read into a multi-second scrape, and — worse — would do it while a fleet scan may hold
    the lock. The Transactions panel above is where a fetch is asked for; this reads what that
    produced.

    ⚠ NO SHEET IS **NOT** ZERO REALISED. Returning 0 here would publish the open positions' figure
    as the year's total, understating it by exactly the amount nobody had looked up — silently,
    and with a total that still reconciles against nothing. `None` propagates all the way to the
    UI, which says "fetch the transactions first" instead of showing a number.
    """
    from airs_transacties import ParsedSheet, realised_results  # noqa: PLC0415

    try:
        rows = (supabase.table("airs_transactie_snapshot").select("columns,kinds,rows")
                .eq("portefeuille", portefeuille).limit(1).execute().data or [])
    except Exception as e:  # noqa: BLE001 — a cache fault must not break the panel
        _log.warning("[airs_reconciliation] transactions unavailable for %s (%s: %s)",
                     portefeuille, type(e).__name__, e)
        return None, None
    if not rows:
        return None, None
    r = rows[0]
    sheet = ParsedSheet(columns=r.get("columns") or [], kinds=r.get("kinds") or {},
                        rows=r.get("rows") or [])
    return sheet, realised_results(sheet)


def account_return_reconciliation(portefeuille: str) -> dict:
    """One account's book YTD lined up against what its open positions explain."""
    from airs_reconciliation import open_side_from_rows, reconcile  # noqa: PLC0415

    from routers._airs_accounts import _year_perf, account_holdings  # noqa: PLC0415

    book = _year_perf().get(portefeuille)
    detail = account_holdings(portefeuille)
    rows = detail.get("rows") or []
    side = open_side_from_rows(rows)
    # ⚠ NET, and the tax is ADDED because AIRS books withholding negative. This is the income of
    # funds the book no longer holds — already measured by the Mutaties join, which is why it can
    # leave the residual instead of sitting inside it unnamed.
    sold_income = ((detail.get("dividend_sold_eur") or 0.0)
                   + (detail.get("dividend_sold_tax_eur") or 0.0))

    sheet, realised = _realised(portefeuille)
    # ⚠ THE NAMES THAT ARE GENUINELY CLOSED OUT ARE DECIDED BY ABSENCE FROM THE HOLDINGS, NOT BY
    # PRESENCE IN THE SALES. A partial sale leaves the position open — Synopsys was sold on
    # 2026-01-22 and is still held — so labelling every sold name "closed" would be wrong for most
    # of them. Matched on AIRS's own string, EXACTLY: both sides are AIRS names truncated at the
    # same width, and nothing fuzzy belongs here.
    held = {r.get("holding_name") for r in rows if r.get("holding_name")}
    legs = sorted((realised.legs.values() if realised else []),
                  key=lambda leg: -abs(leg.realised_ytd_eur))
    r = reconcile(
        book, side, sold_income, detail.get("dividend_sold_funds") or [],
        transaction_rows=(len(sheet.rows) if sheet else None),
        realised_ytd_eur=(realised.realised_ytd_eur
                          if realised and not realised.unreadable else None),
        realised_names=len(legs),
        realised_note=(realised.unreadable if realised else None),
        unknown_transaction_types=(realised.unknown_types if realised else {}),
        # ⚠ THE HELD LEG'S OWN CLOCK. It is the VOLK snapshot date and the book's result is the ATT
        # report's — two downloads, routinely a day apart, and one day of market movement on a
        # EUR 1.4m book is tens of thousands of euros of "unexplained" residual. Passed so the
        # check can say "the calendar" rather than "a missing position".
        holdings_as_of=detail.get("as_of"),
    )
    return {
        "portefeuille": portefeuille,
        "realised": [{
            "fonds": leg.fonds,
            "sales": leg.sales,
            "quantity": leg.quantity,
            "proceeds_eur": leg.proceeds_eur,
            "cost_eur": leg.cost_eur,
            "realised_ytd_eur": leg.realised_ytd_eur,
            # ⚠ SURFACED, because it is the whole reason `Res. YtD` is used rather than
            # proceeds − cost. Non-zero means part of this gain was made in an earlier year and is
            # correctly NOT in this year's total.
            "prior_year_eur": leg.prior_year_eur,
            "first": leg.first,
            "last": leg.last,
            # Sold entirely, versus trimmed and still running. The distinction the sales sheet
            # cannot make on its own.
            "closed_out": leg.fonds not in held,
        } for leg in legs],
        "buys_eur": (realised.buys_eur if realised else None),
        "buy_count": (realised.buy_count if realised else None),
        "as_of": detail.get("as_of"),
        "periode": (book or {}).get("periode"),
        "months": (book or {}).get("months"),
        "book_return_pct": r.book_return_pct,
        "book_result_eur": r.book_result_eur,
        "book_start_eur": r.book_start_eur,
        "book_end_eur": r.book_end_eur,
        "deposits_eur": r.deposits_eur,
        "withdrawals_eur": r.withdrawals_eur,
        "costs_eur": r.costs_eur,
        "book_reconciles": r.book_reconciles,
        "open_return_pct": r.open.return_pct,
        "open_result_eur": r.open.result_eur,
        "open_start_eur": r.open.start_eur,
        "open_end_eur": r.open.end_eur,
        "open_priced": r.open.priced,
        "open_unpriced": r.open.unpriced,
        "sold_income_eur": r.sold_income_eur,
        "sold_funds": r.sold_funds,
        "start_gap_eur": r.start_gap_eur,
        "realised_ytd_eur": r.realised_ytd_eur,
        "realised_names": r.realised_names,
        "realised_note": r.realised_note,
        "total_result_eur": r.total_result_eur,
        "total_return_pct": r.total_return_pct,
        "return_basis": r.return_basis,
        "residual_vs_book_eur": r.residual_vs_book_eur,
        "reconciles": r.reconciles,
        "holdings_as_of": r.holdings_as_of,
        "book_as_of": r.book_as_of,
        "dates_aligned": r.dates_aligned,
        "residual_reason": r.residual_reason,
        "unexplained_eur": r.unexplained_eur,
        "gap_pp": r.gap_pp,
        "transaction_rows": r.transaction_rows,
        "unknown_transaction_types": r.unknown_transaction_types,
    }
