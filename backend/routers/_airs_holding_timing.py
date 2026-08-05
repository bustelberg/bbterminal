"""The loader behind the Analyse modal's per-holding "why did the trading matter" popup.

⚠ IT READS THE CACHED TRANSACTIONS AND NEVER FETCHES. This opens on a click inside a modal; a
headless AIRS scrape behind it would cost seconds and could collide with a fleet scan holding the
session lock. Same rule as every other read in this modal.
"""
from __future__ import annotations

import logging

from deps import supabase

_log = logging.getLogger(__name__)


def holding_timing(portfolio_id: int, holding_name: str) -> dict:
    """One held position's year, split into buy-and-hold plus the effect of each trade."""
    from airs_capital import detect_split  # noqa: PLC0415
    from airs_timing import analyse_timing  # noqa: PLC0415
    from airs_transacties import ParsedSheet, trades as parse_trades  # noqa: PLC0415

    from routers._airs_account_links import list_account_links  # noqa: PLC0415
    from routers._airs_accounts import account_holdings  # noqa: PLC0415

    link = next((a for a in list_account_links()["accounts"]
                 if a.get("model_portfolio_id") == portfolio_id), None)
    if not link:
        return {"available": False, "name": holding_name,
                "note": "No Dynamic portfolio is paired with this one, so it has no trades to read."}
    portefeuille = link["portefeuille"]

    detail = account_holdings(portefeuille)
    # ⚠ MATCHED ON AIRS'S OWN STRING, EXACTLY. Both sides are AIRS names truncated at the same
    # width; nothing fuzzy belongs here (see `_airs_holding_isin` for what fuzzy matching costs).
    row = next((r for r in (detail.get("rows") or [])
                if r.get("holding_name") == holding_name), None)
    if not row:
        return {"available": False, "name": holding_name,
                "note": "This instrument is not in the book's current holdings — a position that "
                        "has been sold out has no 'held to today' to compare against."}

    cached = (supabase.table("airs_transactie_snapshot").select("columns,kinds,rows")
              .eq("portefeuille", portefeuille).limit(1).execute().data or [])
    if not cached:
        from airs_transacties import LOAD_TRANSACTIONS_HINT  # noqa: PLC0415
        return {"available": False, "name": holding_name,
                "note": "This book's transactions have not been fetched yet, so what it traded "
                        f"cannot be read. {LOAD_TRANSACTIONS_HINT}"}
    sheet = ParsedSheet(columns=cached[0].get("columns") or [],
                        kinds=cached[0].get("kinds") or {}, rows=cached[0].get("rows") or [])

    qty = float(row.get("quantity") or 0)
    start_val = float(row.get("start_value_eur") or 0)
    cur_val = row.get("current_value_eur")

    # ⚠ THE SPLIT IS PROVEN HERE, NOT ASSUMED — same two gates as the ledger, from the same
    # function, so this popup and the table's own figures cannot disagree about the basis.
    events = [r for r in sheet.rows
              if r.get("Fonds") == holding_name and r.get("Tt") not in ("A", "V")]
    split_date = min((r.get("Datum") for r in events if r.get("Datum")), default=None)
    ratio = None
    if events and qty > 0 and start_val > 0:
        deposited = sum(float(r.get("Aantal") or 0) for r in events)
        pre = [abs(float(r.get("Waarde  EUR") or r.get("Waarde  EUR.1") or 0)) / float(r["Aantal"])
               for r in sheet.rows
               if r.get("Fonds") == holding_name and r.get("Tt") in ("A", "V")
               and float(r.get("Aantal") or 0) > 0
               and (not split_date or (r.get("Datum") or "") < split_date)]
        ratio = detect_split(qty, deposited, start_val / qty, pre)
        if ratio is None:
            return {"available": False, "name": holding_name,
                    "note": "Shares were deposited into this position during the year and we "
                            "could not prove what the deposit was, so its trades and its holding "
                            "sit on different share bases. Nothing here can be compared."}

    mine = [t for t in parse_trades(sheet) if t.fonds == holding_name]
    income = float(row.get("dividend_eur") or 0) + float(row.get("dividend_tax_eur") or 0)
    airs_result = ((float(cur_val) - start_val + income)
                   if (cur_val is not None and start_val) else None)

    from routers._airs_transacties import ytd_window  # noqa: PLC0415
    van, tot = ytd_window()
    a = analyse_timing(holding_name, qty, start_val, float(cur_val) if cur_val is not None else None,
                       mine, split_ratio=ratio, split_date=split_date,
                       income_eur=income, airs_result_eur=airs_result,
                       period_start=van, period_end=tot)
    if a is None:
        return {"available": False, "name": holding_name,
                "note": "This position has no opening value to measure against — it was not held "
                        "when the year opened."}
    if not a.reconciles:
        # ⚠ LOUD. The three lines are presented as a decomposition; if they do not add up they are
        # three numbers beside each other and the UI must not imply otherwise.
        _log.warning("[timing] %s / %s does NOT reconcile: residual EUR %.2f",
                     portefeuille, holding_name, a.residual_eur)
    return {
        "available": True,
        "name": a.name,
        "portefeuille": portefeuille,
        "qty_open": a.qty_open,
        "qty_now": a.qty_now,
        "price_open_eur": a.price_open_eur,
        "price_now_eur": a.price_now_eur,
        "buy_hold_eur": a.buy_hold_eur,
        "timing_eur": a.timing_eur,
        "actual_eur": a.actual_eur,
        "open_value_eur": a.open_value_eur,
        "buy_hold_pct": a.buy_hold_pct,
        "timing_pp": a.timing_pp,
        "actual_pct": a.actual_pct,
        "residual_eur": a.residual_eur,
        "reconciles": a.reconciles,
        "airs_result_eur": a.airs_result_eur,
        "restatement_eur": a.restatement_eur,
        "income_eur": a.income_eur,
        "split_ratio": a.split_ratio,
        "period_start": a.period_start,
        "period_end": a.period_end,
        "note": None,
        "trades": [{
            "datum": t.datum, "kind": t.kind, "quantity": t.quantity,
            "price_eur": t.price_eur, "amount_eur": t.amount_eur,
            "effect_eur": t.effect_eur, "rescaled": t.rescaled,
            "move_pct": t.move_pct, "effect_pp": t.effect_pp,
        } for t in a.trades],
    }
