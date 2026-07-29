"""The one table: a portfolio, by the name you gave it, on AIRS's own numbers.

WHAT IT JOINS, AND WHY EACH SIDE IS THERE
    A portfolio exists in AIRS as TWO rows that share nothing but a strategy:

        the Fixed one (`_FX`/`_AFS`)   weights + ISINCode + your nickname — AIRS values none of it
        the Dynamic one (`_DYN`)       the real book: quantities, EUR values, returns — NO ISIN

    Measured: of 58 Fixed portfolios with a composition and 31 AIRS-valued Dynamic ones, the
    overlap is ZERO. Neither is the portfolio; the pair is. This composes them into one row so a
    reader does not have to hold two tables in their head and a naming convention between them.

    NAME comes from the Fixed side (`display_name` — the nickname; all 28 linked pairs have one).
    EVERY NUMBER comes from the Dynamic side, because AIRS is the system of record for what a
    book made and we are not.

⚠ THE PAIRING IS MOSTLY UNCONFIRMED, AND THE ROW SAYS SO. 27 of the 28 links are `guess` — an
    exact stem match nobody has approved (`_airs_account_links`). A guess is not a small doubt
    here: the risk variants of a strategy hold the SAME instruments (BUS_FTS_Bepoff/DEF/NEU_AFS
    share 27 of 27 ISINs), so a mis-pairing shows a book's real money under another strategy's
    name and nothing on the row would look wrong. `link_source` rides along for that reason.

⚠ AN UNLINKED DYNAMIC PORTFOLIO IS NOT DROPPED. It has AIRS numbers and no nickname — 23 of the
    51, the benchmarks among them. Hiding them would make this table quietly disagree with the
    Dynamic table it summarises; they appear with the AIRS name and no ISINs behind them.
"""
from __future__ import annotations

import asyncio

from deps import supabase


def list_overview() -> list[dict]:
    """One row per AIRS Dynamic portfolio, named by the Fixed portfolio it runs."""
    from ._airs_account_links import list_account_links  # noqa: PLC0415  (circular at import)
    from ._airs_accounts import list_accounts  # noqa: PLC0415

    links = {a["portefeuille"]: a for a in list_account_links()["accounts"]}
    models = {m["id"]: m for m in (supabase.table("airs_model_portfolio")
                                   .select("id,name,display_name,omschrijving,portfolio_type")
                                   .limit(500).execute().data or [])}
    positions: dict[int, int] = {}
    for p in (supabase.table("airs_model_portfolio_position").select("portfolio_id")
              .limit(20000).execute().data or []):
        positions[p["portfolio_id"]] = positions.get(p["portfolio_id"], 0) + 1

    out: list[dict] = []
    for a in list_accounts():
        link = links.get(a["portefeuille"]) or {}
        m = models.get(link.get("model_portfolio_id")) if link.get("model_portfolio_id") else None
        out.append({
            # The name a human gave it. Falls back to AIRS's code rather than to a blank: an
            # unlinked book is still a book, and a nameless row is unreadable.
            "name": (m or {}).get("display_name") or a["portefeuille"],
            "description": (m or {}).get("omschrijving"),
            "dynamic_portefeuille": a["portefeuille"],
            "fixed_name": (m or {}).get("name"),
            "fixed_portfolio_id": (m or {}).get("id"),
            "fixed_type": (m or {}).get("portfolio_type"),
            # How many ISINs the pairing can reach. None = unlinked, which is not 0 (that would
            # claim a Fixed portfolio holding nothing).
            "isins": positions.get((m or {}).get("id")) if m else None,
            "link_source": link.get("source") or "none",
            "link_reason": link.get("reason"),
            # --- AIRS's own, all of it, all the year's. Never recomputed here. ---
            "as_of": a.get("as_of"),
            "periode": a.get("periode"),
            "months": a.get("months"),
            "ytd_pct": a.get("ytd_pct"),
            "latest_month_pct": a.get("latest_month_pct"),
            "price_result_eur": a.get("price_result_eur"),
            "income_eur": a.get("income_eur"),
            "investment_result_eur": a.get("investment_result_eur"),
            "deposits_eur": a.get("deposits_eur"),
            "withdrawals_eur": a.get("withdrawals_eur"),
            "begin_value_eur": a.get("begin_value_eur"),
            "end_value_eur": a.get("end_value_eur"),
            "holdings": a.get("holdings"),
            "reconciles": a.get("reconciles"),
            # Carried straight through from `list_accounts` — the overview is a composition of that
            # row with its Fixed pairing, so a caveat about the row's freshness belongs on it.
            "missing_reports": a.get("missing_reports") or [],
            "residual_eur": a.get("residual_eur"),
        })
    # Linked first (they are the ones with a name), then by that name.
    out.sort(key=lambda r: (r["fixed_name"] is None, (r["name"] or "").lower()))
    return out


async def list_overview_async() -> list[dict]:
    return await asyncio.to_thread(list_overview)
