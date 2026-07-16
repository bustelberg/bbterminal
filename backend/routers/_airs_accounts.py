"""The AIRS ACCOUNTS — what the books actually made, on AIRS's own numbers.

WHY THIS EXISTS BESIDE THE MODEL VIEW, RATHER THAN REPLACING IT
    A model portfolio (`*_FX`) is a COMPOSITION — weights, no holdings — so AIRS has nothing to
    value and no Vermogensoverzicht exists for one. Measured: of 58 models with a composition and
    39 accounts with AIRS values, the overlap is ZERO. They answer different questions:

        the model    "would this strategy work?"      -> priced from yfinance; nothing else can
                                                          value a set of weights
        the account  "what did this book make?"       -> AIRS knows, authoritatively

    The gap between them is implementation drift, timing and fees, and it is worth seeing.

WHY AIRS AND NOT YFINANCE, HERE
    Measured 2026-07-16 on AMD: AIRS's implied prices agree with our yfinance series to +0.5% at
    the open and -0.8% at the close, and AIRS is THREE DAYS FRESHER (2026-07-16 vs 2026-07-13).
    But the real prize is coverage: `TOPS_OFF_BEH_DYN` holds Leonteq AMC certificates that Yahoo
    has no listing for — the zero-bar guard refuses them, correctly — and AIRS values 7 of 7 where
    the yfinance path prices 0 of 9. AIRS is the custodian's system; it does not need a listing.

⚠⚠ THE PORTFOLIO RETURN IS `cumulatief_rendement`, AND NEVER `eindvermogen / beginvermogen`.

    THE VALUE RATIO IS NOT A RETURN. It is the return only when nothing was deposited or
    withdrawn, and these are real accounts. AIRS publishes BOTH numbers and they are not close:

        `rendement`  == eindvermogen/beginvermogen - 1   -- exact, in 38 of 38 accounts
        `cumulatief_rendement`                            -- AIRS's own, flow-aware

        AITopSelectie OFF DYN     ratio  -5.85%   actual  +46.12%   gap +51.97pp
        BUS_BM_AAN_ww_EUR_2026_d         +0.40%           +14.29%       +13.90pp
        BUS_FTS_BEPOFF_DYN               +2.43%            -5.08%        -7.51pp

    31 of 38 disagree by more than a point. Summing the holdings' values and dividing
    (`sum(current)/sum(start)`) is the SAME wrong number wearing different arithmetic — it was the
    obvious way to build this view, and it would have reported -5.85% on a book that made +46%.

⚠ THE HOLDINGS DO NOT SUM TO THE PORTFOLIO RETURN, AND THAT IS CORRECT.
    They are different quantities, not a reconciliation that failed:
      - each holding's figure is a PRICE return (AIRS restates `Beginwaarde lopend jaar` to the
        current quantity — measured on 32 of 36 quantity changes; the 4 that do not are KLA's
        10:1 split, where the VALUE correctly does not move);
      - the portfolio's figure is flow-aware AND includes `opbrengsten` (income), which no price
        return contains.
    The /portfolios MODEL view has the opposite property — its holdings weight exactly to its
    total — so a reader arriving from there will expect these to tie. They must be told.
"""
from __future__ import annotations

import asyncio

from deps import supabase

# AIRS reports a portfolio's own return; we never recompute it. These are the columns it uses.
_PERF_COLS = ("portefeuille,periode,beginvermogen,koersresultaat,opbrengsten,"
              "beleggingsresultaat,eindvermogen,rendement,cumulatief_rendement")


def _latest_perf() -> dict[str, dict]:
    """The freshest performance row per account. `periode` is the window's END; `beginvermogen`
    is the year's opening capital, so each row is 1 Jan -> `periode`."""
    rows = (supabase.table("airs_performance").select(_PERF_COLS)
            .order("periode", desc=True).limit(5000).execute().data or [])
    out: dict[str, dict] = {}
    for r in rows:
        out.setdefault(r["portefeuille"], r)      # ordered desc, so the first seen is freshest
    return out


def _holding_counts() -> tuple[dict[str, int], dict[str, str]]:
    """(holdings per account, that account's snapshot date) — off the freshest snapshot only."""
    rows = (supabase.table("airs_holding").select("portefeuille,as_of_date,holding_name")
            .limit(20000).execute().data or [])
    newest: dict[str, str] = {}
    for r in rows:
        d = str(r["as_of_date"])
        if d > newest.get(r["portefeuille"], ""):
            newest[r["portefeuille"]] = d
    counts: dict[str, int] = {}
    for r in rows:
        if str(r["as_of_date"]) == newest.get(r["portefeuille"]):
            counts[r["portefeuille"]] = counts.get(r["portefeuille"], 0) + 1
    return counts, newest


def list_accounts() -> list[dict]:
    """Every AIRS account with a reported return, freshest first."""
    perf = _latest_perf()
    counts, newest = _holding_counts()
    out: list[dict] = []
    for name, r in perf.items():
        begin, end = r.get("beginvermogen"), r.get("eindvermogen")
        out.append({
            "portefeuille": name,
            "periode": str(r["periode"]) if r.get("periode") else None,
            "as_of": newest.get(name),
            "begin_value_eur": begin,
            "end_value_eur": end,
            # ⚠ AIRS'S OWN RETURN. Flow-aware. Never `end/begin - 1` — see the module docstring;
            # that reads -5.85% on a book that made +46.12%.
            "ytd_pct": r.get("cumulatief_rendement"),
            # The naive value ratio AIRS also publishes, carried ONLY so the gap can be shown.
            # It is not an alternative answer; it is the wrong one, and seeing it beside the right
            # one is what stops someone recomputing it by hand later.
            "value_ratio_pct": r.get("rendement"),
            # AIRS's own split of the result: price vs income. `koersresultaat` is the "price
            # gains" a reader is usually after; `opbrengsten` is dividends and coupons, which no
            # price return contains.
            "price_result_eur": r.get("koersresultaat"),
            "income_eur": r.get("opbrengsten"),
            "investment_result_eur": r.get("beleggingsresultaat"),
            "holdings": counts.get(name),          # None = we hold no snapshot for it
        })
    out.sort(key=lambda x: (x["portefeuille"] or "").lower())
    return out


def account_holdings(portefeuille: str) -> dict:
    """One account's freshest snapshot: every position, with AIRS's own EUR values.

    Each `ytd_pct` is a PRICE return — `Beginwaarde lopend jaar` is restated to the current
    quantity, so it is not contaminated by a purchase. It will NOT sum to the account's return:
    that one is flow-aware and includes income. See the module docstring.
    """
    rows = (supabase.table("airs_holding")
            .select("as_of_date,holding_name,quantity,currency,weight,start_value_eur,"
                    "current_value_eur,ytd_return_eur,ytd_return_pct,ytd_return_local_pct")
            .eq("portefeuille", portefeuille).limit(2000).execute().data or [])
    if not rows:
        return {"portefeuille": portefeuille, "as_of": None, "rows": []}
    as_of = max(str(r["as_of_date"]) for r in rows)
    snap = [r for r in rows if str(r["as_of_date"]) == as_of]
    snap.sort(key=lambda r: -(r.get("current_value_eur") or 0))
    perf = _latest_perf().get(portefeuille) or {}
    return {
        "portefeuille": portefeuille,
        "as_of": as_of,
        # Repeated here so the panel can state, on the same screen as the positions, that these
        # do not add up to it — and why.
        "ytd_pct": perf.get("cumulatief_rendement"),
        "price_result_eur": perf.get("koersresultaat"),
        "income_eur": perf.get("opbrengsten"),
        "rows": [{
            "holding_name": r["holding_name"],
            "quantity": r.get("quantity"),
            "currency": r.get("currency"),
            "weight": r.get("weight"),
            "start_value_eur": r.get("start_value_eur"),
            "current_value_eur": r.get("current_value_eur"),
            "ytd_return_eur": r.get("ytd_return_eur"),
            # ⚠ None where `Beginwaarde` is 0 — a position not held at the year's open (or a cash
            # line). Its YTD is UNDEFINED, not 0%: dividing by zero would be infinite and calling
            # it flat would be a claim. `parse_airs_excel` already refuses it; this preserves the
            # refusal rather than coalescing it to a number.
            "ytd_return_pct": r.get("ytd_return_pct"),
            "ytd_return_local_pct": r.get("ytd_return_local_pct"),
        } for r in snap],
    }


async def list_accounts_async() -> list[dict]:
    return await asyncio.to_thread(list_accounts)


async def account_holdings_async(portefeuille: str) -> dict:
    return await asyncio.to_thread(account_holdings, portefeuille)
