"""Effective positions — the euros behind the weights, and the currencies they sit in.

    Eᵢ = qᵢ · Pᵢ · Xᵢ        (quantity × price × FX to EUR)

⚠⚠ WE DO NOT COMPUTE THAT PRODUCT, AND SAYING SO MATTERS MORE THAN COMPUTING IT WOULD. `airs_holding`
carries `quantity` and `currency`, but it also carries `current_value_eur` — AIRS's OWN valuation of
the position, already in euros. That is the custodian's number: it is what the client's statement
says, it is struck on AIRS's own valuation date, and it already embeds whatever conventions AIRS
applies. Re-deriving it from our yfinance close and our FX rate would produce a SECOND figure that
disagrees with the statement by small amounts on most rows and by large ones wherever an instrument
is mis-mapped — and the panel would have no way to say which was right.

So `Eᵢ` here IS `current_value_eur`, folded per issuer. The formula above is how you would build it
if the valuation were not given; it is documented because the four caveats attached to it still
apply, and two of them land differently once the number is somebody else's:

  * ISSUER AGGREGATION — ours, and shared: `_active_share.build_issuer_weights`.
  * TRADE DATE vs SETTLEMENT DATE — ⚠ AIRS'S CONVENTION, NOT OURS, AND WE CANNOT VERIFY IT FROM
    HERE. The Vermogensoverzicht is a valuation report; it exposes no flag saying which basis it
    used. A book with a trade in the last few days can therefore differ from a trade-date view by
    that trade's value, and nothing in our data would show it. Stated rather than assumed away.
  * CASH IN THE DENOMINATOR — ours, and both answers are returned.
  * CURRENCY EXPOSURE — ours, and tracked separately below.

⚠⚠ THE CURRENCY IS THE LISTING'S, WHICH IS THE EXPOSURE YOU ACTUALLY BEAR, and it is deliberately
NOT the company's reporting currency. If the book holds Nestlé on SIX in CHF, its euro value moves
with CHF/EUR — that is a fact about the position. The economic argument (Nestlé earns worldwide, so
the "real" exposure is diversified) is true and is a DIFFERENT claim, softer and unmeasurable from
here. `_airs_portfolio_analysis` already learned this the other way round: classifying an INDEX by
listing currency read 91% USD against 98% by reporting currency, and there the reporting currency
was right because the question was about the issuers. Here the question is about the money.
"""
from __future__ import annotations

# ⚠ THE MODULE, NOT ITS NAMES, FOR `_grid_by_isin`. A `from … import _grid_by_isin` binds this
# module's own reference at import time, so patching it on `_active_share` — which is how every
# sibling here is exercised — silently misses this caller and it goes to the real database instead.
# The symptom was quiet and plausible: an empty grid means no company name, so the issuer key falls
# back to the HOLDING's name, and "Shell LSE" / "Shell AMS" became two issuers where the sibling
# views correctly saw one. Same trap either way round; the module reference has no version of it.
from routers import _active_share as _as
from routers._active_share import IssuerError, build_issuer_weights


def compute_exposure(holdings: list[dict], benchmark: str) -> dict:
    """Effective position per issuer in EUR, plus the currency split of the sleeve."""
    try:
        built = build_issuer_weights(holdings, benchmark)
    except IssuerError as e:
        return {"available": False, "reason": e.reason, "benchmark": benchmark}

    port = built["port"]
    stocks_w, total_all = built["stocks_w"], built["total_all"]

    stocks = [h for h in holdings
              if not h.get("is_fund")
              and (h.get("isin") or "").strip()
              and float(h.get("weight_pct") or 0) > 0]
    isins = sorted({(h["isin"] or "").strip().upper() for h in stocks})
    grid = _as._grid_by_isin(isins)

    # ⚠ THE VALUATION IS OPTIONAL. An ad-hoc basket has weights and no euros — it is not a book —
    # so every euro figure below is None there and the view falls back to the percentages. Zero
    # would be a claim that the position is worthless.
    have_values = any(h.get("value_eur") is not None for h in stocks)

    by_issuer: dict[str, dict] = {}
    by_ccy: dict[str, dict] = {}
    missing_ccy = 0.0
    for h in stocks:
        isin = (h["isin"] or "").strip().upper()
        g = grid.get(isin) or {}
        key = _as._issuer_key(g.get("gf_company_name") or g.get("name") or h.get("name")) \
            or f"isin:{isin}"
        w = float(h["weight_pct"]) / stocks_w * 100.0
        val = h.get("value_eur")
        val = float(val) if val is not None else None

        slot = by_issuer.setdefault(key, {
            "key": key, "name": port.get(key, {}).get("name") or h.get("name") or isin,
            "weight_pct": 0.0, "value_eur": 0.0 if have_values else None, "lines": 0,
            "currencies": set(),
        })
        slot["weight_pct"] += w
        slot["lines"] += 1
        if val is not None and slot["value_eur"] is not None:
            slot["value_eur"] += val

        # ⚠ THE HOLDING'S OWN CURRENCY FIRST, the grid's only as a fallback. The payload's value is
        # what the modal is showing; the grid is our mapping of the ISIN, and where the two differ
        # the one the reader can see wins.
        ccy = (h.get("currency") or g.get("currency") or "").strip().upper()
        if not ccy:
            missing_ccy += w
            continue
        slot["currencies"].add(ccy)
        c = by_ccy.setdefault(ccy, {"currency": ccy, "weight_pct": 0.0,
                                    "value_eur": 0.0 if have_values else None, "issuers": set()})
        c["weight_pct"] += w
        if val is not None and c["value_eur"] is not None:
            c["value_eur"] += val
        c["issuers"].add(key)

    rows = sorted(by_issuer.values(), key=lambda r: -r["weight_pct"])
    for r in rows:
        # ⚠ NAMED WHEN A SINGLE ISSUER SPANS CURRENCIES — two listings of one company in two
        # currencies is one position and two FX exposures, and the issuer fold hides that by design.
        r["currencies"] = sorted(r["currencies"])

    ccys = sorted(by_ccy.values(), key=lambda c: -c["weight_pct"])
    for c in ccys:
        c["issuers"] = len(c["issuers"])

    sleeve_eur = (sum(float(h["value_eur"]) for h in stocks if h.get("value_eur") is not None)
                  if have_values else None)
    book_eur = (sum(float(h["value_eur"]) for h in holdings if h.get("value_eur") is not None)
                if have_values else None)

    return {
        "available": True,
        "benchmark": benchmark,
        "has_values": have_values,
        "issuers": len(rows),
        "lines": len(stocks),
        # ⚠ LINES MINUS ISSUERS IS THE FOLD, made visible. "49 lines, 47 issuers" is the one-line
        # explanation for why this panel counts differently from the Holdings table.
        "folded_lines": len(stocks) - len(rows),
        "sleeve_eur": sleeve_eur,
        "book_eur": book_eur,
        "stocks_pct": (stocks_w / total_all * 100.0) if total_all > 0 else None,
        # What is NOT in the sleeve: funds, cash, bonds, and any line without a usable ISIN.
        "other_eur": (None if sleeve_eur is None or book_eur is None else book_eur - sleeve_eur),
        "positions": rows,
        "currencies": ccys,
        # ⚠ WEIGHT WE COULD NOT ASSIGN A CURRENCY TO. Folding it into EUR would be the flattering
        # default — it makes a book look more domestic than it is.
        "currency_unknown_pct": missing_ccy,
        "unresolved": len(built["unresolved"]),
    }
