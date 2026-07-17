"""Attaching an ISIN to an AIRS account's holdings — and refusing to trust the name that did it.

THE PROBLEM
    An account (`airs_holding`) carries real quantities and real EUR values, and NO ISIN — only
    `Fondsomschrijving`, a fund name. Its model (`airs_model_portfolio_position`) carries the
    ISINs and nothing AIRS will value. Pairing the two portfolios is `_airs_account_links`; this
    module does the row-level join inside a confirmed pair.

    Measured end-to-end on BUS_Defensief_Dyn <-> BUS_Defensief_FX: 40 of 41 holdings resolve,
    the leftover being a model position (`Ish DJS GSD 100`, DE000A0F5UH1) the book does not hold.

FOUR THINGS MAKE IT WORK, AND EACH ONE WAS MEASURED FAILING WITHOUT THE OTHERS
    1. DEDUPE THE ACCOUNT FIRST. It lists one instrument on several lines — BUS_Defensief_Dyn
       has `6,5% Rabobank Certificaten 14-perp.` at 2.60% AND at 0.01%. 41 rows, 40 instruments.
       Leave them and the 1:1 assignment must place the spare somewhere: it put it on an
       unrelated orphan at score 33.
    2. SCORE AGAINST EVERY NAME WE HAVE FOR THE ISIN, not just the model's truncated `Fonds`.
       `Xtrackers World Utilities EUR` and the model's `db x-track MSCI W Utilit` share NOT ONE
       WORD (DWS renamed the range); it resolves only through Yahoo's "Xtrackers MSCI World
       Utilities". `Invesco BulletShares 2028` vs the model's `Invesco BulletShares 29` is the
       same story — and there the ISIN-side name says 2028, i.e. the MODEL's label is the wrong
       one.
    3. ASSIGN 1:1 GLOBALLY, NEVER PER-ROW. Letting each holding take its own best match sent
       `Vanguard ESG Global Corp Bond` to the **iShares** row at 75 — beating the right answer
       by 5 points — while the real iShares holding wanted the same row at 80. One model row
       cannot serve two holdings, and only a global assignment knows that.
    4. GATE ON THE PRICE, NOT ON THE NAME SCORE. The score is worthless as confidence here:
       `Effectenrekening` -> `Liquiditeiten` scores 28 and is RIGHT (cash, by elimination); the
       duplicate line scored 33 and was WRONG. The implied price is independent evidence.

⚠ THE NAME CANNOT SEE A SHARE CLASS, AND THAT IS THE WHOLE DANGER.
    `IE00BNDS1P30` and `IE00BNDS1Q47` are both "Vanguard ESG Global Corporate Bond UCITS ETF
    EUR Hedged" — Acc and Inc. Identical names, different ISINs, and they COMPOUND DIFFERENTLY.
    No string comparison will ever separate them. The price does, instantly: €4.79 vs €3.99, and
    the account's implied €3.98 picks Inc without ambiguity.

    The same check found a real discrepancy: BUS_Defensief_Dyn holds `iShares Global Corp Bond
    ETF EUR H Dist` at €4.17/unit while the model's ISIN `IE00BJSFQW37` is the fund's **USD
    (Dist)** class at €77.94 — 19x apart, both quoted EUR, so FX cannot explain it. The row
    pairing is right (one such position on each side); the ISIN it hands back is not what the
    book holds. Surfaced as `price_mismatch`, never silently corrected: we cannot tell from here
    whether AIRS's model carries a wrong ISIN or the book genuinely drifted onto another share
    class — and the second one is a finding, not a bug.

⚠ THE PRICE CHECK MUST FX-CONVERT OR IT IS NOISE. The account's implied price is EUR;
    `asset_price.close` is the listing's own currency. Without conversion `Investor AB` reads a
    ratio of 0.09 — which is just 1/11, EUR/SEK — and Linde and Berkshire read 0.86 and pass
    only by luck. Every non-EUR holding false-alarms while a genuine EUR mismatch hides in the
    noise. `_rate` is shared with the benchmark, so the pence trap (GBp is not a currency) is
    handled in one place.
"""
from __future__ import annotations

import asyncio
import re
from datetime import date, timedelta

from deps import supabase
from rapidfuzz import fuzz

from timeseries import load_series

from ._benchmark_index import _fx_to_eur, _rate

# Below this, a pairing is reported but flagged: the model and the book have drifted, or the
# name is a coincidence. Chosen loose on purpose — the NAME is not the gate, the price is, and a
# floor tight enough to drop the duplicate (33) also drops cash (28) and Rabobank (56), both of
# which are correct.
_WEAK_NAME = 60.0

# AIRS's own `Beleggingscategorie`, off the model position. Its own classification, not ours —
# there is nothing to infer and nothing to get wrong.
#
# ⚠ IT CLASSIFIES WHAT A HOLDING INVESTS IN, NOT ITS WRAPPER. An equity ETF is AAND; a bond ETF
# is OBL. So "ETF" is NOT a sibling of these and must never be made one: 10 of the 11 bond ISINs
# are ETFs, and on BUS_Defensief_FX a bucket for them would move 43.20 of the 48.65% bond sleeve
# out of Bonds — a defensive book reading as though it held almost none. The wrapper is a second
# axis (`is_etf`), reported beside the class, never instead of it.
_ASSET_CLASS = {
    "AAND": "Equity",          # Aandelen
    "OBL": "Bonds",            # Obligaties
    "VAS": "Real estate",      # Vastgoed — REITs (Aedifica, Digital Realty, Welltower); 29 ISINs
    "ALTBEL": "Alternatives",  # Alternatieve beleggingen
}
# The cash line, which carries no ISIN and no category. Named explicitly: a blank category is NOT
# by itself cash — the model also holds an ISIN-less `Brown & Brown` stub, and calling that cash
# would put an equity in the cash bucket.
_CASH_NAMES = {"effectenrekening", "liquiditeiten"}

# ⚠ `ETF` MUST BE A WORD, NOT A SUBSTRING. `name ILIKE '%ETF%'` matches **Netflix** — n-ETF-lix —
# and files it as a fund. Measured: of the model's ISINs, that test flags exactly one EQUITY, and
# it is Netflix.
_ETF_WORD = re.compile(r"\bETF\b", re.I)


def _is_etf(grid_row: dict | None) -> bool:
    """Is this instrument a fund wrapper?

    `leonteq_product_type` is authoritative but INCOMPLETE — it types 19 of the model's ISINs as
    ETF and leaves 40 with no type at all, among which 11 are plainly ETFs (iShares, Vanguard,
    VanEck…). So the type is trusted first and the name only fills its gaps.

    ⚠ 'UCITS' alone does not cover the gap either: `iShares J.P. Morgan EM Corporate Bond ETF`
    carries no UCITS in its name. Both tests are needed, and the ETF one must be word-bounded.
    """
    if not grid_row:
        return False
    if (grid_row.get("leonteq_product_type") or "").strip().upper() == "ETF":
        return True
    name = grid_row.get("name") or ""
    return bool(_ETF_WORD.search(name)) or "UCITS" in name.upper()

# The implied price may legitimately differ from our last close: AIRS marks a different day, and
# a bond ETF barely moves. Beyond this it is not the same instrument — the share-class errors
# this exists to catch are 19x and 20x, not 10%.
_PRICE_TOL = 0.15


def _norm(s: str | None) -> str:
    """Lowercase, punctuation to spaces, collapsed. ⚠ SPACES ARE KEPT: `token_sort_ratio` needs
    tokens, and stripping them silently degrades it to a character ratio — which scores
    `Vanguard ESG Global Corp Bond` against the iShares row highly enough to win."""
    s = re.sub(r"[^a-z0-9 ]", " ", (s or "").lower())
    return re.sub(r"\s+", " ", s).strip()


def _score(account_name: str, position: dict, grid: dict[str, dict]) -> float:
    """Best match over every name the instrument is known by. See point 2 in the docstring."""
    names = [position.get("fonds")]
    g = grid.get(position.get("isin") or "")
    if g:
        names += [g.get("name"), g.get("openfigi_name"), g.get("leonteq_name")]
    a = _norm(account_name)
    return max((fuzz.token_sort_ratio(a, _norm(n)) for n in names if n), default=0.0)


def _assign(scores: list[list[float]]) -> dict[int, int]:
    """A 1:1 assignment, best pair first.

    Not the Hungarian algorithm — this is greedy over globally sorted pairs, and it is NOT
    guaranteed optimal in general. It is used because it is identical to optimal HERE: checked
    against `scipy.optimize.linear_sum_assignment` over all 12 paired accounts (40x41 down to
    10x10) and it produced the SAME ISIN on every single row, 0 differences. That buys a real
    scipy dependency for nothing.

    ⚠ Do not "simplify" this to each row taking its own best match. That is the greedy that
    fails, and it fails on the case that matters (Vanguard -> iShares).
    """
    n, m = len(scores), len(scores[0]) if scores else 0
    pairs = sorted(((scores[i][j], i, j) for i in range(n) for j in range(m)), key=lambda t: -t[0])
    used_a: set[int] = set()
    used_b: set[int] = set()
    out: dict[int, int] = {}
    for _, i, j in pairs:
        if i in used_a or j in used_b:
            continue
        used_a.add(i)
        used_b.add(j)
        out[i] = j
    return out


def _dedupe(holdings: list[dict]) -> list[dict]:
    """One row per INSTRUMENT. See point 1: the account bills one instrument on several lines,
    and a 1:1 assignment over rows must then place the spare on something."""
    agg: dict[str, dict] = {}
    for h in holdings:
        k = (h.get("holding_name") or "").strip()
        if not k:
            continue
        if k in agg:
            cur = agg[k]
            cur["quantity"] = (cur.get("quantity") or 0) + (h.get("quantity") or 0)
            for k in ("current_value_eur", "start_value_eur", "ytd_return_eur"):
                cur[k] = (cur.get(k) or 0) + (h.get(k) or 0)
            cur["weight"] = (cur.get("weight") or 0) + (h.get("weight") or 0)
            cur["lines"] = cur.get("lines", 1) + 1
        else:
            agg[k] = {**h, "lines": 1}
    return list(agg.values())


def _last_closes(isins: list[str], as_of: str) -> dict[str, dict]:
    """{isin: {close, currency, date}} — the instrument's own latest close, as we hold it.

    ONE `COPY` for every instrument (`timeseries.load_series`), not a query per ISIN: a
    41-holding account is 41 round trips the other way. It is also why this does not simply
    `.in_()` over `asset_price` — 41 instruments x a 90-day window is ~3,700 rows against
    PostgREST's silent 1,000-row cap, and the rows it drops would read as "no price", i.e. as
    an unpriceable holding rather than an error.

    The window reaches back 90 days because a thinly-traded line may not have printed recently;
    a holding whose last close predates that is genuinely stale and reports `unpriced`.
    """
    rows: list[dict] = []
    for i in range(0, len(isins), 100):
        rows += (supabase.table("asset_execution")
                 .select("isin,analysis_id,currency,yahoo_symbol,name")
                 .in_("isin", isins[i:i + 100]).execute().data or [])
    by_aid = {r["analysis_id"]: r for r in rows if r.get("analysis_id")}
    if not by_aid:
        return {}
    start = (date.fromisoformat(as_of) - timedelta(days=90)).isoformat()
    df = load_series(list(by_aid), "yf.close", start, as_of)
    out: dict[str, dict] = {}
    if df.empty:
        return out
    # Latest row per instrument.
    df = df.sort_values("date").groupby("entity_id", as_index=False).last()
    for rec in df.to_dict("records"):
        r = by_aid.get(rec["entity_id"])
        if not r or rec.get("close") is None:
            continue
        out[r["isin"]] = {"close": float(rec["close"]), "currency": r.get("currency"),
                          "date": str(rec["date"])[:10], "symbol": r.get("yahoo_symbol"),
                          "name": r.get("name")}
    return out


# Display order. Cash and Unclassified last: they are not asset classes anyone allocates to,
# they are what is left.
_SEGMENT_ORDER = ["Equity", "Bonds", "Real estate", "Alternatives", "Cash", "Unclassified"]


def _segments(rows: list[dict]) -> list[dict]:
    """One row per asset class: exposure, and what that exposure returned.

    ⚠ THE RETURN AND THE WEIGHT DO NOT COVER THE SAME HOLDINGS, ON PURPOSE.
        A holding with no opening value has an UNDEFINED return — it was not held when the year
        opened. It is real exposure, so it counts in `value_eur` and `weight_pct`; but putting it
        in `sum(current)/sum(start)` would report its entire value as gain. Measured: cash is
        exactly this (34 of 38 such rows, up to EUR 1,000,000 — VTopSelectie's whole book), and
        so is a SHORT: TOPS_BEOFF_BEH_DYN holds Nestle India at -3,504 shares and -EUR 44,680,
        with no opening value. `return_pct` therefore spans only the priced part, and
        `priced_value_eur` states how much that is. A segment where they differ is saying so.

    ⚠ IT IS A PRICE RETURN, LIKE THE HOLDINGS IT IS BUILT FROM. AIRS restates each opening value
        to the current quantity, so `sum(current)/sum(start) - 1` is the basket's price return on
        exactly the basis each row already uses. It carries no income and is not flow-aware, so
        the segments do NOT sum to the portfolio's own figure — the same caveat the holdings
        already carry, and the reason this is not computed against `cumulatief_rendement`.
    """
    by: dict[str, list[dict]] = {}
    for r in rows:
        by.setdefault(r["asset_class"], []).append(r)
    total = sum((r.get("current_value_eur") or 0) for r in rows)

    out: list[dict] = []
    for name, rs in by.items():
        value = sum((r.get("current_value_eur") or 0) for r in rs)
        # Only holdings with a real opening value can carry a return.
        priced = [r for r in rs if (r.get("start_value_eur") or 0) != 0
                  and r.get("current_value_eur") is not None]
        start = sum((r.get("start_value_eur") or 0) for r in priced)
        now = sum((r.get("current_value_eur") or 0) for r in priced)
        out.append({
            "asset_class": name,
            "holdings": len(rs),
            "value_eur": round(value, 2),
            # ⚠ EVERY MONEY CELL IN THE HEADER IS THE SUM OF THE COLUMN BENEATH IT — that is the
            # invariant a reader checks, so `start_value_eur` sums ALL the rows (an unpriced one
            # contributes its 0), not just the priced ones. It follows that
            # `value_eur - start_value_eur != gain_eur` wherever a segment holds something with
            # no opening value: the difference is that holding's whole value, which is exposure
            # and not gain. The rows below say the same thing, and `return_pct` is starred.
            "start_value_eur": round(sum((r.get("start_value_eur") or 0) for r in rs), 2),
            "weight_pct": round(100 * value / total, 2) if total else None,
            # The sum of the Gain column: an unpriced row's gain is None, not 0.
            "gain_eur": round(now - start, 2) if priced else None,
            "return_pct": round(100 * (now / start - 1), 2) if start else None,
            "priced_value_eur": round(now, 2),
            # ⚠ ETFs are counted, never bucketed: an equity ETF is Equity. Stated as a share of
            # the segment so "Bonds 48.65%, of which 43.20% via ETFs" is one row, not two.
            "etf_value_eur": round(sum((r.get("current_value_eur") or 0)
                                       for r in rs if r.get("is_etf")), 2),
        })
    out.sort(key=lambda s: (_SEGMENT_ORDER.index(s["asset_class"])
                            if s["asset_class"] in _SEGMENT_ORDER else 99))
    return out


def resolve_account_isins(portefeuille: str) -> dict:
    """One account's holdings with an ISIN attached to each, and a verdict on every one."""
    from ._airs_account_links import list_account_links  # noqa: PLC0415  (circular at module level)

    links = list_account_links()
    row = next((a for a in links["accounts"] if a["portefeuille"] == portefeuille), None)
    if not row or not row.get("model_portfolio_id"):
        return {"portefeuille": portefeuille, "model_name": None,
                "reason": (row or {}).get("reason") or "no account by that name",
                "rows": [], "unmatched_model_positions": []}

    snap = (supabase.table("airs_holding").select("as_of_date")
            .eq("portefeuille", portefeuille).order("as_of_date", desc=True)
            .limit(1).execute().data or [])
    if not snap:
        return {"portefeuille": portefeuille, "model_name": row["model_name"],
                "reason": "no holdings snapshot stored", "rows": [],
                "unmatched_model_positions": []}
    as_of = str(snap[0]["as_of_date"])
    holdings = _dedupe(supabase.table("airs_holding")
                       .select("holding_name,quantity,currency,weight,current_value_eur,"
                               "start_value_eur,ytd_return_eur")
                       .eq("portefeuille", portefeuille).eq("as_of_date", as_of)
                       .limit(500).execute().data or [])
    pos = (supabase.table("airs_model_portfolio_position")
           .select("fonds,isin,percentage,categorie,sector")
           .eq("portfolio_id", row["model_portfolio_id"]).limit(500).execute().data or [])
    if not holdings or not pos:
        return {"portefeuille": portefeuille, "model_name": row["model_name"], "as_of": as_of,
                "reason": "nothing to match", "rows": [], "unmatched_model_positions": []}

    isins = [p["isin"] for p in pos if p.get("isin")]
    grid: dict[str, dict] = {}
    for i in range(0, len(isins), 100):
        for g in (supabase.table("asset_grid")
                  .select("isin,name,openfigi_name,leonteq_name,leonteq_product_type")
                  .in_("isin", isins[i:i + 100]).execute().data or []):
            grid[g["isin"]] = g

    scores = [[_score(h["holding_name"], p, grid) for p in pos] for h in holdings]
    pairing = _assign(scores)

    closes = _last_closes(isins, as_of)
    ccys = {c["currency"] for c in closes.values() if c.get("currency")}
    # The window only has to reach the close we compare against; `_rate` walks back to the last
    # rate on or before it, so a fortnight covers any holiday run.
    fx = _fx_to_eur(ccys, (date.fromisoformat(as_of) - timedelta(days=21)).isoformat(), as_of) if ccys else {}

    rows = []
    for i, h in enumerate(holdings):
        j = pairing.get(i)
        p = pos[j] if j is not None else None
        name_score = scores[i][j] if j is not None else 0.0
        isin = (p or {}).get("isin")
        qty, val = h.get("quantity"), h.get("current_value_eur")
        implied = (float(val) / float(qty)) if qty and val else None

        # ⚠ Convert OUR close into EUR — never compare it raw to an EUR-implied price.
        native_eur = None
        c = closes.get(isin or "")
        if c:
            r = _rate(fx, c.get("currency"), c.get("date") or as_of)
            if r:
                native_eur = c["close"] / r

        ratio = (implied / native_eur) if (implied and native_eur) else None
        if ratio is None:
            verdict = "unpriced"          # nothing to check it against; NOT a pass
        elif abs(ratio - 1.0) <= _PRICE_TOL:
            verdict = "ok"
        else:
            verdict = "price_mismatch"
        # AIRS's own category, via the model position this holding matched. A holding we could
        # not pair has none — `Unclassified`, never quietly folded into Equity, which is the
        # bucket a reader would least question.
        cat = (p or {}).get("categorie") or ""
        if cat in _ASSET_CLASS:
            asset_class = _ASSET_CLASS[cat]
        elif (h["holding_name"] or "").strip().lower() in _CASH_NAMES:
            asset_class = "Cash"
        else:
            asset_class = "Unclassified"
        rows.append({
            "holding_name": h["holding_name"],
            "lines": h.get("lines", 1),
            "asset_class": asset_class,
            "categorie": cat or None,
            "sector": (p or {}).get("sector") or None,
            "is_etf": _is_etf(grid.get(isin or "")),
            "quantity": qty,
            "currency": h.get("currency"),
            "weight": h.get("weight"),
            "current_value_eur": val,
            "start_value_eur": h.get("start_value_eur"),
            "ytd_return_eur": h.get("ytd_return_eur"),
            "isin": isin,
            "model_fonds": (p or {}).get("fonds"),
            "model_pct": (p or {}).get("percentage"),
            "name_score": round(name_score, 1),
            "weak_name": name_score < _WEAK_NAME,
            "implied_price_eur": round(implied, 4) if implied else None,
            "our_price_eur": round(native_eur, 4) if native_eur else None,
            "price_ratio": round(ratio, 4) if ratio else None,
            "verdict": verdict,
            "our_instrument": (c or {}).get("name"),
        })
    rows.sort(key=lambda r: -(r["current_value_eur"] or 0))
    taken = {pairing[i] for i in pairing}
    segments = _segments(rows)
    return {
        "portefeuille": portefeuille,
        "model_name": row["model_name"],
        "model_source": row["source"],
        "as_of": as_of,
        "rows": rows,
        "segments": segments,
        # A model position no holding claimed. Real drift — the book does not hold it.
        "unmatched_model_positions": [
            {"fonds": p.get("fonds"), "isin": p.get("isin"), "percentage": p.get("percentage")}
            for j, p in enumerate(pos) if j not in taken
        ],
    }


async def resolve_account_isins_async(portefeuille: str) -> dict:
    return await asyncio.to_thread(resolve_account_isins, portefeuille)
