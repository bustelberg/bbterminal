"""Attaching an ISIN to an AIRS account's holdings — and refusing to trust the name that did it.

THE PROBLEM
    An account (`airs_holding`) carries real quantities and real EUR values, and — until
    2026-07-23 — NO ISIN, only `Fondsomschrijving`, a fund name. Its model
    (`airs_model_portfolio_position`) carries the ISINs and nothing AIRS will value. Pairing the
    two portfolios is `_airs_account_links`; this module does the row-level join inside a pair.

    Measured end-to-end on BUS_Defensief_Dyn <-> BUS_Defensief_FX: 40 of 41 holdings resolve,
    the leftover being a model position (`Ish DJS GSD 100`, DE000A0F5UH1) the book does not hold.

⚠ THE VERMOGENSOVERZICHT NOW CARRIES `ISIN-code`, AND WHERE IT DOES, NONE OF THE BELOW APPLIES.
    Switched on in AirSPMS 2026-07-23. The book states its own ISIN, so the join is EXACT: no
    scoring, no assignment, no leftover to place. `isin_source` says which route a row took —
    `book` (exact) / `override` (a human) / `model` (the name match).

    THE NAME ROUTE IS NOT DEAD AND MUST NOT BE DELETED. Every snapshot taken before that date has
    no ISIN and is what history is made of; the cash line never has one; and a portfolio whose
    export omits the column still has to resolve. The four mechanisms below are what makes that
    fallback work, and they are all still live for those rows.

    THE PRICE CHECK MATTERS MORE, NOT LESS. On a name match it tests the pairing. On an exact
    ISIN there is no pairing to test — so it tests OUR series for that instrument instead, and a
    mismatch means our own listing is wrong (the Stuttgart/Vienna trap), which is a finding we
    previously had no way to make at all.

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

⚠ A 1:1 ASSIGNMENT MUST PLACE EVERY HOLDING, INCLUDING ONE THE MODEL DOES NOT CONTAIN.
    When the stored model snapshot predates a swap in AIRS, the book holds an instrument that has
    no position to pair with — and the assignment does not get to say "none". It hands the holding
    whatever orphan is left over, at any score, and we published that ISIN as the answer.

    Measured 2026-07-23 on the four BUS_* books: AIRS's Fixed portfolio now holds `Invesco Wld EW
    ETF Acc` (IE000OEF25S1), our snapshot (positions_datum 2025-04-28) still holds `Ish DJS GSD
    100` (DE000A0F5UH1), and every one of the four reported the Invesco holding AS DE000A0F5UH1.
    BUS_WTS_Duurzaam_Dyn (snapshot 2025-02-14) scattered six: Merck -> Amazon, Chipotle -> Apple,
    Novo Nordisk -> Nvidia, Eli Lilly -> Netflix, Lululemon -> Alphabet, Adobe -> Zoetis.

    NEITHER SIGNAL ALONE CAN REFUSE THESE, WHICH IS WHY IT SURVIVED SO LONG. A low score is not
    grounds to reject (cash scores 28 and is right); a contradicted price is not either (it is
    the module's most valuable FINDING — the right row carrying the wrong share class). It is the
    CONJUNCTION that is decisive: the name says these are different instruments and the price
    agrees, independently. Measured over all 28 paired accounts, the two populations do not
    overlap or even come close — the 10 wrong-instrument rows score 34.5-47.6, the 8 genuine
    share-class findings score 80.5-100, and `_WEAK_NAME` (60) sits in the empty gap between them.

    So a weak-name AND price-contradicted pairing is REFUSED (`verdict='unmatched'`, no ISIN) and
    its position goes back to `unmatched_model_positions`, where it belongs: the book does not
    hold it. The rejected candidate rides along in `rejected_isin`/`rejected_fonds` so the row can
    say what it declined, rather than showing a bare blank that reads as "cash".

    ⚠ THIS IS A GUARD, NOT A REPAIR. The stale snapshot is the bug; re-scan the model portfolio.

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


# ── The asset-class label a reader sees (and the allocation bar sums to) ──────────────────────
# Six buckets. Only EQUITY carries the ETF split (Equity vs Equity ETF) — AIRS's `categorie` knows
# an equity ETF invests in equity (AAND) and a bond ETF in bonds (OBL), so a bond ETF is Bonds, not
# "ETF Bonds". Defined here, where every signal is at hand, so the column and the bar cannot drift.
BUCKET_EQUITY = "Equity"
BUCKET_EQUITY_ETF = "Equity ETF"
BUCKET_BONDS = "Bonds"
BUCKET_ALTS = "Alternatives"
BUCKET_CASH = "Cash"
BUCKET_UNKNOWN = "Unclassified"
BUCKET_ORDER = [BUCKET_EQUITY, BUCKET_EQUITY_ETF, BUCKET_BONDS, BUCKET_ALTS, BUCKET_CASH, BUCKET_UNKNOWN]

# asset_grid.asset_class values that mean "a fund wrapper" (yfinance/leonteq vocabulary).
_GRID_FUND_CLASSES = {"etf", "fund", "etc", "etp"}

# yfinance speaks two spellings for a couple of sectors; canonicalise so the column doesn't show
# one sector twice. And a `sector` of literally `etf`/`equity`/`bond`… is a leftover from the
# asset-class fallback, not a sector — a fund is opaque, so it reads "—", never a fake sector.
_SECTOR_ALIASES = {"Basic Materials": "Materials", "Financial Services": "Financials"}
_NOT_A_SECTOR = {"equity", "bonds", "bond", "commodity", "short commodity", "crypto", "etf", "fund"}


def _display_sector(raw: str | None) -> str | None:
    """The instrument's own yfinance sector (asset_grid) — canonical spelling, or None when it is
    not a real sector (a fund we cannot look through, or an asset-class leftover)."""
    if not raw or raw.strip().lower() in _NOT_A_SECTOR:
        return None
    return _SECTOR_ALIASES.get(raw, raw)

# Fixed-income tells for the FALLBACK only (when AIRS gave no categorie). A coupon rate in the name
# ("6,5% Rabobank Certificaten 14-perp.") is the strongest; the rest are bond/fund name fragments.
_BOND_WORDS = re.compile(
    r"\b(bond|obligat|coupon|perp|treasur|gilt|bund|govie|sovereign|"
    r"floating\s*rate|frn|senior\s*(?:notes?|debt)|subordinat|high\s*yield|"
    r"aggregate|corp(?:orate)?\s*bond)\b|\d[\.,]?\d*\s*%",
    re.I)


def _looks_like_bond(*names: str | None) -> bool:
    hay = " ".join(n for n in names if n)
    return bool(_BOND_WORDS.search(hay))


def classify_bucket(asset_class: str | None, is_etf: bool, isin: str | None,
                    name: str | None, grid: dict | None) -> str:
    """The single best asset-class label for one holding — the column a reader sees.

    Signals, STRONGEST FIRST, and it stops at the first that decides:
      1. AIRS's own `categorie` (already mapped into `asset_class`) — it classifies what a holding
         INVESTS IN, so an equity ETF is Equity and a bond ETF is Bonds. Present for every paired
         holding, so this is the usual answer.
      2. The asset grid's yfinance class (equity / a fund class / crypto / commodity).
      3. The name — a coupon rate or a bond word.
    Returns 'Unclassified' ONLY when nothing above decides — an honest "unsure", never a guess
    dressed as a fact. Only EQUITY splits on the ETF wrapper; Bonds/Alternatives/Cash do not.
    """
    g = grid or {}
    # 1a. Cash — no instrument at all, or an explicit cash line.
    if asset_class == BUCKET_CASH or (not isin and (name or "").strip().lower() in _CASH_NAMES):
        return BUCKET_CASH
    # 1b. AIRS's own class (the strongest signal).
    if asset_class == BUCKET_BONDS:
        return BUCKET_BONDS
    if asset_class in (BUCKET_ALTS, "Real estate"):
        return BUCKET_ALTS
    if asset_class == BUCKET_EQUITY:
        return BUCKET_EQUITY_ETF if is_etf else BUCKET_EQUITY
    # 2/3. No AIRS class (an unpaired holding) — fall back to the grid, then the name.
    gac = (g.get("asset_class") or "").lower()
    if gac == "bond" or _looks_like_bond(name, g.get("name"), g.get("leonteq_name")):
        return BUCKET_BONDS
    if gac in ("crypto", "commodity"):
        return BUCKET_ALTS
    if gac == "equity":
        return BUCKET_EQUITY
    if is_etf or gac in _GRID_FUND_CLASSES:
        # A fund we cannot see into, with no bond tell — the overwhelming default is an equity ETF.
        return BUCKET_EQUITY_ETF
    # 4. Nothing decided — say so.
    return BUCKET_UNKNOWN


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


def pairing_refused(verdict: str, name_score: float) -> bool:
    """Is this pairing the leftover of a stale model snapshot rather than a real match?

    ⚠ BOTH SIGNALS, NEVER EITHER ALONE — see the module docstring. A weak name is not grounds to
    refuse (cash scores 28 and is right); a contradicted price is not either (it is the module's
    most valuable finding). Only their conjunction is decisive, and measured over all 28 paired
    accounts the two populations sit either side of `_WEAK_NAME` with nothing in between:
    wrong-instrument 34.5-47.6, genuine share-class findings 80.5-100.
    """
    return verdict == "price_mismatch" and name_score < _WEAK_NAME


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
            for k in ("current_value_eur", "start_value_eur", "ytd_return_eur",
                      "fund_result_eur", "fx_result_eur"):
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


def _load_isin_overrides() -> dict[str, dict]:
    """{normalised holding name: row} — the identities a human supplied by hand.

    Keyed on the NAME (see the migration): what instrument a fund name denotes does not depend on
    which book holds it, and the measured case appears in four books at once. Small table, read
    whole — there is nothing to filter it by that would not cost more than reading it.
    """
    rows = (supabase.table("airs_holding_isin_override")
            .select("holding_name,isin,note").limit(1000).execute().data or [])
    return {(r["holding_name"] or "").strip().casefold(): r for r in rows if r.get("isin")}


def _load_bucket_overrides(isins: list[str]) -> dict[str, str]:
    """{isin: bucket} for the ISINs a user has manually pinned. An override is a property of the
    INSTRUMENT (asset_bucket_override, keyed by ISIN) and beats the calculated class."""
    out: dict[str, str] = {}
    uniq = sorted({i for i in isins if i})
    for i in range(0, len(uniq), 100):
        for r in (supabase.table("asset_bucket_override")
                  .select("isin,bucket").in_("isin", uniq[i:i + 100]).execute().data or []):
            out[r["isin"]] = r["bucket"]
    return out


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

    ⚠ IT IS THE START-WEIGHTED VALUE CHANGE — `Σnow / Σstart − 1`, the basket's actual price return,
        equivalently each holding's return weighted by its OPENING value (beginwaarde). NOT weighted
        by the CURRENT value: a holding up +148% has tripled its share of the book, so current-value
        weighting lets that one winner dominate and inflates the figure (measured: AITopSelectie read
        +56.11% current-weighted vs +41.98% true, against a +43.08% book). Start-weighting is the
        unbiased number and the one that lines up with the book. It carries no income and is not
        flow-aware, so the segments still do NOT exactly sum to `cumulatief_rendement` (income + flow
        timing), which is why it is not computed against that — but it is close, not 14pp off.
    """
    # Group by the CALCULATED CLASS (the six-bucket `bucket` — incl. any manual override), not the
    # raw AIRS asset_class: Equity and Equity ETF split apart, and a bond ETF sits under Bonds.
    by: dict[str, list[dict]] = {}
    for r in rows:
        by.setdefault(r.get("bucket") or BUCKET_UNKNOWN, []).append(r)
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
            # The output field keeps the name `asset_class` (frontend/model unchanged), but it now
            # carries the calculated bucket — the segment IS the Class group.
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
            # The gain split, summed — NULL when the snapshot predates the AIRS-own columns (all
            # None), so the segment shows "—" rather than a false €0.
            "fund_eur": (round(sum(v for r in rs
                                   if (v := r.get("fund_result_eur")) is not None), 2)
                         if any(r.get("fund_result_eur") is not None for r in rs) else None),
            "fx_eur": (round(sum(v for r in rs
                                 if (v := r.get("fx_result_eur")) is not None), 2)
                       if any(r.get("fx_result_eur") is not None for r in rs) else None),
            "return_pct": round(100 * (now / start - 1), 2) if start else None,
            "priced_value_eur": round(now, 2),
            # ⚠ ETFs are counted, never bucketed: an equity ETF is Equity. Stated as a share of
            # the segment so "Bonds 48.65%, of which 43.20% via ETFs" is one row, not two.
            "etf_value_eur": round(sum((r.get("current_value_eur") or 0)
                                       for r in rs if r.get("is_etf")), 2),
        })
    out.sort(key=lambda s: (BUCKET_ORDER.index(s["asset_class"])
                            if s["asset_class"] in BUCKET_ORDER else 99))
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
                       .select("holding_name,isin,quantity,currency,weight,current_value_eur,"
                               "start_value_eur,ytd_return_eur,fund_result_eur,fx_result_eur")
                       .eq("portefeuille", portefeuille).eq("as_of_date", as_of)
                       .limit(500).execute().data or [])
    pos = (supabase.table("airs_model_portfolio_position")
           .select("fonds,isin,percentage,categorie,sector")
           .eq("portfolio_id", row["model_portfolio_id"]).limit(500).execute().data or [])
    if not holdings or not pos:
        return {"portefeuille": portefeuille, "model_name": row["model_name"], "as_of": as_of,
                "reason": "nothing to match", "rows": [], "unmatched_model_positions": []}

    # ── Identity, strongest source first ────────────────────────────────────────────────────
    # 1. AIRS'S OWN `ISIN-code` ON THE BOOK ROW. Switched on 2026-07-23; where present there is
    #    nothing to infer and the join below is exact.
    # 2. A hand-supplied pin, for a holding the model has no position for.
    # 3. The name match — the original route, and still the only one for pre-2026-07-23 snapshots.
    # 1 and 2 are taken OUT of the 1:1 assignment: their identity is settled, and leaving them in
    # would let a settled holding consume some unrelated leftover position — the very bug this
    # module exists to answer.
    own = {i: (h.get("isin") or None) for i, h in enumerate(holdings)}
    pinned = _load_isin_overrides()
    pin_of = {i: (None if own[i] else pinned.get((h["holding_name"] or "").strip().casefold()))
              for i, h in enumerate(holdings)}
    free = [i for i in range(len(holdings)) if not own[i] and not pin_of[i]]
    # The model position carrying the same ISIN — for AIRS's `categorie` and the model weight, NOT
    # for identity. Absent is fine and is not drift on our side: the Class falls back to the grid.
    by_isin = {p["isin"]: j for j, p in enumerate(pos) if p.get("isin")}

    isins = [p["isin"] for p in pos if p.get("isin")]
    isins += [x for x in own.values() if x]
    isins += [x["isin"] for x in pin_of.values() if x]
    grid: dict[str, dict] = {}
    for i in range(0, len(isins), 100):
        for g in (supabase.table("asset_grid")
                  .select("isin,name,openfigi_name,leonteq_name,leonteq_product_type,"
                          "country,continent,msci_region,asset_class,sector")
                  .in_("isin", isins[i:i + 100]).execute().data or []):
            grid[g["isin"]] = g

    # Scored over the FREE holdings only, then mapped back to real holding indices.
    scores = [[_score(holdings[i]["holding_name"], p, grid) for p in pos] for i in free]
    assigned = _assign(scores) if free else {}
    pairing = {free[a]: j for a, j in assigned.items()}
    score_of = {free[a]: scores[a][j] for a, j in assigned.items()}

    overrides = _load_bucket_overrides(isins)   # manual Class pins, keyed by ISIN — they win
    closes = _last_closes(isins, as_of)
    ccys = {c["currency"] for c in closes.values() if c.get("currency")}
    # The window only has to reach the close we compare against; `_rate` walks back to the last
    # rate on or before it, so a fortnight covers any holiday run.
    fx = _fx_to_eur(ccys, (date.fromisoformat(as_of) - timedelta(days=21)).isoformat(), as_of) if ccys else {}

    rows = []
    for i, h in enumerate(holdings):
        pin, mine = pin_of[i], own[i]
        # An ISIN-bearing holding is joined to its model position EXACTLY, for the category only.
        j = by_isin.get(mine) if mine else pairing.get(i)
        p = pos[j] if j is not None else None
        name_score = score_of.get(i, 0.0)
        # ⚠ NEITHER A BOOK ISIN NOR A PIN SKIPS THE VERIFICATION — they decide IDENTITY only. The
        # price check below runs on them exactly as on a name match. On an exact ISIN it is no
        # longer testing the pairing (there is none); it tests OUR series for that instrument, and
        # a mismatch there means our listing is wrong — a more valuable finding, not a weaker one.
        isin = mine or (pin["isin"] if pin else (p or {}).get("isin"))
        qty, val = h.get("quantity"), h.get("current_value_eur")
        implied = (float(val) / float(qty)) if qty and val else None
        # What we declined, if we end up declining it. Kept so the row can name its dead end.
        rejected_isin = rejected_fonds = None

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
        # ⚠ THE ASSIGNMENT COULD NOT SAY "NONE", SO SAY IT HERE. Two independent signals both
        # reject this pairing: the name says a different instrument and the price agrees. That is
        # the leftover of a stale model snapshot, not a share-class finding — so hand back NO
        # ISIN rather than one we have twice been told is wrong. See the module docstring.
        # ⚠ ONLY A NAME MATCH CAN BE REFUSED. `mine` was joined exactly and a pin has no pairing
        # at all; for both, `name_score` is 0.0 because nothing was ever scored — feeding that to
        # `pairing_refused` would discard a KNOWN ISIN on the strength of a score that does not
        # exist. The refusal is about the inference, and here there is none.
        if not mine and p is not None and pairing_refused(verdict, name_score):
            rejected_isin, rejected_fonds = isin, (p or {}).get("fonds")
            verdict, isin, p = "unmatched", None, None
            pairing.pop(i, None)          # its position is UNMATCHED — the book does not hold it
            native_eur = ratio = None
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
        # Country / continent / region come straight from the execution instrument's own yfinance
        # geo (asset_grid), joined by ISIN — the same per-row data the Instruments grid shows.
        # `region` is the MSCI ACWI region (North America / Europe / Pacific / EM…). ⚠ For an ETF
        # these describe its LISTING, not its holdings (the grid can't see inside a fund).
        g = grid.get(isin or "") or {}
        is_etf = _is_etf(g)
        # The smart six-bucket label — a manual override (pinned by ISIN) wins over the calculated
        # class. `bucket_overridden` tells the UI which rows a user has set by hand.
        override = overrides.get(isin or "")
        bucket = override or classify_bucket(asset_class, is_etf, isin, h["holding_name"], g)
        rows.append({
            "holding_name": h["holding_name"],
            "lines": h.get("lines", 1),
            "asset_class": asset_class,
            # The smart six-bucket label (Equity | Equity ETF | Bonds | Alternatives | Cash |
            # Unclassified) — the /portfolios "Class" column, and what the allocation bar sums.
            "bucket": bucket,
            "bucket_overridden": bool(override),
            "categorie": cat or None,
            "sector": _display_sector(g.get("sector")),
            "country": g.get("country") or None,
            "continent": g.get("continent") or None,
            "region": g.get("msci_region") or None,
            "is_etf": is_etf,
            "quantity": qty,
            "currency": h.get("currency"),
            "weight": h.get("weight"),
            "current_value_eur": val,
            "start_value_eur": h.get("start_value_eur"),
            "ytd_return_eur": h.get("ytd_return_eur"),
            "fund_result_eur": h.get("fund_result_eur"),
            "fx_result_eur": h.get("fx_result_eur"),
            "isin": isin,
            # WHERE the identity came from, because the three are not equally strong and a reader
            # cannot tell by looking at the digits:
            #   book     AIRS's own `ISIN-code` on the holding — exact, nothing inferred
            #   override a human supplied it (the model had no position for the holding)
            #   model    the name match — a guess, however good, and the reason `verdict` exists
            "isin_source": ("book" if mine else "override" if pin else "model" if isin else None),
            # True = a human supplied this ISIN because the model had no position for the holding.
            # The UI badges it: a pinned identity is not a match, and must not read as one.
            "isin_overridden": bool(pin),
            "isin_override_note": (pin or {}).get("note"),
            "model_fonds": (p or {}).get("fonds"),
            "model_pct": (p or {}).get("percentage"),
            # Meaningless where nothing was scored (an exact ISIN, or a pin), so `None` rather
            # than a 0.0 that would render as "we matched this at zero confidence".
            "name_score": None if (pin or mine) else round(name_score, 1),
            "weak_name": None if (pin or mine) else name_score < _WEAK_NAME,
            "implied_price_eur": round(implied, 4) if implied else None,
            "our_price_eur": round(native_eur, 4) if native_eur else None,
            "price_ratio": round(ratio, 4) if ratio else None,
            "verdict": verdict,
            "our_instrument": (c or {}).get("name"),
            # Only set on `unmatched`: the leftover position we declined, so the row can say what
            # it refused instead of showing a blank that reads as "this holding has no ISIN".
            "rejected_isin": rejected_isin,
            "rejected_fonds": rejected_fonds,
        })
    rows.sort(key=lambda r: -(r["current_value_eur"] or 0))
    # A position is claimed by the assignment OR by any settled identity naming the same ISIN.
    # Without that second clause an exactly-joined or pinned holding would leave its own position
    # reading as "not held here" — drift that is not there, invented by the fix for drift that is.
    settled = {x["isin"] for x in pin_of.values() if x} | {x for x in own.values() if x}
    taken = {pairing[i] for i in pairing}
    taken |= {j for j, p in enumerate(pos) if p.get("isin") in settled}
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
