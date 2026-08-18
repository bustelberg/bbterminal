"""One AIRS account's holdings, each with its instrument identity and a check on our price for it.

⚠ THE FIXED↔DYNAMIC PAIRING IS GONE (2026-07-23), AND SO IS EVERYTHING IT NEEDED.
    This module used to recover each holding's ISIN by fuzzy-matching its fund name against a
    PAIRED model portfolio's positions, assigning 1:1 globally, and gating on the price. All of
    that is deleted: ~200 lines of scoring, assignment and refusal logic, plus the pairing itself.

    It is worth recording WHY it existed, because the replacement looks trivial by comparison and
    the trap it removes was expensive:

      - The book carried no ISIN, only `Fondsomschrijving`. The model carried the ISINs.
      - The pairing between the two portfolios was a NAME GUESS on 27 of 28 accounts, and the risk
        variants of a strategy hold the SAME instruments — so a mis-pairing filed a real book's
        money under another strategy's name and nothing else on the row looked wrong.
      - A 1:1 assignment cannot answer "none". When the stored model predated a swap in AIRS, the
        leftover position was handed to whatever holding was left: measured, four books reported
        `Invesco Wld EW ETF Acc` as DE000A0F5UH1, and BUS_WTS_Duurzaam scattered six more
        (Merck -> Amazon, Chipotle -> Apple, Novo Nordisk -> Nvidia...).
      - Even after guarding that, a settled holding's position stayed in the candidate pool, so a
        cash line took Hermes' ISIN — and because the Class override is keyed BY ISIN, pinning the
        cash row's Class moved Hermes with it.

    Every one of those failure modes is structurally impossible now. The book states its own ISIN.

WHERE EACH FIELD COMES FROM NOW
    isin        the Vermogensoverzicht's own `ISIN-code` column, or a hand-supplied pin for a row
                that has none (`airs_holding_isin_override`, keyed by holding name)
    Class       `classify_bucket` over the asset grid + the name. AIRS's `categorie` is gone with
                the pairing; the yfinance sector carries Real Estate and the name carries bonds.
    sector/geo  the asset grid (yfinance), joined by that ISIN
    drift       the book's OWN `MODEL` report (`airs_model_weight`) — lines the strategy names
                and the book does not hold

⚠ THE PRICE CHECK IS WHAT SURVIVES, AND IT IS NOW WORTH MORE. It never tested the name; it tested
    the instrument. With the ISIN a guess, a mismatch was ambiguous — bad pairing, or bad ISIN?
    With the ISIN stated by the custodian, a mismatch can only mean OUR price series is wrong for
    that instrument (the Stuttgart/Vienna wrong-listing trap). That is a finding we could not
    make at all before.

⚠ AND IT MUST FX-CONVERT OR IT IS NOISE. The account's implied price is EUR; `asset_price.close`
    is the listing's own currency. Without conversion `Investor AB` reads a ratio of 0.09 — which
    is just 1/11, EUR/SEK — while Linde and Berkshire pass at 0.86 only by luck. `_rate` is shared
    with the benchmark, so the pence trap (GBp is not a currency) is handled in one place.

⚠ DEDUPE THE ACCOUNT FIRST. AIRS bills one instrument on several lines — `6,5% Rabobank
    Certificaten 14-perp.` appears at 2.60% AND 0.01%. This mattered enormously to the assignment
    and still matters to the table: two rows for one instrument is two rows a reader must reconcile.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from contextlib import contextmanager
from datetime import date, timedelta

from common.pg import load_rows_via_copy
from deps import supabase
from routers._airs_ref import model_weights_for as ref_model_weights_for, models as ref_models
from timeseries import load_series

from ._airs_portfolio_links import link_key, resolve_links
from ._benchmark_index import _fx_to_eur, _rate

_log = logging.getLogger(__name__)

_HOLDING_GRID_COLS = ("isin,name,openfigi_name,leonteq_name,leonteq_product_type,"
                      "country,continent,msci_region,asset_class,sector")


@contextmanager
def _phase(store: dict, name: str):
    """Time one step of the expand and record it in milliseconds.

    ⚠ EXPANDING A ROW FIRES THREE ENDPOINTS AND USED TO TAKE SECONDS FOR NO STATED REASON. This one
    does a dozen distinct things — several DB reads, an FX load, a link resolution, and (since the
    price check started refreshing stale series) potentially a run of YAHOO calls. "It takes a
    while" is unactionable; "freshen 4,100ms, closes 90ms, everything else 200ms" names the step to
    argue with. The numbers ride along in the payload so they land in the operator's console rather
    than only in a server log nobody has open.
    """
    t0 = time.perf_counter()
    try:
        yield
    finally:
        store[name] = round((time.perf_counter() - t0) * 1000)

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
# FIVE buckets, one per thing a holding INVESTS IN. A bond ETF is Bonds and an equity ETF is
# Equity: the wrapper is not the asset class.
#
# ⚠⚠ `Equity ETF` WAS A SIXTH BUCKET AND WAS RETIRED 2026-08-18. It split the equity sleeve on the
# WRAPPER while every other bucket split on the underlying, so the bar answered two different
# questions at once — and a book's equity exposure could not be read off it without adding two
# slices together. Bonds never had the split (a bond ETF has always been Bonds), which is what made
# the equity one inconsistent rather than merely redundant.
#
# ⚠⚠ AND RETIRING IT COST SOMETHING THAT HAD TO BE REPLACED, NOT JUST DELETED. `Equity` was
# implicitly the "operating companies only" bucket — the Analyse modal gates owner-earnings
# blending on `bucket === 'Equity'` precisely because a fund has no earnings to blend and this app
# does not look through funds. With ETFs now inside Equity that guarantee is gone from the bucket,
# so the fund-ness travels on its own field instead (`is_fund`, set in
# `_airs_portfolio_analysis._reclassify_book_rows`). A merge that dropped the distinction entirely
# would have quietly fed ETFs to the fundamentals blender.
BUCKET_EQUITY = "Equity"
BUCKET_BONDS = "Bonds"
BUCKET_ALTS = "Alternatives"
BUCKET_CASH = "Cash"
BUCKET_UNKNOWN = "Unclassified"
BUCKET_ORDER = [BUCKET_EQUITY, BUCKET_BONDS, BUCKET_ALTS, BUCKET_CASH, BUCKET_UNKNOWN]

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
# ⚠ `ibond` AND `bd` ARE NOT PADDING. With AIRS's `categorie` gone the name is the ONLY bond tell
# left for a fund whose grid row is wrong or missing, and both of these were measured failing:
#   `iShares iBonds 2032 Term Corp UCITS ETF USD` — not in asset_grid at all, and `\bbond` does
#      NOT match "iBonds" (no word boundary before the b), so it classified as an equity ETF.
#   `iShares Euro HY Corp Bd ETF EUR` — in the grid as asset_class 'equity', sector 'equity',
#      which is simply wrong for a bond fund; "Bd" is the only thing on the row that says bond.
_BOND_WORDS = re.compile(
    r"\b(bond|obligat|coupon|perp|treasur|gilt|bund|govie|sovereign|bd|"
    r"floating\s*rate|frn|senior\s*(?:notes?|debt)|subordinat|high\s*yield|"
    r"aggregate|corp(?:orate)?\s*bond)\b|ibond|fixed\s*income|\d[\.,]?\d*\s*%",
    re.I)

# yfinance's sector for a listed property company. AIRS used to say `VAS` and we bucketed it
# Alternatives; with `categorie` gone this is the same fact from the source we already trust for
# sector/region. 40 holdings ride on it (Simon Property, Prologis, Welltower, Aedifica, Vonovia…),
# and without it every REIT silently becomes an ordinary equity.
_REAL_ESTATE_SECTOR = "real estate"


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
    dressed as a fact.

    ⚠ NO BUCKET SPLITS ON THE WRAPPER. Every one of them names what the holding invests in, which
    is why an equity ETF and a share of Apple land in the same place — see the ⚠⚠ on
    `BUCKET_EQUITY`. `is_etf` survives as a parameter only for the fallback, where "a fund" is the
    difference between Equity and Unclassified.
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
        # ⚠ `is_etf` NO LONGER CHANGES THE ANSWER HERE — an equity ETF invests in equity. It is
        # still a parameter because the fallback below needs it to tell a fund from an unknown.
        return BUCKET_EQUITY
    # 2/3. No AIRS class — the grid, then the name. Since `categorie` was dropped (2026-07-23)
    # this is the ONLY path for every holding, so the order below decides the whole column.
    gac = (g.get("asset_class") or "").lower()
    if gac == "bond" or _looks_like_bond(name, g.get("name"), g.get("leonteq_name")):
        return BUCKET_BONDS
    if gac in ("crypto", "commodity"):
        return BUCKET_ALTS
    # ⚠ BEFORE the equity test, or every REIT is an ordinary equity. yfinance's own sector, the
    # same field the Sector column shows — this replaces AIRS's `VAS` exactly.
    if (g.get("sector") or "").strip().lower() == _REAL_ESTATE_SECTOR:
        return BUCKET_ALTS
    if gac == "equity":
        return BUCKET_EQUITY
    if is_etf or gac in _GRID_FUND_CLASSES:
        # A fund we cannot see into, with no bond tell — the overwhelming default is an equity
        # fund, and an equity fund holds equity.
        return BUCKET_EQUITY
    # 4. Nothing decided — say so.
    return BUCKET_UNKNOWN


# The implied price may legitimately differ from our last close: AIRS marks a different day, and
# a bond ETF barely moves. Beyond this it is not the same instrument — the share-class errors
# this exists to catch are 19x and 20x, not 10%.
_PRICE_TOL = 0.15


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


# How many of one account's instruments a single expand may fetch. The refresh is normally a
# no-op — the 06:00 tick keeps held instruments current — so this only bites the first read after
# an outage, where finishing 25 and saying so beats a two-minute page load.
_ONDEMAND_REFRESH_LIMIT = 5

# ISINs this process has already freshened, and on which day. Expanding a row must not re-ask Yahoo
# about an instrument it asked about a minute ago — see `_freshen`.
_FRESHENED: dict[str, str] = {}

# ⚠ ONE DAY BEHIND THE MARKET IS STALE **FOR THIS CALLER**, and the fleet default is not.
# `DEFAULT_STALE_DAYS = 3` answers "is it worth a Yahoo call to top this series up", where clearing
# a weekend cheaply matters over 6,000 instruments. It is the wrong question here. This check makes
# an ACCUSATION — "our listing is wrong" — inside a 15% band calibrated for a same-day comparison,
# and two sessions of an ordinary mover blow straight through 15%. Measured 2026-07-30: Applied
# Materials (AMAT, NasdaqGS, USD, a flawless mapping) went 516.89 -> 436.45 over two sessions; our
# close was 07-27 against a 07-29 anchor — TWO days, inside the fleet threshold, so nothing was
# fetched — and the row read ratio 0.846 and a red mismatch. On the current close it is 1.002.
# The anchor only advances when the market has actually published, so a weekend or a holiday still
# flags nothing; "behind the anchor at all" is exactly the condition we want to fix before judging.
_CHECK_STALE_DAYS = 1

# Days our close may sit from the day AIRS valued the book before the check refuses to draw any
# conclusion from it. `_freshen` runs first, so reaching this means Yahoo has nothing newer — a
# dormant or delisted line. Four covers a weekend plus one holiday (Friday's close against a
# Tuesday valuation) and nothing more: a 15% band cannot survive a longer gap on any real mover,
# which is how a false accusation gets made.
_MAX_CLOSE_LAG_DAYS = 4


def _freshen(isins: list[str]) -> None:
    """Fetch the GAP for any of this account's instruments whose close lags the market.

    ⚠ THE CHECK IS ONLY AS GOOD AS THE PRICE UNDER IT, AND A STALE PRICE FAILS IT SILENTLY. The
    comparison is AIRS's implied price against our own close, with a 15% tolerance meant to catch
    share-class errors of 19x and 20x. A series that simply stopped updating drifts past 15% on any
    ordinary mover and is then reported as `price_mismatch` — "our listing is wrong" about a listing
    that is perfect. Measured 2026-07-29: AMD went 552.33 -> 430.05 (-22%) while our newest bar
    anywhere sat six days back, and the row read as a wrong listing with every stored bar matching
    Yahoo to the cent.

    ⚠ IT STANDS DOWN WHILE THE INGEST WORKER IS LIVE, exactly as the 06:00 tick does. Yahoo answers
    an overloaded caller with an EMPTY result rather than a 429, and an empty candidate set is how a
    resolution lands on a thin foreign listing. A price check is never worth risking that.

    ⚠ DETECTS BEFORE IT FETCHES. `refresh_stale` runs `find_stale` first — a few queries, one
    grouped COPY and one canary probe — so the usual case, everything current, costs no per-symbol
    Yahoo calls at all. Best effort throughout: a failed refresh leaves the old close in place, and
    `_MAX_CLOSE_LAG_DAYS` then stops the check drawing a conclusion from it.

    ⚠ IT IS A SAFETY NET, NOT THE REFRESH — AND FORGETTING THAT COST 12 SECONDS A CLICK. The first
    version asked Yahoo about EVERY holding on EVERY expand: `stale_days=1` means anything a day
    behind the market anchor qualifies, which overnight is the whole book, and `extend_series` is a
    round trip apiece. Measured 2026-07-30 with per-phase timing: 11,537 ms of an 11,793 ms expand
    — 97% of it — and again on the next click, for the same instruments.

    The actual keeper of these prices is the 06:00 tick, which now covers account holdings too
    (`price_refresh.held_isins` unions `airs_holding`). So this only has to catch what slipped
    through, and it is bounded twice:
      * `_ONDEMAND_REFRESH_LIMIT` per expand — no single click can be slow;
      * `_FRESHENED` per process per day — a second expand of the same book costs nothing.
    Whatever is still behind afterwards is not judged: it gets the `stale_price` verdict, which is
    the honest answer rather than an expensive attempt at a better one.
    """
    if not isins:
        return
    today = date.today().isoformat()
    want = [i for i in isins if _FRESHENED.get(i) != today]
    if not want:
        return
    try:
        from asset_pipeline import price_refresh, queue as _q  # noqa: PLC0415

        if _q.is_worker_active():
            _log.info("[airs price check] refresh SKIPPED — the ingest queue worker is live")
            return
        # ⚠ `stale_days=_CHECK_STALE_DAYS`, NOT the fleet default — see the constant. One detection
        # pass, not two: `refresh_stale` finds them itself, so calling `find_stale` here as well
        # would cost a second canary probe on every expand.
        r = price_refresh.refresh_stale(isins=set(want), stale_days=_CHECK_STALE_DAYS,
                                        limit=_ONDEMAND_REFRESH_LIMIT)
        # ⚠ MARKED WHATEVER THE OUTCOME. "Yahoo has nothing newer" is as final an answer as a
        # successful fetch — re-asking about a dormant listing on every click is the exact cost
        # this memo exists to remove.
        for i in want:
            _FRESHENED[i] = today
        if r.get("stale"):
            _log.info("[airs price check] %d of %d instrument(s) behind %s — %d moved, %d had "
                      "nothing newer, %d failed, %d over the per-read cap",
                      r["stale"], r["considered"], r.get("global_latest"), r.get("moved", 0),
                      r.get("unchanged", 0), r.get("failed", 0), r.get("skipped", 0))
    except Exception as e:  # noqa: BLE001 — the holdings table must render without a refresh
        _log.warning("[airs price check] refresh failed (%s: %s) — comparing against what we hold",
                     type(e).__name__, e)


def _close_lag_days(close: dict | None, as_of: str) -> int | None:
    """Days between the close we hold and the day AIRS valued the book. None when either is absent.

    Pure, so the one judgement the price check makes about time is unit-testable.
    """
    if not close or not close.get("date"):
        return None
    try:
        return (date.fromisoformat(as_of) - date.fromisoformat(close["date"])).days
    except ValueError:
        return None


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


def _account_owner_model_id(portefeuille: str) -> int:
    """The model an ACCOUNT runs — the account's analogue of "self" for the link gates.

    Returns 0 when the account is unpaired: `resolve_links`/`linkable_context` compare it with
    `p["id"]`, so a value no portfolio can have simply excludes nothing, which is the correct
    behaviour for an account whose strategy we do not know.
    """
    try:
        rows = (supabase.table("airs_account_model_link")
                .select("portefeuille,model_portfolio_id").limit(500).execute().data or [])
    except Exception:  # noqa: BLE001 — an unpaired account must still list its holdings
        return 0
    want = (portefeuille or "").strip().lower()
    for r in rows:
        if (r.get("portefeuille") or "").strip().lower() == want:
            return int(r.get("model_portfolio_id") or 0)
    return 0


def _link_fields(lk, pf_names: dict[int, str]) -> dict:
    """The four link columns a row carries, from a `ResolvedLink` (or nothing)."""
    if lk is None:
        return {"linked_portfolio_id": None, "linked_portfolio_name": None,
                "link_source": None, "link_confidence": None, "link_reason": None}
    return {
        "linked_portfolio_id": lk.linked_portfolio_id,
        "linked_portfolio_name": pf_names.get(lk.linked_portfolio_id) if lk.linked_portfolio_id else None,
        "link_source": lk.source,
        # ⚠ NULL for a manual link. A human choice is not a guess, and rendering it at "100%
        # confidence" would put a decision and an estimate in the same visual language.
        "link_confidence": lk.confidence,
        "link_reason": lk.reason,
    }


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
    # raw AIRS asset_class: an equity ETF sits under Equity and a bond ETF under Bonds — the
    # wrapper is not the asset class (`Equity ETF` retired 2026-08-18).
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


def resolve_account_isins(portefeuille: str, *, freshen: bool = True) -> dict:
    """One account's holdings, each with its own ISIN and what we know about that instrument.

    ⚠ NO PAIRING, NO SCORING, NO ASSIGNMENT — all three were deleted 2026-07-23. Everything they
    existed to recover now comes directly from the book:

        the ISIN        the Vermogensoverzicht's own `ISIN-code` column (live 2026-07-23)
        the Class       `classify_bucket` off the asset grid + the name (AIRS's `categorie` is
                        gone; the yfinance sector carries Real Estate, the name carries bonds)
        the model wt    the book's OWN `MODEL` report -> `airs_model_weight`
        drift           the same report's lines that no holding matches

    What is left of the old module is the part that was always the valuable half: the PRICE CHECK.
    It no longer tests a pairing (there is none) — it tests OUR price series for the instrument
    AIRS names, so a mismatch means our listing is wrong. That is a finding we could not make at
    all while the ISIN was itself a guess.
    """
    t: dict[str, int] = {}
    with _phase(t, "snapshot"):
        snap = (supabase.table("airs_holding").select("as_of_date")
                .eq("portefeuille", portefeuille).order("as_of_date", desc=True)
                .limit(1).execute().data or [])
    if not snap:
        return {"portefeuille": portefeuille, "as_of": None,
                "reason": "no holdings snapshot stored", "rows": [], "segments": [],
                "unmatched_model_positions": []}
    as_of = str(snap[0]["as_of_date"])
    with _phase(t, "holdings"):
        holdings = _dedupe(supabase.table("airs_holding")
                           .select("holding_name,isin,quantity,currency,weight,current_value_eur,"
                                   "start_value_eur,ytd_return_eur,fund_result_eur,fx_result_eur")
                           .eq("portefeuille", portefeuille).eq("as_of_date", as_of)
                           .limit(500).execute().data or [])
    if not holdings:
        return {"portefeuille": portefeuille, "as_of": as_of, "reason": "no holdings",
                "rows": [], "segments": [], "unmatched_model_positions": []}

    # Identity: the book's own ISIN, else a hand-supplied pin for a row that has none.
    pinned = _load_isin_overrides()
    pin_of = {i: (None if h.get("isin") else pinned.get((h["holding_name"] or "").strip().casefold()))
              for i, h in enumerate(holdings)}
    isins = [x for x in ((h.get("isin") or (pin_of[i] or {}).get("isin"))
                         for i, h in enumerate(holdings)) if x]

    with _phase(t, "grid"):
        grid: dict[str, dict] = {}
        # ONE COPY instead of ceil(len/100) round trips; the chunked loop is the fallback.
        _rows = load_rows_via_copy("asset_grid", _HOLDING_GRID_COLS, "isin", isins)
        if _rows is None:
            _rows = []
            for i in range(0, len(isins), 100):
                _rows += (supabase.table("asset_grid").select(_HOLDING_GRID_COLS)
                          .in_("isin", isins[i:i + 100]).execute().data or [])
        for g in _rows:
            grid[g["isin"]] = g

    # ⚠ AN EXECUTION ROW IS PRICED FROM ITS *ANALYSIS* INSTRUMENT, WHICH CAN BE A DIFFERENT
    # LISTING — that is the design, not a fault. An ADR's execution row is deliberately served by
    # the main company's instrument (`asset_isin_alias`), and the two do not trade at the same
    # number: TSMC is 1 ADR = 5 ordinary shares, plus an ADR premium. The price check below would
    # call that `price_mismatch` on every such holding, which is a false alarm on a link we made
    # on purpose. They get their own verdict instead — see `cross_listed`.
    from asset_pipeline.isin_alias import load_aliases  # noqa: PLC0415

    with _phase(t, "aliases_overrides"):
        aliased = load_aliases()
        overrides = _load_bucket_overrides(isins)   # manual Class pins, keyed by ISIN — they win
    # ⚠ BEFORE the closes are read, never after: the check below is a comparison, and half of it
    # comes from here. See `_freshen` — a series that merely stopped updating reads as a wrong
    # listing, which is the loudest finding this table can make.
    # ⚠ SKIPPABLE, AND ONLY THE PRICE CHECK DEPENDS ON IT. `_freshen` exists so the implied-vs-our
    # price comparison below is not drawn against a series that merely stopped updating — it
    # protects `verdict` / `price_ratio` / `implied_price_eur`, which the /portfolios expand shows.
    # The ANALYSIS path reads none of those: it takes the ISIN, the two AIRS valuations, the class
    # and the certificate link, and prices nothing off our own closes any more. It was paying ~3.3s
    # per book for a check it does not display — twice, since the wrapped book is resolved too, so
    # 6.4s of a 9.7s modal open.
    #
    # ⚠ THE PRICES STILL GET FRESHENED, JUST NOT HERE: the 06:00 tick covers account holdings
    # (`price_refresh.held_isins` unions `airs_holding`), and any /portfolios expand runs the full
    # path. This only declines to do the vendor's work on a read that will not show the result.
    with _phase(t, "freshen_prices"):
        if freshen:
            _freshen(isins)
    with _phase(t, "closes"):
        closes = _last_closes(isins, as_of)
    with _phase(t, "fx"):
        ccys = {c["currency"] for c in closes.values() if c.get("currency")}
        fx = _fx_to_eur(ccys, (date.fromisoformat(as_of) - timedelta(days=21)).isoformat(), as_of) if ccys else {}

    # A holding that IS another model portfolio, wrapped as a Leonteq certificate. Same fact, same
    # store and same guesser as the model-portfolio positions table — `airs_model_portfolio_link`
    # is keyed on the HOLDING, not on (parent, holding), so a link decided on either screen is the
    # same decision and neither can disagree with the other.
    #
    # ⚠ THE SELF-EXCLUSION GATE NEEDS AN OWNER, AND AN ACCOUNT IS NOT A MODEL. `resolve_links`
    # takes the id of the portfolio whose rows these are, so it can refuse a self-reference. Here
    # the rows belong to an ACCOUNT, whose analogue is the model that account RUNS: a certificate
    # of its own strategy is precisely the wrapper cycle the gate exists to stop (TOPS_STS_L holds
    # 'Star Selection Index' at 100%). Unpaired account -> no self to exclude, and `guess_link`
    # already treats an owner it cannot match as "exclude nothing".
    with _phase(t, "links"):
        owner_id = _account_owner_model_id(portefeuille)
        link_rows = [{"isin": (h.get("isin") or (pin_of[i] or {}).get("isin")),
                      "fonds": h.get("holding_name") or ""}
                     for i, h in enumerate(holdings)]
        links = resolve_links(supabase, owner_id, link_rows)
        # ⚠ The PRETTY name, falling back to AIRS's `Portefeuille` code — see `linkable_context`.
        pf_names = {p["id"]: (p.get("display_name") or p["name"]) for p in (
            ref_models())}

    rows = []
    for i, h in enumerate(holdings):
        pin, mine = pin_of[i], h.get("isin") or None
        isin = mine or (pin or {}).get("isin")
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
        # How far our close sits from the day AIRS valued the book. ⚠ MEASURED AGAINST `as_of`, NOT
        # AGAINST TODAY: the two sides of this comparison must describe the same day, and an account
        # whose snapshot is a fortnight old is correctly compared with a fortnight-old close.
        lag = _close_lag_days(c, as_of)
        if ratio is None:
            verdict = "unpriced"          # nothing to check it against; NOT a pass
        elif abs(ratio - 1.0) <= _PRICE_TOL:
            verdict = "ok"
        elif lag is not None and lag > _MAX_CLOSE_LAG_DAYS:
            # ⚠ A DISAGREEMENT BETWEEN TWO DIFFERENT DAYS IS NOT A DISAGREEMENT. `_freshen` has
            # already tried to close the gap, so reaching here means Yahoo has nothing newer for
            # this line — a delisted or dormant listing. Our price cannot answer the question, and
            # saying `price_mismatch` would answer it wrongly: the gap is time, not identity.
            verdict = "stale_price"
        else:
            verdict = "price_mismatch"
        # ⚠ NOT A MISMATCH, AND NOT 'ok' EITHER. This ISIN is served by another instrument on
        # purpose, so a price difference is EXPECTED and proves nothing about the identity —
        # calling it `ok` would claim the price confirmed something it never tested.
        served_by = aliased.get(isin or "")
        if served_by and verdict == "price_mismatch":
            verdict = "cross_listed"

        g = grid.get(isin or "") or {}
        is_etf = _is_etf(g)
        # ⚠ `asset_class=None` ALWAYS now: AIRS's `categorie` came from the paired model position
        # and there is no pairing. The grid and the name carry it (see `classify_bucket`).
        override = overrides.get(isin or "")
        bucket = override or classify_bucket(None, is_etf, isin, h["holding_name"], g)
        rows.append({
            "holding_name": h["holding_name"],
            "lines": h.get("lines", 1),
            "bucket": bucket,
            "bucket_overridden": bool(override),
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
            # `book` = AIRS's own ISIN-code; `override` = supplied by hand for a row that has none.
            # There is no third source any more — the name match is gone.
            "isin_source": ("book" if mine else "override" if pin else None),
            "isin_overridden": bool(pin),
            "isin_override_note": (pin or {}).get("note"),
            "implied_price_eur": round(implied, 4) if implied else None,
            "our_price_eur": round(native_eur, 4) if native_eur else None,
            "price_ratio": round(ratio, 4) if ratio else None,
            "verdict": verdict,
            "our_instrument": (c or {}).get("name"),
            # WHEN our side of the comparison is from. A ratio without its two dates cannot be
            # argued with — and this is the field that distinguishes "wrong listing" from "old
            # price", which look identical in the number alone.
            "our_price_date": (c or {}).get("date"),
            "price_lag_days": lag,
            # The model portfolio this holding IS, when it is one. `manual` is a human decision
            # and always wins; `auto` is a guess recomputed on every read, so it can never rot
            # against a renamed portfolio. A stored NULL is meaningful — "explicitly not a
            # portfolio" — which is why a dismissed guess does not come back.
            **_link_fields(links.get(link_key(isin, h.get("holding_name") or "")), pf_names),
            # Set when this ISIN is deliberately served by another's instrument. The UI needs it to
            # explain a price difference that is by design rather than flag it as a fault.
            "served_by": served_by,
        })
    rows.sort(key=lambda r: -(r["current_value_eur"] or 0))
    held = {r["holding_name"] for r in rows}
    # Drift, from the book's OWN model report: a line the strategy names and the book does not
    # hold. Same meaning as the old `unmatched_model_positions`, without the pairing.
    unheld = list(ref_model_weights_for(portefeuille).values())
    t["total"] = sum(v for k, v in t.items() if k != "total")
    _log.info("[airs isins] %s: %s", portefeuille,
              ", ".join(f"{k} {v}ms" for k, v in sorted(t.items(), key=lambda kv: -kv[1])))
    return {
        "portefeuille": portefeuille,
        "as_of": as_of,
        # Per-phase milliseconds — see `_phase`. Rides along so the console can say WHICH step
        # was slow, not just that the expand was.
        "timings_ms": t,
        "rows": rows,
        "segments": _segments(rows),
        "unmatched_model_positions": [
            {"fonds": w["fonds"], "isin": None, "percentage": w.get("model_pct")}
            for w in unheld if w["fonds"] not in held
        ],
    }


async def resolve_account_isins_async(portefeuille: str) -> dict:
    return await asyncio.to_thread(resolve_account_isins, portefeuille)
