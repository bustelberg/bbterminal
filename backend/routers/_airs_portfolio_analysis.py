"""Composition of an AIRS model portfolio — sector / region / currency — against a benchmark.

ONE VOCABULARY, OR THE COMPARISON IS A LIE
    The portfolio's holdings live in the ISIN world (`asset_execution`); the SP500 benchmark is
    built from `company` rows. Those are different entity universes, and `company` has no sector
    column at all — its sector would have to come from `universe_membership`, whose SP500 rows
    are EMPTY anyway. Two taxonomies side by side in one bar chart is a chart that invents a
    difference ("Technology" vs "Information Technology" is not a tilt).

    So BOTH sides are classified from the SAME source: `asset_grid`'s yfinance attributes, joined
    by ISIN. That is not a convenience — it is the reason the chart means anything. Measured: all
    493 SP500 members are present in `asset_grid` with `status='ok'` and a sector, so the
    benchmark loses nothing by being expressed this way.

⚠ WE DO NOT LOOK THROUGH FUNDS, AND THAT MUST BE VISIBLE.
    An ETF is a basket. Its *listing* tells you nothing about what it holds:

      * SECTOR   — 24 of the 26 held ETFs have a "sector" of literally `etf` or `Equity`. Those
                   are not sectors. Counting them as one would put ~20% of a portfolio into a
                   phantom bucket and quietly deflate every real sector's share.
      * REGION   — an MSCI World ETF listed in Amsterdam is not European exposure.
      * CURRENCY — that same ETF quoted in EUR holds mostly USD assets. The listing currency is
                   not the currency exposure.

    We have no constituent data for these funds, so we cannot decompose them. The honest move is
    to say so: every fund folds into `Unclassified` on ALL THREE axes — a confident wrong sector
    split would be worse than one bucket that reads "we can't see inside this".

    (For a single-stock holding the listing currency IS a fair proxy for currency exposure, and
    the domicile a fair proxy for region. Imperfect, standard, and not misleading.)

⚠ CASH IS A BUCKET, NOT A GAP — the same rule as the returns: its drag is a fact.

⚠ ONE COMPANY, ONE ROW, on the benchmark side. GuruFocus puts the FULL company market cap on
    every share class, so Alphabet (GOOGL + GOOG) would contribute its cap TWICE — 11.3% of the
    S&P's total weight, fictional. `_benchmark_index._members` already dedupes; we reuse it
    rather than re-deriving the weights and re-introducing the bug.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from datetime import date

from asset_pipeline.geo import MSCI_REGION, msci_region_of
from common.pg import load_rows_via_copy
from deps import IN_CHUNK_SIZE, supabase
from routers._airs_ref import model as ref_model, mutaties_for as ref_mutaties_for, positions_for as ref_positions_for
from routers._asset_benchmark import index_returns
from routers._asset_benchmark import members as _members
from routers._benchmark_index import SP500_LABEL

CASH_BUCKET = "Cash"
_log = logging.getLogger(__name__)

UNKNOWN_BUCKET = "Unclassified"
# A fund is a black box on these axes — an ETF's listing tells you nothing about its holdings — so
# it FOLDS INTO Unclassified rather than being split into a sector/region/currency it never had.
# (Kept as its own name for the callers that still reference it; the value is UNKNOWN_BUCKET.)
FUND_BUCKET = UNKNOWN_BUCKET

_FUND_CLASSES = {"etf", "fund", "etc", "etp", "crypto", "commodity"}

# The four real MSCI regions, read off the map itself so this cannot drift from `geo`. `_region`
# validates the stored column against these before trusting it: anything else (a NULL, a country
# name, a future spelling) would otherwise open a bucket of its own and read as a region.
_MSCI_REGIONS = frozenset(MSCI_REGION.values())

# ⚠ YAHOO SPEAKS TWO SECTOR VOCABULARIES, AND THE OVERLAP IS A SILENT ATTRIBUTION BUG.
#
#     Materials          535 rows        Basic Materials       32 rows
#     Financials         748 rows        Financial Services    82 rows
#
# Same sector, two names, depending on when the row was resolved. Left unmerged they are two
# BUCKETS: a chart shows one sector twice, and — far worse — Brinson reads a portfolio holding
# "Financial Services" against an index holding "Financials" as a bucket the index does not own.
# That scores as a phantom ALLOCATION bet with ZERO selection, when in truth the portfolio and the
# index are in the same sector and the only question is which names they picked. Canonicalised
# here, at the single point where a sector becomes a bucket.
_SECTOR_ALIASES = {
    "Basic Materials": "Materials",
    "Financial Services": "Financials",
}

# Values that appear in the `sector` column of an EQUITY row but are not sectors — leftovers from
# the ETF/asset-class fallback. A bucket named "Equity" inside a sector chart is noise wearing a
# label; it is an absence, and it says so.
_NOT_A_SECTOR = {"equity", "bonds", "commodity", "short commodity", "crypto", "etf"}

# ── Asset-class allocation (the portfolio's OWN split, no benchmark) ──────────────────────────
# AIRS's `categorie` classifies what a holding INVESTS IN — an equity ETF is AAND, a bond ETF is
# OBL — so the ETF wrapper is an ORTHOGONAL axis. Only EQUITY is split into direct vs ETF; a bond
# ETF is Bonds. Real estate (VAS, the REITs) folds into Alternatives to match the requested buckets.
_CATEGORIE_TO_CLASS = {"AAND": "Equity", "OBL": "Bonds", "VAS": "Real estate", "ALTBEL": "Alternatives"}
# Bucket order for the allocation bar. Must match `_airs_holding_isin.BUCKET_ORDER`; a literal here
# (not an import) to avoid a module-level cycle — `classify_bucket` is imported per-call instead.
_ALLOC_ORDER = ["Equity", "Equity ETF", "Bonds", "Alternatives", "Cash", UNKNOWN_BUCKET]


def _bench_start_caps(label: str, start: str | None) -> dict[str, float]:
    """`{isin: market cap at `start`}` for an index — or `{}` when there is no window to open at.

    ⚠ THE COMPOSITION CHART'S INDEX BAR IS DRAWN AGAINST A PORTFOLIO WEIGHED AT THE WINDOW'S OPEN,
    so it has to be weighed there too. `_members` carries `market_cap_eur` (today) and that is what
    the bars used until 2026-08-10 — which made the tilt a subtraction across two bases and put a
    figure on screen that contradicted both the axis note above it and the drill-down beneath it.
    Measured on SP500 Technology: 34.90% today against 31.24% at the open.

    ⚠ THE SAME `index_rows` THE ATTRIBUTION USES, not a second reconstruction. That function already
    backs each constituent's start cap out through its price (`_window_rows`), which is the whole
    reason the attribution is not look-ahead biased; a private copy here would be a second place for
    that to rot. Returning a dict rather than rows keeps the caller's classification untouched.

    ⚠ EMPTY ON ANY FAILURE, AND EMPTY MEANS "KEEP TODAY'S CAPS". A benchmark bar drawn from a
    partial start-cap map would be renormalised over whichever constituents happened to resolve —
    a quietly different index. Falling back to the basis the chart used for a year is the smaller
    wrong, and the axis note already tells the reader which one is in force.
    """
    if not start:
        return {}
    try:
        from ._asset_benchmark import index_rows  # noqa: PLC0415

        rows, _coverage = index_rows(label, start)
    except Exception as e:  # noqa: BLE001 — the chart must not fail over a weighting refinement
        _log.warning("[analysis] start-of-window caps for %s unavailable (%s: %s); "
                     "the index bar stays on today's caps", label, type(e).__name__, e)
        return {}
    out = {r["isin"]: float(r["start_cap_eur"]) for r in rows
           if r.get("isin") and (r.get("start_cap_eur") or 0) > 0}
    return out


def _weigh_alloc(items: list[tuple[float, str]]) -> list[dict]:
    """Sum the (weight, bucket) pairs into ordered percentage slices (drops empty buckets).

    ⚠ `holdings` COUNTS THE EXPANDED LEGS, WHICH IS THE POINT OF COUNTING THEM. After the
    certificates are looked through, a slice is no longer "one certificate" — ToppenbergBeheer
    Defensief's Stocks sleeve is 9 lines in AIRS and 160-odd real companies underneath. A weight
    alone cannot tell those apart, and they are not the same portfolio: 66% in one bond ETF and
    66% spread over sixty names carry different risk and read identically on a pie.
    """
    total = sum(w for w, _ in items)
    if total <= 0:
        return []
    agg: dict[str, float] = defaultdict(float)
    cnt: dict[str, int] = defaultdict(int)
    for w, b in items:
        agg[b] += w
        cnt[b] += 1
    return [{"bucket": b, "pct": agg[b] / total * 100.0, "holdings": cnt[b]}
            for b in _ALLOC_ORDER if agg.get(b)]


def _sector(raw: str | None) -> str:
    if not raw or raw.strip().lower() in _NOT_A_SECTOR:
        return UNKNOWN_BUCKET
    return _SECTOR_ALIASES.get(raw, raw)


_GRID_COLS = ("isin,name,sector,country,msci_region,domicile_country,currency,"
              "market_cap_currency,asset_class,status")


def _grid(isins: list[str]) -> dict[str, dict]:
    # ONE COPY instead of ceil(len/200) round trips — see `load_rows_via_copy`. The chunked
    # PostgREST loop below is the fallback and is what runs when the direct connection is
    # unavailable; both return the same rows (verified field for field, types included).
    rows = load_rows_via_copy("asset_grid", _GRID_COLS, "isin", isins)
    if rows is None:
        rows = []
        for i in range(0, len(isins), IN_CHUNK_SIZE):
            rows += (supabase.table("asset_grid").select(_GRID_COLS)
                     .in_("isin", isins[i:i + IN_CHUNK_SIZE]).execute().data or [])
    out: dict[str, dict] = {}
    for r in rows:
        if r.get("status") == "ok":
            out[r["isin"]] = r
    return out


def _foreign_listing(row: dict) -> bool:
    """Is this row priced on a venue in a DIFFERENT currency than the company is valued in?

    ⚠ A DIAGNOSTIC, NOT A CURIOSITY. It is the wrong-listing bug, visible in the benchmark: 40 of
    the 491 S&P 500 members are mapped to a European or Canadian venue (Corning on Stuttgart, WR
    Berkley on Munich, Ciena on Xetra, Exxon on a Canadian line). It is why the naive currency
    split called the S&P 500 "12% EUR", which it plainly is not — and it corrupts the benchmark's
    PRICE SERIES too, not merely this chart. Counted and returned so the reader is told.
    """
    mc, lc = row.get("market_cap_currency"), row.get("currency")
    return bool(mc and lc and mc != lc)


def _country_by_code() -> dict[str, str]:
    rows = supabase.table("country").select("country_code,country_name").execute().data or []
    return {r["country_code"]: r["country_name"] for r in rows if r.get("country_code")}


def _region(row: dict, isin: str | None, codes: dict[str, str]) -> str:
    """The issuer's region — from WHERE THE COMPANY IS, never from where we happen to price it.

    ⚠ `asset_grid.msci_region` CANNOT BE USED DIRECTLY. It comes from `geo.resolve_geo`, which is
    documented to fall back to the LISTING country when the domicile is unknown — sane for the
    instrument grid, catastrophic here. Yahoo returns no domicile for a thin German regional line,
    and our grid prices a number of US megacaps on exactly those:

        LLY.SG   Eli Lilly    on Stuttgart   EUR 873bn  -> "Germany" -> EUROPE
        CHV.DU   Chevron      on Dusseldorf  EUR 322bn  -> "Germany" -> EUROPE
        IBM.HM   IBM          on Hamburg     EUR 221bn  -> "Germany" -> EUROPE

    That is how the S&P 500 came out **7.2% Europe**, which is nonsense — it is an index of US
    companies. 54 members were classified Europe; most were US megacaps on German venues.

    So: domicile first; then the ISIN's own country prefix (the issuer's registration — `US…` for
    Eli Lilly, `IE…` for Linde, `CH…` for Chubb, which is exactly what separates the fake
    Europeans from the real ones); and if neither, UNKNOWN. The listing venue is never consulted.

    ⚠⚠ A DOMICILE THAT EXISTS BUT HAS NO MSCI REGION FALLS THROUGH TO THE ISIN, AND IT DID NOT USED
    TO. `if dom: return msci_region_of(dom) or UNKNOWN` gave up on the spot, so a known domicile
    OUTSIDE MSCI's map — every incorporation haven, and a few real markets MSCI does not index —
    never reached the line below it. MercadoLibre is the case that surfaced it: domiciled Uruguay
    (its Montevideo head office, which Yahoo reports correctly), incorporated in Delaware, ISIN
    `US58733R1023`, and it read `Unclassified` in the region tab while `/asset-pipeline` said North
    America. Two screens, one company, no error.

    ⚠⚠ AND THEN THE STORED COLUMN IS THE **LAST** RESORT — never the first, which is the whole
    difference. The 18 members left over above are incorporated in havens (Cayman, Bermuda,
    Luxembourg, Isle of Man, Macau), so neither their domicile nor their ISIN prefix is a market and
    both steps above yield nothing; leaving those as `Unclassified` threw away an answer
    `/asset-pipeline` already shows, on 0.24% of ACWI. Reaching step 3 means the domicile did not
    map, so by construction `asset_grid.msci_region` is `resolve_geo`'s VENUE fallback — it is our
    choice of listing talking, and it is used only where the issuer's own geography has said nothing.

    That ordering is what keeps the S&P fix intact: those 54 megacaps have NO domicile and a `US…`
    ISIN, so step 2 answers first and this line is never reached for them. Verified after the change:
    S&P Europe still 2.1%, ACWI North America unchanged to 0.1pp.

    ⚠ WHAT IT GETS WRONG, AND WHY THE FIX IS NOT HERE. Where our venue choice is the wrong listing,
    this inherits it: `asset_grid` prices Kingsoft on Stuttgart (`3K1.SG`, EUR 6,550/day), Li Ning on
    Stuttgart (`LNLB.SG`, EUR 2,594/day) and Orient Overseas on Munich (`ORI1.MU`, EUR 3,056/day), so
    three HONG KONG companies are bucketed EUROPE — together 0.02% of ACWI. That is a listing defect
    (their real lines are 3888/2331/0316.HK) and it belongs in `repoint_primary_listing.py`, not in a
    special case here: every such row is wrong in the price series too, which no region rule can fix.
    The other fifteen come out right or defensibly so — Zhen Ding EM (Taiwan), Entain Europe (LSE),
    Allegro/Zabka EM (Warsaw), InPost Europe (Amsterdam), Sands China Pacific (HKSE, which is MSCI's
    own answer), Arch/Everest/Credo North America (genuinely US businesses, S&P 500 members).

    Known limit, and it is the ISIN-country limit generally: the prefix is where the paper was
    REGISTERED, not where the business is. An ADR carries a US ISIN even for a foreign issuer; a
    Cayman- or Bermuda-incorporated issuer carries `KY…`/`BM…`, which MSCI does not index either, so
    it stays `UNKNOWN_BUCKET` (18 of the 21 unclassified ACWI members, 0.35% of the index — Li Ning,
    Sands China, XP, StoneCo, ArcelorMittal, Entain…); and an odd-but-VALID prefix is taken at face
    value — Patria Bank, a Romanian bank on the BVB, carries `MYL1295OO004`, whose check digit is
    good and whose `MY` reads Malaysia, i.e. Emerging Markets by the right family and the wrong
    country. Getting those right needs MSCI's own country assignment (business location + listing),
    which is not derivable from any field we hold — an override table, not a heuristic.
    """
    dom = row.get("domicile_country")
    # ⚠ NOT `if dom: return … or UNKNOWN` — see above. An unmapped domicile is not an answer.
    if dom:
        reg = msci_region_of(dom)
        if reg:
            return reg
    if isin and len(isin) >= 2:
        name = codes.get(isin[:2].upper())
        if name:
            reg = msci_region_of(name)
            if reg:
                return reg
    # ⚠ LAST, AND ONLY BECAUSE BOTH STEPS ABOVE SAID NOTHING — see the docstring. Validated against
    # the real region names rather than passed through: the column is nullable and a stray value
    # would otherwise open a bucket of its own in the chart, which reads as a region.
    stored = row.get("msci_region")
    if stored in _MSCI_REGIONS:
        return stored
    return UNKNOWN_BUCKET


def _buckets(row: dict | None, is_cash: bool, isin: str | None = None,
             codes: dict[str, str] | None = None) -> tuple[str, str, str]:
    """(sector, region, currency) for one holding. The whole honesty of the chart lives here."""
    if is_cash:
        return CASH_BUCKET, CASH_BUCKET, CASH_BUCKET
    if not row:
        return UNKNOWN_BUCKET, UNKNOWN_BUCKET, UNKNOWN_BUCKET
    # A fund is opaque on ALL THREE axes — its listing venue, sector and quote currency say nothing
    # about what it holds — so it folds into Unclassified (FUND_BUCKET == UNKNOWN_BUCKET).
    if (row.get("asset_class") or "").lower() in _FUND_CLASSES:
        return FUND_BUCKET, FUND_BUCKET, FUND_BUCKET
    return (
        _sector(row.get("sector")),
        _region(row, isin, codes or {}),
        # ⚠ `market_cap_currency`, NOT the listing currency. Same disease as the region above: the
        # listing is OUR choice of venue, and where that choice is wrong it invents exposure the
        # company does not have — pricing Corning off Stuttgart does not make it a euro asset. On
        # the S&P 500 the listing field says 91% USD and this one says 98%; the gap is 40
        # mis-mapped rows, not a fact about the index. Falls back to the listing only when the
        # company reports no cap currency at all.
        row.get("market_cap_currency") or row.get("currency") or UNKNOWN_BUCKET,
    )


def _weigh(items: list[tuple[float, tuple[str, str, str]]]) -> dict[str, dict[str, float]]:
    """Sum weights per bucket, per axis, and normalise each axis to 100%."""
    axes: dict[str, dict[str, float]] = {"sector": defaultdict(float),
                                         "region": defaultdict(float),
                                         "currency": defaultdict(float)}
    for w, (sec, reg, cur) in items:
        axes["sector"][sec] += w
        axes["region"][reg] += w
        axes["currency"][cur] += w
    out: dict[str, dict[str, float]] = {}
    for axis, d in axes.items():
        total = sum(d.values())
        out[axis] = {k: (v / total * 100.0) for k, v in d.items()} if total > 0 else {}
    return out


def _with_start_weights(holdings: list[dict], start_weights: dict[str, float]) -> list[dict]:
    """Attach each holding's START-of-window weight, taken from the legs the charts are built on.

    ⚠ JOINED BY ISIN, AND THE JOIN IS SAFE BECAUSE BOTH SIDES ARE ALREADY MERGED BY ISIN. Both
    `_book_port_items` and `book_legs` run `_expand_book_rows`, which ends in `merge_by_isin` — so
    each ISIN is one row on each side and this cannot fan out.

    ⚠ `None` FOR A ROW WITH NO ISIN (cash) — not 0.0. Cash genuinely has a start value; we simply
    have no key to reach it by here, and a 0 would state something false about a real position.
    A 0.0 that DOES arrive is meaningful: bought after the window opened.
    """
    return [{**h, "weight_start_pct": start_weights.get(h.get("isin") or "")} for h in holdings]


def _basis_axes(portfolio_id: int, source: str, effective: str | None,
                bucket_filter: str | None) -> dict | None:
    """The three composition axes on the ATTRIBUTION BASIS — the same weights the Brinson table
    shows, from the same function.

    ⚠ THIS IS A DELIBERATE CHANGE OF QUESTION (2026-07-31), MADE ON REQUEST. These bars used to be
    "what do we hold now": today's EUR value over the whole equity sleeve. They are now "what did
    we hold when the window opened, among the holdings we can attribute" — Beginwaarde over the
    attributable legs, renormalised to 100%. The two differ by more than rounding (Technology 36%
    → 39.1%; ASML 7.30% → 5.75%) and the second is what the attribution table has always shown.

    ⚠ WHAT THAT COSTS, RETURNED SO IT CAN BE SHOWN RATHER THAN DISCOVERED. A holding bought during
    the window has no Beginwaarde and is absent; an unpriceable one has no return and is absent.
    `excluded` + `attributable_pct` carry both, per axis, and the UI puts them on screen. Weight
    that silently leaves a percentage is the failure the coverage floors elsewhere exist to stop.

    ⚠ ONE RULE DECIDES MEMBERSHIP, NOT TWO. The sector axis used to restrict to the
    {Equity, Equity ETF} sleeve AND let the classifier fold the rest into Unclassified. Two
    overlapping rules for one question is how the panels diverged; the ladder in `split_legs` is
    now the only one, so the bar matches its Brinson row by construction.

    Returns None when nothing can be weighed on this basis — the caller falls back rather than
    drawing an empty chart as though the portfolio held nothing.
    """
    from ._airs_attribution_basis import (  # noqa: PLC0415  (cycle at module import)
        AXIS_IDX, portfolio_legs, renormalise, split_legs, window_start,
    )

    start = window_start(source, "ytd", effective)
    if not start:
        return None
    legs = portfolio_legs(source, portfolio_id, effective, start)
    if not legs:
        return None
    # ⚠ A CLASS FILTER WE CANNOT APPLY IS A REFUSAL, NOT A NO-OP. Only the book legs carry an asset
    # class; the model path has none. Ignoring the filter there would chart every class's sectors
    # under a "Stocks" selection, and applying it would empty the chart — so hand back to the
    # caller's fallback, which classifies from its own loader and can filter honestly.
    if bucket_filter and not any(leg.get("asset_class") for leg in legs):
        return None

    grid = _grid(sorted({leg["isin"] for leg in legs if leg.get("isin")}))
    codes = _country_by_code()
    out: dict[str, dict] = {}
    for axis, idx in AXIS_IDX.items():
        attributable, excluded, total_w = split_legs(legs, idx, grid, codes)
        # ⚠ THE CLASS FILTER NARROWS THE NUMERATOR AND THE DENOMINATOR TOGETHER, or the bars stop
        # summing to 100 and every one of them silently means something else.
        #
        # ⚠ AND IT MUST NARROW `total_w` AND `excluded` TOO. It did not, and the ratio that came
        # out was a MIXED one: Stocks-with-a-sector over the WHOLE book. With Stocks selected the
        # card then read "87% of the book has a sector" — true of the book, but presented under a
        # Stocks-only chart, where it reads as an accusation that 13% of the STOCKS are
        # unclassified. Every stock in the measured portfolio has a sector; the 13% was five ETFs
        # and a cash line, which are not stocks and were never candidates for this chart. Filtered
        # consistently, the same portfolio reports 100% and the notice disappears, which is the
        # honest answer.
        if bucket_filter:
            attributable = [i for i in attributable if i.get("asset_class") == bucket_filter]
            excluded = [i for i in excluded if i.get("asset_class") == bucket_filter]
            total_w = sum(i["weight_pct"] for i in (*attributable, *excluded))
        denom = renormalise(attributable)
        if denom <= 0:
            out[axis] = {"weights": {}, "holdings": {}, "excluded": excluded,
                         "attributable_pct": 0.0, "positions": 0}
            continue
        weights: dict[str, float] = defaultdict(float)
        holdings: dict[str, list[dict]] = {}
        for i in attributable:
            w = i["weight_pct"] / denom * 100.0
            weights[i["bucket"]] += w
            holdings.setdefault(i["bucket"], []).append({
                "name": (i.get("grid_row") or {}).get("name") or i.get("airs_name"),
                "isin": i.get("isin"),
                "weight_pct": w,
                "asset_class": i.get("asset_class"),
                "classified_as": i["bucket"],
                "via_names": i.get("via_names") or [],
            })
        for legs_ in holdings.values():
            legs_.sort(key=lambda h: -h["weight_pct"])
        out[axis] = {
            "weights": dict(weights),
            "holdings": holdings,
            "excluded": excluded,
            # How much of the whole book this axis actually speaks for. NOT assumed to be 100.
            "attributable_pct": (denom / total_w * 100.0) if total_w > 0 else 0.0,
            # ⚠ THE ONLY EXCLUSION THAT IS A GAP. An ETF has no sector and a cash line is not a
            # sector bet — those are answers, and they already have their own slice in the
            # allocation chart. An UNPRICED holding is different in kind: a real position, in a
            # real sector, absent from the bars — so its sector reads lower than it is, and
            # elsewhere that exact hole credited a model +1.73pp for "avoiding" a sector it held
            # 6% of. Reported separately because only this one deserves a warning; lumping the two
            # made a perfectly ordinary 13% in ETFs look like a defect.
            "unpriced_pct": (sum(e["weight_pct"] for e in excluded if e["reason"] == "unpriced")
                             / total_w * 100.0) if total_w > 0 else 0.0,
            "positions": len(attributable),
        }
    out["_start"] = start
    # ⚠ THE SAME NUMERATOR THE BARS USE, KEYED BY ISIN — so the Holdings table can print the start
    # weight beside the current one and the reader's own division actually works. `book_legs`
    # already expresses `weight_pct` as Beginwaarde ÷ Σ Beginwaarde over the WHOLE book, which is
    # the right denominator here: it makes the column directly comparable to `weight_now_pct` (also
    # whole-book) and leaves exactly one documented step to a bar — divide by `attributable_pct`.
    #
    # ⚠ A 0.0 HERE IS A FACT, NOT A BLANK: the position was bought after the window opened, so it
    # has no start value. That is the one case where "now" and "start" cannot be reconciled at all,
    # and the table has to say so rather than print an empty cell.
    out["_start_weights"] = {leg["isin"]: leg["weight_pct"] for leg in legs if leg.get("isin")}
    return out


def _axis_holdings(items: list[tuple[float, tuple[str, str, str]]],
                   labels: list[dict]) -> dict[str, dict[str, list[dict]]]:
    """The rows behind every bar: per axis, per bucket, the holdings and their weights.

    ⚠ NORMALISED BY THE AXIS TOTAL — THE SAME DIVISION `_weigh` DOES, SO Σ OVER A BUCKET **IS**
    THAT BAR. That identity is the entire purpose of this function: a drill-down whose rows sum to
    something near-but-not-equal to the number that opened it converts one unexplained figure into
    two. It is computed here rather than in the UI for the same reason the sibling drill-downs are
    handed their series — a second implementation of the denominator is a second denominator.

    ⚠ AND THE DENOMINATOR IS PER AXIS, NOT PER PORTFOLIO. `sector` is weighed over the EQUITY
    sleeve while `region`/`currency` are weighed over every long position, so the caller passes a
    different `items` list for each and the same holding legitimately carries two different
    weights. Sharing one total across the three would make two of the axes wrong.

    Why this exists at all: the attribution table has always shipped its own per-bucket holdings
    (`rows[].portfolio_holdings`, rebased to ITS denominator), so it is self-verifying, while the
    composition chart shipped aggregates only. The two answer the same-sounding question —
    "how much Technology do we hold" — over different denominators and different weight bases, and
    with only one side inspectable a reader had no way to discover that. Measured on Bustelberg
    Offensief: 36% here against 39.1% there, both correct.
    """
    out: dict[str, dict[str, list[dict]]] = {"sector": {}, "region": {}, "currency": {}}
    total = sum(w for w, _b in items)
    if total <= 0:
        return out
    for (w, buckets), lab in zip(items, labels):
        for axis, bucket in zip(("sector", "region", "currency"), buckets):
            out[axis].setdefault(bucket, []).append({
                **lab,
                "weight_pct": w / total * 100.0,
                # The raw classification value, so a reader can see WHY a holding landed in this
                # bucket rather than having to trust that it did.
                "classified_as": bucket,
            })
    for axis in out:
        for legs in out[axis].values():
            legs.sort(key=lambda h: -h["weight_pct"])
    return out


def _apply_book_source(result: dict, benchmark_label: str) -> None:
    """Swap the PRIMARY portfolio return for AIRS's own book number (`cumulatief_rendement`), and
    re-price the benchmark over the book's window — the calendar year, 1 Jan -> today.

    AIRS reports the book only over the calendar year, flow-aware and INCLUDING income, and keeps
    NO composition history — so 'since inception' has no book equivalent and is cleared rather than
    left showing the yfinance model's number under a 'book' banner. `strategy_ytd_pct` still carries
    the yfinance figure, so the Book-vs-strategy drift tile is unaffected by the swap.
    """
    jan1 = f"{date.today().year}-01-01"
    p_ytd = result.get("book_ytd_pct")             # already computed by `_book_return`
    bench = index_returns(benchmark_label, [jan1]) if p_ytd is not None else {}
    b_ytd = (bench.get(jan1) or {}).get("eur_pct")
    result.update({
        "source": "book",
        "ytd_from": jan1 if p_ytd is not None else None,
        "portfolio_ytd_pct": p_ytd,
        # `book_as_of` was set by `_book_return` (runs in both source modes); the benchmark stays
        # yfinance, so `benchmark_as_of` from the model path above is left as-is.
        "portfolio_as_of": result.get("book_as_of"),
        "benchmark_ytd_pct": b_ytd,
        "ytd_excess_pct": (p_ytd - b_ytd) if (p_ytd is not None and b_ytd is not None) else None,
        # AIRS has no since-inception for the book — clear the model's rather than mislabel it.
        "since_from": None,
        "portfolio_since_pct": None,
        "benchmark_since_pct": None,
        "since_excess_pct": None,
        "ytd_is_since": False,
    })


def _returns(portfolio_id: int, effective: str | None, benchmark_label: str,
             source: str = "model") -> dict:
    """EUR return of the model vs the benchmark — over the SAME two windows, both times.

    `source="book"` swaps the PRIMARY portfolio return for AIRS's own book figure (see
    `_apply_book_source`); the yfinance model is still computed (it pins `strategy_ytd_pct` for the
    drift tile) and the benchmark stays yfinance either way, so the two remain comparable.

    ⚠ A BENCHMARK MEASURED OVER A DIFFERENT WINDOW IS NOT A BENCHMARK, IT IS A NUMBER.
        The model's "YTD" opens at `max(1 Jan, its inception)` — for the 27 models younger than
        the year that is NOT 1 January. Putting a 6-day portfolio return beside the index's
        full-year return and calling the gap under- or out-performance would be nonsense, and it
        would look exactly like a finding. So the index is priced from the model's OWN
        `ytd_from`, and again from its OWN inception. `index_returns` takes both windows and
        prices them off one load, through the same start-of-window weighting `/benchmarks` uses.

    ⚠ THE PORTFOLIO SIDE IS *READ*, NEVER RECOMPUTED.
        `compute_portfolio_performance` is the one place a model's return is calculated, and the
        table on /portfolios shows exactly it. Re-deriving it here — even "the same way" — is how
        a modal ends up quietly disagreeing with the row that opened it. It costs a price load we
        have already paid for elsewhere; correctness first.
    """
    from routers._airs_portfolio_perf import (  # noqa: PLC0415
        compute_portfolio_performance,
        ytd_anchor_for,
    )

    # ⚠ SCOPED TO THIS PORTFOLIO — same function, same definition, one row's worth of work. It
    # used to price all 56 models and keep one, which was 5.56s of a modal open. See
    # `only_portfolio_id`: the narrowing touches only the load WINDOWS, which are lower bounds,
    # so the row that comes back is identical.
    perf = next((x for x in compute_portfolio_performance(only_portfolio_id=portfolio_id)
                 if x["portfolio_id"] == portfolio_id), None)
    if not perf:
        return {}

    ytd_from = perf.get("ytd_from") or ytd_anchor_for(effective)
    windows = [w for w in (ytd_from, effective) if w]
    # ⚠ THE BENCHMARK IS PRICED IN THE SAME WORLD AS THE PORTFOLIO — yfinance (`asset_price`),
    # not GuruFocus (`metric_data`). The portfolio's return comes from `asset_price`; pricing the
    # index off a different vendor would compare two price universes with different adjustment
    # conventions and different FX, and call the difference alpha. (It is also the only source
    # that can price ACWI at all: GuruFocus does not sell us the UK or India.) Since 2026-07-16
    # the /portfolios Benchmarks panel is on this path too — `_benchmark_index.compute_index` is
    # no longer any route's basis and survives only as the SPY cross-check of the METHOD.
    bench = index_returns(benchmark_label, windows) if windows else {}

    b_ytd = (bench.get(ytd_from) or {}).get("eur_pct") if ytd_from else None
    b_since = (bench.get(effective) or {}).get("eur_pct") if effective else None
    p_ytd, p_since = perf.get("ytd_pct"), perf.get("since_model_pct")
    yf_asof = (perf.get("sources") or {}).get("yf_close")

    result = {
        "source": "model",
        "ytd_from": ytd_from,
        "since_from": effective,
        "portfolio_ytd_pct": p_ytd,
        # As-of dates for the per-value provenance ⓘ. The model return and the benchmark are both
        # yfinance close series; `_apply_book_source` overrides `portfolio_as_of` with the book
        # snapshot date when the source is the AIRS book.
        "portfolio_as_of": yf_asof,
        "benchmark_as_of": yf_asof,
        # The yfinance strategy YTD, pinned so the Book-vs-strategy sub-tile stays meaningful even
        # when `source=book` makes the primary column the AIRS book.
        "strategy_ytd_pct": p_ytd,
        "benchmark_ytd_pct": b_ytd,
        # The excess. Stated, so nobody subtracts two numbers measured over windows they did not
        # check were the same.
        "ytd_excess_pct": (p_ytd - b_ytd) if (p_ytd is not None and b_ytd is not None) else None,
        "portfolio_since_pct": p_since,
        "benchmark_since_pct": b_since,
        "since_excess_pct": ((p_since - b_since)
                             if (p_since is not None and b_since is not None) else None),
        # A young model's YTD *is* its since-inception return — same window, by construction. The
        # UI says so rather than showing the reader two identical rows and letting them wonder.
        "ytd_is_since": bool(effective and ytd_from == effective),
        **_book_return(portfolio_id, ytd_from, p_ytd),
    }
    result["book_available"] = bool(result.get("book_portefeuille"))
    # `source=book` overrides the PRIMARY portfolio return with AIRS's own; benchmark stays yfinance.
    if source == "book":
        _apply_book_source(result, benchmark_label)
    return result


def _book_return(portfolio_id: int, ytd_from: str | None, model_ytd: float | None) -> dict:
    """What the BOOK made, beside what the STRATEGY made — and the gap, when there is one.

    This modal describes the FIXED portfolio: a set of weights, priced from yfinance. The row
    that opens it shows the DYNAMIC book: real positions, valued by AIRS. They are different
    objects and they disagree — measured across the 28 linked pairs, median 3.12pp and up to
    6.58pp (EuropaTopSelectie: the book -1.28%, the strategy +5.30%). THAT GAP IS THE POINT — it
    is implementation drift, timing and fees, and nothing else on the page answers it.

    ⚠ THE GAP IS ONLY A GAP WHEN THE TWO WINDOWS ARE THE SAME, and for 9 of 28 they are not.
        AIRS's `cumulatief_rendement` is always the calendar year (measured: all 51 accounts hold
        7 months, so the book side is never partial). The model's YTD opens at
        `max(1 Jan, inception)`. MomentumTopSelectie's model is TWELVE DAYS old: setting its
        -3.04% against the book's -2.67% for the year and subtracting produces a number that
        reads exactly like drift and means nothing. So the difference is computed ONLY when the
        model's window opens on 1 January; otherwise `comparable` is false and the reason is
        carried instead of a figure.

    ⚠ THE BOOK'S RETURN IS *READ*, NEVER RECOMPUTED — same rule the portfolio side already
        follows. `_year_perf` is where a book's year is assembled; re-deriving it here is how
        this modal would quietly disagree with the row that opened it.
    """
    from routers._airs_account_links import list_account_links  # noqa: PLC0415

    link = next((a for a in list_account_links()["accounts"]
                 if a.get("model_portfolio_id") == portfolio_id), None)
    if not link:
        return {"book_portefeuille": None, "book_ytd_pct": None, "book_as_of": None,
                "book_comparable": None,
                "book_reason": "No Dynamic portfolio is paired with this one."}

    book_ytd = link.get("ytd_pct")
    # The book's freshness — the latest AIRS scan of its holdings. Always returned (both source
    # modes) so the Book-vs-strategy tile's ⓘ can date the book number regardless of the toggle.
    book_as_of = _book_snapshot_date(link["portefeuille"])
    aligned = ytd_from == f"{date.today().year}-01-01"
    if book_ytd is None:
        reason = "AIRS reports no return for the paired book."
    elif not aligned:
        reason = (f"Not comparable: this strategy's window opens {ytd_from}, while AIRS measures "
                  f"the book over the whole year. Different windows — the difference would not "
                  f"be drift.")
    else:
        reason = None
    return {
        "book_portefeuille": link["portefeuille"],
        "book_ytd_pct": book_ytd,
        "book_as_of": book_as_of,
        "book_comparable": bool(aligned and book_ytd is not None),
        "book_reason": reason,
        # The strategy minus the book: what the weights promised, less what the book delivered.
        "book_gap_pct": (round(model_ytd - book_ytd, 2)
                         if (aligned and book_ytd is not None and model_ytd is not None)
                         else None),
    }



def _expand_book_rows(rows: list[dict]) -> list[dict]:
    """Book holdings with each linked certificate replaced by the stocks of the model it IS.

    The certificate's EUR value is split across that model's composition by its own percentages,
    so the book's total value is unchanged — only its resolution improves.

    ⚠ START VALUE IS SPLIT ON THE SAME PROPORTIONS AS THE CURRENT ONE. It has to travel with it:
    the per-bucket return is `Σnow ÷ Σstart − 1`, so expanding `current_value_eur` alone would
    hand every expanded leg a return computed against a start of zero.

    ⚠ A CERTIFICATE WITH NOTHING BEHIND IT IS LEFT WHOLE — dropping it would delete real book
    value, and every percentage here is a share of a total that would silently shrink.
    """
    from ._airs_lookthrough import _datum_of, _positions_of  # noqa: PLC0415

    # ⚠ `sources` IS STAMPED HERE, WHERE THE SPLIT HAPPENS, because this is the only place that
    # still knows how much of a leg came from where. One entry per ROUTE IN — `label=None` for the
    # book's own shares — carried through `merge_by_isin`, which concatenates them.
    out: list[dict] = []
    for r in rows:
        target = r.get("linked_portfolio_id")
        direct_src = [{"label": None, "model_id": None,
                       "value_eur": float(r.get("current_value_eur") or 0),
                       "start_value_eur": float(r.get("start_value_eur") or 0)}]
        if not target:
            # held directly — no strategy in between
            out.append({**r, "via_names": [], "via_holding_names": [], "sources": direct_src})
            continue
        child = _positions_of(target, _datum_of(target))
        inner = sum(float(c.get("percentage") or 0) for c in child)
        if not child or inner <= 0:
            # A certificate with nothing behind it stays whole, so the book holds IT — the route in
            # is direct, and labelling it with the strategy it wraps would claim a look-through
            # that did not happen.
            out.append({**r, "via_names": [], "via_holding_names": [], "sources": direct_src})
            continue
        cur = float(r.get("current_value_eur") or 0)
        start = float(r.get("start_value_eur") or 0)
        for c in child:
            share = float(c.get("percentage") or 0) / inner
            if share <= 0:
                continue
            out.append({
                **{k: v for k, v in r.items() if k not in
                   ("isin", "holding_name", "current_value_eur", "start_value_eur", "bucket")},
                "isin": c.get("isin"),
                "holding_name": c.get("fonds"),
                "current_value_eur": cur * share,
                "start_value_eur": start * share,
                # ⚠ The parent's Class is NOT inherited: a certificate classified "Equity" would
                # stamp that on a bond the child holds. Cleared so the shared classifier re-derives
                # it from the child instrument's own grid row.
                "bucket": None,
                "linked_portfolio_id": None,
                # WHICH strategy put us in this instrument. The certificate's own name is the only
                # record of it once its value has been split across the model behind it.
                "via_names": ([r["linked_portfolio_name"]]
                              if r.get("linked_portfolio_name") else []),
                # ⚠ THE CERTIFICATE'S OWN AIRS NAME, which `via_names` does NOT carry — that is
                # the STRATEGY's name ("StarTopSelectie Offensief"), while the ledger is keyed by
                # the INSTRUMENT the book actually traded ("Star Selection Index"). Without it a
                # leg cannot find the flows it arrived through, and the only honest thing left to
                # say about its invested capital is nothing at all.
                "via_holding_names": ([r["holding_name"]] if r.get("holding_name") else []),
                # ...and HOW MUCH came that way. `via_names` alone cannot distinguish a position
                # held entirely through a certificate from one that is 96% the book's own shares.
                # ⚠ `model_id` RIDES ALONG, because the route's return has to come from the book
                # behind THIS certificate specifically. Two certificates wrapping two strategies
                # can both hold NVIDIA, and each book values its own position differently.
                "sources": [{"label": r.get("linked_portfolio_name") or "via a certificate",
                             "model_id": target,
                             "value_eur": cur * share,
                             "start_value_eur": start * share}],
            })
    # ⚠ ONE LEG PER ISIN. A book can hold a stock directly AND through two certificates — three
    # rows for one instrument. React keys the drill-down by ISIN and treats duplicates as
    # unsupported, free to omit a row, so an unmerged list can silently lose a holding.
    # Both EUR fields are summed: merging the current value alone would leave the merged leg's
    # return computed against one fragment's start.
    from ._airs_lookthrough import merge_by_isin  # noqa: PLC0415

    return _reclassify_book_rows(
        merge_by_isin(out, fields=("current_value_eur", "start_value_eur")))


def _reclassify_book_rows(rows: list[dict]) -> list[dict]:
    """Give every expanded leg its own Class, from the same classifier the rest of the app uses."""
    from routers._airs_holding_isin import classify_bucket  # noqa: PLC0415

    # ⚠⚠ A LEG WITH NO ISIN MUST BE CLASSIFIED TOO — SKIPPING IT DISABLED THE ONE RULE WRITTEN FOR
    # IT. This used to `continue` on `not r.get("isin")`, leaving `bucket` as None (rendered
    # "Unclassified"). But `classify_bucket`'s FIRST rule is
    #     `not isin and name in {"effectenrekening", "liquiditeiten"}` -> Cash
    # which by construction can only ever fire on a row with no ISIN. The guard made it
    # unreachable. Measured: every certificate's own cash line — `Liquiditeiten`, arriving through
    # look-through in 8 books — sat in Unclassified while its `sector` (computed elsewhere, without
    # this guard) correctly read Cash. Two answers for one row, one screen apart.
    #
    # ⚠ The direct cash line was NOT affected, which is why this hid: `Effectenrekening` comes in
    # already bucketed from `resolve_account_isins` and never reaches here. Only EXPANDED legs
    # arrive with `bucket=None`, so only cash inside a certificate was mislabelled.
    todo = [r for r in rows if not r.get("bucket")]
    if not todo:
        return rows
    grid = _grid(sorted({r["isin"] for r in todo if r.get("isin")}))
    for r in todo:
        g = grid.get(r["isin"]) if r.get("isin") else None
        # An ISIN-less, non-cash row still lands on "Unclassified" — an honest unsure, reached by
        # the classifier rather than by never asking it.
        r["bucket"] = classify_bucket(None, _is_fund(g), r.get("isin"),
                                      r.get("holding_name") or "", g)
    return rows


def _is_fund(grid_row: dict | None) -> bool:
    from routers._airs_holding_isin import _is_etf  # noqa: PLC0415

    return _is_etf(grid_row)


def book_unavailable_reason(portfolio_id: int) -> str:
    """WHY this model has no book view — in the words the reader needs, not "no positions".

    ⚠ `_book_port_items` RETURNS `None` FOR THREE DIFFERENT REASONS AND THE MODAL SHOWED ONE
    SENTENCE FOR ALL OF THEM. "No positions to show for this portfolio" was rendered when the
    model is not paired with a book, when the paired book has never been scanned, and when the
    scan returned nothing — three different problems with three different remedies, and it was
    reported alongside a portfolios list that visibly HAS rows. The obvious reading is "the modal
    is broken", and the actual answer was never on screen.

    Called only on the unhappy path, so the extra reads cost nothing in the normal case.
    """
    from routers._airs_account_links import list_account_links  # noqa: PLC0415

    link = next((a for a in list_account_links()["accounts"]
                 if a.get("model_portfolio_id") == portfolio_id), None)
    if not link:
        return ("This model portfolio is not paired with an AIRS book, so there are no valued "
                "holdings to show. The portfolios list can still expand the BOOK's own rows — "
                "that view reads the account directly and needs no pairing. Pair them from the "
                "Link column to see them here.")
    pf = link["portefeuille"]
    n = (supabase.table("airs_holding").select("portefeuille", count="exact")
         .eq("portefeuille", pf).limit(1).execute().count or 0)
    if not n:
        return (f"Paired with the book {pf}, but `airs_holding` has no rows for it — its "
                f"Vermogensoverzicht has never been scraped, or the last scan did not reach it. "
                f"Refresh that portfolio from AIRS.")
    return (f"Paired with the book {pf}, which has {n} stored holding row(s), but none of them "
            f"survived resolution — every line lacks an ISIN we can join on. Check the book's "
            f"holdings on the portfolios list.")


def _book_snapshot_date(portefeuille: str) -> str | None:
    """The newest `airs_holding` snapshot for one account — the clock every AIRS-valued figure on
    this screen is as-of.

    ⚠ A NAMED FUNCTION, NOT AN INLINE QUERY, FOR TWO REASONS. It was written out twice (the book
    items and the legs), so it was two places to keep in step; and inline it was an unstubbable
    database hop in the middle of an otherwise pure function — `TestBookWeighting` reached
    PRODUCTION through it on a developer machine and raised `KeyError: 'SUPABASE_URL'` in CI,
    which is exactly the asymmetry `tests/conftest.py` exists to make impossible. One seam, and a
    test monkeypatches this instead of faking the whole client.
    """
    rows = (supabase.table("airs_holding").select("as_of_date")
            .eq("portefeuille", portefeuille).order("as_of_date", desc=True)
            .limit(1).execute().data or [])
    return str(rows[0]["as_of_date"]) if rows else None


def _airs_position_return(row: dict | None, net_income: float = 0.0) -> float | None:
    """AIRS's own result for one position: (Huidige waarde + net income) ÷ Beginwaarde − 1.

    ⚠ ONE DEFINITION, USED BY EVERY AIRS-SOURCED FIGURE ON THIS SCREEN — the parent's own rows, a
    directly-held leg of a certificate, and a leg valued by the book behind one. It is the same
    arithmetic the expanded portfolio row's `Return` column runs, so a number here can always be
    checked against the row it came from.

    ⚠ IT IS A POSITION RESULT, NOT A PRICE RETURN, and the difference is not academic: AIRS's
    Beginwaarde is the year-open value OR the PURCHASE value for a position opened during the year.
    MasterCard is +2.14% in BUS_Offensief_Dyn's own book (held since January, ≈ the year's price
    move) and +17.62% in StarTopSelectie's (bought later, cheaper) — same instrument, same window,
    two correct answers to two different questions. That is exactly why each figure has to name the
    book it came from.
    """
    if not row:
        return None
    start = row.get("start_value_eur")
    now = row.get("current_value_eur")
    if not start or now is None:
        return None
    return ((float(now) + net_income) / float(start) - 1.0) * 100.0


def _weigh_sources(sources: list[dict] | None, total_w: float) -> list[dict]:
    """The routes into one holding, each as a share of the BOOK, largest first.

    `label=None` is the book's own shares. Entries are AGGREGATED BY (label, model) — a book can
    reach the same instrument through two certificates that wrap the same strategy, and two chips
    with the same name and two different percentages is a puzzle, not a breakdown.

    ⚠ SUMS TO `weight_now_pct` BY CONSTRUCTION: same numerators, same denominator, so the split
    can never disagree with the weight it splits.

    The opening value travels with it, because the RETURN of a split holding is these routes
    weighted by what each held when the window opened — see `_blend_routes`.
    """
    if not sources or total_w <= 0:
        return []
    agg: dict[tuple[str | None, int | None], dict] = {}
    for s in sources:
        v = float(s.get("value_eur") or 0)
        if v <= 0:                      # a route that carries nothing is not a route
            continue
        key = (s.get("label"), s.get("model_id"))
        cur = agg.setdefault(key, {"label": key[0], "model_id": key[1],
                                   "value_eur": 0.0, "start_value_eur": 0.0})
        cur["value_eur"] += v
        cur["start_value_eur"] += float(s.get("start_value_eur") or 0)
    out = sorted(agg.values(), key=lambda s: -s["value_eur"])
    for s in out:
        s["weight_now_pct"] = s["value_eur"] / total_w * 100.0
    return out


def _blend_routes(routes: list[dict]) -> tuple[float | None, list[str]]:
    """The holding's return: its routes weighted by what each held when the window OPENED.

        Σ startᵢ · (1 + rᵢ) ÷ Σ startᵢ − 1

    ⚠ ONE POSITION REACHED TWO WAYS IS STILL ONE POSITION, AND EITHER LEG ALONE MISREPRESENTS IT.
    MasterCard is 95.90% of its opening value held outright (+2.14%, this book's own valuation) and
    4.10% through the Star certificate (+17.62%, StarTopSelectie's) — quoting the first calls the
    holding +2.14% while ignoring a leg that nearly tripled the book's rate on it, and quoting the
    second describes 4% of the position with the other 96% invisible. The blend is +2.77%.

    ⚠ OPENING VALUE, NOT CURRENT — the same rule the rest of this file lives by. A leg that rose
    carries a bigger share of the position today than it held while it was rising, so weighting by
    today's value overstates (measured elsewhere on this book: +11.19% against a true +5.58%).

    ⚠ A ROUTE WITH NO RETURN LEAVES BOTH SIDES. It is dropped from the numerator AND the
    denominator, so the answer is the return of the legs we can actually value rather than one
    silently diluted toward zero by a leg we cannot. `blend_weight_pct` is stamped on the routes
    that DID count, so the card can show the reader exactly which ones spoke.

    Returns (return_pct, the distinct books behind it).
    """
    usable = [s for s in routes
              if s.get("return_pct") is not None and float(s.get("start_value_eur") or 0) > 0]
    denom = sum(float(s["start_value_eur"]) for s in usable)
    for s in routes:
        s["blend_weight_pct"] = (float(s["start_value_eur"]) / denom * 100.0
                                 if (s in usable and denom > 0) else None)
    if not usable or denom <= 0:
        return None, []
    grown = sum(float(s["start_value_eur"]) * (1 + s["return_pct"] / 100.0) for s in usable)
    books = sorted({s["book"] for s in usable if s.get("book")})
    return (grown / denom - 1.0) * 100.0, books


def _wrapped_book_marks(model_ids: set[int]) -> dict[int, dict[str, dict]]:
    """model id → {ISIN → the AIRS valuation of that instrument in the book BEHIND that certificate}.

    A certificate is a wrapper around a model, and that model has an AIRS account of its own with a
    real Vermogensoverzicht: Beginwaarde, Huidige waarde and the journal's dividends, per position.
    That is the book that actually holds the shares, so it is the book that gets to say what they
    did — the alternative was our yfinance series, which answers a DIFFERENT question (the year's
    price move on a listing we picked) and diverged wildly from AIRS on this book: Shopify −25.54%
    against +18.24%, Fair Isaac −32.04% against +15.33%.

    ⚠ KEYED BY MODEL, NOT FLATTENED TO ONE ISIN MAP. Two certificates wrapping two strategies can
    both hold NVIDIA, and each book values ITS OWN position — different purchase dates, different
    results (see `_airs_position_return`). Flattened, one of them would answer for both; here each
    route asks the book it actually came through, and nothing has to be arbitrated or averaged.

    A model with no paired account is simply absent — a certificate can wrap a model nobody holds
    an account for, and that leg falls back to the price series.
    """
    from routers._airs_account_links import list_account_links  # noqa: PLC0415
    from routers._airs_holding_isin import resolve_account_isins  # noqa: PLC0415

    from ._airs_accounts import account_holdings  # noqa: PLC0415

    if not model_ids:
        return {}
    by_model = {a["model_portfolio_id"]: a["portefeuille"]
                for a in list_account_links()["accounts"] if a.get("model_portfolio_id")}
    out: dict[int, dict[str, dict]] = {}
    for mid in sorted(model_ids):
        pf = by_model.get(mid)
        if not pf:
            _log.warning("[analysis] wrapped model %s has no paired AIRS account — its legs fall "
                         "back to the price series", mid)
            continue
        # See `resolve_account_isins(freshen=...)` — this path shows no price-check verdict.
        res = resolve_account_isins(pf, freshen=False)
        child_rows = res.get("rows") or []
        # The journal, for the same reason the parent loads it: a leg that paid a dividend must not
        # read lower here than the identical instrument held directly.
        income = {r["holding_name"]: r for r in (account_holdings(pf).get("rows") or [])}
        marks: dict[str, dict] = {}
        for r in child_rows:
            isin = r.get("isin")
            if not isin:
                continue
            d = income.get(r.get("holding_name")) or {}
            net = (d.get("dividend_eur") or 0.0) + (d.get("dividend_tax_eur") or 0.0)
            ret = _airs_position_return(r, net)
            if ret is None:
                continue
            # ⚠ THE VALUATION ITSELF RIDES ALONG, not only the percentage it implies. A return with
            # no numerator and denominator on screen cannot be checked against the book it claims
            # to come from, and checking it against that book is the entire reason it is preferred
            # over our price series.
            marks[isin] = {"return_pct": ret, "as_of": res.get("as_of"), "portefeuille": pf,
                           "income_eur": net or None,
                           "start_value_eur": float(r["start_value_eur"]),
                           "current_value_eur": float(r["current_value_eur"])}
        out[mid] = marks
        _log.warning("[analysis] wrapped book %s (model %s): %d of %d position(s) carry an AIRS "
                     "return", pf, mid, len(marks), len(child_rows))
    return out


def _book_port_items(portfolio_id: int, codes: dict[str, str]) -> dict | None:
    """The composition as the BOOK actually holds it — weighted by AIRS's EUR values, not the
    model's nominal percentages.

    ⚠ ONLY THE WEIGHTS COME FROM AIRS. The classification (sector / region / currency) still runs
    through the SAME `_grid` + `_buckets` the model side and the benchmark use — yfinance's
    `asset_grid`, joined by ISIN. It has to: the benchmark is classified that way, and two
    taxonomies in one chart invent a tilt ("Technology" vs "Information Technology" is not a bet).
    AIRS's own `BU-Inf.Technol` vocabulary would be exactly that mistake.

    The ISIN comes from the book's own name→ISIN resolution (`resolve_account_isins`, the
    price-gated match), because `airs_holding` carries no ISIN. A wrong SHARE CLASS there is
    harmless for classification — both classes of a fund share sector/region/currency — even
    where it would be a wrong price.

    Returns None when the model has no paired book, or the book resolves to nothing priceable.
    """
    from routers._airs_account_links import list_account_links  # noqa: PLC0415
    from routers._airs_holding_isin import resolve_account_isins  # noqa: PLC0415

    link = next((a for a in list_account_links()["accounts"]
                 if a.get("model_portfolio_id") == portfolio_id), None)
    if not link:
        return None
    rows = (resolve_account_isins(link["portefeuille"], freshen=False).get("rows") or [])
    if not rows:
        return None

    # ⚠ BOTH OF THESE ARE TAKEN BEFORE THE EXPANSION, AND THAT IS THE ENTIRE POINT.
    #
    # `direct_marks` — the parent's OWN valuation of each instrument it holds directly. After
    # `_expand_book_rows` an instrument held BOTH directly and inside a certificate is ONE merged
    # row whose start/current are the sum of the two, and the certificate's half carries the
    # CERTIFICATE's return — so the merged figure is unusable and the directly-held position's own
    # AIRS valuation exists nowhere else. Measured on BUS_Offensief_Dyn: MasterCard is EUR 50,489
    # held directly against EUR 1,991 (3.8%) through the certificate, and it was being priced off
    # yfinance purely because SOME of it arrives wrapped.
    #
    # `wrapped_ids` — the models behind the certificates this book holds. Their own AIRS accounts
    # value every leg the parent cannot: 20 of this book's 23 look-through legs are reachable ONLY
    # through the certificate, so the parent's Vermogensoverzicht has no line for them at all.
    direct_marks = {r["isin"]: r for r in rows
                    if r.get("isin") and not r.get("linked_portfolio_id")}
    wrapped_ids = {r["linked_portfolio_id"] for r in rows if r.get("linked_portfolio_id")}

    # ⚠ THE BOOK SIDE NEEDS THE SAME LOOK-THROUGH, AND FOR A SHARPER REASON. The model side at
    # least held nominal percentages; here the certificates ARE the book — ToppenbergBeheer
    # Defensief holds nine of them — so weighting by AIRS's EUR values and classifying what is
    # left charts "Unclassified 100%". A composition chart that says the portfolio is entirely
    # unclassifiable is not a limitation, it is a wrong answer: the stocks are known, one link
    # away, and the model side is already drawing them.
    rows = _expand_book_rows(rows)

    grid = _grid(sorted({r["isin"] for r in rows if r.get("isin")}))
    items: list[tuple[float, tuple[str, str, str]]] = []
    # Who each item IS — parallel to `items` (same loop, same order), so the composition drill-down
    # can name the rows behind a bar. Identity only; the weight is `items`', because there must be
    # exactly one place a weight comes from.
    labels: list[dict] = []
    alloc_items: list[tuple[float, str]] = []
    # (row, current EUR value, class) for every LONG position — the whole-portfolio holdings table.
    raw_positions: list[tuple[dict, float, str]] = []
    classified_w = total_w = 0.0
    foreign = holdings = 0
    for r in rows:
        # ⚠ Weight is the position VALUE, and a non-positive one is skipped — same rule the model
        # side applies to a 0% weight. That drops a short (Nestle India at -EUR 44,680) and an
        # overdraft cash line from the *composition*: a bar chart of what the book is LONG.
        w = float(r.get("current_value_eur") or 0)
        if w <= 0:
            continue
        total_w += w
        holdings += 1
        isin = r.get("isin")
        is_cash = r.get("asset_class") == "Cash" or not isin
        grow = grid.get(isin) if isin else None
        b = _buckets(grow, is_cash=is_cash, isin=isin, codes=codes)
        if b[0] != UNKNOWN_BUCKET:
            classified_w += w
        if grow and _foreign_listing(grow):
            foreign += 1
        items.append((w, b))
        labels.append({"name": r.get("holding_name"), "isin": isin,
                       "asset_class": r.get("bucket") or UNKNOWN_BUCKET,
                       "via_names": r.get("via_names") or []})
        # The book row is already classified by resolve_account_isins (the shared classifier).
        alloc_items.append((w, r.get("bucket") or UNKNOWN_BUCKET))
        raw_positions.append((r, w, r.get("bucket") or UNKNOWN_BUCKET))
    if not items:
        return None
    # Per-bucket return = the START-WEIGHTED value change, Σnow ÷ Σstart − 1 (equivalently each
    # holding's return weighted by its OPENING value). NOT current-value weighted: a holding up +148%
    # has tripled its share of the book, so weighting by current value lets one winner dominate and
    # inflates the figure (AITopSelectie: +56.11% current-weighted vs +41.98% true, book +43.08%).
    # For the allocation pie's legend + the sleeve views. Alongside it the per-HOLDING detail
    # (start-weight + return), so a non-equity sleeve's contribution breakdown reconciles to the
    # sleeve figure: Σ over a bucket of (startᵢ / Σstart) · retᵢ == that bucket's return above, exactly.
    # ⚠ THE INCOME IS LOADED HERE, BEFORE ANY RETURN IS FORMED, BECAUSE EVERY RETURN ON THIS
    # SCREEN HAS TO INCLUDE IT. AIRS's own headline (`cumulatief_rendement`) is flow-aware and
    # carries dividends; the per-holding column is `(current + net income) ÷ Beginwaarde − 1`, the
    # same figure the expanded row shows. A class subtotal computed on price alone therefore sat
    # between two totals and disagreed with both — measured on EuropaTopSelect OFF DYN, the Equity
    # sleeve read −2.79% while 17 of its 27 holdings had paid a dividend that the rows above and
    # the tile below both counted. Three bases on one screen, all AIRS-sourced, all defensible
    # separately.
    #
    # `_direct_result` is the row's OWN loader (the Mutaties journal, keyed on `holding_name`), so
    # the two surfaces cannot drift. ⚠ The tax is ADDED — AIRS books withholding as a negative, so
    # `gross + tax` IS the net; `- tax` overstates every foreign holding by twice the withholding.
    from ._airs_accounts import _direct_result  # noqa: PLC0415

    # ⚠ PRE-EXPANSION NAMES TOO. `merge_by_isin` keeps ONE name for an instrument held both directly
    # and through a certificate, and it need not be the parent's — so asking the journal only for
    # post-expansion names can lose the income of a position the parent holds itself.
    _income, _sold = _direct_result(
        link["portefeuille"],
        {r.get("holding_name") for r in rows if r.get("holding_name")}
        | {r.get("holding_name") for r in direct_marks.values() if r.get("holding_name")})

    def _net_income(r: dict) -> float:
        d = _income.get(r.get("holding_name"))
        return ((d.gross_eur or 0.0) + (d.tax_eur or 0.0)) if d else 0.0

    priced = [(r, float(r.get("start_value_eur") or 0), float(r["current_value_eur"]))
              for r in rows
              if float(r.get("start_value_eur") or 0) != 0 and r.get("current_value_eur") is not None]
    total_start = sum(s for _r, s, _n in priced) or 1.0
    priced_by_id = {id(r): (s, n) for r, s, n in priced}

    # ⚠ EVERY LONG POSITION, not only the priced ones — this list is also the whole-portfolio
    # holdings table, and a cash line or an unpriceable structured product that silently vanished
    # from it would leave a table whose classes do not add up to the pie beside them.
    #
    # ⚠ TWO WEIGHTS, TWO DENOMINATORS, AND SWAPPING THEM IS A REAL BUG:
    #   `weight_pct`      OPENING-value share of the PRICED book. None when we could not price the
    #                     position over the window. This is the one that makes a class's
    #                     contribution reconcile: Σ over a class of (startᵢ/Σstart)·retᵢ IS that
    #                     class's return, exactly. Weighting by CURRENT value instead lets one
    #                     winner dominate (a holding up +148% has tripled its share of the book —
    #                     AITopSelectie reads +56.11% current-weighted against a true +41.98%).
    #   `weight_now_pct`  CURRENT-value share of the WHOLE book — the same number the allocation
    #                     pie is drawn from, so per-class subtotals in the table equal the pie's
    #                     slices to the decimal. A table that disagrees with the chart directly
    #                     above it is read as a bug in both.
    # ⚠ A LOOKED-THROUGH LEG HAS NO BOOK RETURN OF ITS OWN, AND SPLITTING ONE IS A FABRICATION.
    # `_expand_book_rows` divides a certificate's start AND current value by the same composition
    # share, so every instrument behind it comes out with the CERTIFICATE's return — measured on
    # ToppenbergBeheer Defensief, 135 stocks carried just 37 distinct returns, one per certificate,
    # and NVIDIA reported +0.08% (its wrapper's figure) against its own +2.82% over the window.
    # The number was not noise, it was confident and wrong, and it is the kind nobody re-checks.
    #
    # The book simply does not know what NVIDIA did — it knows what the certificate did. So the
    # per-instrument return comes from the instrument's OWN EUR price series, through the same
    # `compute_holding_marks` that produces the arithmetic behind a portfolio's YTD elsewhere:
    # same anchor, same split adjustment, same per-date FX. Aggregates are untouched — a class
    # return is still the book's own value change, which is the right answer to "what did my money
    # in this class do" and ties to the portfolio figure.
    from ._airs_lookthrough import _datum_of  # noqa: PLC0415
    from ._airs_portfolio_perf import compute_holding_marks, ytd_anchor_for  # noqa: PLC0415

    anchor = ytd_anchor_for(_datum_of(portfolio_id))
    marks = compute_holding_marks(
        sorted({r["isin"] for r, _w, _b in raw_positions if r.get("isin")}), anchor)

    # ⚠ AIRS'S OWN RETURN FOR A DIRECTLY-HELD ROW; THE YFINANCE SERIES ONLY WHERE THE BOOK CANNOT
    # ANSWER. The look-through argument above is right and stays — a certificate's value change
    # belongs to the WRAPPER, so splitting it across the 135 stocks inside gives every one of them
    # the wrapper's number (NVIDIA read +0.08% against its own +2.82%). But it was applied to
    # EVERY row, including the ones the book values directly, and for those AIRS knows the answer
    # exactly: Fortinet in AITopSelectie OFF DYN is +111.74% by AIRS's own Beginwaarde → Huidige
    # waarde (plus its net dividend) and +108.65% off our yfinance series. Both are defensible;
    # having the modal show one while the row that opened it shows the other is not.
    #
    # `via_names` is what tells the two apart: non-empty means the row was exploded out of a
    # certificate and its share of the book's value change is synthetic.
    #
    # The income is `_net_income` above — ONE load, shared with the bucket aggregation, so a class
    # subtotal and the rows under it cannot end up on different bases.
    _airs_n = _look_n = _direct_via_n = _wrapped_n = _blend_n = _none_n = 0

    # The books behind the certificates this one holds. Loaded ONCE, and only when something is
    # actually wrapped — an unwrapped book pays nothing for this.
    wrapped_marks = _wrapped_book_marks(wrapped_ids)

    # ⚠ THE DATE A NUMBER IS AS-OF BELONGS TO THE NUMBER, NOT TO THE PAYLOAD.
    # The analysis publishes `as_of = positions_datum` — the MODEL COMPOSITION's effective date,
    # 2025-12-30 for AITopSelectie. That is a true fact about the weights the model declares, and
    # it is the wrong clock for anything valued by the book: these returns are AIRS valuations
    # dated 2026-08-01. Stamped with the payload-level date, the modal reported the same
    # +111.74% as the portfolios row while calling it 216 days old against the row's 2 — one
    # number, two ages, and no way for a reader to tell which surface to believe.
    #
    # So each holding carries its own. A book-valued row is as-of the SNAPSHOT the valuation came
    # from; a look-through row is as-of the last close of the instrument's OWN series, which is a
    # different date again and can trail it.
    book_as_of = _book_snapshot_date(link["portefeuille"])

    holdings_detail: list[dict] = []
    for r, w, b_alloc in raw_positions:
        isin = r.get("isin")
        grow = grid.get(isin) if isin else None
        # The holding's own quote currency — NOT folded to Unclassified the way the fund axes are.
        # For a bond/ETF class the quote currency is a fair first-order FX signal (a EUR-quoted line
        # vs a USD one), which is exactly what the currency chart is for.
        cur = (grow.get("market_cap_currency") or grow.get("currency")) if grow else None
        # ⚠ THE SECTOR IS THE CHART'S BUCKET, TAKEN FROM THE SAME `_buckets` — not the raw
        # `asset_grid.sector`. The holdings table sits directly under the sector bars, so a row
        # reading "Financial Services" beside a bar reading "Financials" is a reader's problem to
        # arbitrate and both are ours to have avoided. It follows that a fund reads Unclassified
        # here (its listing says nothing about what it holds) and cash reads Cash — the same
        # answers the bars give, which is what makes a row findable behind a bar.
        sec = _buckets(grow, is_cash=(r.get("asset_class") == "Cash" or not isin),
                       isin=isin, codes=codes)[0]
        pr = priced_by_id.get(id(r))
        mk = marks.get(isin) if isin else None
        via = r.get("via_names") or []

        # ⚠ EVERY ROUTE IS VALUED SEPARATELY, BY THE BOOK THAT ACTUALLY HOLDS IT, AND THE HOLDING'S
        # RETURN IS THEIR BLEND. One position reached two ways is still one position, and either
        # leg alone misrepresents it — see `_blend_routes`. Each route asks in turn:
        #
        #   * the book's OWN valuation of its own shares (`label is None`). For a purely direct row
        #     that is the row itself; for a split row it is the PRE-EXPANSION line, because the
        #     merged row's start/current are contaminated by the certificate's proportional split.
        #     Before this, ANY via tag sent the whole row to yfinance, which is how MasterCard —
        #     96% of it held outright — came to be priced off a listing.
        #   * the book BEHIND that certificate (`model_id`), for the part that arrives wrapped.
        #     20 of this book's 23 look-through legs exist ONLY there.
        #
        # Our yfinance series is the last resort, for a holding no AIRS book values at all.
        d = _income.get(r.get("holding_name"))
        net_income = _net_income(r)
        own_book = None
        routes = _weigh_sources(r.get("sources"), total_w)
        direct = direct_marks.get(isin or "") if via else None
        for rt in routes:
            if rt["model_id"] is None:
                # This book's own shares. `direct` is set only on a split row; on a purely direct
                # row the route's own start/current already ARE the clean ones.
                src_row = direct if direct is not None else r
                rt["return_pct"] = _airs_position_return(
                    {"start_value_eur": rt["start_value_eur"], "current_value_eur": rt["value_eur"]}
                    if direct is None else src_row, _net_income(src_row))
                rt["book"] = link["portefeuille"] if rt["return_pct"] is not None else None
                rt["as_of"] = book_as_of
                # ⚠ THE VALUATION THE RETURN WAS COMPUTED FROM, which for a split row is the
                # DIRECT position's — not this route's slice of the book. They coincide on a
                # purely direct row and diverge on a split one, and printing the slice beside the
                # direct position's return would show two numbers whose ratio is not the third.
                src_vals = src_row if direct is not None else rt
                rt["book_start_value_eur"] = float(src_vals.get("start_value_eur") or 0) or None
                rt["book_current_value_eur"] = (
                    float(src_vals.get("current_value_eur") or src_vals.get("value_eur") or 0) or None)
                rt["book_income_eur"] = _net_income(src_row) or None
                if rt["return_pct"] is not None and direct is not None:
                    # The income + journal line belong to the position the figure came from.
                    net_income = _net_income(direct)
                    d = _income.get(direct.get("holding_name"))
            else:
                wm = (wrapped_marks.get(rt["model_id"]) or {}).get(isin or "")
                rt["return_pct"] = wm["return_pct"] if wm else None
                rt["book"] = wm["portefeuille"] if wm else None
                # ⚠ THE WRAPPED BOOK'S OWN SNAPSHOT DATE, which trails the parent's (measured 5
                # days on BUS_Offensief_Dyn). Stamping it with the parent's would age-check a
                # number against a scan it never came from.
                rt["as_of"] = wm["as_of"] if wm else None
                # That book's own valuation of ITS position — the numbers behind `return_pct`, so
                # the card can print the division rather than assert its result.
                rt["book_start_value_eur"] = wm.get("start_value_eur") if wm else None
                rt["book_current_value_eur"] = wm.get("current_value_eur") if wm else None
                rt["book_income_eur"] = wm.get("income_eur") if wm else None
        own, books = _blend_routes(routes)
        if own is not None:
            own_src, own_est = "airs", False
            # ⚠ A BLEND IS ONLY AS FRESH AS ITS STALEST LEG. The oldest contributing snapshot, not
            # this book's — claiming today's date for a number half-built from a five-day-old scan
            # is the same lie as stamping a look-through row with the parent's clock.
            dates = sorted(rt["as_of"] for rt in routes
                           if rt.get("blend_weight_pct") is not None and rt.get("as_of"))
            own_as_of = dates[0] if dates else book_as_of
            # Named only when ONE book produced it; a blend belongs to neither alone, and the
            # routes carry the per-leg attribution the card renders.
            own_book = books[0] if len(books) == 1 else None
            if len(books) > 1:
                _blend_n += 1
            elif not via:
                _airs_n += 1
            elif own_book == link["portefeuille"]:
                _direct_via_n += 1
            else:
                _wrapped_n += 1
        else:
            # Nobody's book values it: a leg whose wrapped model has no paired account, or a row
            # AIRS cannot value on both ends (bought mid-window, no Beginwaarde). The instrument's
            # own EUR series is the only honest answer left.
            own = mk.get("return_pct") if mk else None
            own_src, own_est = ("yfinance", bool(mk.get("start_interpolated"))) if mk else (None, False)
            # This listing's OWN latest close — not the book's snapshot and not the fleet's. A
            # thinly-traded line can sit weeks behind both, and that is the row worth doubting.
            own_as_of = (mk.get("end_date") or mk.get("last_close")) if mk else None
            # ⚠ COUNTED APART. "priced off a listing instead of off the book" and "nobody can price
            # this at all" are different outcomes, and rolling them together hides the second: the
            # only two rows left on this book are cash lines with no ISIN, which is the right
            # answer, and a single counter would have reported them as a yfinance fallback.
            if own is not None:
                _look_n += 1
            else:
                _none_n += 1

        holdings_detail.append({
            "name": r.get("holding_name"),
            "isin": isin,
            "bucket": b_alloc,
            "sector": sec,
            "currency": cur,
            "via_names": via,
            # The certificate INSTRUMENT names behind those routes. Parallel to `via_names` (the
            # strategies) because only this one keys the book's ledger — see `_via_capital`.
            "via_holding_names": r.get("via_holding_names") or [],
            # ⚠ THE ROUTES IN, EACH AS A SHARE OF THE BOOK — so they SUM to `weight_now_pct`, the
            # column beside them. A share of the ROW ("96% direct") answers a different question and
            # ties to nothing else on screen; it rides along in the UI's tooltip instead. Each also
            # carries its OWN return, its book, and the share of the blend it spoke for, which is
            # the arithmetic behind `own_return_pct` — one list, so the split shown beside the
            # Weight column and the split behind the Return column cannot be two different things.
            "sources": routes,
            "weight_pct": (pr[0] / total_start * 100.0) if pr else None,
            "weight_now_pct": w / total_w * 100.0 if total_w else 0.0,
            # ⚠ THE EUROS BEHIND THE RESULT COLUMNS, at the EXPANDED granularity — so a
            # looked-through leg carries its share of the certificate's value, exactly as its
            # weight already does. `_expand_book_rows` splits start AND current by the same
            # composition share, so summing these over the expanded rows reproduces the book's own
            # held result: expansion moves value between rows, it does not create or destroy it.
            #
            # ⚠ THIS IS A VALUE SPLIT, NOT A RETURN CLAIM. The Return column stays
            # `own_return_pct` (the instrument's own), because handing every stock inside a
            # certificate the wrapper's PERCENTAGE is the documented lie (NVIDIA +0.08% against its
            # own +2.82%). A euro amount is different in kind: the certificate really did produce
            # it, and this row really is that share of the certificate.
            "start_value_eur": (float(r.get("start_value_eur")) or None
                                if r.get("start_value_eur") else None),
            "current_value_eur": (float(r["current_value_eur"])
                                  if r.get("current_value_eur") is not None else None),
            # Unconditional, unlike `own_income_eur` below, which is suppressed when another book
            # produced the return. The money reached THIS book either way, so it belongs in this
            # book's result.
            "income_eur": net_income or None,
            "return_pct": ((pr[1] / pr[0] - 1.0) * 100.0) if pr else None,
            "own_return_pct": own,
            "own_return_from": anchor,
            # WHICH of the two answers this row got. Two rows in one column measured different
            # ways, with nothing saying which is which, is the thing this whole change is undoing.
            "own_return_source": own_src,
            # WHICH AIRS book said so. `own_return_source` alone stopped being enough the moment a
            # figure could come from a book other than this one: MasterCard is +2.14% here and
            # +17.62% in StarTopSelectie's, both AIRS, both right, and a column that shows one
            # without naming the book is unfalsifiable. None on a yfinance row.
            "own_return_book": own_book,
            # ⚠ PER ROW, because the bases have different clocks — see `book_as_of` above.
            "own_return_as_of": own_as_of,
            "own_income_eur": (net_income if d and own_book == link["portefeuille"] else None),
            # A sparse yfinance series gets an interpolated opening mark, and it has to say so.
            "own_return_estimated": own_est,
        })
    # WARNING, not info: uvicorn leaves the root logger at WARNING, so an `info` line is invisible
    # in production — and this is the line that says which of the two return bases each row got.
    _log.warning(
        "[analysis] %s: per-holding returns — %d from this book (Beginwaarde -> Huidige waarde + "
        "net income, identical to the expanded row's Return column), %d BLENDED across this book "
        "and the book(s) behind a certificate (opening-value weighted), %d from this book's own "
        "DIRECT valuation alone, %d from a wrapped book alone, %d from the yfinance series (no "
        "AIRS book values them: an unpaired wrapped model, or no opening value anywhere), %d with "
        "no return at all (cash lines, and rows with no ISIN to join on)",
        link["portefeuille"], _airs_n, _blend_n, _direct_via_n, _wrapped_n, _look_n, _none_n)

    # ⚠ NO `bucket_returns` HERE ANY MORE, AND IT MUST NOT COME BACK. This function computed
    # `(Σ current + income) ÷ Σ start` per class, which OMITS whatever the class banked by selling:
    # measured on AITopSelectie, it reported Stocks at +43.53% against a class that made +44.16%,
    # with the EUR 6,307 realised on trims missing from the rate while sitting in the Result column
    # beside it. The class return is now derived ONCE, in `compute_portfolio_analysis`, from the
    # rows the table actually renders — so the allocation legend and the class subtotal cannot be
    # two different numbers. Leaving this here as an unused second answer is how it would drift
    # back in.
    return {"items": items, "labels": labels,
            "alloc_items": alloc_items,
            "holdings_detail": holdings_detail,
            "classified_w": classified_w, "total_w": total_w, "foreign": foreign,
            # The snapshot every figure above is valued at — carried out so the payload can stamp
            # the weight columns with it instead of with the composition's effective date.
            "book_as_of": book_as_of,
            "holdings": holdings, "portefeuille": link["portefeuille"]}


def _realised_block(portfolio_id: int) -> dict:
    """What this model's paired BOOK realised on sales this year — the leg the holdings table
    cannot show, because a sold position has no row left.

    ⚠ IT READS `account_return_reconciliation`, THE ONE PLACE THAT ASSEMBLES A BOOK'S YEAR. The
    /portfolios "Total return" panel shows exactly this; re-deriving it here — even "the same way"
    — is how a modal ends up quietly disagreeing with the surface it was checked against. Same
    rule `_returns` and `_book_return` already follow.

    ⚠ IT NEVER FETCHES FROM AIRS. The reconciliation reads the CACHED Transacties snapshot; the
    modal opens on a click, and a headless scrape behind it would cost seconds and could collide
    with a fleet scan holding the session lock. An unfetched book yields `available: false` with a
    reason, which the UI states rather than showing an empty list that reads as "sold nothing".

    ⚠ AN UNPAIRED MODEL HAS NO BOOK, SO NO SOLD LEG EXISTS — absent, not empty. A model portfolio
    is a set of weights; only a book buys and sells.
    """
    from airs_reconciliation import contributions  # noqa: PLC0415

    from routers._airs_account_links import list_account_links  # noqa: PLC0415
    from routers._airs_return_reconciliation import account_return_reconciliation  # noqa: PLC0415

    link = next((a for a in list_account_links()["accounts"]
                 if a.get("model_portfolio_id") == portfolio_id), None)
    if not link:
        return {"available": False,
                "note": "No Dynamic portfolio is paired with this one, so there are no "
                        "transactions to read — a model is a set of weights; only a book trades."}
    try:
        rec = account_return_reconciliation(link["portefeuille"])
    except Exception as e:  # noqa: BLE001 — the sold block must never break the modal
        _log.warning("[analysis] realised block failed for %s (%s: %s)",
                     link["portefeuille"], type(e).__name__, e)
        return {"available": False, "portefeuille": link["portefeuille"],
                "note": f"Could not read this book's transactions ({type(e).__name__})."}

    # ⚠ NULL IS NOT ZERO — the distinction the whole panel rests on. No cached sheet means the
    # realised leg is UNKNOWN, and an empty "Sold this year" list would state that the book sold
    # nothing, which on BUS_Offensief_Dyn would hide EUR 28,656 of realised loss.
    if rec.get("realised_ytd_eur") is None:
        from airs_transacties import LOAD_TRANSACTIONS_HINT  # noqa: PLC0415
        return {"available": False, "portefeuille": link["portefeuille"],
                "note": (rec.get("realised_note")
                         or "This book's transactions have not been fetched yet, so what it "
                            f"realised on sales is unknown. {LOAD_TRANSACTIONS_HINT}")}

    ledger = _position_ledger(link["portefeuille"], rec)
    c = contributions(rec)
    _log.warning("[analysis] %s realised %s over %d name(s); %s%% of the year's movement is "
                 "outside the holdings table", link["portefeuille"], rec.get("realised_ytd_eur"),
                 rec.get("realised_names") or 0,
                 None if c["realised_share_of_result_pct"] is None
                 else round(c["realised_share_of_result_pct"], 1))
    return {
        "available": True,
        "portefeuille": link["portefeuille"],
        "note": None,
        # The book's own opening capital — the ONE denominator every figure below sits on.
        "basis_eur": c["basis_eur"],
        # ⚠ False on a book with deposits or withdrawals: `result ÷ opening capital` is not a
        # return there, so the percentages are withheld and only the euro amounts stand.
        "comparable": c["comparable"],
        "held_pct": c["held_pct"],
        "realised_pct": c["realised_pct"],
        "sold_income_pct": c["sold_income_pct"],
        "total_pct": c["total_pct"],
        "held_eur": rec.get("open_result_eur"),
        "realised_eur": rec.get("realised_ytd_eur"),
        "sold_income_eur": rec.get("sold_income_eur"),
        "book_ytd_pct": rec.get("book_return_pct"),
        "residual_eur": rec.get("residual_vs_book_eur"),
        # ⚠ None is UNKNOWN, not False — the two sides can be valued a day apart (VOLK snapshot vs
        # ATT report), and that difference is market movement, not a missing position.
        "reconciles": rec.get("reconciles"),
        "holdings_as_of": rec.get("holdings_as_of"),
        "book_as_of": rec.get("book_as_of"),
        "dates_aligned": rec.get("dates_aligned"),
        "residual_reason": rec.get("residual_reason"),
        # ⚠ WHAT THE WEIGHT-BASED VIEWS CANNOT SEE — the composition bars and Brinson are built on
        # start weights, and a sold position has none that is recoverable (see `contributions`).
        # They report this share instead of quietly omitting it.
        "realised_share_of_result_pct": c["realised_share_of_result_pct"],
        "legs": c["legs"],
        # ⚠ EVERY POSITION THE BOOK TOUCHED, held and sold, on ONE weight both kinds can carry.
        # See `airs_capital` for why that weight is average invested capital and not a 1-January
        # snapshot (AITopSelectie's equities were worth EUR 40,319 on 1 Jan against a EUR 1m book —
        # it opened the year in cash).
        **ledger,
    }


def _child_book_ledgers(holdings: list[dict]) -> dict[str, dict]:
    """`{child book: {instrument: ledger position}}` for every certificate this book looks through.

    ⚠⚠ THE LOOK-THROUGH IS THE ONLY HONEST WAY TO GET A PER-STOCK INVESTED CAPITAL, because the
    flows exist — just not in this book. Bustelberg bought ONE certificate; `StarTopSelectie OFF
    DYN` is the book that actually bought Shopify, and it has Shopify's purchases, its dates and
    its sizes. Splitting the certificate's capital by today's weights (the obvious shortcut) hands
    every leg the identical number and measures nothing per stock. This reads the real thing.

    ⚠ IT IS THE STRATEGY'S MONEY-WEIGHTED RETURN, NOT THIS BOOK'S, and the tooltip must say so.
    Bustelberg's own experience depends on when IT bought the certificate; the strategy's depends
    on when IT bought Shopify. They are different questions and only the second is answerable from
    stored flows. This is the same compromise `own_return_pct` already makes — that column is
    likewise the child book's number — so the two columns stay consistent with each other.

    ⚠ ONE LEDGER PER CHILD BOOK, not per leg: `_position_ledger` is several queries and a
    reconciliation, and a 22-leg certificate would otherwise pay for them 22 times.
    """
    from routers._airs_return_reconciliation import account_return_reconciliation  # noqa: PLC0415

    books: set[str] = set()
    for h in holdings:
        for s in h.get("sources") or []:
            if s.get("label") is not None and s.get("book"):
                books.add(str(s["book"]))
    out: dict[str, dict] = {}
    for b in sorted(books):
        try:
            rec = account_return_reconciliation(b)
            # No transactions for the child means no flows, so no Modified Dietz — the same gap
            # one level down, and it refuses the same way rather than inventing a denominator.
            if rec.get("realised_ytd_eur") is None:
                _log.warning("[analysis] look-through into %s has no invested capital: its "
                             "transactions have never been fetched", b)
                out[b] = {}
                continue
            led = _position_ledger(b, rec)
            out[b] = {p["name"]: p for p in (led.get("positions") or []) if p.get("name")}
        except Exception as e:  # noqa: BLE001 — a child book must never break the parent's modal
            _log.warning("[analysis] look-through into %s failed (%s: %s)", b, type(e).__name__, e)
            out[b] = {}
    return out


def _via_capital(h: dict, by_name: dict, child_ledgers: dict[str, dict] | None = None) -> dict:
    """The certificate's own invested-capital figures, for a leg held through exactly one.

    ⚠ A LEG INSIDE A CERTIFICATE HAS NO FLOWS OF ITS OWN — AIRS trades the wrapper. That makes its
    money-weighted return genuinely unknowable, and this does NOT invent one: it attributes the
    WRAPPER's, under keys the tooltip can label. The blank column stays blank.

    ⚠ ONLY WHEN THERE IS EXACTLY ONE ROUTE IN. A stock reached through two certificates has two
    different invested-capital experiences, and "the" wrapper figure does not exist — naming one
    of them would pick a winner at random. Same rule for a leg the book ALSO holds directly: the
    row is then part its own position and part the certificate's, so a single wrapper figure would
    describe only some of it.
    """
    names = h.get("via_holding_names") or []
    if len(names) != 1 or len(h.get("via_names") or []) != 1:
        return {}
    # Held directly as well as through the wrapper — `sources` carries one entry per route in, and
    # a `label` of None is the book's own shares.
    srcs = [s for s in (h.get("sources") or []) if s.get("label") is not None]
    if len(srcs) != 1 or len(srcs) != len(h.get("sources") or []):
        return {}
    src = srcs[0]

    # ── FIRST CHOICE: the child book's OWN position, which has this instrument's real purchases.
    child = (child_ledgers or {}).get(str(src.get("book") or "")) or {}
    pos = child.get(h.get("name") or "") or {}
    if pos.get("return_pct") is not None:
        # ⚠ THE RATE TRANSFERS, THE EUROS DO NOT. The child book put its own money in; this book
        # owns a SLICE of that position, so the capital is scaled by the slice — otherwise the
        # column reports the strategy's balance sheet inside someone else's portfolio. Scaling
        # both sides leaves the rate untouched, which is the point.
        book_val = float(src.get("book_current_value_eur") or 0)
        share = (float(src.get("value_eur") or 0) / book_val) if book_val > 0 else None
        cap = pos.get("avg_capital_eur")
        return {
            "via_holding_name": names[0],
            "capital_source": "lookthrough",
            "capital_book": src.get("book"),
            "money_weighted_return_pct": pos.get("return_pct"),
            "avg_capital_eur": (round(cap * share, 2)
                                if (cap is not None and share is not None) else None),
            # Unscaled, so the card can show whose position was actually measured.
            "via_avg_capital_eur": cap,
        }

    # ── FALLBACK: no flows in the child, so only the WRAPPER can be measured. One figure for the
    # whole certificate, shipped under its own key and never as this leg's return.
    led = by_name.get(names[0]) or {}
    if led.get("return_pct") is None:
        return {}
    return {
        "via_holding_name": names[0],
        "via_money_weighted_return_pct": led.get("return_pct"),
        "via_avg_capital_eur": led.get("avg_capital_eur"),
    }


def _position_ledger(portefeuille: str, rec: dict) -> dict:
    """The book's whole year, one row per instrument — the merged held+sold list.

    ⚠ THE INCOME IS JOINED PER NAME, INCLUDING NAMES NO LONGER HELD. `_direct_result` rolls the
    orphans up into one total (right for the account row, which has no row to put them on); here
    every name gets its own row, so the roll-up would lose the attribution. `direct_result` is
    read directly for that reason — same journal, same function, one level less aggregation.
    """
    from datetime import date as _date  # noqa: PLC0415

    from airs_capital import build_ledger, contribution_pct, money_weighted_return_pct  # noqa: PLC0415
    from airs_mutaties import Mutatie, direct_result  # noqa: PLC0415
    from airs_transacties import ParsedSheet, trades  # noqa: PLC0415

    from routers._airs_transacties import ytd_window  # noqa: PLC0415

    van, tot = ytd_window()
    rows = (supabase.table("airs_transactie_snapshot").select("columns,kinds,rows")
            .eq("portefeuille", portefeuille).limit(1).execute().data or [])
    if not rows:
        # Unreachable in practice — `_realised_block` already refuses on `realised_ytd_eur is None`
        # and never calls this. Kept as a floor, and deliberately NOT given its own "reason" field:
        # the reason is authored once, upstream, where the refusal actually happens.
        return {"positions": [], "capital_coverage_ratio": None, "avg_capital_eur": None}
    sheet = ParsedSheet(columns=rows[0].get("columns") or [], kinds=rows[0].get("kinds") or {},
                        rows=rows[0].get("rows") or [])

    volk = (supabase.table("airs_holding")
            .select("holding_name,quantity,start_value_eur,current_value_eur")
            .eq("portefeuille", portefeuille)
            .eq("as_of_date", _book_snapshot_date(portefeuille) or "").execute().data or [])

    muts = ref_mutaties_for(portefeuille)
    income = {f: d.net_eur for f, d in direct_result([Mutatie(
        grootboek=m["grootboek"], fonds=m["fonds"], omschrijving="",
        boekdatum=_date.fromisoformat(str(m["boekdatum"])) if m.get("boekdatum") else None,
        amount_eur=float(m["amount_eur"]))
        for m in muts]).by_fonds.items()}

    # ⚠ THE NAMES WHOSE QUANTITY ARITHMETIC CANNOT BE TRUSTED — anything carrying a transaction
    # type we do not interpret. `trades()` drops those rows (it emits only buys and sells), so the
    # ledger would never learn of them; they have to be read off the sheet directly. Measured: a
    # `D` row on KLA-Tencor added 279 shares in a 10:1 split, leaving its February purchase in
    # PRE-split units against a POST-split holding — `qty_now − bought` gave 296 where the truth is
    # 170, and the money-weighted return read +39.81% instead of +56.67%.
    unknown = {r.get("Fonds") for r in sheet.rows
               if r.get("Fonds") and r.get("Tt") not in ("A", "V")}
    unknown = {n for n in unknown if isinstance(n, str)}

    # ⚠ A DEPOSIT THAT CAN BE PROVEN A SPLIT IS RESCALED; ONE THAT CANNOT IS STILL REFUSED.
    # `detect_split` needs two things this loader has and the ledger does not: the deposited
    # quantity, and the per-share prices of the trades that happened BEFORE it. See its docstring
    # for why both columns must agree before anything is rescaled.
    from airs_capital import detect_split  # noqa: PLC0415

    by_name = {r.get("holding_name"): r for r in volk if r.get("holding_name")}
    splits: dict[str, float] = {}
    for name in unknown:
        v = by_name.get(name) or {}
        qty = float(v.get("quantity") or 0)
        start_val = float(v.get("start_value_eur") or 0)
        if qty <= 0 or start_val <= 0:
            continue
        events = [r for r in sheet.rows
                  if r.get("Fonds") == name and r.get("Tt") not in ("A", "V")]
        deposited = sum(float(r.get("Aantal") or 0) for r in events)
        first = min((r.get("Datum") for r in events if r.get("Datum")), default=None)
        prices = [abs(float(r.get("Waarde  EUR") or r.get("Waarde  EUR.1") or 0))
                  / float(r["Aantal"])
                  for r in sheet.rows
                  if r.get("Fonds") == name and r.get("Tt") in ("A", "V")
                  and float(r.get("Aantal") or 0) > 0
                  and (not first or (r.get("Datum") or "") < first)]
        ratio = detect_split(qty, deposited, start_val / qty, prices)
        if ratio:
            splits[name] = ratio
    if splits:
        _log.warning("[analysis] %s: proven split(s) rescaled — %s", portefeuille,
                     ", ".join(f"{k} {v:.4f}:1" for k, v in splits.items()))

    led = build_ledger(volk, trades(sheet), income, rec.get("book_start_eur"),
                       _date.fromisoformat(van), _date.fromisoformat(tot),
                       unknown_names=unknown, splits=splits)
    return {
        "positions": [{
            "name": p.name,
            "held": p.held,
            "closed_out": p.closed_out,
            # ⚠ BOTH BLANK WHEN THE QUANTITY ARITHMETIC IS REFUSED, not just the ratio. Leaving the
            # capital visible would print a number that is only the flows (the opening leg having
            # been skipped) — a partial figure in a column headed "Avg capital invested" is worse
            # than none, because it looks whole.
            "opening_eur": None if p.capital_unknown else p.opening_eur,
            "avg_capital_eur": None if p.capital_unknown else p.avg_capital_eur,
            "capital_unknown": p.capital_unknown,
            # ⚠ Descriptive — a share of the year's CAPITAL, not of the return. The column that
            # adds up is `contribution_pct`.
            "weight_pct": p.weight_pct,
            "held_result_eur": p.held_result_eur,
            "realised_result_eur": p.realised_result_eur,
            "income_eur": p.income_eur,
            "result_eur": p.result_eur,
            "contribution_pct": contribution_pct(p, led.basis_eur),
            "return_pct": money_weighted_return_pct(p),
            "sales": p.sales,
            "first_sale": p.first_sale,
            "last_sale": p.last_sale,
            "prior_year_eur": p.prior_year_eur,
        } for p in led.positions],
        "avg_capital_eur": led.avg_capital_eur,
        # ⚠ REPORTED, NEVER ASSUMED TO BE 1. Modified Dietz ignores the price path within a
        # position and the de-restatement is its own approximation; measured 0.998 and 1.023.
        "capital_coverage_ratio": led.capital_coverage_ratio,
        "ledger_result_eur": led.total_result_eur,
    }


def _variant_bands(name: str | None, omschrijving: str | None) -> dict:
    """The portfolio's risk profile, and the allocation policy recorded for it.

    ⚠ THE CLASSIFIER IS THE ONE THE APP ALREADY HAS — `portfolio_variant`, the same function the
    correlation matrix filters by. It reads AIRS's own NAME first and the description second, and
    its rule ORDER is load-bearing: "bep offensief" contains "offensief", so Beperkt Offensief must
    be tested first or `BUS_Bep_offensief_FX` lands in the wrong profile. Writing a second
    "look at the end of the name" matcher here would be a second answer to one question, and it
    would get that trap wrong — which is exactly the bug that module exists to document.

    ⚠ NO PROFILE IS AN ANSWER, NOT A FAILURE. 8 of the 42 models are not offered at a risk profile
    at all (the themed TopSelectie funds, Risicodragend/Risicomijdend). They get `variant: null` and
    no bands, and the chart simply draws none — inventing "Neutraal" for them would put a policy on
    a product that has none.
    """
    from ._airs_allocation_bands import load_bands  # noqa: PLC0415
    from ._airs_portfolio_variant import portfolio_variant  # noqa: PLC0415

    variant = portfolio_variant(name, omschrijving)
    if not variant:
        _log.warning("[analysis] %r is not offered at a risk profile — no allocation bands drawn",
                     name)
        return {"variant": None, "bands": []}
    bands = [b for b in load_bands()
             if b["variant"] == variant
             # A cell with nothing set is not a band. Sending it would draw a zero-width region at
             # the origin, which reads as "the policy says hold none of this".
             and any(b[f] is not None for f in ("min_pct", "default_pct", "max_pct"))]
    _log.warning("[analysis] %r -> profile %s, %d band(s) recorded", name, variant, len(bands))
    return {"variant": variant, "bands": bands}


def compute_portfolio_analysis(portfolio_id: int,
                               benchmark_label: str = SP500_LABEL,
                               weight_by: str = "model",
                               source: str = "model",
                               bucket_filter: str | None = None) -> dict:
    """The portfolio's composition beside the benchmark's, on one set of buckets.

    `source` ("model" | "book") picks where the RETURN numbers come from: the yfinance model
    reconstruction, or AIRS's own book (`cumulatief_rendement`). The benchmark is yfinance either
    way. Composition weighting is a separate axis — see `weight_by`.

    `weight_by`:
      "model" (default) — the strategy's nominal weights (`percentage`). What it is DESIGNED to
                          hold. The panel is about the strategy, so this is the default.
      "book"            — what the paired AIRS book ACTUALLY holds, weighted by EUR value. Only
                          the weights change; the classification and the benchmark are identical.
                          Falls back to "model" (with `weight_note`) when there is no book.
    """
    # ⚠ THE CLOCK STARTS ON THE FIRST LINE, NOT AT THE BOOK LOAD. It used to be initialised 100
    # lines down, so the composition read, the certificate look-through, the asset-grid join and
    # the whole BENCHMARK side — its members, its grid and its classification — were outside every
    # phase this endpoint reports. Measured on BUS_Neutraal_FX that was ~400ms of a ~3.4s load
    # attributed to nothing, which is also why the old note here concluded "the split is flat":
    # the unmeasured part cannot show up as a peak. The phases now sum to the wall clock.
    _t: dict[str, int] = {}
    _t0 = time.perf_counter()

    def _phase(name: str) -> None:
        nonlocal _t0
        now = time.perf_counter()
        _t[name] = int((now - _t0) * 1000)
        _t0 = now

    # Both reads go through `_airs_ref`, which asks the ONE canonical question per table so the
    # per-request memo can collapse what were 8 + 7 separate round trips. See its module note.
    p = ref_model(portfolio_id)
    if not p:
        return {"portfolio_id": portfolio_id, "name": None, "axes": [], "holdings": 0}

    pos = ref_positions_for(portfolio_id)
    if p.get("positions_datum"):
        pos = [r for r in pos if r.get("datum") == p["positions_datum"]]

    # ⚠ LOOK THROUGH THE CERTIFICATES FIRST, OR THIS CHARTS THE WRONG PORTFOLIO. Nine of
    # ToppenbergBeheer Defensief's twelve positions are Leonteq certificates that ARE other
    # models, carrying 44.56% of it. Unexpanded they are unpriceable CH ISINs, so the sector
    # breakdown is drawn over the remaining 55% — two bond ETFs and a cash line — and presented
    # as the portfolio's composition. Expanding replaces each with the stocks it actually holds.
    from ._airs_lookthrough import expand_positions  # noqa: PLC0415  (cycle at module level)

    pos, lookthrough = expand_positions(portfolio_id, p.get("positions_datum"), pos)

    # --- the portfolio side -------------------------------------------------------------
    from routers._airs_holding_isin import (  # noqa: PLC0415  (avoid a module-level cycle)
        _load_bucket_overrides, classify_bucket)
    codes = _country_by_code()
    held = sorted({r["isin"] for r in pos if r.get("isin")})
    overrides = _load_bucket_overrides(held)   # manual Class pins win, so the bar matches the column
    grid = _grid(held)
    port_items: list[tuple[float, tuple[str, str, str]]] = []
    # Identity per item, parallel to `port_items` — see `_axis_holdings`.
    port_labels: list[dict] = []
    alloc_items: list[tuple[float, str]] = []
    classified_w = total_w = 0.0
    port_foreign = 0
    for r in pos:
        w = float(r.get("percentage") or 0)
        if w <= 0:
            continue
        total_w += w
        isin = r.get("isin")
        row = grid.get(isin) if isin else None
        b = _buckets(row, is_cash=not isin, isin=isin, codes=codes)
        if b[0] not in (UNKNOWN_BUCKET,):
            classified_w += w
        if row and _foreign_listing(row):
            port_foreign += 1
        port_items.append((w, b))
        # Asset class from AIRS's `categorie` (what it invests in); then the shared classifier, so a
        # model position and the same holding in the book table land in the identical bucket.
        ac = "Cash" if not isin else _CATEGORIE_TO_CLASS.get((r.get("categorie") or "").strip().upper())
        is_etf = bool(row) and (row.get("asset_class") or "").lower() in _FUND_CLASSES
        bucket = overrides.get(isin or "") or classify_bucket(ac, is_etf, isin, r.get("fonds"), row)
        alloc_items.append((w, bucket))
        port_labels.append({"name": r.get("fonds"), "isin": isin, "asset_class": bucket,
                            "via_names": r.get("via_names") or []})

    # --- the benchmark side -------------------------------------------------------------
    # Deduped, one row per company (the GOOGL/GOOG double-count is 11.3% of the index), and drawn
    # from the ASSET world so it is classified and priced exactly like the portfolio is.
    bench, bench_coverage = _members(benchmark_label)
    bench_isins = sorted({m["isin"] for m in bench if m.get("isin")})
    bgrid = _grid(bench_isins)
    bench_items: list[tuple[float, tuple[str, str, str]]] = []
    # Same rows, keyed by ISIN, so the start-of-window caps can be swapped in below without
    # re-classifying anything: the bucket a constituent sits in does not depend on when it
    # was weighed.
    bench_rows: list[tuple[float, tuple[str, str, str], str]] = []
    bench_classified = bench_total = 0.0
    bench_foreign = 0
    for m in bench:
        cap = float(m.get("market_cap_eur") or 0)
        if cap <= 0:
            continue
        bench_total += cap
        row = bgrid.get(m.get("isin") or "")
        b = _buckets(row, is_cash=False, isin=m.get("isin"), codes=codes)
        if b[0] != UNKNOWN_BUCKET:
            bench_classified += cap
        if row and _foreign_listing(row):
            bench_foreign += 1
        bench_items.append((cap, b))
        bench_rows.append((cap, b, m.get("isin") or ""))

    # ── Book weighting override ─────────────────────────────────────────────────────────────
    # The model side is always built (it is the fallback). When the reader asks for book weights
    # and a priced book exists, swap the portfolio items for the book's — nothing else moves, so
    # the benchmark and the classification stay exactly as they were.
    # The paired book drives book-weighting (if asked) AND the per-bucket returns for the pie —
    # a return is a property of the held instruments, not the weighting basis. Loaded once.
    # ⚠ PHASE TIMING, RETURNED TO THE CLIENT. This endpoint is seconds long and the browser could
    # only see the total — "Loading composition…" for 5s with nothing saying which of its eight
    # loads was responsible. The AIRS expand has carried per-phase timings for exactly this reason;
    # this had none. The split is fairly flat, which is itself the finding — there is no one hot
    # query to fix, which is what sent the 2026-08-11 profile after the round-trip COUNT instead
    # (212 of them, 103 byte-identical repeats — see `compute_portfolio_analysis_async`).
    _phase("composition_and_benchmark")
    book = _book_port_items(portfolio_id, codes)
    _phase("book_holdings")
    # ⚠ SAY WHY THERE IS NO BOOK, HERE, EVERY TIME — not only when book WEIGHTS were asked for.
    # The holdings table is empty whenever `book` is None regardless of `weight_by`, and it was
    # the one surface with no explanation attached. WARNING level because uvicorn leaves the root
    # logger there, so an `info` line never reaches production — and production is where this was
    # reported ("No positions to show" beside a portfolios list that plainly has rows).
    # ⚠ COMPUTED BEFORE THE PAYLOAD, because `book_holdings` now needs it: the result columns are
    # grafted onto the holdings rows so the Holdings table is ONE table that adds up.
    realised_block = _realised_block(portfolio_id)
    book_note = None if book else book_unavailable_reason(portfolio_id)
    if book_note:
        _log.warning("[analysis] portfolio %s has no book view: %s", portfolio_id, book_note)
    else:
        _log.warning("[analysis] portfolio %s: book view has %d holding(s) from %s",
                     portfolio_id, len(book.get("holdings_detail") or []),
                     book.get("portefeuille"))
    weight_basis, weight_note = "model", None
    port_holdings = len([r for r in pos if r.get("isin")])
    if weight_by == "book":
        if book:
            port_items = book["items"]
            port_labels = book["labels"]
            alloc_items = book["alloc_items"]
            classified_w, total_w = book["classified_w"], book["total_w"]
            port_foreign, port_holdings = book["foreign"], book["holdings"]
            weight_basis = "book"
        else:
            weight_note = "No priced book to weight by — showing the model's own weights."

    # The charts show the (optionally) filtered asset-class sleeve — click a bar of the allocation
    # bar to sub-select. `alloc_items` is parallel to `port_items` (same loop), so zip to filter;
    # the allocation bar itself stays FULL (below), so the reader can re-select.
    #
    # ⚠ REGION and CURRENCY describe EVERY holding; SECTOR describes only EQUITY. A bond or a fund
    # has no equity sector — it would pile into "Unclassified" and drown the real sectors — so the
    # sector axis is computed over the equity sleeve alone. Each is then intersected with whatever
    # class the allocation bar has selected (so selecting Bonds empties the sector chart, as it
    # should — sector is not relevant there).
    _EQUITY = {"Equity", "Equity ETF"}
    general_items = port_items
    general_labels = port_labels
    if bucket_filter:
        keep = [(pi, lb) for pi, ai, lb in zip(port_items, alloc_items, port_labels)
                if ai[1] == bucket_filter]
        general_items = [pi for pi, _lb in keep]
        general_labels = [lb for _pi, lb in keep]
    # ⚠ THE SECTOR DENOMINATOR IS THE EQUITY SLEEVE, AND SELECTING A CLASS MUST NOT MOVE IT.
    # This used to intersect with `bucket_filter` as well, so picking "Stocks" dropped Equity ETFs
    # out of the denominator and every sector percentage rose: Technology 34.41% -> 35.88% on
    # ToppenbergBeheer Defensief, +1.07 to +1.47pp across the three Toppenberg books.
    #
    # Arithmetically that was correct — a different question, honestly answered. As an interface
    # it is not: the bar you clicked reported one number and reported another once clicked, so the
    # act of inspecting a figure changed it. A reader cannot tell that from a bug, and the first
    # thing they lose is trust in the chart they were about to rely on.
    #
    # Sector is an EQUITY view either way (a bond has no sector), so the equity sleeve is the only
    # denominator that answers one question consistently. Selecting a class now re-colours the
    # chart without moving it.
    # ⚠ BUT A NON-EQUITY SELECTION STILL EMPTIES IT. Picking Bonds or Cash must not leave the
    # equity sector chart standing — sector is not a question about a bond, and showing the
    # stocks' sectors under a "Bonds" selection would attribute them to the wrong sleeve. So the
    # filter still decides WHETHER the chart is drawn; it just no longer decides its denominator.
    sector_keep = ([] if (bucket_filter and bucket_filter not in _EQUITY)
                   else [(pi, lb) for pi, ai, lb in zip(port_items, alloc_items, port_labels)
                         if ai[1] in _EQUITY])
    sector_items = [pi for pi, _lb in sector_keep]
    sector_labels = [lb for _pi, lb in sector_keep]
    pw_general, pw_sector = _weigh(general_items), _weigh(sector_items)
    # ⚠⚠ THE INDEX IS WEIGHED ON THE SAME BASIS AS THE PORTFOLIO IT IS DRAWN AGAINST, AND IT WAS
    # NOT (fixed 2026-08-10). `bench_items` above carries `market_cap_eur` — the cap TODAY — while
    # the portfolio bars moved to the attribution basis (Beginwaarde, the window's open) in
    # 2026-07-31. So every bar pair compared a start-weighted book against a today-weighted index,
    # and `diff_pct` — the TILT, the entire reason the two are side by side — was a subtraction
    # across two bases.
    #
    # Measured on SP500 Technology: **34.90% on today's caps, 31.24% at the window's open.** The
    # chart printed 35% under an axis note reading "Start-of-window weights", and the drill-down
    # (which has always used `index_rows(label, start)`) printed 31.24% — a 3.66pp gap that read as
    # a broken panel and was really two honest numbers under one label.
    #
    # ⚠ ONLY WHEN THERE IS A WINDOW TO OPEN. Without a priced book there is no Beginwaarde and the
    # portfolio bars fall back to current value — so the index falls back with it, and the axis
    # note already says the basis is not the attribution one. Two fallbacks, one basis, either way.
    # The rows behind the bars, on each axis's OWN denominator — see `_axis_holdings`.
    dd_general = _axis_holdings(general_items, general_labels)
    dd_sector = _axis_holdings(sector_items, sector_labels)

    # ⚠ NAMED, NOT INFERRED. The sector axis divides by the equity sleeve and the other two by
    # every long position, so "our weight" means a different denominator per chart. The drill-down
    # prints this sentence rather than leaving a reader to reverse-engineer which total a bar is a
    # share of — the exact question that made the composition's 36% look inconsistent with the
    # attribution table's 39.1% for the same sector.
    _basis_note = {
        "sector": "the equity sleeve (shares and equity ETFs)",
        "region": "every long position",
        "currency": "every long position",
    }
    weight_field = ("each position's current EUR value" if weight_basis == "book"
                    else "the model's stated percentage")

    # ⚠ THE BARS ARE WEIGHED ON THE ATTRIBUTION BASIS (2026-07-31) — see `_basis_axes`. The
    # current-value path above still runs: it feeds the allocation pie and the holdings table,
    # which are point-in-time views of what is held and must NOT move to a January basis. Only
    # these three axes changed, and only so a sector bar equals its own Brinson row.
    basis_axes = _basis_axes(portfolio_id, source, p.get("positions_datum"), bucket_filter)
    # ⚠⚠ A CONSTITUENT WITH NO START CAP IS DROPPED, NOT FALLEN BACK TO TODAY'S. The first version
    # of this used `bench_start.get(isin, cap)`, which quietly re-created the very thing it was
    # fixing: a handful of names weighed today inside a bar weighed at the open, and a denominator
    # that then matched neither basis. Measured on SP500 Technology it read 31.92% against the
    # drill-down's 31.24% — a 0.68pp gap from about two constituents. Dropping them puts the bar on
    # exactly the constituent set `index_rows` weighs, so the two agree by construction rather than
    # to within a rounding.
    bench_start = _bench_start_caps(benchmark_label, (basis_axes or {}).get("_start"))
    bw = _weigh([(bench_start[isin], b) for _cap, b, isin in bench_rows if isin in bench_start]
                if bench_start else bench_items)
    _phase("axes")

    # ⚠⚠ THE CLASS RETURN IS RE-DERIVED FROM THE ENRICHED ROWS, AND `_book_port_items`' OWN
    # `bucket_returns` IS DELIBERATELY DISCARDED. That one is `(Σ current + income) ÷ Σ start`,
    # which OMITS whatever the class banked by selling — the identical defect the Holdings table's
    # class Return had. Measured on AITopSelectie: it reported Stocks at +43.53% against a class
    # that made +44.16%, the EUR 6,307 realised on trims missing from the rate while sitting in the
    # Result column two cells away. Two class returns a point apart, on one screen, is the pair a
    # reader cannot arbitrate.
    #
    # Recomputed here — after `_with_results` — from EXACTLY the rows the table renders, so the
    # allocation legend and the class subtotal cannot be two different numbers. One formula, one
    # place: Σ result ÷ Σ opening value, over the rows that HAVE an opening value.
    #
    # ⚠ A CLOSED-OUT POSITION HAS NO CLASS, so its realised result is not in any bucket here. That
    # is correct rather than missing: it has no sector, no ISIN and no current weight either, which
    # is why the table gives it its own group outside the classes. The figure that accounts for it
    # is `Contribution`, on the book's own capital.
    enriched_holdings = _with_results(
        _with_start_weights(book["holdings_detail"] if book else [],
                            (basis_axes or {}).get("_start_weights") or {}),
        realised_block)
    _agg: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0])   # [Σ start, Σ result]
    for _h in enriched_holdings:
        _start = _h.get("start_value_eur") or 0.0
        if _start > 0:
            _agg[_h["bucket"]][0] += _start
            _agg[_h["bucket"]][1] += _h.get("result_eur") or 0.0
    bucket_returns = {b: v[1] / v[0] * 100.0 for b, v in _agg.items() if v[0]}
    # ⚠ THE CLASS'S SHARE OF THE BOOK'S YEAR, in POINTS — a different question from the return
    # beside it, on the book's own opening capital rather than the class's. These ADD; the returns
    # do not, because each of those sits on its own denominator.
    #
    # ⚠ THEY DO NOT ADD TO THE WHOLE BOOK, AND THE CALLER MUST SAY SO. A position sold out during
    # the year has no asset class — no sector, no ISIN, no current weight — so no bar can carry it.
    # Measured on BUS_Offensief_Dyn: the classes come to +8.211pp against a book that made +5.827%,
    # the missing -2.384pp being eight names it no longer holds. `realised.positions` carries them
    # and the UI prints the remainder beneath the bars, because a set of parts that silently misses
    # the total is the exact failure this modal keeps removing.
    _contrib: dict[str, float] = defaultdict(float)
    for _h in enriched_holdings:
        if _h.get("contribution_pct") is not None:
            _contrib[_h["bucket"]] += _h["contribution_pct"]
    # ⚠ CASH IS 0%, NOT UNDEFINED — it has no `Beginwaarde` to divide by, so the rule above leaves
    # it out and the bar reads a dash. That dash says "unknown" about the one asset whose return is
    # certain. Set explicitly, so the allocation legend and the class row agree here too rather
    # than one of them going quiet.
    if any(h["bucket"] == CASH_BUCKET for h in enriched_holdings):
        bucket_returns.setdefault(CASH_BUCKET, 0.0)

    axes = []
    for axis in ("sector", "region", "currency"):
        ba = (basis_axes or {}).get(axis)
        if ba and ba["weights"]:
            pw_axis, dd_axis = ba["weights"], ba["holdings"]
            note = (f"Share of the attributable holdings on the {axis} axis, by each position's "
                    f"value on {basis_axes['_start']} (Beginwaarde) — the SAME weights the "
                    f"Attribution table uses.")
            positions, excluded = ba["positions"], ba["excluded"]
            attributable_pct, unpriced_pct = ba["attributable_pct"], ba["unpriced_pct"]
        else:
            # ⚠ FALLBACK, AND IT IS A DIFFERENT QUESTION — SO IT SAYS SO. No paired book (or
            # nothing attributable) means there is no Beginwaarde to weigh by. Drawing an empty
            # chart would claim the portfolio holds nothing; drawing the current-value bars
            # without a word would put a differently-based number under the same heading.
            pw = pw_sector if axis == "sector" else pw_general
            dd = dd_sector if axis == "sector" else dd_general
            pw_axis, dd_axis = pw[axis], dd[axis]
            note = (f"Share of {_basis_note[axis]}, by {weight_field}. "
                    f"⚠ Not the Attribution basis — no start-of-window values are available here.")
            positions = len(sector_items if axis == "sector" else general_items)
            excluded, attributable_pct, unpriced_pct = [], None, None
        keys = set(pw_axis) | set(bw[axis])
        rows = [{
            "bucket": k,
            "portfolio_pct": pw_axis.get(k, 0.0),
            "benchmark_pct": bw[axis].get(k, 0.0),
            # The tilt. It is the whole point of putting the two side by side.
            "diff_pct": pw_axis.get(k, 0.0) - bw[axis].get(k, 0.0),
            # ⚠ THE ROWS SUM TO `portfolio_pct` EXACTLY — same division, done once. A bucket the
            # portfolio does not hold (an unowned sector the benchmark has) is an empty list, which
            # is a finding rather than missing data.
            "holdings": dd_axis.get(k, []),
        } for k in keys]
        rows.sort(key=lambda r: -max(r["portfolio_pct"], r["benchmark_pct"]))
        axes.append({
            "axis": axis,
            "rows": rows,
            "basis": note,
            "positions": positions,
            # ⚠ WHAT THE BASIS LEAVES OUT, PER AXIS. Named, weighted and reasoned, because this
            # basis drops a mid-window purchase and an unpriceable holding entirely — neither can
            # be expressed on it, and a percentage that quietly loses weight is the one failure
            # this whole module is written to avoid.
            #
            # ⚠ BUT NOT EVERY EXCLUSION IS A LOSS. A fund, a bond and a cash line have no sector
            # BY DEFINITION and are not Stocks in the first place — they are their own slices of
            # the allocation chart, and presenting them as weight the sector chart "cannot handle"
            # made an ordinary 13% in ETFs read as a defect. `unpriced_pct` is the one that is.
            "attributable_pct": attributable_pct,
            "unpriced_pct": unpriced_pct,
            "excluded": [{"name": (e.get("grid_row") or {}).get("name") or e.get("airs_name"),
                          "isin": e.get("isin"), "weight_pct": e["weight_pct"],
                          # The Class it already carries in our own system — which is what makes
                          # "this was never a stock" visible instead of implied.
                          "asset_class": e.get("asset_class"),
                          "reason": e["reason"]} for e in excluded],
        })

    return {
        "portfolio_id": portfolio_id,
        "name": p["name"],
        # ⚠ THE COMPOSITION'S EFFECTIVE DATE — NOT THE DATE ANY FIGURE IS VALUED AT. It is when
        # the MODEL declared these weights (2025-12-30 for AITopSelectie). Every book-valued
        # number on this screen is as-of `holdings_as_of` below, which for that same portfolio is
        # 2026-08-01 — 216 days apart. Using this one as a provenance timestamp made the modal
        # report the row's own +111.74% as 216 days old while the row called it 2.
        "as_of": p.get("positions_datum"),
        # The snapshot the book valuations come from — the clock for the weight columns and for
        # every `own_return_source == "airs"` row. Null in model mode, where the holdings table is
        # priced from yfinance and each row carries its own `own_return_as_of` instead.
        "holdings_as_of": (book or {}).get("book_as_of"),
        # The risk profile this model is offered at, and the allocation policy that goes with it —
        # so the chart can draw the band each class is SUPPOSED to sit in, over the bar showing
        # where it actually sits. See `_variant_bands`.
        **_variant_bands(p.get("name"), p.get("omschrijving")),
        # ⚠ WHICH BOOK IS "THIS" BOOK — needed the moment a Return could come from ANOTHER one. A
        # row valued by the account behind a certificate carries that account's name in
        # `own_return_book`, and without this the reader has nothing to compare it against, so
        # every AIRS row would have to be labelled or none could be.
        "book_portefeuille": (book or {}).get("portefeuille"),
        "benchmark": benchmark_label,
        "benchmark_members": len(bench_items),
        "holdings": port_holdings,
        # Which side the portfolio bars describe: the model's nominal weights, or the book's
        # actual EUR holdings. `weight_note` is set only when "book" was asked for and refused.
        # Milliseconds per phase, so the browser can say WHERE the wait went — see .
        "timings_ms": _t,
        "weight_basis": weight_basis,
        "weight_note": weight_note,
        # Why the holdings table is empty, when it is. Null when there IS a book view.
        "book_note": book_note,
        # Coverage, always — a composition renormalised over a fraction of the model is the same
        # invention the returns refuse to make. The reader gets to see it.
        "covered_pct": (classified_w / total_w * 100.0) if total_w > 0 else 0.0,
        "benchmark_covered_pct": ((bench_classified / bench_total * 100.0)
                                  if bench_total > 0 else 0.0),
        # ⚠ Rows priced on a venue whose currency differs from the company's own — the
        # wrong-listing bug, surfaced. 40 of the S&P's 491 sit on European/Canadian lines, which
        # is what made the naive currency split read "12% EUR". It also means the benchmark's
        # PRICE SERIES is drawn off those venues. Not hidden: shown, and counted.
        "foreign_listings": port_foreign,
        "benchmark_foreign_listings": bench_foreign,
        # ⚠ How much of the INDEX we could price. Never assumed: ACWI's missing names are a whole
        # country at a time (GuruFocus sells no UK or India; yfinance has them but some were never
        # ingested), and a cap-weighted index renormalised over the rest does not lose that
        # weight — it redistributes it into everything else. That is a bias, and the reader is
        # told rather than left to assume 100%.
        "benchmark_universe_members": bench_coverage.get("universe_members") or 0,
        "benchmark_priced": bench_coverage.get("priced") or 0,
        "benchmark_coverage_pct": bench_coverage.get("covered_pct"),
        "returns": _returns_timed(portfolio_id, p.get("positions_datum"), benchmark_label,
                                  source, _phase),
        # ⚠ THE COMPOSITION WAS EXPANDED, AND THAT MUST BE VISIBLE. These charts are drawn over
        # the stocks BEHIND the certificates, not over the twelve lines AIRS stores. Without
        # this the reader cannot tell a portfolio that genuinely holds 22 names from one that
        # holds three certificates — and cannot check the figures against the composition table,
        # which still shows the unexpanded rows.
        "looked_through_pct": lookthrough["looked_through_pct"],
        # Weight still inside a certificate we could NOT expand — its target has no stored
        # composition. Not dropped (that would delete the weight silently); reported.
        "opaque_pct": lookthrough["opaque_pct"],
        "looked_through": lookthrough["expanded"],
        "axes": axes,
        # The portfolio's own asset-class split, on the active weighting basis; each slice carries
        # the bucket's value-weighted YTD price return (from the paired book), for the pie legend.
        "allocation": [{**s, "return_pct": bucket_returns.get(s["bucket"]),
                        "contribution_pct": _contrib.get(s["bucket"])}
                       for s in _weigh_alloc(alloc_items)],
        # Per-holding book detail (bucket / currency / start-weight / return) — the source for a
        # non-equity sleeve's contribution + currency view, where sector-vs-SP500 says nothing.
        # Empty when no book is paired (a model with no book has no per-holding returns).
        #
        # ⚠ `weight_start_pct` IS GRAFTED ON FROM THE AXES' OWN LEGS, NOT RECOMPUTED. It is the
        # numerator the sector bars are built from, so the table and the chart cannot disagree
        # about what a position weighed in January — which is the entire reason the column exists.
        # Both weight columns are whole-book shares, so they sit beside each other honestly:
        # ASML 5.00% at the start against 7.02% now IS the story, and a bar is that start weight
        # divided by the axis's `attributable_pct`.
        # ⚠ THE RESULT COLUMNS ARE GRAFTED ON HERE, so the Holdings table is ONE table that adds
        # up rather than a composition view beside a separate ledger. See `_with_results`.
        "book_holdings": enriched_holdings,
        # ⚠ THE HOLDINGS TABLE IS ONLY HALF THE YEAR, AND UNTIL NOW NOTHING SAID SO. Every figure
        # above it is built from positions the book STILL HOLDS; a name sold in March has no row
        # and its result is invisible. Measured on BUS_Offensief_Dyn, that is EUR -28,656 — 41% of
        # the year's movement, and enough on its own to reverse a sector's verdict in the
        # attribution panel. This block carries it, on the book's own opening capital so held +
        # sold + income adds to the book's YTD exactly.
        "realised": realised_block,
    }


def _with_results(holdings: list[dict], realised: dict) -> list[dict]:
    """Attach each row's RESULT — unrealised + realised + income — and its contribution.

    ⚠ ONE TABLE, BECAUSE THE TWO WERE ANSWERING ONE QUESTION IN TWO PLACES. The composition view
    could not add up (a name sold in March has no row) and the ledger could not show a sector or an
    ISIN. Merged, the columns Unrealised / Realised / Income sum to Result, and Result over the
    book's opening capital sums to the book's own year.

    ⚠ THE REALISED LEG JOINS BY NAME, EXACTLY, AND ONLY LANDS ON A ROW THAT STILL EXISTS. Measured
    on BUS_Offensief_Dyn: of 13 traded names, 5 are trims of positions still held (they get their
    realised result here) and 8 are gone entirely — EVERY orphan is `closed_out`, which is the
    whole reason the join is safe. Those 8 have no holdings row by definition and are carried
    separately in `realised.positions`, for the UI to render as its own group.

    ⚠ A LOOKED-THROUGH LEG NEVER MATCHES, AND MUST NOT. AIRS trades the CERTIFICATE, not the stocks
    inside it, so an instrument reached through one has no transactions of its own — 21 of
    BUS_Offensief's 52 rows. Its unrealised result is still real (its share of the certificate's
    value change), and its realised is correctly absent rather than invented.
    """
    if not holdings:
        return holdings
    basis = realised.get("basis_eur") if realised.get("available") else None
    by_name = {p["name"]: p for p in (realised.get("positions") or []) if p.get("held")}
    # ⚠ THE MONEY-WEIGHTED LEG IS ONLY DEFINED WHERE WE KNOW THE FLOWS, and that is the direct
    # holdings. A leg reached through a certificate has no buys or sells of its own — AIRS trades
    # the WRAPPER — so there is no "money you put in" to divide by, and `None` is the honest
    # answer rather than the certificate's flows split across its contents.
    # ONE ledger per child book, built before the loop — see `_child_book_ledgers`.
    child_ledgers = _child_book_ledgers(holdings)
    out = []
    for h in holdings:
        start, cur = h.get("start_value_eur"), h.get("current_value_eur")
        # ⚠⚠ CASH RETURNS EXACTLY 0%, AND SAYING SO IS NOT THE SAME AS SAYING NOTHING. AIRS books
        # no `Beginwaarde` for the cash line, so the generic rule below leaves every cash cell a
        # dash — and a dash means "we could not work this out", which for cash is false: we know
        # precisely what it earned. It earned nothing.
        #
        # ⚠ AND ITS DRAG IS A FACT. This repo already prices cash at 0% rather than skipping it
        # everywhere else (`portfolio_math.make_cash_holding`, `explain_portfolio_ytd`), for the
        # reason recorded there: dropping it scales a 20%-cash portfolio's return up by 25%. A
        # dash invites exactly that reading — that cash is an unknown to be ignored — where a 0%
        # states the drag.
        #
        # ⚠ ITS INCOME IS STILL ITS OWN. Interest credited to the account is real money and stays
        # in the Income column; only the price leg is asserted to be zero, because a euro is
        # always worth a euro.
        is_cash = h.get("bucket") == CASH_BUCKET
        # ⚠ None, not 0, when the row cannot be valued at BOTH ends — an unpriceable position's
        # result is undefined, and a 0 would state that it went nowhere.
        #
        # ⚠⚠ FOR CASH THE ZERO IS A FALLBACK, NEVER AN OVERRIDE, and getting that backwards is a
        # real bug I shipped: forcing 0 unconditionally would have erased a cash line that AIRS
        # values at BOTH ends. A certificate's own `Liquiditeiten` leg has a real start and current
        # value and moved -EUR 61 over the year (FX on a foreign balance, or a movement inside the
        # wrapper) — that euro is part of the book's own result, so zeroing it would have silently
        # broken the reconciliation the total row asserts. The 0 applies ONLY where there was
        # nothing to compute, which is the case the dash was wrong about.
        unreal = (round(cur - start, 2) if (start and cur is not None)
                  else 0.0 if is_cash
                  else None)
        income = h.get("income_eur") or 0.0
        led = by_name.get(h.get("name") or "") or {}
        realised_eur = led.get("realised_result_eur") or 0.0
        total = (None if unreal is None and not realised_eur and not income
                 else round((unreal or 0.0) + realised_eur + income, 2))
        # ⚠ NOT `led["return_pct"]` BLINDLY — a looked-through leg never matches a ledger position
        # (see above), so `led` is empty for it and both fields stay None. Only a row the book
        # holds DIRECTLY has an average invested capital to divide by.
        avg_cap = led.get("avg_capital_eur")
        out.append({**h,
                    "unrealised_eur": unreal,
                    "realised_result_eur": realised_eur or None,
                    "result_eur": total,
                    # ⚠ WHAT THE MONEY ACTUALLY MADE, as opposed to what the instrument did. The
                    # `Return` column beside it divides by AIRS's RESTATED Beginwaarde — today's
                    # quantity at January's price — which deliberately erases your timing so the
                    # figure describes the stock. This one divides by the capital that was really
                    # tied up, flow-weighted by when it went in, and its numerator carries the
                    # dividends (net of withholding) and anything realised on a mid-year sale.
                    # Measured: KLA-Tencor is +55.62% as an instrument and +30.94% on the money,
                    # because more of it was bought later at higher prices.
                    "avg_capital_eur": avg_cap,
                    "money_weighted_return_pct": led.get("return_pct"),
                    # ⚠ WHICH OF THE TWO REASONS THE CELL IS BLANK. Both produce a `None`, and they
                    # are not the same fact: a leg inside a certificate has no flows because AIRS
                    # trades the wrapper, while a directly-held position with a `D` (Deponering)
                    # row has flows we cannot put on one basis. One tooltip for both told a reader
                    # that KLA-Tencor — held outright — was inside a certificate, which is simply
                    # untrue and sends them looking for a wrapper that does not exist.
                    "capital_unknown": bool(led.get("capital_unknown")),
                    # ── THE WRAPPER'S OWN FIGURE, for a leg that can never have one.
                    # ⚠⚠ IT IS NOT THIS LEG'S RETURN AND MUST NEVER BE PUT IN THIS LEG'S COLUMN.
                    # AIRS bought ONE certificate; splitting its capital by today's weights would
                    # hand every leg the identical number (measured: all 22 StarTopSelectie legs
                    # would read -3.86%), which looks like 22 per-stock measurements and is one
                    # measurement copied 22 times. Shopify did not return -3.86% on the money —
                    # the certificate did. So it ships under its OWN key, for the tooltip to
                    # attribute, and `money_weighted_return_pct` stays null.
                    **_via_capital(h, by_name, child_ledgers),
                    "contribution_pct": (total / basis * 100.0)
                    if (total is not None and basis) else None,
                    # ⚠ 0%, NOT a dash — same reason as the price leg above, and same direction:
                    # a FALLBACK where nothing could be computed, never an override of a figure
                    # AIRS actually produced.
                    **({"own_return_pct": 0.0}
                       if is_cash and h.get("own_return_pct") is None else {})})
    return out


async def compute_portfolio_analysis_async(portfolio_id: int,
                                           benchmark_label: str = SP500_LABEL,
                                           weight_by: str = "model",
                                           source: str = "model",
                                           bucket_filter: str | None = None) -> dict:
    """The Analyse modal's one request.

    ⚠⚠ THE READ MEMO IS OPENED **HERE**, AT THE REQUEST BOUNDARY, NOT INSIDE THE COMPUTATION.
    Measured on BUS_Neutraal_FX, one press issued **212 database round trips of which 103 were
    byte-identical repeats** — `airs_performance` nine times, `airs_model_portfolio` five,
    `asset_grid` three, the SP500 universe id six, and the benchmark's whole price panel THREE
    times through COPY. No module is at fault: this endpoint is a dozen collaborating loaders
    (look-through, book ledger, benchmark, attribution basis, axes) each correctly fetching what
    it needs, and the duplication only exists in their composition.

    A request is exactly the scope over which "the database did not change under us" is a safe
    assumption, so that is the scope of the memo — not a TTL, not a process-wide cache. See
    `common/read_cache.py`.

    ⚠ THE CONTEXT REACHES THE WORKER THREAD BECAUSE `to_thread` COPIES IT. That is the whole
    reason this can be a ContextVar rather than something threaded through fifteen signatures;
    it is also why the memo must be opened OUTSIDE the `to_thread` call rather than within the
    sync function.

    ⚠ AND THE SYNC FUNCTION KEEPS ITS OWN BEHAVIOUR UNCHANGED. `compute_portfolio_analysis` is
    still callable from a script or a test with no memo at all, which is what an offline caller
    should get: no shared state, no question about how old an answer is.

    ⚠ AND A SECOND, WIDER CACHE SITS IN FRONT OF THE MEMO — `_analysis_cache`. The memo above
    removes REPEATS WITHIN one request; it can do nothing about the same request arriving twice,
    which is the common case here (toggle the benchmark and back, switch `weight_by`, reopen the
    same row) and costs the full ~7s each time. That outer cache is keyed on a FINGERPRINT OF THE
    DATA, not a clock, so it cannot serve a stale figure — see its module note for why a TTL was
    the wrong instrument on a page whose discipline is "current or absent".
    """
    from common.read_cache import read_cache  # noqa: PLC0415

    from routers import _analysis_cache as ac  # noqa: PLC0415

    key = (portfolio_id, benchmark_label, weight_by, source, bucket_filter)
    # The fingerprint is a database read, so it goes to a thread like everything else here.
    fp = await asyncio.to_thread(ac.fingerprint)
    hit = ac.get(key, fp)
    if hit is not None:
        return hit

    with read_cache(f"analysis:{portfolio_id}"):
        out = await asyncio.to_thread(compute_portfolio_analysis, portfolio_id, benchmark_label,
                                      weight_by, source, bucket_filter)
    # ⚠ Stored against the fingerprint read BEFORE the computation, deliberately. If a write lands
    # mid-computation the payload is a mix of both states — filing it under the OLD fingerprint
    # means the next request (which sees the new one) misses and recomputes. Filing it under a
    # fingerprint taken afterwards would publish that mixed payload as if it were the new state.
    ac.put(key, fp, out)
    return out


def portfolio_basket_request(portfolio_id: int):
    """A model portfolio's holdings as a `BasketRequest` — the bridge that lets the whole portfolio
    reuse the same basket engines (performance, owner earnings, price series) an instrument / group
    does. 404 when the portfolio has no priceable positions."""
    from fastapi import HTTPException  # noqa: PLC0415

    from routers._asset_financials import BasketHolding, BasketRequest  # noqa: PLC0415

    p = ref_model(portfolio_id)
    if not p:
        raise HTTPException(404, f"No model portfolio {portfolio_id}.")
    pos = ref_positions_for(portfolio_id)
    if p.get("positions_datum"):
        pos = [r for r in pos if r.get("datum") == p["positions_datum"]]
    holdings = [BasketHolding(isin=r["isin"], weight=float(r.get("percentage") or 0))
                for r in pos if r.get("isin") and float(r.get("percentage") or 0) > 0]
    if not holdings:
        raise HTTPException(404, "No priceable holdings in this portfolio.")
    return BasketRequest(holdings=holdings, label=p.get("name"))


def compute_portfolio_risk_windows(portfolio_id: int):
    """The whole portfolio's returns+risk windows (Analyse → Risk section). Its holdings, priced
    as ONE value-weighted EUR basket — the same 2/4/8-year table a single instrument or a sleeve
    gets. Daily-yfinance only, so this exists only in the yfinance world (AIRS keeps no daily
    history), which is why the Analyse modal gates it behind the yfinance source."""
    from routers._asset_financials import _basket_performance  # noqa: PLC0415

    return _basket_performance(portfolio_basket_request(portfolio_id))


async def compute_portfolio_risk_windows_async(portfolio_id: int):
    return await asyncio.to_thread(compute_portfolio_risk_windows, portfolio_id)


def _returns_timed(portfolio_id, effective, benchmark_label, source, phase):
    """`_returns`, with its cost reported. It is the single most expensive phase of the modal
    (the benchmark index plus this portfolio's own performance), and it was invisible."""
    out = _returns(portfolio_id, effective, benchmark_label, source)
    phase("returns_and_benchmark")
    return out


def _basket_returns(holdings, benchmark_label: str) -> dict:
    """YTD EUR return of an arbitrary basket vs the benchmark — the Analyse Return tile for a stock
    or a group. A basket has no AIRS book and no inception, so 'since inception' is null and the
    figure is always yfinance (`asset_price`), priced the same way the benchmark is."""
    from datetime import date  # noqa: PLC0415

    from routers._asset_financials import BasketRequest, _basket_index_series  # noqa: PLC0415

    jan1 = f"{date.today().year}-01-01"
    dates, values, _cov = _basket_index_series(BasketRequest(holdings=list(holdings)), 2)
    p_ytd = p_asof = None
    if len(dates) >= 2:
        base_i = None
        for i, d in enumerate(dates):
            if d <= jan1:
                base_i = i
        if base_i is not None and float(values[base_i]) > 0:
            p_ytd = (float(values[-1]) / float(values[base_i]) - 1.0) * 100.0
            p_asof = dates[-1]
    bench = index_returns(benchmark_label, [jan1]) if p_ytd is not None else {}
    b_ytd = (bench.get(jan1) or {}).get("eur_pct")
    return {
        "source": "model", "ytd_from": jan1, "since_from": None,
        "portfolio_ytd_pct": p_ytd, "portfolio_as_of": p_asof, "benchmark_as_of": p_asof,
        "strategy_ytd_pct": p_ytd, "benchmark_ytd_pct": b_ytd,
        "ytd_excess_pct": (p_ytd - b_ytd) if (p_ytd is not None and b_ytd is not None) else None,
        "portfolio_since_pct": None, "benchmark_since_pct": None, "since_excess_pct": None,
        "ytd_is_since": False, "book_available": False,
    }


def _classify_items(holdings, codes):
    """(port_items, labels, classified_w, total_w, foreign, holding_count) for a basket — the
    portfolio side of the composition, built exactly like `compute_portfolio_analysis` does for a
    model. `labels` is parallel to `items` and carries identity only (see `_axis_holdings`)."""
    held = sorted({h.isin for h in holdings if h.isin})
    grid = _grid(held)
    items: list[tuple[float, tuple[str, str, str]]] = []
    labels: list[dict] = []
    classified = total = 0.0
    foreign = count = 0
    for h in holdings:
        w = float(h.weight or 0)
        if w <= 0:
            continue
        total += w
        row = grid.get(h.isin) if h.isin else None
        b = _buckets(row, is_cash=not h.isin, isin=h.isin, codes=codes)
        if b[0] != UNKNOWN_BUCKET:
            classified += w
        if row and _foreign_listing(row):
            foreign += 1
        if h.isin:
            count += 1
        items.append((w, b))
        # A basket carries no asset-class sleeve — every leg is in every axis's denominator, which
        # is why the basket path has no sector/general split below.
        labels.append({"name": (row or {}).get("name") or getattr(h, "name", None) or h.isin,
                       "isin": h.isin, "asset_class": None, "via_names": []})
    return items, labels, classified, total, foreign, count


def compute_basket_analysis(holdings, benchmark_label: str = SP500_LABEL, name: str | None = None) -> dict:
    """Composition + return of an ARBITRARY basket (a single stock, a group, an ad-hoc set) beside
    the benchmark — the same shape `compute_portfolio_analysis` returns, so ONE Analyse view serves
    a stock (a basket of one) and a portfolio alike. yfinance only: a basket has no AIRS book."""
    codes = _country_by_code()
    (port_items, port_labels, classified_w, total_w,
     port_foreign, port_holdings) = _classify_items(holdings, codes)

    bench, bench_coverage = _members(benchmark_label)
    bench_isins = sorted({m["isin"] for m in bench if m.get("isin")})
    bgrid = _grid(bench_isins)
    bench_items: list[tuple[float, tuple[str, str, str]]] = []
    bench_classified = bench_total = 0.0
    bench_foreign = 0
    for m in bench:
        cap = float(m.get("market_cap_eur") or 0)
        if cap <= 0:
            continue
        bench_total += cap
        row = bgrid.get(m.get("isin") or "")
        b = _buckets(row, is_cash=False, isin=m.get("isin"), codes=codes)
        if b[0] != UNKNOWN_BUCKET:
            bench_classified += cap
        if row and _foreign_listing(row):
            bench_foreign += 1
        bench_items.append((cap, b))

    pw, bw = _weigh(port_items), _weigh(bench_items)
    dd = _axis_holdings(port_items, port_labels)
    axes = []
    for axis in ("sector", "region", "currency"):
        keys = set(pw[axis]) | set(bw[axis])
        rows = [{"bucket": k, "portfolio_pct": pw[axis].get(k, 0.0),
                 "benchmark_pct": bw[axis].get(k, 0.0),
                 "diff_pct": pw[axis].get(k, 0.0) - bw[axis].get(k, 0.0),
                 "holdings": dd[axis].get(k, [])} for k in keys]
        rows.sort(key=lambda r: -max(r["portfolio_pct"], r["benchmark_pct"]))
        # ⚠ ONE DENOMINATOR ON ALL THREE AXES HERE, unlike a model portfolio: a basket has no
        # asset-class sleeve to divide by, so every leg is in every axis's total.
        axes.append({"axis": axis, "rows": rows,
                     "basis": "Share of the whole basket, by each holding's stated weight.",
                     "positions": len(port_items)})

    return {
        "portfolio_id": None, "name": name, "as_of": None,
        "benchmark": benchmark_label, "benchmark_members": len(bench_items),
        "holdings": port_holdings, "weight_basis": "model", "weight_note": None,
        "covered_pct": (classified_w / total_w * 100.0) if total_w > 0 else 0.0,
        "benchmark_covered_pct": (bench_classified / bench_total * 100.0) if bench_total > 0 else 0.0,
        "foreign_listings": port_foreign, "benchmark_foreign_listings": bench_foreign,
        "benchmark_universe_members": bench_coverage.get("universe_members") or 0,
        "benchmark_priced": bench_coverage.get("priced") or 0,
        "benchmark_coverage_pct": bench_coverage.get("covered_pct"),
        "returns": _basket_returns(holdings, benchmark_label),
        "axes": axes,
        # A basket has no AIRS book, so no per-holding book returns — the non-equity sleeve view is
        # a portfolio-only feature.
        "book_holdings": [],
        # ⚠ AND IT HAS TO SAY SO. This is the OTHER route to an empty holdings table, and from the
        # reader's side the two are identical: the same blank panel, on a portfolio whose rows they
        # can see on the list behind it. A basket is opened when the book is NOT paired with a
        # model portfolio (`fixed_portfolio_id` is null), which is a fact about the pairing, not
        # about the holdings.
        "book_note": ("Opened as a basket of ISINs, because this book is not paired with a model "
                      "portfolio. The charts above are computed from its holdings, but the valued "
                      "per-holding table is a paired-portfolio feature — pair it from the Link "
                      "column to get it."),
    }


async def compute_basket_analysis_async(holdings, benchmark_label: str = SP500_LABEL,
                                        name: str | None = None) -> dict:
    """The same modal over an ad-hoc basket — same memo, same reason (see the portfolio twin).
    It loads the identical benchmark side, which is where the repeated COPY of the whole price
    panel lives."""
    from common.read_cache import read_cache  # noqa: PLC0415

    with read_cache(f"basket:{name or len(holdings or [])}"):
        return await asyncio.to_thread(compute_basket_analysis, holdings, benchmark_label, name)
