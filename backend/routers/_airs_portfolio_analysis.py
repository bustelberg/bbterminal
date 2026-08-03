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

from asset_pipeline.geo import msci_region_of
from deps import IN_CHUNK_SIZE, supabase
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


def _grid(isins: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for i in range(0, len(isins), IN_CHUNK_SIZE):
        rows = (supabase.table("asset_grid")
                .select("isin,name,sector,country,msci_region,domicile_country,currency,"
                        "market_cap_currency,asset_class,status")
                .in_("isin", isins[i:i + IN_CHUNK_SIZE]).execute().data or [])
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

    Known limit, and it is the ISIN-country limit generally: an ADR carries a US ISIN even for a
    foreign issuer, so a domicile-less ADR would read North America. It only bites when Yahoo
    gave us no domicile at all — and for an ADR it usually does.
    """
    dom = row.get("domicile_country")
    if dom:
        return msci_region_of(dom) or UNKNOWN_BUCKET
    if isin and len(isin) >= 2:
        name = codes.get(isin[:2].upper())
        if name:
            return msci_region_of(name) or UNKNOWN_BUCKET
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
    _bh = (supabase.table("airs_holding").select("as_of_date")
           .eq("portefeuille", link["portefeuille"]).order("as_of_date", desc=True)
           .limit(1).execute().data or [])
    book_as_of = str(_bh[0]["as_of_date"]) if _bh else None
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

    out: list[dict] = []
    for r in rows:
        target = r.get("linked_portfolio_id")
        if not target:
            out.append({**r, "via_names": []})   # held directly — no strategy in between
            continue
        child = _positions_of(target, _datum_of(target))
        inner = sum(float(c.get("percentage") or 0) for c in child)
        if not child or inner <= 0:
            out.append({**r, "via_names": []})
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

    need = sorted({r["isin"] for r in rows if r.get("isin") and not r.get("bucket")})
    if not need:
        return rows
    grid = _grid(need)
    for r in rows:
        if r.get("bucket") or not r.get("isin"):
            continue
        g = grid.get(r["isin"])
        r["bucket"] = classify_bucket(None, _is_fund(g), r["isin"], r.get("holding_name") or "", g)
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
    rows = (resolve_account_isins(link["portefeuille"]).get("rows") or [])
    if not rows:
        return None

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

    _income, _sold = _direct_result(
        link["portefeuille"], {r.get("holding_name") for r in rows if r.get("holding_name")})

    def _net_income(r: dict) -> float:
        d = _income.get(r.get("holding_name"))
        return ((d.gross_eur or 0.0) + (d.tax_eur or 0.0)) if d else 0.0

    bucket_agg: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0])  # [Σ start, Σ now+income]
    priced = [(r, float(r.get("start_value_eur") or 0), float(r["current_value_eur"]))
              for r in rows
              if float(r.get("start_value_eur") or 0) != 0 and r.get("current_value_eur") is not None]
    total_start = sum(s for _r, s, _n in priced) or 1.0
    for _r, start, now in priced:
        b = _r.get("bucket") or UNKNOWN_BUCKET
        bucket_agg[b][0] += start
        bucket_agg[b][1] += now + _net_income(_r)
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
    _airs_n = _look_n = 0

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
    _bh = (supabase.table("airs_holding").select("as_of_date")
           .eq("portefeuille", link["portefeuille"]).order("as_of_date", desc=True)
           .limit(1).execute().data or [])
    book_as_of = str(_bh[0]["as_of_date"]) if _bh else None

    holdings_detail: list[dict] = []
    for r, w, b_alloc in raw_positions:
        isin = r.get("isin")
        grow = grid.get(isin) if isin else None
        # The holding's own quote currency — NOT folded to Unclassified the way the fund axes are.
        # For a bond/ETF class the quote currency is a fair first-order FX signal (a EUR-quoted line
        # vs a USD one), which is exactly what the currency chart is for.
        cur = (grow.get("market_cap_currency") or grow.get("currency")) if grow else None
        pr = priced_by_id.get(id(r))
        mk = marks.get(isin) if isin else None
        via = r.get("via_names") or []

        # Directly held AND valued by AIRS on both ends -> AIRS's own total return, which is the
        # identical number the expanded row's `Return` column shows.
        d = _income.get(r.get("holding_name"))
        net_income = _net_income(r)
        if not via and pr and pr[0]:
            own = ((pr[1] + net_income) / pr[0] - 1.0) * 100.0
            own_src, own_est = "airs", False
            own_as_of = book_as_of          # the snapshot the valuation came from
            _airs_n += 1
        else:
            # Look-through, or a row AIRS cannot value on both ends (bought mid-window, no
            # Beginwaarde). The instrument's own EUR series is the only honest answer left.
            own = mk.get("return_pct") if mk else None
            own_src, own_est = ("yfinance", bool(mk.get("start_interpolated"))) if mk else (None, False)
            # This listing's OWN latest close — not the book's snapshot and not the fleet's. A
            # thinly-traded line can sit weeks behind both, and that is the row worth doubting.
            own_as_of = (mk.get("end_date") or mk.get("last_close")) if mk else None
            _look_n += 1

        holdings_detail.append({
            "name": r.get("holding_name"),
            "isin": isin,
            "bucket": b_alloc,
            "currency": cur,
            "via_names": via,
            "weight_pct": (pr[0] / total_start * 100.0) if pr else None,
            "weight_now_pct": w / total_w * 100.0 if total_w else 0.0,
            "return_pct": ((pr[1] / pr[0] - 1.0) * 100.0) if pr else None,
            "own_return_pct": own,
            "own_return_from": anchor,
            # WHICH of the two answers this row got. Two rows in one column measured different
            # ways, with nothing saying which is which, is the thing this whole change is undoing.
            "own_return_source": own_src,
            # ⚠ PER ROW, because the two bases have different clocks — see `book_as_of` above.
            "own_return_as_of": own_as_of,
            "own_income_eur": (net_income if d and not via else None),
            # A sparse yfinance series gets an interpolated opening mark, and it has to say so.
            "own_return_estimated": own_est,
        })
    # WARNING, not info: uvicorn leaves the root logger at WARNING, so an `info` line is invisible
    # in production — and this is the line that says which of the two return bases each row got.
    _log.warning(
        "[analysis] %s: per-holding returns — %d from AIRS (Beginwaarde -> Huidige waarde + net "
        "income, identical to the expanded row's Return column), %d from the yfinance series "
        "(look-through rows, or no opening value in the book)",
        link["portefeuille"], _airs_n, _look_n)

    bucket_returns = {b: (v[1] / v[0] - 1) * 100.0 for b, v in bucket_agg.items() if v[0]}
    return {"items": items, "labels": labels,
            "alloc_items": alloc_items, "bucket_returns": bucket_returns,
            "holdings_detail": holdings_detail,
            "classified_w": classified_w, "total_w": total_w, "foreign": foreign,
            # The snapshot every figure above is valued at — carried out so the payload can stamp
            # the weight columns with it instead of with the composition's effective date.
            "book_as_of": book_as_of,
            "holdings": holdings, "portefeuille": link["portefeuille"]}


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
    p = (supabase.table("airs_model_portfolio")
         .select("id,name,positions_datum").eq("id", portfolio_id).limit(1).execute().data or [])
    if not p:
        return {"portfolio_id": portfolio_id, "name": None, "axes": [], "holdings": 0}
    p = p[0]

    pos = (supabase.table("airs_model_portfolio_position")
           .select("isin,fonds,percentage,datum,categorie")
           .eq("portfolio_id", portfolio_id).execute().data or [])
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

    # ── Book weighting override ─────────────────────────────────────────────────────────────
    # The model side is always built (it is the fallback). When the reader asks for book weights
    # and a priced book exists, swap the portfolio items for the book's — nothing else moves, so
    # the benchmark and the classification stay exactly as they were.
    # The paired book drives book-weighting (if asked) AND the per-bucket returns for the pie —
    # a return is a property of the held instruments, not the weighting basis. Loaded once.
    # ⚠ PHASE TIMING, RETURNED TO THE CLIENT. This endpoint is seconds long and the browser could
    # only see the total — "Loading composition…" for 5s with nothing saying which of its eight
    # loads was responsible. The AIRS expand has carried per-phase timings for exactly this reason;
    # this had none. Measured, the split is flat (no single hotspot), which is itself the finding —
    # and only visible once it is reported.
    _t: dict[str, int] = {}
    _t0 = time.perf_counter()

    def _phase(name: str) -> None:
        nonlocal _t0
        now = time.perf_counter()
        _t[name] = int((now - _t0) * 1000)
        _t0 = now

    book = _book_port_items(portfolio_id, codes)
    _phase("book_holdings")
    # ⚠ SAY WHY THERE IS NO BOOK, HERE, EVERY TIME — not only when book WEIGHTS were asked for.
    # The holdings table is empty whenever `book` is None regardless of `weight_by`, and it was
    # the one surface with no explanation attached. WARNING level because uvicorn leaves the root
    # logger there, so an `info` line never reaches production — and production is where this was
    # reported ("No positions to show" beside a portfolios list that plainly has rows).
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
    bucket_returns = book["bucket_returns"] if book else {}

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
    bw = _weigh(bench_items)
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
    _phase("axes")

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
        "allocation": [{**s, "return_pct": bucket_returns.get(s["bucket"])}
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
        "book_holdings": _with_start_weights(book["holdings_detail"] if book else [],
                                             (basis_axes or {}).get("_start_weights") or {}),
    }


async def compute_portfolio_analysis_async(portfolio_id: int,
                                           benchmark_label: str = SP500_LABEL,
                                           weight_by: str = "model",
                                           source: str = "model",
                                           bucket_filter: str | None = None) -> dict:
    return await asyncio.to_thread(compute_portfolio_analysis, portfolio_id, benchmark_label,
                                   weight_by, source, bucket_filter)


def portfolio_basket_request(portfolio_id: int):
    """A model portfolio's holdings as a `BasketRequest` — the bridge that lets the whole portfolio
    reuse the same basket engines (performance, owner earnings, price series) an instrument / group
    does. 404 when the portfolio has no priceable positions."""
    from fastapi import HTTPException  # noqa: PLC0415

    from routers._asset_financials import BasketHolding, BasketRequest  # noqa: PLC0415

    p = (supabase.table("airs_model_portfolio")
         .select("id,name,positions_datum").eq("id", portfolio_id).limit(1).execute().data or [])
    if not p:
        raise HTTPException(404, f"No model portfolio {portfolio_id}.")
    p = p[0]
    pos = (supabase.table("airs_model_portfolio_position")
           .select("isin,percentage,datum").eq("portfolio_id", portfolio_id).execute().data or [])
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
    return await asyncio.to_thread(compute_basket_analysis, holdings, benchmark_label, name)
