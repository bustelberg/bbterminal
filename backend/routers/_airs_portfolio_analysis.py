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
    to say so: every fund goes into ONE bucket, `Fund (not looked through)`, on ALL THREE axes.
    A portfolio that is 40% ETF then shows a 40% bar that reads "we can't see inside this" —
    which is TRUE, and far more useful than a confident, wrong sector split.

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
from collections import defaultdict
from datetime import date

from asset_pipeline.geo import msci_region_of
from deps import IN_CHUNK_SIZE, supabase
from routers._asset_benchmark import index_returns
from routers._asset_benchmark import members as _members
from routers._benchmark_index import SP500_LABEL

# The single bucket every fund lands in, on every axis. Named, not blank — a blank reads as
# "nothing", and this is emphatically something.
FUND_BUCKET = "Fund (not looked through)"
CASH_BUCKET = "Cash"
UNKNOWN_BUCKET = "Unclassified"

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
    # A fund is opaque on ALL THREE axes — see the module docstring. Not just its sector: its
    # listing venue and quote currency say nothing about what it holds either.
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

    perf = next((x for x in compute_portfolio_performance()
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

    grid = _grid(sorted({r["isin"] for r in rows if r.get("isin")}))
    items: list[tuple[float, tuple[str, str, str]]] = []
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
    if not items:
        return None
    return {"items": items, "classified_w": classified_w, "total_w": total_w,
            "foreign": foreign, "holdings": holdings, "portefeuille": link["portefeuille"]}


def compute_portfolio_analysis(portfolio_id: int,
                               benchmark_label: str = SP500_LABEL,
                               weight_by: str = "model",
                               source: str = "model") -> dict:
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
           .select("isin,fonds,percentage,datum")
           .eq("portfolio_id", portfolio_id).execute().data or [])
    if p.get("positions_datum"):
        pos = [r for r in pos if r.get("datum") == p["positions_datum"]]

    # --- the portfolio side -------------------------------------------------------------
    codes = _country_by_code()
    held = sorted({r["isin"] for r in pos if r.get("isin")})
    grid = _grid(held)
    port_items: list[tuple[float, tuple[str, str, str]]] = []
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
    weight_basis, weight_note = "model", None
    port_holdings = len([r for r in pos if r.get("isin")])
    if weight_by == "book":
        book = _book_port_items(portfolio_id, codes)
        if book:
            port_items = book["items"]
            classified_w, total_w = book["classified_w"], book["total_w"]
            port_foreign, port_holdings = book["foreign"], book["holdings"]
            weight_basis = "book"
        else:
            weight_note = "No priced book to weight by — showing the model's own weights."

    pw, bw = _weigh(port_items), _weigh(bench_items)

    axes = []
    for axis in ("sector", "region", "currency"):
        keys = set(pw[axis]) | set(bw[axis])
        rows = [{
            "bucket": k,
            "portfolio_pct": pw[axis].get(k, 0.0),
            "benchmark_pct": bw[axis].get(k, 0.0),
            # The tilt. It is the whole point of putting the two side by side.
            "diff_pct": pw[axis].get(k, 0.0) - bw[axis].get(k, 0.0),
        } for k in keys]
        rows.sort(key=lambda r: -max(r["portfolio_pct"], r["benchmark_pct"]))
        axes.append({"axis": axis, "rows": rows})

    return {
        "portfolio_id": portfolio_id,
        "name": p["name"],
        "as_of": p.get("positions_datum"),
        "benchmark": benchmark_label,
        "benchmark_members": len(bench_items),
        "holdings": port_holdings,
        # Which side the portfolio bars describe: the model's nominal weights, or the book's
        # actual EUR holdings. `weight_note` is set only when "book" was asked for and refused.
        "weight_basis": weight_basis,
        "weight_note": weight_note,
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
        "returns": _returns(portfolio_id, p.get("positions_datum"), benchmark_label, source),
        "axes": axes,
    }


async def compute_portfolio_analysis_async(portfolio_id: int,
                                           benchmark_label: str = SP500_LABEL,
                                           weight_by: str = "model",
                                           source: str = "model") -> dict:
    return await asyncio.to_thread(compute_portfolio_analysis, portfolio_id, benchmark_label,
                                   weight_by, source)


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
    """(port_items, classified_w, total_w, foreign, holding_count) for a basket — the portfolio
    side of the composition, built exactly like `compute_portfolio_analysis` does for a model."""
    held = sorted({h.isin for h in holdings if h.isin})
    grid = _grid(held)
    items: list[tuple[float, tuple[str, str, str]]] = []
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
    return items, classified, total, foreign, count


def compute_basket_analysis(holdings, benchmark_label: str = SP500_LABEL, name: str | None = None) -> dict:
    """Composition + return of an ARBITRARY basket (a single stock, a group, an ad-hoc set) beside
    the benchmark — the same shape `compute_portfolio_analysis` returns, so ONE Analyse view serves
    a stock (a basket of one) and a portfolio alike. yfinance only: a basket has no AIRS book."""
    codes = _country_by_code()
    port_items, classified_w, total_w, port_foreign, port_holdings = _classify_items(holdings, codes)

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
    axes = []
    for axis in ("sector", "region", "currency"):
        keys = set(pw[axis]) | set(bw[axis])
        rows = [{"bucket": k, "portfolio_pct": pw[axis].get(k, 0.0),
                 "benchmark_pct": bw[axis].get(k, 0.0),
                 "diff_pct": pw[axis].get(k, 0.0) - bw[axis].get(k, 0.0)} for k in keys]
        rows.sort(key=lambda r: -max(r["portfolio_pct"], r["benchmark_pct"]))
        axes.append({"axis": axis, "rows": rows})

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
    }


async def compute_basket_analysis_async(holdings, benchmark_label: str = SP500_LABEL,
                                        name: str | None = None) -> dict:
    return await asyncio.to_thread(compute_basket_analysis, holdings, benchmark_label, name)
