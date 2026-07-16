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


def _returns(portfolio_id: int, effective: str | None, benchmark_label: str) -> dict:
    """EUR return of the model vs the benchmark — over the SAME two windows, both times.

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

    return {
        "ytd_from": ytd_from,
        "since_from": effective,
        "portfolio_ytd_pct": p_ytd,
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
    }


def compute_portfolio_analysis(portfolio_id: int,
                               benchmark_label: str = SP500_LABEL) -> dict:
    """The portfolio's composition beside the benchmark's, on one set of buckets."""
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
        "holdings": len([r for r in pos if r.get("isin")]),
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
        "returns": _returns(portfolio_id, p.get("positions_datum"), benchmark_label),
        "axes": axes,
    }


async def compute_portfolio_analysis_async(portfolio_id: int,
                                           benchmark_label: str = SP500_LABEL) -> dict:
    return await asyncio.to_thread(compute_portfolio_analysis, portfolio_id, benchmark_label)
