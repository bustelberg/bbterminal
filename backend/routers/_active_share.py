"""Active share — how much of the book's EQUITY sleeve is not the benchmark.

    AS = ½ · Σ |wᵢᵖ − wᵢᵇ|          Overlap = Σ min(wᵢᵖ, wᵢᵇ) = 1 − AS

0% is the index; 100% shares no name with it. The ½ is not cosmetic: every overweight has a
matching underweight by construction (both vectors sum to 1), so without it every difference is
counted twice.

 THE MATCH IS ON THE ISSUER, NOT ON THE LINE, AND THIS IS THE WHOLE DIFFICULTY. Alphabet A and
Alphabet C are two ISINs and one company; an ADR and its home line are two ISINs and one company.
Matching on ISIN alone reports a book that holds GOOG against an index carrying GOOGL as having NO
overlap in Alphabet — a ~4% swing on a US book, invented entirely by the identifier. So both sides
collapse to an issuer key first, and both sides SUM their lines into it.

 THE KEY IS THE ONE `_asset_benchmark.members` ALREADY DEDUPES ON — the GuruFocus company name,
normalised. That is not a convenience: the index side is already collapsed by it (Yahoo reports the
full company cap on every share class, so `members` keeps one row per company), so keying the
portfolio the same way is what makes the two vectors talk about the same objects. A second notion
of "same issuer" here would disagree with the index's own dedupe, and the disagreement would show
up as active share.

 BOTH VECTORS MUST SUM TO 1 OR THE ½ IS WRONG. The portfolio is renormalised over the individual
stocks alone — funds, cash, bonds and unpriceable lines are dropped, not zero-weighted. Leaving
cash in at its real weight would report liquidity as "active", which is a defensible measure but a
DIFFERENT one, and mixing the two is how a 30% figure becomes uncomparable with anybody else's.
`stocks_pct` says how much of the book this sleeve actually is, so the renormalisation is never
silent.

 THE BENCHMARK IS NOT FLOAT-ADJUSTED and is priced over the names we can bridge — the same two
caveats `_asset_benchmark` carries, and they land differently here than on a return. A missing
constituent does not reduce the index's weight in the rest, it INFLATES it (renormalisation), so an
unheld name we cannot price makes active share read slightly LOW. `benchmark_covered_pct` is
returned so the figure is never read as exact.
"""
from __future__ import annotations

import re
from copy import deepcopy
from datetime import date
from threading import RLock

from common.pg import load_rows_via_copy
from deps import IN_CHUNK_SIZE, supabase
from index_universe.acwi.identity import issuer_key, membership_key


def is_direct_equity_stock(holding: dict) -> bool:
    """Whether a risk input is a direct operating-company equity position."""
    if holding.get("is_fund") or holding.get("bucket") not in (None, "", "Equity"):
        return False
    # AIRS can classify perpetual coupon certificates as Equity. Reuse the classifier's
    # fixed-income name guard so they never become an ACWI stock merely because they have an ISIN.
    from routers._airs_holding_isin import _looks_like_bond  # noqa: PLC0415
    return not _looks_like_bond(holding.get("name"))

# What a portfolio line must have before it can be compared with an index at all.
#  `currency` IS THE LISTING'S, and the exposure view needs it — see `_portfolio_exposure`.
_GRID_COLS = "isin,gf_company_name,name,currency,status,analysis_id,yahoo_symbol"
_EXECUTION_COLS = "isin,analysis_id,yahoo_symbol,name,status"

_ETF_NAME = re.compile(r"\b(etf|ucits|ishares|vanguard|invesco)\b", re.I)


def _policy_bucket(holding: dict) -> str:
    """Display bucket for an Active Share policy row."""
    # AIRS occasionally labels an ETF as plain Equity. Bonds win first because a bond ETF is
    # still a bond in this comparison.
    if holding.get("bucket") == "Bonds":
        return "Bonds"
    if (holding.get("bucket") == "ETF Equity" or holding.get("is_fund")
            or _ETF_NAME.search(str(holding.get("name") or ""))):
        return "Stock ETFs"
    return "Stocks"


def _issuer_key(name: str | None) -> str:
    """The comparison key for one issuer.

     IT MUST MATCH `_asset_benchmark.members`' OWN DEDUPE KEY, which is
    `(gf_company_name or name).strip().lower()`. Everything below is applied to BOTH sides, so the
    two stay in step; the suffix strip is the only thing added, and it is added on both.

     SUFFIX-ONLY, AND NOT A FUZZY MATCH. `asset_pipeline.resolve.same_company` exists for
    resolving an unknown listing and scores similarity — the wrong tool here, because a false
    positive is not a bad listing, it is two different companies fused into one row of a risk
    report. `token_set_ratio` scores a SUBSET as 100 (`eur` ⊂ `Shell PLC EUR`), which is exactly
    the failure mode a holdings list is full of. Exact-after-normalising refuses more than it
    should and never invents an overlap.

     THE TWO STRIPS ALTERNATE UNTIL THE NAME STOPS CHANGING, AND RUNNING THEM IN SEQUENCE IS A
    BUG I SHIPPED INTO THE FIRST VERSION OF THIS FILE. "Alphabet Inc Class C" ends in the class
    marker, so the corporate-form loop matches nothing; strip the class afterwards and you are left
    with "alphabet inc" against the index's "alphabet" — a full 100% active share in the single
    most-owned company in the world, which is precisely the failure this function exists to
    prevent. Neither strip can be "the last one". Bounded: each pass makes the string strictly
    shorter or the loop stops.
    """
    return issuer_key(name)


def _grid_by_isin(isins: list[str]) -> dict[str, dict]:
    """`asset_grid` rows for the book's ISINs — the bridge from a line to its issuer name."""
    if not isins:
        return {}
    rows = load_rows_via_copy("asset_grid", _GRID_COLS, "isin", isins)
    if rows is None:
        rows = []
        for i in range(0, len(isins), IN_CHUNK_SIZE):
            rows += (supabase.table("asset_grid").select(_GRID_COLS)
                     .in_("isin", isins[i:i + IN_CHUNK_SIZE]).execute().data or [])
    out: dict[str, dict] = {}
    for r in rows:
        key = (r.get("isin") or "").strip().upper()
        if not key:
            continue
        #  One row per ISIN, preferring one that carries a company name. `asset_grid` is one row
        # per EXECUTION, so a company traded on several venues appears several times and only some
        # of those rows may have bridged to a company. Taking whichever came back first would make
        # the issuer key depend on row order.
        prev = out.get(key)
        if prev is None or ((r.get("gf_company_name") or "") and not (prev.get("gf_company_name") or "")):
            out[key] = r
    return out


def _execution_by_isin(isins: list[str]) -> dict[str, dict]:
    """The authoritative saved execution identity for each requested ISIN.

    ``asset_grid`` is a denormalised reporting view.  It is useful for display data, but an
    Active Share issuer match must not disappear while that view is rebuilding or lagging behind
    ``asset_execution``.  The latter is the exact identity source used to resolve ACWI members.
    Only healthy executions are eligible: a failed resolver must never manufacture an overlap.
    """
    if not isins:
        return {}
    rows = load_rows_via_copy("asset_execution", _EXECUTION_COLS, "isin", isins)
    if rows is None:
        rows = []
        for i in range(0, len(isins), IN_CHUNK_SIZE):
            rows += (supabase.table("asset_execution").select(_EXECUTION_COLS)
                     .in_("isin", isins[i:i + IN_CHUNK_SIZE]).execute().data or [])
    out: dict[str, dict] = {}
    for row in rows:
        isin = (row.get("isin") or "").strip().upper()
        if isin and row.get("status") == "ok" and row.get("analysis_id") is not None:
            out[isin] = row
    return out


def _fold(entries: list[tuple[str, str, float]]) -> dict[str, dict]:
    """Sum weights onto issuer keys. `entries` is (key, display name, weight).

     SUMMING, NOT REPLACING — a book holding both Alphabet classes holds ONE position in Alphabet
    for this purpose, and taking either line alone would understate the overlap by the other.
    """
    out: dict[str, dict] = {}
    for key, name, w in entries:
        slot = out.setdefault(key, {"key": key, "name": name, "weight_pct": 0.0})
        slot["weight_pct"] += w
        # The longest name wins — "Alphabet Inc Class C" is more informative than "Alphabet".
        if len(name or "") > len(slot["name"] or ""):
            slot["name"] = name
    return out


class IssuerError(Exception):
    """No comparable sleeve, with the reason already phrased for the reader."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def build_issuer_weights(holdings: list[dict], benchmark: str,
                         benchmark_start: str | None = None) -> dict:
    """Both sides of the comparison, folded onto ISSUER keys and each summing to 100.

     SHARED BY ACTIVE SHARE AND CONCENTRATION so the two describe the same objects. Both ask
    questions about ISSUERS — "how far from the index is this book", "how much of it sits in its
    ten largest" — and a second folding here would let a book hold 49 lines on one view and 47
    issuers on the other with nothing on screen saying why.

    Returns `{port, bench, stocks_w, total_all, unresolved, coverage}`; `port` and `bench` are
    `{key: {key, name, weight_pct}}`, each summing to 100. Raises `IssuerError` rather than
    returning a sentinel — every caller turns it into the same `{available: False, reason}`.
    """
    # ── the book: individual stocks only, renormalised to 1 ────────────────────────────────────
    total_all = sum(float(h.get("weight_pct") or 0) for h in holdings)
    stocks = [h for h in holdings
              if is_direct_equity_stock(h)
              and (h.get("isin") or "").strip()
              and float(h.get("weight_pct") or 0) > 0]
    stocks_w = sum(float(h["weight_pct"]) for h in stocks)
    if stocks_w <= 0:
        raise IssuerError("This book holds no individual stocks with an ISIN to compare.")

    grid = _grid_by_isin(sorted({(h.get("isin") or "").strip().upper() for h in stocks}))
    p_entries: list[tuple[str, str, float]] = []
    unresolved: list[dict] = []
    for h in stocks:
        isin = (h.get("isin") or "").strip().upper()
        g = grid.get(isin) or {}
        raw = g.get("gf_company_name") or g.get("name") or h.get("name")
        key = _issuer_key(raw)
        #  An unkeyable line still counts as active, and falls back to its ISIN so it can never
        # collide with another. It is genuinely a position the index does not have a matching row
        # for — dropping it would renormalise the rest upward and quietly LOWER active share, which
        # is the flattering direction. It is listed so the reader can see what could not be matched.
        if not key:
            key = f"isin:{isin}"
            unresolved.append({"name": h.get("name"), "isin": isin,
                               "weight_pct": float(h["weight_pct"]) / stocks_w * 100.0})
        p_entries.append((key, h.get("name") or raw or isin,
                          float(h["weight_pct"]) / stocks_w * 100.0))
    port = _fold(p_entries)

    index = _benchmark_weights(benchmark, benchmark_start)
    bench, coverage = index["bench"], index["coverage"]
    # Join the two data vendors through our asset identity, never through display spelling.
    # ``asset_grid`` can temporarily lag the execution record after a resolver repair.  Fill
    # missing or unmatched identities from ``asset_execution`` — the same source ACWI membership
    # uses — so a real ACWI name cannot turn into a dash during that refresh window.
    canonical = index["issuer_by_asset"]
    if canonical:
        missing_identity_isins = [
            (h.get("isin") or "").strip().upper() for h in stocks
            if (h.get("isin") or "").strip().upper()
            and (grid.get((h.get("isin") or "").strip().upper()) or {}).get("analysis_id") not in canonical
        ]
        executions = _execution_by_isin(sorted(set(missing_identity_isins)))
        bridged_entries: list[tuple[str, str, float]] = []
        for (key, name, weight), holding in zip(p_entries, stocks):
            isin = (holding.get("isin") or "").strip().upper()
            # A stale grid ID must not mask a healthy execution row with the canonical identity.
            analysis_id = (grid.get(isin) or {}).get("analysis_id")
            if analysis_id not in canonical:
                analysis_id = (executions.get(isin) or {}).get("analysis_id", analysis_id)
            bridged_entries.append((canonical.get(int(analysis_id), key)
                                    if analysis_id is not None else key, name, weight))
        p_entries = bridged_entries
        port = _fold(p_entries)

    membership_date = coverage.get('workbook_as_of') if benchmark.upper() == 'ACWI' and not benchmark_start else None
    if membership_date:
        # Membership is an issuer fact even if the execution/grid bridge is absent or stale.
        # These exact keys also handle the dated EQR/AVB successor without borrowing old prices.
        p_entries = [(next((candidate for candidate in (
            membership_key(key, membership_date), membership_key(name, membership_date)) if candidate in bench),
            membership_key(key, membership_date)), name, weight) for key, name, weight in p_entries]
        port = _fold(p_entries)
    return {"port": port, "bench": bench, "stocks_w": stocks_w, "total_all": total_all,
            "unresolved": unresolved, "coverage": coverage,
            "issuer_by_isin": {(holding.get('isin') or '').strip().upper(): entry[0]
                               for holding, entry in zip(stocks, p_entries)}}


def _risk_profile_holding(holding: dict) -> bool:
    """A Stocks/Bonds constituent for the reviewed policy comparison."""
    return (holding.get("bucket") in {"Equity", "ETF Equity", "Bonds"}
            and bool((holding.get("isin") or "").strip())
            and float(holding.get("weight_pct") or 0) > 0)


def build_policy_issuer_weights(holdings: list[dict], benchmark: str,
                                benchmark_start: str | None,
                                policy_weights: dict[str, float]) -> dict:
    """Whole Stocks/Bonds book versus its fixed ACWI/AGGH policy benchmark.

    Bonds remain in this version: allocation to the bond sleeve is one of the policy decisions
    being measured. AGGH.AS is a single transparent benchmark position because we do not have
    constituent-level membership data for the aggregate-bond ETF.
    """
    try:
        stock_target = float(policy_weights.get("Stocks", 0.0))
        bond_target = float(policy_weights.get("Bonds", 0.0))
    except (TypeError, ValueError):
        raise IssuerError("The risk-profile benchmark weights are invalid.")
    if stock_target < 0 or bond_target < 0 or abs(stock_target + bond_target - 100.0) > 0.01:
        raise IssuerError("The risk-profile benchmark must contain Stocks and Bonds totaling 100%.")

    if policy_weights.get("EquityOnly"):
        built = build_issuer_weights(holdings, benchmark, benchmark_start)
        return {**built,
                "port": {key: {**row,
                               "bucket": "Stocks"} for key, row in built["port"].items()},
                "bench": {key: {**row,
                                "bucket": "Stocks"} for key, row in built["bench"].items()},
                "label": "100% ACWI", "whole_policy": True}

    selected = [h for h in holdings if _risk_profile_holding(h)]
    total = sum(float(h["weight_pct"]) for h in selected)
    if total <= 0:
        raise IssuerError("This book holds no Stocks or Bonds constituent to compare.")

    direct = [h for h in selected if is_direct_equity_stock(h)]
    # The stored execution symbol is the only quote link we may show. Fetch it for every policy
    # holding, including ETFs, but never manufacture a Yahoo URL from a fund name.
    grid = _grid_by_isin(sorted({(h.get("isin") or "").strip().upper() for h in selected}))
    # Do not duplicate the ACWI issuer bridge here. The conventional Active Share builder has
    # the reviewed workbook/asset identity join (including cross-listings and accented names such
    # as Hermès and L'Oréal); policy mode must reuse it rather than approximate it a second way.
    direct_built = (build_issuer_weights(holdings, benchmark, benchmark_start) if direct else None)
    index = _benchmark_weights(benchmark, benchmark_start)
    entries: list[tuple[str, str, float]] = []
    unresolved: list[dict] = []
    buckets: dict[str, str] = {}
    symbols: dict[str, str] = {}
    direct_isins = {(h.get("isin") or "").strip().upper() for h in direct}

    port = {}
    if direct_built:
        direct_scale = direct_built["stocks_w"] / total
        direct_buckets_by_name = {
            str(h.get("name") or ""): _policy_bucket(h) for h in direct
        }
        port = {
            key: {**row, "weight_pct": row["weight_pct"] * direct_scale,
                  "bucket": direct_buckets_by_name.get(str(row.get("name") or ""), "Stocks")}
            for key, row in direct_built["port"].items()
        }
        unresolved.extend({**row, "weight_pct": row["weight_pct"] * direct_scale}
                          for row in direct_built["unresolved"])

    for h in selected:
        isin = (h.get("isin") or "").strip().upper()
        name = h.get("name") or isin
        # Bonds and funds are investable positions but not ACWI companies. Keeping their ISIN as
        # the identity makes the lack of overlap explicit instead of silently dropping it.
        if isin in direct_isins:
            # Already folded and identity-matched by `build_issuer_weights` above.
            continue
        key = f"isin:{isin}"
        if key.startswith("isin:"):
            unresolved.append({"name": name, "isin": isin,
                               "weight_pct": float(h["weight_pct"]) / total * 100.0})
        entries.append((key, name, float(h["weight_pct"]) / total * 100.0))
        buckets[key] = _policy_bucket(h)
        symbol = str((grid.get(isin) or {}).get("yahoo_symbol") or "").strip()
        if symbol:
            symbols[key] = symbol

    bench = {
        key: {**row, "weight_pct": row["weight_pct"] * stock_target / 100.0}
        for key, row in (direct_built or index)["bench"].items()
    }
    if bond_target > 0:
        bench["benchmark:aggh.as"] = {
            "key": "benchmark:aggh.as", "name": "AGGH.AS", "weight_pct": bond_target,
        }
    label = " + ".join(
        f"{weight:g}% {name}"
        for name, weight in (("ACWI", stock_target), ("AGGH.AS", bond_target)) if weight > 0
    )
    stocks_w = sum(float(h["weight_pct"]) for h in selected if h.get("bucket") != "Bonds")
    for key, row in _fold(entries).items():
        if key in port:
            port[key]["weight_pct"] += row["weight_pct"]
        else:
            port[key] = {**row, "bucket": buckets.get(key, "Stocks")}
        port[key]["bucket"] = buckets.get(key, port[key].get("bucket", "Stocks"))
        port[key]["yahoo_symbol"] = symbols.get(key)
    for key, row in bench.items():
        row["bucket"] = "Bonds" if key == "benchmark:aggh.as" else "Stocks"
    return {"port": port, "bench": bench, "stocks_w": stocks_w, "total_all": total,
            "unresolved": unresolved, "coverage": index["coverage"], "label": label,
            "whole_policy": True}


def compute_active_share(holdings: list[dict], benchmark: str,
                         benchmark_start: str | None = None,
                         benchmark_weights: dict[str, float] | None = None,
                         *, held_only: bool = False) -> dict:
    """Active share of the book's individual stocks against `benchmark`.

    `holdings` are the rows the Analyse modal is already showing — `{isin, weight_pct, name,
    is_fund}`.  THE WEIGHTS ARE THE ONES ON SCREEN, taken rather than recomputed: this panel sits
    one click from the Holdings table, and a risk figure derived from a second weighting would be
    a number the table cannot be made to reproduce.
    """
    try:
        built = (build_policy_issuer_weights(holdings, benchmark, benchmark_start, benchmark_weights)
                 if benchmark_weights else build_issuer_weights(holdings, benchmark, benchmark_start))
    except IssuerError as e:
        return {"available": False, "reason": e.reason, "benchmark": benchmark}
    sleeve_port, bench = built["port"], built["bench"]
    stocks_w, total_all = built["stocks_w"], built["total_all"]
    unresolved, coverage = built["unresolved"], built["coverage"]

    # Generic books use the conventional equity sleeve; risk-profile books compare their entire
    # investable Stocks/Bonds sleeve with the reviewed policy benchmark.
    port = sleeve_port

    # ── ½ Σ |wᵖ − wᵇ| over the UNION ───────────────────────────────────────────────────────────
    rows: list[dict] = []
    for key in set(port) | set(bench):
        p = port.get(key, {}).get("weight_pct", 0.0)
        b = bench.get(key, {}).get("weight_pct", 0.0)
        rows.append({
            "name": (port.get(key) or bench.get(key))["name"],
            "portfolio_pct": p,
            "benchmark_pct": b,
            "in_benchmark": key in bench,
            "active_pct": p - b,
            "held": key in port,
            "residual": False,
            "workbook_weight_pct": (bench.get(key) or {}).get("workbook_weight_pct"),
            "yfinance_return_pct": (bench.get(key) or {}).get("yfinance_return_pct"),
            "normalisation_total": (bench.get(key) or {}).get("normalisation_total"),
            "weight_calculation": (bench.get(key) or {}).get("weight_calculation"),
            # Policy rows retain their Stocks/Bonds identity so the UI can separate the two
            # sleeves without trying to infer an asset class from a display name.
            "bucket": (port.get(key) or bench.get(key) or {}).get("bucket"),
            "yahoo_symbol": (port.get(key) or bench.get(key) or {}).get("yahoo_symbol"),
        })
    active_share = sum(abs(r["active_pct"]) for r in rows) / 2.0
    overlap = sum(min(r["portfolio_pct"], r["benchmark_pct"]) for r in rows)

    # Table and headline metrics use the same normalised weights.
    rows.sort(key=lambda row: -abs(row["active_pct"]))

    held = [r for r in rows if r["held"] and not r["residual"]]
    in_bench = [r for r in held if r["in_benchmark"]]
    return {
        "available": True,
        "benchmark": built.get("label") or benchmark,
        "active_share_pct": active_share,
        #  Returned, not left to the client as `100 − AS`. The identity is the POINT (it is what
        # makes active share a share of something), and a reader checking it against the two
        # printed numbers must find it holds to the digit rather than to the rounding.
        "overlap_pct": overlap,
        "stocks_pct": (stocks_w / total_all * 100.0) if total_all > 0 else None,
        #  The two operands, not just their ratio. `stocks_pct` is the one figure on this panel a
        # reader cannot check against anything on screen — the Holdings table shows the lines, not
        # the two sums — so the card prints the division and needs both halves to do it. Returned
        # rather than re-derived on the client, which would be a second definition of "a stock".
        "stocks_weight": stocks_w,
        "total_weight": total_all,
        "n_holdings": len(held),
        "n_in_benchmark": len(in_bench),
        # The book's weight that sits in names the index does not hold at all — the part of active
        # share that is SELECTION rather than sizing.
        "off_benchmark_pct": sum(r["portfolio_pct"] for r in held if not r["in_benchmark"]),
        "benchmark_members": len(bench),
        "benchmark_covered_pct": coverage.get("covered_pct"),
        #  When the index side was measured — a RANGE, and separate from the book's own date.
        # The two sides of this comparison are read from different places on different schedules
        # (the book from AIRS, the caps from Yahoo), so one "as of" over the pair would be a date
        # that is true of neither. The panel prints both and lets the reader judge whether they are
        # close enough to compare, which is a judgement only they can make.
        "benchmark_caps_from": coverage.get("caps_from"),
        "benchmark_caps_to": coverage.get("caps_to"),
        "benchmark_caps_unstamped": int(coverage.get("caps_unstamped") or 0),
        "unresolved": sorted(unresolved, key=lambda r: -r["weight_pct"]),
        # Everything the book holds, plus the index names it is most under-weight in. Sorted by the
        # size of the bet either way, which is the order a risk report is read in.
        "rows": sorted(held if held_only else rows, key=lambda r: -abs(r["active_pct"])),
    }



_benchmark_lock = RLock()


def _benchmark_weights(benchmark: str, benchmark_start: str | None) -> dict:
    """Share the expensive index rollforward across books until its input data changes.

    The lock coalesces simultaneous opens; copies keep a caller from mutating another's answer.
    The existing analysis fingerprint invalidates on price, FX or identity updates, including
    writes from other workers. Deployment starts a fresh cache from the bundled file.
    """
    from routers._analysis_cache import leg
    from routers import _prepared_analyse as prepared

    revision, saved = (prepared.read('acwi', 'acwi:ACWI')
                       if benchmark.upper() == 'ACWI' and benchmark_start is None else (None, None))
    if saved is not None:
        saved['issuer_by_asset'] = {int(key): value for key, value in saved['issuer_by_asset'].items()}
        return saved

    cache_key = ("risk-workbook-identity-v9", benchmark.upper(), benchmark_start, date.today().isoformat(),
                 revision, prepared.code_version() if revision is not None else None)
    with _benchmark_lock:
        return deepcopy(leg(cache_key, lambda: _load_benchmark_weights(benchmark, benchmark_start)))


def _load_benchmark_weights(benchmark: str, benchmark_start: str | None) -> dict:
    from routers._asset_benchmark import index_rows, members

    issuer_by_asset: dict[int, str] = {}
    # ── the index: cap weights over the names we can price ─────────────────────────────────────
    if benchmark_start:
        # Match Attribution's benchmark snapshot. `index_rows` rolls each current cap back to the
        # opening price and applies the benchmark's own cap rule, so a January AIRS book is never
        # compared with September ACWI weights.
        mem, coverage = index_rows(benchmark, benchmark_start)
        positive = [m for m in mem if float(m.get("weight_pct") or 0) > 0]
        if not positive:
            raise IssuerError(f"No priced constituents for {benchmark} at {benchmark_start}.")
        bench = _fold([
            (_issuer_key(m.get("company_name")) or f"isin:{(m.get('isin') or '').upper()}",
             m.get("company_name") or "", float(m["weight_pct"]))
            for m in positive
        ])
        dates = [m.get("start_date") for m in positive if m.get("start_date")]
        coverage = {**coverage,
                    "caps_from": min(dates) if dates else benchmark_start,
                    "caps_to": max(dates) if dates else benchmark_start,
                    "caps_unstamped": len(positive) - len(dates)}
    elif benchmark.upper() == "ACWI":
        # The iShares workbook supplies the actual dated ACWI weights. Roll each forward by its
        # yfinance EUR return, then normalise: this is today's composition implied by that file,
        # not a substitute full-market-cap index.
        from index_universe.acwi.asset_membership import resolve  # noqa: PLC0415
        from index_universe.acwi.holdings import load_acwi_holdings
        raw_members, _ = load_acwi_holdings()
        file_rows, file_stats = resolve()
        file_weights = {int(r["analysis_id"]): float(r.get("weight_pct") or 0) for r in file_rows}
        issuer_by_asset = {int(r["analysis_id"]): _issuer_key(r.get("name")) for r in file_rows}
        from datetime import date, timedelta
        from index_universe.acwi.reconstruction import _parse_xls_as_of
        from routers._asset_benchmark import window_marks, _asset_closes
        from routers._benchmark_index import _window_rows, _fx_to_eur
        parsed = _parse_xls_as_of(str(file_stats.get("as_of") or ""))
        if parsed is None:
            raise IssuerError("The ACWI workbook has no valid weight snapshot date.")
        start = parsed.isoformat()
        end = date.today().isoformat()
        ids = list(file_weights)
        columns = "analysis_id,isin,currency,name,gf_company_name"
        grid_rows = load_rows_via_copy("asset_grid", columns, "analysis_id", ids)
        if grid_rows is None:
            grid_rows = []
            for offset in range(0, len(ids), IN_CHUNK_SIZE):
                grid_rows += (supabase.table("asset_grid").select(columns)
                              .in_("analysis_id", ids[offset:offset + IN_CHUNK_SIZE]).execute().data or [])
        grid = {int(row["analysis_id"]): row for row in grid_rows}
        price_members = []
        for row in file_rows:
            aid = int(row["analysis_id"])
            g = grid.get(aid, {})
            # The ACWI workbook's constituent name is the stable fallback. A local asset-grid
            # row may have a blank display name; preferring it made the benchmark top-five table
            # show weights beside empty company cells.
            price_members.append({"company_id": aid, "company_name": row.get("name") or g.get("gf_company_name") or g.get("name"),
                                  "isin": g.get("isin"), "currency": g.get("currency") or row.get("currency"),
                                  "gurufocus_ticker": row.get("yahoo_symbol"), "market_cap_eur": 1.0})
        lookback = (parsed - timedelta(days=45)).isoformat()
        marks = window_marks(ids, lookback, start, end)
        fx = _fx_to_eur({m.get("currency") or "USD" for m in price_members}, lookback, end)
        priced, _ = _window_rows(price_members, {} if marks else _asset_closes(ids, lookback, end),
                                 fx, start, marks=marks or None, allow_single_mark=True)
        coverage = {"workbook_as_of": start}
        grown = [(r, file_weights.get(int(r["company_id"]), 0.0) * (1 + float(r.get("return_eur_pct") or 0) / 100))
                 for r in priced if file_weights.get(int(r["company_id"]), 0) > 0 and r.get("return_eur_pct") is not None]
        total = sum(weight for _r, weight in grown)
        if total <= 0:
            raise IssuerError(f"ACWI weights dated {start} have no usable stored opening prices and FX. "
                              "The daily market-data refresh needs to prepare these inputs.")
        bench = _fold([
            (_issuer_key(r.get("company_name")) or f"isin:{(r.get('isin') or '').upper()}",
             r.get("company_name") or "", weight / total * 100.0)
            for r, weight in grown
        ])
        prices_by_issuer: dict[str, list[dict]] = {}
        for r, grown_weight in grown:
            key = _issuer_key(r.get("company_name")) or f"isin:{(r.get('isin') or '').upper()}"
            slot = bench[key]
            slot["workbook_weight_pct"] = slot.get("workbook_weight_pct", 0.0) + file_weights[int(r["company_id"])]
            slot["grown_weight"] = slot.get("grown_weight", 0.0) + grown_weight
            prices_by_issuer.setdefault(key, []).append({k: r.get(k) for k in (
                "ticker", "start_date", "end_date", "start_price_eur", "end_price_eur")})
        for slot in bench.values():
            slot["yfinance_return_pct"] = (slot["grown_weight"] / slot["workbook_weight_pct"] - 1) * 100
            slot["normalisation_total"] = total
            slot["weight_calculation"] = {
                "as_of": start, "initial_weight_pct": slot["workbook_weight_pct"],
                "return_eur_pct": slot["yfinance_return_pct"],
                "normalisation_total": total, "result_pct": slot["weight_pct"],
                "prices": prices_by_issuer[slot["key"]],
            }
        # A workbook constituent with a temporarily missing price is still an ACWI constituent.
        # Keep its issuer identity in the comparison (at zero rolled-forward weight) so a held
        # name is never labelled outside the index merely because its price row was unavailable.
        grown_ids = {int(r["company_id"]) for r, _weight in grown}
        for file_row in file_rows:
            aid = int(file_row["analysis_id"])
            if aid in grown_ids:
                continue
            name = str(file_row.get("name") or file_row.get("ticker") or "")
            if not name:
                continue
            key = _issuer_key(name)
            if key:
                bench.setdefault(key, {"key": key, "name": name, "weight_pct": 0.0})
        # Asset resolution and price coverage must never decide membership. A raw source
        # member remains visible even if it has no stored listing at all.
        for source_row in raw_members:
            key = membership_key(source_row.get('Name'), start)
            if key:
                bench.setdefault(key, {'key': key, 'name': source_row['Name'], 'weight_pct': 0.0})
        priced_ids = {int(row['company_id']) for row, _ in grown}
        source_weight = sum(float(str(row.get('Weight (%)') or 0).replace(',', '.')) for row in raw_members)
        coverage = {**coverage, "workbook_as_of": start,
                    'covered_pct': (sum(weight for aid, weight in file_weights.items() if aid in priced_ids)
                                    / source_weight * 100) if source_weight > 0 else None}
    else:
        mem, coverage = members(benchmark)
        cap_total = sum(float(m.get("market_cap_eur") or 0) for m in mem)
        if not mem or cap_total <= 0:
            raise IssuerError(f"No priced constituents for {benchmark}.")
        bench = _fold([
            (_issuer_key(m.get("company_name")) or f"isin:{(m.get('isin') or '').upper()}",
             m.get("company_name") or "",
             float(m.get("market_cap_eur") or 0) / cap_total * 100.0)
            for m in mem if float(m.get("market_cap_eur") or 0) > 0
        ])

    return {"bench": bench, "coverage": coverage, "issuer_by_asset": issuer_by_asset}
