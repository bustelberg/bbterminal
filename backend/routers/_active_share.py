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

from common.pg import load_rows_via_copy
from deps import IN_CHUNK_SIZE, supabase


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
_GRID_COLS = "isin,gf_company_name,name,currency,status,analysis_id"

# Trailing corporate forms, stripped before two names are called the same issuer. Deliberately
# SHORT and suffix-only — see `_issuer_key`.
_SUFFIX = re.compile(
    r"[\s,.]+(inc|inc\.|corp|corporation|co|company|plc|ltd|limited|nv|n\.v\.|sa|s\.a\.|ag|se|ab|"
    r"as|a/s|oyj|spa|s\.p\.a\.|holding|holdings|group|the)\.?$", re.I)

# A SHARE-CLASS or depositary marker — the part of a name that identifies a LINE, not a company.
#  Anchored at the end and applied in the same loop as `_SUFFIX`; see `_issuer_key`.
_CLASS = re.compile(
    r"[\s,.]+(class\s+[a-c]|cl\s+[a-c]|series\s+[a-c]|[ab]\s+shares|shares\s+[ab]|"
    r"sponsored\s+adr|unsponsored\s+adr|adr|ads|reg\s*s|registered)\.?$", re.I)


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
    s = (name or "").strip().lower()
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s.replace("&", " and ").replace("-", " "))
    for _ in range(6):
        # A share-class marker is a LINE, not an issuer — the whole point of the exercise.
        t = _CLASS.sub("", _SUFFIX.sub("", s)).strip(" ,.")
        if t == s or not t:
            break
        s = t
    return s


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
    from routers._asset_benchmark import index_rows, members  # noqa: PLC0415  (module cycle)

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
        file_rows, file_stats = resolve()
        file_weights = {int(r["analysis_id"]): float(r.get("weight_pct") or 0) for r in file_rows}
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
        grid = {}
        for offset in range(0, len(ids), IN_CHUNK_SIZE):
            for row in (supabase.table("asset_grid").select("analysis_id,isin,currency,name,gf_company_name")
                        .in_("analysis_id", ids[offset:offset + IN_CHUNK_SIZE]).execute().data or []):
                grid[int(row["analysis_id"])] = row
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
                                 fx, start, marks=marks or None)
        coverage = {"workbook_as_of": start}
        grown = [(r, file_weights.get(int(r["company_id"]), 0.0) * (1 + float(r.get("return_eur_pct") or 0) / 100))
                 for r in priced if file_weights.get(int(r["company_id"]), 0) > 0 and r.get("return_eur_pct") is not None]
        total = sum(weight for _r, weight in grown)
        if total <= 0:
            raise IssuerError("ACWI workbook weights could not be rolled forward to today.")
        bench = _fold([
            (_issuer_key(r.get("company_name")) or f"isin:{(r.get('isin') or '').upper()}",
             r.get("company_name") or "", weight / total * 100.0)
            for r, weight in grown
        ])
        for r, grown_weight in grown:
            key = _issuer_key(r.get("company_name")) or f"isin:{(r.get('isin') or '').upper()}"
            slot = bench[key]
            slot["workbook_weight_pct"] = slot.get("workbook_weight_pct", 0.0) + file_weights[int(r["company_id"])]
            slot["grown_weight"] = slot.get("grown_weight", 0.0) + grown_weight
        for slot in bench.values():
            slot["yfinance_return_pct"] = (slot["grown_weight"] / slot["workbook_weight_pct"] - 1) * 100
            slot["normalisation_total"] = total
            slot["weight_calculation"] = {
                "as_of": start, "initial_weight_pct": slot["workbook_weight_pct"],
                "return_eur_pct": slot["yfinance_return_pct"],
                "normalisation_total": total, "result_pct": slot["weight_pct"],
                "prices": [{k: r.get(k) for k in ("ticker", "start_date", "end_date", "start_price_eur", "end_price_eur")}
                           for r, _w in grown if (_issuer_key(r.get("company_name")) or f"isin:{(r.get('isin') or '').upper()}") == slot["key"]],
            }
        # A workbook constituent with a temporarily missing price is still an ACWI constituent.
        # Keep its issuer identity in the comparison (at zero rolled-forward weight) so a held
        # name is never labelled outside the index merely because its price row was unavailable.
        for file_row in file_rows:
            aid = int(file_row["analysis_id"])
            if aid in {int(r["company_id"]) for r, _weight in grown}:
                continue
            name = str(file_row.get("name") or file_row.get("ticker") or "")
            if not name:
                continue
            key = _issuer_key(name)
            if key:
                bench.setdefault(key, {"key": key, "name": name, "weight_pct": 0.0})
        coverage = {**coverage, "workbook_as_of": start}
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


    return {"port": port, "bench": bench, "stocks_w": stocks_w, "total_all": total_all,
            "unresolved": unresolved, "coverage": coverage}


def compute_active_share(holdings: list[dict], benchmark: str,
                         benchmark_start: str | None = None) -> dict:
    """Active share of the book's individual stocks against `benchmark`.

    `holdings` are the rows the Analyse modal is already showing — `{isin, weight_pct, name,
    is_fund}`.  THE WEIGHTS ARE THE ONES ON SCREEN, taken rather than recomputed: this panel sits
    one click from the Holdings table, and a risk figure derived from a second weighting would be
    a number the table cannot be made to reproduce.
    """
    try:
        built = build_issuer_weights(holdings, benchmark, benchmark_start)
    except IssuerError as e:
        return {"available": False, "reason": e.reason, "benchmark": benchmark}
    sleeve_port, bench = built["port"], built["bench"]
    stocks_w, total_all = built["stocks_w"], built["total_all"]
    unresolved, coverage = built["unresolved"], built["coverage"]

    # Active share is an equity-sleeve measure. The builder has already normalised its direct
    # stock vector to 100%; funds, cash, bonds and fixed-income certificates do not reappear as a
    # synthetic residual bet against ACWI.
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
            "active_pct": p - b,
            "held": key in port,
            "residual": False,
            "workbook_weight_pct": (bench.get(key) or {}).get("workbook_weight_pct"),
            "yfinance_return_pct": (bench.get(key) or {}).get("yfinance_return_pct"),
            "normalisation_total": (bench.get(key) or {}).get("normalisation_total"),
            "weight_calculation": (bench.get(key) or {}).get("weight_calculation"),
        })
    active_share = sum(abs(r["active_pct"]) for r in rows) / 2.0
    overlap = sum(min(r["portfolio_pct"], r["benchmark_pct"]) for r in rows)

    # Keep the stock-sleeve Active share identity above, but display the literal AIRS share of
    # the complete book. The input weights retain that denominator even after non-stocks filter out.
    for row in rows:
        row["portfolio_pct"] *= stocks_w / 100.0
        row["active_pct"] = row["portfolio_pct"] - row["benchmark_pct"]
    rows.sort(key=lambda row: -abs(row["active_pct"]))

    held = [r for r in rows if r["held"] and not r["residual"]]
    in_bench = [r for r in held if r["benchmark_pct"] > 0]
    return {
        "available": True,
        "benchmark": benchmark,
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
        "off_benchmark_pct": sum(r["portfolio_pct"] for r in held if r["benchmark_pct"] <= 0),
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
        "rows": sorted(rows, key=lambda r: -abs(r["active_pct"])),
    }
