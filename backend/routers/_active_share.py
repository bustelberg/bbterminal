"""Active share — how much of the book's EQUITY sleeve is not the benchmark.

    AS = ½ · Σ |wᵢᵖ − wᵢᵇ|          Overlap = Σ min(wᵢᵖ, wᵢᵇ) = 1 − AS

0% is the index; 100% shares no name with it. The ½ is not cosmetic: every overweight has a
matching underweight by construction (both vectors sum to 1), so without it every difference is
counted twice.

⚠⚠ THE MATCH IS ON THE ISSUER, NOT ON THE LINE, AND THIS IS THE WHOLE DIFFICULTY. Alphabet A and
Alphabet C are two ISINs and one company; an ADR and its home line are two ISINs and one company.
Matching on ISIN alone reports a book that holds GOOG against an index carrying GOOGL as having NO
overlap in Alphabet — a ~4% swing on a US book, invented entirely by the identifier. So both sides
collapse to an issuer key first, and both sides SUM their lines into it.

⚠ THE KEY IS THE ONE `_asset_benchmark.members` ALREADY DEDUPES ON — the GuruFocus company name,
normalised. That is not a convenience: the index side is already collapsed by it (Yahoo reports the
full company cap on every share class, so `members` keeps one row per company), so keying the
portfolio the same way is what makes the two vectors talk about the same objects. A second notion
of "same issuer" here would disagree with the index's own dedupe, and the disagreement would show
up as active share.

⚠ BOTH VECTORS MUST SUM TO 1 OR THE ½ IS WRONG. The portfolio is renormalised over the individual
stocks alone — funds, cash, bonds and unpriceable lines are dropped, not zero-weighted. Leaving
cash in at its real weight would report liquidity as "active", which is a defensible measure but a
DIFFERENT one, and mixing the two is how a 30% figure becomes uncomparable with anybody else's.
`stocks_pct` says how much of the book this sleeve actually is, so the renormalisation is never
silent.

⚠ THE BENCHMARK IS NOT FLOAT-ADJUSTED and is priced over the names we can bridge — the same two
caveats `_asset_benchmark` carries, and they land differently here than on a return. A missing
constituent does not reduce the index's weight in the rest, it INFLATES it (renormalisation), so an
unheld name we cannot price makes active share read slightly LOW. `benchmark_covered_pct` is
returned so the figure is never read as exact.
"""
from __future__ import annotations

import re

from common.pg import load_rows_via_copy
from deps import IN_CHUNK_SIZE, supabase

# What a portfolio line must have before it can be compared with an index at all.
# ⚠ `currency` IS THE LISTING'S, and the exposure view needs it — see `_portfolio_exposure`.
_GRID_COLS = "isin,gf_company_name,name,currency,status,analysis_id"

# Trailing corporate forms, stripped before two names are called the same issuer. Deliberately
# SHORT and suffix-only — see `_issuer_key`.
_SUFFIX = re.compile(
    r"[\s,.]+(inc|inc\.|corp|corporation|co|company|plc|ltd|limited|nv|n\.v\.|sa|s\.a\.|ag|se|ab|"
    r"as|a/s|oyj|spa|s\.p\.a\.|holding|holdings|group|the)\.?$", re.I)

# A SHARE-CLASS or depositary marker — the part of a name that identifies a LINE, not a company.
# ⚠ ANCHORED AT THE END and applied in the same loop as `_SUFFIX`; see `_issuer_key`.
_CLASS = re.compile(
    r"[\s,.]+(class\s+[a-c]|cl\s+[a-c]|series\s+[a-c]|[ab]\s+shares|shares\s+[ab]|"
    r"sponsored\s+adr|unsponsored\s+adr|adr|ads|reg\s*s|registered)\.?$", re.I)


def _issuer_key(name: str | None) -> str:
    """The comparison key for one issuer.

    ⚠⚠ IT MUST MATCH `_asset_benchmark.members`' OWN DEDUPE KEY, which is
    `(gf_company_name or name).strip().lower()`. Everything below is applied to BOTH sides, so the
    two stay in step; the suffix strip is the only thing added, and it is added on both.

    ⚠ SUFFIX-ONLY, AND NOT A FUZZY MATCH. `asset_pipeline.resolve.same_company` exists for
    resolving an unknown listing and scores similarity — the wrong tool here, because a false
    positive is not a bad listing, it is two different companies fused into one row of a risk
    report. `token_set_ratio` scores a SUBSET as 100 (`eur` ⊂ `Shell PLC EUR`), which is exactly
    the failure mode a holdings list is full of. Exact-after-normalising refuses more than it
    should and never invents an overlap.

    ⚠⚠ THE TWO STRIPS ALTERNATE UNTIL THE NAME STOPS CHANGING, AND RUNNING THEM IN SEQUENCE IS A
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
        # ⚠ ONE ROW PER ISIN, PREFERRING ONE THAT CARRIES A COMPANY NAME. `asset_grid` is one row
        # per EXECUTION, so a company traded on several venues appears several times and only some
        # of those rows may have bridged to a company. Taking whichever came back first would make
        # the issuer key depend on row order.
        prev = out.get(key)
        if prev is None or ((r.get("gf_company_name") or "") and not (prev.get("gf_company_name") or "")):
            out[key] = r
    return out


def _fold(entries: list[tuple[str, str, float]]) -> dict[str, dict]:
    """Sum weights onto issuer keys. `entries` is (key, display name, weight).

    ⚠ SUMMING, NOT REPLACING — a book holding both Alphabet classes holds ONE position in Alphabet
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


def build_issuer_weights(holdings: list[dict], benchmark: str) -> dict:
    """Both sides of the comparison, folded onto ISSUER keys and each summing to 100.

    ⚠⚠ SHARED BY ACTIVE SHARE AND CONCENTRATION so the two describe the same objects. Both ask
    questions about ISSUERS — "how far from the index is this book", "how much of it sits in its
    ten largest" — and a second folding here would let a book hold 49 lines on one view and 47
    issuers on the other with nothing on screen saying why.

    Returns `{port, bench, stocks_w, total_all, unresolved, coverage}`; `port` and `bench` are
    `{key: {key, name, weight_pct}}`, each summing to 100. Raises `IssuerError` rather than
    returning a sentinel — every caller turns it into the same `{available: False, reason}`.
    """
    from routers._asset_benchmark import members  # noqa: PLC0415  (module cycle)

    # ── the book: individual stocks only, renormalised to 1 ────────────────────────────────────
    total_all = sum(float(h.get("weight_pct") or 0) for h in holdings)
    stocks = [h for h in holdings
              if not h.get("is_fund")
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
        # ⚠ AN UNKEYABLE LINE STILL COUNTS AS ACTIVE, and falls back to its ISIN so it can never
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
    mem, coverage = members(benchmark)
    cap_total = sum(float(m.get("market_cap_eur") or 0) for m in mem)
    if not mem or cap_total <= 0:
        raise IssuerError(f"No priced constituents for {benchmark}.")
    bench = _fold([(_issuer_key(m.get("company_name")) or f"isin:{(m.get('isin') or '').upper()}",
                    m.get("company_name") or "", float(m.get("market_cap_eur") or 0) / cap_total * 100.0)
                   for m in mem if float(m.get("market_cap_eur") or 0) > 0])


    return {"port": port, "bench": bench, "stocks_w": stocks_w, "total_all": total_all,
            "unresolved": unresolved, "coverage": coverage}


def compute_active_share(holdings: list[dict], benchmark: str) -> dict:
    """Active share of the book's individual stocks against `benchmark`.

    `holdings` are the rows the Analyse modal is already showing — `{isin, weight_pct, name,
    is_fund}`. ⚠ THE WEIGHTS ARE THE ONES ON SCREEN, taken rather than recomputed: this panel sits
    one click from the Holdings table, and a risk figure derived from a second weighting would be
    a number the table cannot be made to reproduce.
    """
    try:
        built = build_issuer_weights(holdings, benchmark)
    except IssuerError as e:
        return {"available": False, "reason": e.reason, "benchmark": benchmark}
    port, bench = built["port"], built["bench"]
    stocks_w, total_all = built["stocks_w"], built["total_all"]
    unresolved, coverage = built["unresolved"], built["coverage"]

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
        })
    active_share = sum(abs(r["active_pct"]) for r in rows) / 2.0
    overlap = sum(min(r["portfolio_pct"], r["benchmark_pct"]) for r in rows)

    held = [r for r in rows if r["held"]]
    in_bench = [r for r in held if r["benchmark_pct"] > 0]
    return {
        "available": True,
        "benchmark": benchmark,
        "active_share_pct": active_share,
        # ⚠ RETURNED, NOT LEFT TO THE CLIENT AS `100 − AS`. The identity is the POINT (it is what
        # makes active share a share of something), and a reader checking it against the two
        # printed numbers must find it holds to the digit rather than to the rounding.
        "overlap_pct": overlap,
        "stocks_pct": (stocks_w / total_all * 100.0) if total_all > 0 else None,
        "n_holdings": len(port),
        "n_in_benchmark": len(in_bench),
        # The book's weight that sits in names the index does not hold at all — the part of active
        # share that is SELECTION rather than sizing.
        "off_benchmark_pct": sum(r["portfolio_pct"] for r in held if r["benchmark_pct"] <= 0),
        "benchmark_members": len(bench),
        "benchmark_covered_pct": coverage.get("covered_pct"),
        "unresolved": sorted(unresolved, key=lambda r: -r["weight_pct"]),
        # Everything the book holds, plus the index names it is most under-weight in. Sorted by the
        # size of the bet either way, which is the order a risk report is read in.
        "rows": sorted(rows, key=lambda r: -abs(r["active_pct"])),
    }
