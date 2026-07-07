"""Resolve OpenFIGI + yfinance for every asset-pipeline row still missing either,
FAST and throttle-safe, printing per-ISIN progress (FOUND vs MISSING).

Speed: OpenFIGI is fetched in BATCHES (100 ISINs/request with a key), then each
row is resolved via `fast_resolve` — the Yahoo symbol is built directly from
OpenFIGI's ticker + Bloomberg exchange code and validated with ONE chart call
(no Yahoo search). ~1-3 Yahoo calls/ISIN instead of ~5-10, so we finish before
Yahoo soft-throttles. `--search-fallback` runs the slow search resolver for rows
the fast path can't build a symbol for (off by default).

Throttle-safety:
  * yfinance goes through the shared adaptive throttle in `yahoo.py`.
  * A CIRCUIT BREAKER aborts the run if the last `--breaker` rows that HAD a
    Yahoo symbol candidate all came back empty — the signature of a Yahoo
    soft-throttle (empty results, not a 429) — so we stop instead of mis-marking
    real companies `not_found`. Persisted rows resume on re-run.
  * Rows are processed QUEUED-FIRST (never-tried placeholders before retries).

IMPORTANT: stop the backend (`uvicorn`) or set DISABLE_SCHEDULER=1 first — its
scheduler drains the same queue and competes for the Yahoo throttle.

    uv run python scripts/asset_resolve_missing.py                 # all missing
    uv run python scripts/asset_resolve_missing.py --limit 200     # a test slice
    uv run python scripts/asset_resolve_missing.py --search-fallback
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402,F401  # loads env + Supabase before the pipeline imports
from deps import supabase  # noqa: E402

from asset_pipeline import fast_resolve as _fast  # noqa: E402
from asset_pipeline import openfigi, store, yahoo  # noqa: E402
from asset_pipeline.resolve import resolve  # noqa: E402
from asset_pipeline.yahoo import YahooThrottled  # noqa: E402

# Leonteq productTypes that ARE exchange-listed on Yahoo. Everything else (FUNDS,
# BONDS, FUTURE, LISTED_OPTION, FX) has no yfinance price series — skip the Yahoo
# call and mark it not_found directly.
_YAHOO_TYPES = {"EQUITY", "ETF"}
# Liquid control tickers across regions — a breaker's "is Yahoo actually down?"
# probe. If ANY returns data, an all-empty window is a data cluster, not throttle.
_CANARIES = ("HON", "SAP.DE", "NESN.SW", "7203.T", "SHEL.L")


def _load_targets(include_bonds: bool) -> list[str]:
    """ISINs missing yfinance (status != ok) OR OpenFIGI (openfigi_figi null),
    QUEUED-FIRST (never-tried placeholders before retries). Paginated."""
    rows: list[dict] = []
    off = 0
    while True:
        r = (
            supabase.table("asset_execution")
            .select("isin, status, openfigi_figi")
            .order("execution_id")
            .range(off, off + 999)
            .execute()
            .data
        ) or []
        rows += r
        if len(r) < 1000:
            break
        off += 1000

    def needs(x: dict) -> bool:
        if x.get("status") == "bond" and not include_bonds:
            return False
        return x.get("status") != "ok" or not x.get("openfigi_figi")

    todo = [x for x in rows if needs(x)]
    # queued (never attempted) first — that's the real new value.
    todo.sort(key=lambda x: 0 if x.get("status") == "queued" else 1)
    return [x["isin"] for x in todo]


def _load_underlying_targets() -> list[str]:
    """OK rows that are single-underlying crypto/commodity WRAPPERS whose ANALYSIS
    series isn't (yet) the right underlying — to (re-)map them to ETH-USD/BTC-USD/
    gold… Catches both self-mapped ETPs (analysis = their own listing) AND ones
    mis-mapped to the WRONG underlying (the old xbt->BTC bug). Idempotent: a row
    already on the correct underlying is skipped."""
    from asset_pipeline.resolve import (  # noqa: PLC0415
        _detect_underlying, _is_leveraged, _looks_like_wrapper,
    )
    execs: list[dict] = []
    off = 0
    while True:
        r = (
            supabase.table("asset_execution").select("isin, name, asset_class, analysis_id")
            .eq("status", "ok").range(off, off + 999).execute().data
        ) or []
        execs += r
        if len(r) < 1000:
            break
        off += 1000
    # current analysis symbol per execution (chunked lookup)
    aids = sorted({e["analysis_id"] for e in execs if e.get("analysis_id")})
    sym_by_id: dict[int, str] = {}
    for i in range(0, len(aids), 200):
        r = (
            supabase.table("asset_analysis").select("analysis_id, symbol")
            .in_("analysis_id", aids[i:i + 200]).execute().data
        ) or []
        for a in r:
            sym_by_id[a["analysis_id"]] = a.get("symbol")
    out: list[str] = []
    for e in execs:
        nm = e.get("name") or ""
        u = _detect_underlying(nm)
        if not u or _is_leveraged(nm) or not _looks_like_wrapper(nm, e.get("asset_class") or ""):
            continue
        cur = sym_by_id.get(e.get("analysis_id"))
        if cur not in u[2]:            # not already on the (a) correct underlying
            out.append(e["isin"])
    return out


def _load_leonteq_csv(path: Path) -> dict[str, tuple[str, str, str, str]]:
    """{ISIN: (bloomberg_ticker, ric, name, productType)} from the lynqs CSV — the
    identity the fast path builds Yahoo symbols from + the type gate. Missing file
    -> {} (OpenFIGI-only)."""
    out: dict[str, tuple[str, str, str, str]] = {}
    if not path.exists():
        return out
    with path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            isin = (r.get("isin") or "").strip().upper()
            if isin:
                out[isin] = (r.get("ticker") or "", r.get("ric") or "",
                             r.get("name") or "", (r.get("productType") or "").upper())
    return out


def _load_relist_targets(leonteq: dict[str, tuple]) -> dict[str, float]:
    """{ISIN: current_adv} for OK equities whose RESOLVED listing differs from the
    Leonteq PRIMARY. The Leonteq bbg ticker/RIC name the intended exchange (`NVDA UQ`
    / `NVDA.OQ` -> NVDA), so any row whose stored `yahoo_symbol` isn't that primary
    is on the wrong listing (NVIDIA on NVD.SG). Re-resolving re-points it. The
    current ADV is carried so the re-resolve only UPGRADES — a throttled miss on the
    primary can't strand it worse. Idempotent."""
    out: dict[str, float] = {}
    off = 0
    while True:
        r = (
            supabase.table("asset_execution").select("isin, yahoo_symbol, med_adv_eur")
            .eq("status", "ok").eq("asset_class", "equity")
            .range(off, off + 999).execute().data
        ) or []
        for x in r:
            lq = leonteq.get((x.get("isin") or "").upper())
            if not lq:
                continue
            prim = _fast.build_candidates(x["isin"], [], (lq[0], lq[1]))
            cur = (x.get("yahoo_symbol") or "").upper()
            if prim and prim[0].upper() != cur:
                out[x["isin"]] = x.get("med_adv_eur") or 0.0
        if len(r) < 1000:
            break
        off += 1000
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Fast, throttle-safe OpenFIGI+yfinance fill.")
    ap.add_argument("--workers", type=int, default=4, help="concurrent resolves (Yahoo's own semaphore caps ~4; default 4)")
    ap.add_argument("--limit", type=int, default=0, help="cap rows (0 = all)")
    ap.add_argument("--include-bonds", action="store_true", help="also retry status='bond'")
    ap.add_argument("--search-fallback", action="store_true", help="slow search resolve when the fast path can't build a symbol")
    ap.add_argument("--breaker", type=int, default=40, help="abort after N consecutive candidate-having rows that all returned empty (throttle guard; 0 = off)")
    ap.add_argument("--leonteq-csv", default=str(Path(__file__).resolve().parent.parent / "lynqs_universe_all.csv"),
                    help="lynqs CSV for the Bloomberg ticker/RIC identity the fast path uses")
    ap.add_argument("--reresolve-underlyings", action="store_true",
                    help="re-map already-resolved single-underlying crypto/commodity ETPs to their underlying (ETH-USD/BTC-USD/…)")
    ap.add_argument("--reresolve-listing", action="store_true",
                    help="re-resolve US equities stranded on a thin foreign listing to their most-liquid (US primary) listing")
    args = ap.parse_args()

    leonteq = _load_leonteq_csv(Path(args.leonteq_csv))
    print(f"Loaded {len(leonteq):,} Leonteq identities from {args.leonteq_csv}", flush=True)

    relist_old: dict[str, float] = {}
    if args.reresolve_underlyings:
        targets = _load_underlying_targets()
    elif args.reresolve_listing:
        relist_old = _load_relist_targets(leonteq)
        targets = list(relist_old.keys())
    else:
        targets = _load_targets(args.include_bonds)
    if args.limit:
        targets = targets[: args.limit]
    total = len(targets)
    if not total:
        print("Nothing to do - every row already has OpenFIGI + yfinance.", flush=True)
        return

    figi_chunk = 100 if os.environ.get("OPENFIGI_API_KEY") else 10
    print(f"Resolving {total:,} rows (fast path, OpenFIGI batches of {figi_chunk}, "
          f"{args.workers} concurrent"
          f"{', search fallback' if args.search_fallback else ''})...\n", flush=True)

    stop = threading.Event()          # circuit-breaker / throttle trip
    banned = threading.Event()        # hard Yahoo ban
    plock = threading.Lock()
    blk = threading.Lock()            # serialize the breaker's canary check
    counts = {"found": 0, "missing": 0, "retry": 0, "error": 0}
    done = {"n": 0}
    recent: deque[int] = deque(maxlen=max(1, args.breaker))  # 1=found, 0=had-candidate-but-empty
    t0 = time.time()

    def _log(isin: str, verdict: str, of: bool, yf: bool, name: str, tail: str) -> None:
        with plock:
            done["n"] += 1
            mark = f"[OF:{'ok' if of else 'x '} yf:{'ok' if yf else 'x '}]"  # ASCII
            print(f"[{done['n']:>5}/{total}] {isin}  {verdict:7}  {mark}  {name[:38]:38}  {tail}", flush=True)

    def _one(isin: str, figi_map: dict) -> None:
        if stop.is_set() or banned.is_set():
            return
        rows = figi_map.get(isin.strip().upper(), [])
        fig = openfigi.extract_columns(rows)
        of_found = bool(fig.get("openfigi_figi"))
        lq = leonteq.get(isin.strip().upper())
        lq_id = (lq[0], lq[1]) if lq else None                 # (bbg_ticker, ric)
        name_hint = fig.get("openfigi_name") or (lq[2] if lq else None)
        ptype = lq[3] if lq else ""
        # Type gate: FUNDS / BONDS / FUTURE / LISTED_OPTION / FX aren't yfinance
        # equities — mark not_found WITHOUT a Yahoo call (and don't feed the breaker).
        if lq and ptype not in _YAHOO_TYPES:
            store.upsert_unmapped(isin, "not_found", f"Leonteq productType={ptype} - no yfinance series",
                                  None, name_hint, figi=fig)
            counts["missing"] += 1
            _log(isin, "SKIP", of_found, False, name_hint or "", f"({ptype})")
            return
        had_candidate = bool(_fast.build_candidates(isin, rows, lq_id))
        try:
            res = _fast.fast_resolve(isin, rows, name_hint, lq_id)
            if res is None and args.search_fallback:
                res = resolve(isin, with_candles=False, figi_hint=fig)
                if not (res.get("analysis") or {}).get("symbol"):
                    res = None
            if res is None and had_candidate:
                # We BUILT a valid Yahoo symbol but got nothing back — ambiguous
                # (genuinely delisted vs Yahoo soft-throttle returning empty). Do
                # NOT persist not_found; leave the row 'queued' so a later pass
                # retries it. This makes the run throttle-safe by construction.
                recent.append(0)
                counts["retry"] += 1
                _log(isin, "RETRY", of_found, False, name_hint or "", "(left queued)")
            elif res is None:
                # No Yahoo symbol could be built from Leonteq/OpenFIGI identity —
                # genuinely unresolvable by the fast path. Mark not_found (-> the
                # grid's missing badges).
                store.upsert_unmapped(isin, "not_found", "fast path: no Yahoo symbol candidate",
                                      None, fig.get("openfigi_name"), figi=fig)
                counts["missing"] += 1
                _log(isin, "MISSING", of_found, False, fig.get("openfigi_name") or "", "")
            else:
                if args.reresolve_listing:
                    # Upgrade-only: never replace the current listing with a
                    # thinner one (a throttled miss on the primary must not strand
                    # us worse than we already are).
                    old = relist_old.get(isin.strip().upper(), 0.0)
                    new_adv = (res.get("execution") or {}).get("med_adv_eur") or 0.0
                    if new_adv <= old:
                        recent.append(1)
                        _log(isin, "KEEP", of_found, True, name_hint or "",
                             f"(kept EUR{old/1e6:.1f}M; no more-liquid listing)")
                        return
                ids = store.upsert_asset(res, figi=fig)
                an = res["analysis"]
                r = store.store_series(ids["analysis_id"], an["symbol"], an.get("first_ts"))
                recent.append(1)
                counts["found"] += 1
                ex = res.get("execution") or {}
                _log(isin, "FOUND", of_found, True, ex.get("name") or an.get("name") or "",
                     f"-> {an.get('symbol')} ({ex.get('currency') or '?'}, {r:,} bars)")
        except YahooThrottled:
            banned.set()
            with plock:
                print(f"  ... {isin}  Yahoo HARD rate-limit - stopping.", flush=True)
            return
        except Exception as e:  # noqa: BLE001
            counts["error"] += 1
            _log(isin, "ERROR", of_found, False, f"{type(e).__name__}: {e}", "")
        # Circuit breaker: a full window of candidate-having rows that ALL came
        # back empty COULD be a soft-throttle — but also a legit cluster of
        # unpriceable names. Verify with control tickers before stopping: if any
        # canary returns data, Yahoo is fine (false alarm) -> reset and continue.
        if args.breaker and len(recent) == recent.maxlen and sum(recent) == 0 and not stop.is_set():
            with blk:
                if len(recent) == recent.maxlen and sum(recent) == 0 and not stop.is_set():
                    alive = any(yahoo.chart(c, rng="3mo") for c in _CANARIES[:2])
                    if alive:
                        recent.clear()  # not throttle, just a cluster of empties
                    else:
                        stop.set()
                        with plock:
                            print("\n!! Circuit breaker: window all-empty AND control tickers empty "
                                  "- Yahoo is soft-throttling. Stopping to avoid mis-marking rows. "
                                  "Wait ~20-30 min and re-run to resume.", flush=True)

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        for cstart in range(0, total, figi_chunk):
            if stop.is_set() or banned.is_set():
                break
            chunk = targets[cstart:cstart + figi_chunk]
            try:
                figi_map = openfigi.lookup_isins(chunk)
            except Exception:  # noqa: BLE001
                figi_map = {}
            futures = [ex.submit(_one, isin, figi_map) for isin in chunk]
            for _f in as_completed(futures):
                pass

    try:
        store.set_default_executions()
    except Exception:  # noqa: BLE001
        pass

    dt = time.time() - t0
    print(f"\nDone in {dt:0.0f}s - {counts['found']:,} found, {counts['retry']:,} left-queued (retry), "
          f"{counts['missing']:,} missing (no symbol), {counts['error']:,} errors of {total:,} "
          f"({done['n']:,} processed).", flush=True)
    if stop.is_set() or banned.is_set():
        print("Stopped early (throttle) - re-run to resume the rest.", flush=True)
        sys.exit(3)


if __name__ == "__main__":
    main()
