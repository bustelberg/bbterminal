"""Backfill market_cap_eur on every analysis instrument via Yahoo's v7 quote.

Market cap is LISTING-INDEPENDENT (a company's size regardless of which listing
our price series resolved to) — the robust liquidity/size signal where per-listing
ADV isn't (a US mega-cap stranded on a thin foreign line reads €1.6M ADV but is
still a $4T company). But a SECONDARY listing (NVD.SG) returns null marketCap, so
per analysis instrument we query TWO candidate symbols — the resolved analysis
symbol AND the Leonteq-derived PRIMARY (NVDA from `NVDA UQ`) — and take whichever
Yahoo returns a market cap for. Converted to EUR via the fx_rate the rest of the
pipeline uses. Batched (100/quote request); re-runnable.

    uv run python scripts/asset_backfill_marketcap.py
    uv run python scripts/asset_backfill_marketcap.py --only-missing
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402,F401
from deps import supabase  # noqa: E402

from asset_pipeline import yahoo  # noqa: E402
from asset_pipeline.fast_resolve import _from_leonteq  # noqa: E402


def _load_leonteq(path: Path) -> dict[str, tuple[str, str]]:
    out: dict[str, tuple[str, str]] = {}
    if not path.exists():
        return out
    with path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            isin = (r.get("isin") or "").strip().upper()
            if isin:
                out[isin] = (r.get("ticker") or "", r.get("ric") or "")
    return out


def _paged(table: str, select: str, **eq) -> list[dict]:
    rows: list[dict] = []
    off = 0
    while True:
        q = supabase.table(table).select(select)
        for k, v in eq.items():
            q = q.eq(k, v)
        r = q.range(off, off + 999).execute().data or []
        rows += r
        if len(r) < 1000:
            break
        off += 1000
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill market_cap_eur via Yahoo quote.")
    ap.add_argument("--only-missing", action="store_true", help="skip analyses that already have market_cap_eur")
    ap.add_argument("--leonteq-csv", default=str(Path(__file__).resolve().parent.parent / "lynqs_universe_all.csv"))
    args = ap.parse_args()

    leonteq = _load_leonteq(Path(args.leonteq_csv))
    analyses = _paged("asset_analysis", "analysis_id, symbol, asset_class, market_cap_eur")
    if args.only_missing:
        analyses = [a for a in analyses if a.get("market_cap_eur") is None]
    # one execution isin per analysis (to build the Leonteq primary symbol)
    execs = _paged("asset_execution", "analysis_id, isin")
    isin_by_analysis: dict[int, str] = {}
    for e in execs:
        aid = e.get("analysis_id")
        if aid is not None and aid not in isin_by_analysis:
            isin_by_analysis[aid] = e.get("isin")

    # Per analysis: candidate quote symbols = the resolved symbol + the Leonteq
    # primary (equities/ETFs only; a crypto/commodity underlying like BTC-USD is
    # itself the market-cap source).
    cand_by_analysis: dict[int, list[str]] = {}
    all_syms: set[str] = set()
    for a in analyses:
        aid, sym, ac = a["analysis_id"], a.get("symbol"), a.get("asset_class")
        cands: list[str] = []
        if sym:
            cands.append(sym)
        if ac in ("equity", "etf"):
            isin = isin_by_analysis.get(aid)
            lq = leonteq.get((isin or "").upper()) if isin else None
            if lq:
                prim = _from_leonteq(lq[0], lq[1])
                if prim:
                    cands.append(prim[0])
        seen: dict[str, None] = {}
        for c in cands:
            if c:
                seen.setdefault(c, None)
        cand_by_analysis[aid] = list(seen.keys())
        all_syms.update(seen.keys())

    print(f"{len(analyses):,} analyses, {len(all_syms):,} distinct quote symbols — fetching…", flush=True)
    quotes = yahoo.quote(sorted(all_syms))
    print(f"quote returned {len(quotes):,} symbols with data.", flush=True)

    now = datetime.now(timezone.utc).isoformat()
    updated = mcap = 0
    for a in analyses:
        aid = a["analysis_id"]
        native = ccy = None
        for c in cand_by_analysis.get(aid, []):
            q = quotes.get(c)
            if q and q.get("marketCap"):
                native, ccy = q["marketCap"], q.get("currency")
                break
        eur = None
        if native and ccy:
            fx = yahoo.fx_to_eur(ccy) or 0.0
            eur = round(native * fx, 2) if fx else None
        supabase.table("asset_analysis").update({
            "market_cap_native": native, "market_cap_currency": ccy,
            "market_cap_eur": eur, "market_cap_checked_at": now,
        }).eq("analysis_id", aid).execute()
        updated += 1
        if eur:
            mcap += 1
        if updated % 500 == 0:
            print(f"  {updated:,}/{len(analyses):,} written ({mcap:,} with a market cap)…", flush=True)

    print(f"\nDone — {updated:,} analyses updated, {mcap:,} have a market cap.", flush=True)


if __name__ == "__main__":
    main()
