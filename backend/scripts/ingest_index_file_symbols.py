"""Ingest the index file's constituents that have no asset yet — SYMBOL-FIRST, no ISIN needed.

    uv run python scripts/ingest_index_file_symbols.py                    # report only
    uv run python scripts/ingest_index_file_symbols.py --limit 5 --apply  # prove the path
    uv run python scripts/ingest_index_file_symbols.py --apply            # the whole backlog

⚠⚠ THIS IS WHAT CLOSES THE ACWI COVERAGE WARNING, AND THE REASON IT CAN EXIST IS THAT
   `asset_analysis` IS KEYED ON `symbol`, NOT ON AN ISIN. The add-by-ISIN pipeline
   (`asset_execution.isin` is NOT NULL) is the usual door into the asset world, and for these names
   it is a locked one: the iShares export carries Ticker + Exchange and **no ISIN column**,
   GuruFocus does not sell us India at all, and OpenFIGI maps ISIN->FIGI, never ticker->ISIN. So
   "get the ISINs first" has no supplier. It also turns out not to be needed — `WIPRO.NS` has
   **7,659 bars and no `asset_execution` row at all**, which is the existing proof that a
   symbol-only asset works end to end. `index_file_membership` links `universe_id -> analysis_id`
   directly, so an asset created here joins the rebuilt index without ever touching an ISIN.

WHAT IT CLOSES, measured 2026-09-02 against the 15-Apr-2026 file:

    iShares ACWI file            2,270 equity rows
      Yahoo symbol derivable     2,224   (45 fail on an exchange missing from `EXCHANGE_SUFFIX`
                                          — Santiago, Philippines, Kuwait, Kosdaq: a separate fix)
      already an asset           1,708
      MISSING                      516   <- this script
        National Stock Exchange Of India   159   <- the whole India gap
        XBSP (B3 Brazil)                    38
        Toronto Stock Exchange              32
        Korea Exchange                      28
        Johannesburg                        27

⚠ IT DOES NOT LINK ANYTHING. Creating the assets is all this does; `index_universe.acwi.
  asset_membership.sync()` is what re-derives `index_file_membership`, and it should be run after.
  Two steps on purpose: the ingest is slow and interruptible, the link is fast and idempotent, and
  a half-ingested run should still leave a consistent membership.

⚠⚠ PACED, AND IT STOPS ON A THROTTLE RATHER THAN PUSHING THROUGH. Yahoo answers an overloaded
  caller with an EMPTY LIST, not a 429 — so hammering it does not fail loudly, it silently writes
  assets with no bars and marks names as having no data when they have plenty. `asset_pipeline.
  yahoo` already paces every request and raises `YahooThrottled` after its cooldowns; that
  exception is deliberately allowed to end the run. Re-running resumes: anything already created is
  skipped by the symbol lookup.

⚠ AND IT STANDS DOWN IF THE INGEST WORKER IS LIVE, for the same reason — two concurrent Yahoo
  consumers is exactly the load that produces the empty answers.
"""
from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: F401,E402  — loads .env before anything reads it
from asset_pipeline import store, yahoo  # noqa: E402
from asset_pipeline.queue import is_worker_active  # noqa: E402
from deps import IN_CHUNK_SIZE, supabase  # noqa: E402
from index_universe.acwi.holdings import load_acwi_holdings  # noqa: E402
from index_universe.acwi.yahoo_map import yahoo_symbol  # noqa: E402


def _wanted() -> tuple[dict[str, dict], Counter]:
    """{yahoo_symbol: file row} for every equity line we can name, + the unmappable exchanges."""
    rows, as_of = load_acwi_holdings()
    print(f"iShares ACWI file: {len(rows)} equity rows, as of {as_of}")
    want: dict[str, dict] = {}
    unmapped: Counter = Counter()
    for r in rows:
        sym = yahoo_symbol(r.get("Ticker", ""), r.get("Exchange", ""), r.get("Location", ""))
        if sym:
            want[sym] = r
        else:
            unmapped[r.get("Exchange", "?")] += 1
    return want, unmapped


def _existing(symbols: list[str]) -> set[str]:
    have: set[str] = set()
    for i in range(0, len(symbols), IN_CHUNK_SIZE):
        for a in (supabase.table("asset_analysis").select("symbol")
                  .in_("symbol", symbols[i:i + IN_CHUNK_SIZE]).execute().data or []):
            have.add(a["symbol"])
    return have


def _create(symbol: str, row: dict) -> int:
    """Create the `asset_analysis` row, then pull its full series. Returns bars stored.

    ⚠ THE ASSET IS ONLY KEPT IF IT HAS BARS. An asset row with no series is worse than no row: it
    joins the index, contributes a name, and prices nothing — which is precisely the silent
    weight-redistribution the coverage warning exists to report. A symbol Yahoo will not price is
    deleted again and reported, not left behind looking ingested.

    ⚠⚠ THE CURRENCY COMES FROM YAHOO'S CHART META, NEVER FROM THE FILE'S `Currency` COLUMN. That
    column is the FUND's reporting currency — every row of the iShares export says `USD`, with the
    listing currency recoverable only via `FX Rate` — so taking it would have stamped **USD on
    every Indian stock**, which trade in INR at ~93 to the dollar. Nothing would have errored:
    `asset_price` would hold correct INR closes labelled USD, and every EUR conversion downstream
    would be wrong by the INR/USD rate while looking entirely plausible. Caught only by reading the
    rows back after the first three ingested. Yahoo's own `meta.currency` says `INR`.

    ⚠ ONE CHART CALL, NOT TWO. `store_series(…, first_ts=None)` probes `rng=3mo` itself to find the
    first trade date; fetching that probe here and passing `first_ts` in means the currency costs
    no extra request.
    """
    meta = (yahoo.chart(symbol, rng="3mo") or {}).get("meta") or {}
    first_ts = meta.get("firstTradeDate")
    ins = (supabase.table("asset_analysis").insert({
        "symbol": symbol,
        "label": (row.get("Name") or symbol)[:200],
        "sector": row.get("Sector") or None,
        "currency": meta.get("currency") or None,
        "asset_class": "equity",
    }).execute().data or [])
    if not ins:
        return 0
    aid = ins[0]["analysis_id"]
    try:
        bars = store.store_series(aid, symbol, first_ts) or 0
    except Exception:
        supabase.table("asset_analysis").delete().eq("analysis_id", aid).execute()
        raise
    if bars <= 0:
        supabase.table("asset_analysis").delete().eq("analysis_id", aid).execute()
        return 0
    return bars


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="actually create assets (default: report)")
    ap.add_argument("--limit", type=int, default=0, help="cap how many to ingest (0 = all)")
    ap.add_argument("--exchange", default=None,
                    help="only this file Exchange (e.g. 'National Stock Exchange Of India')")
    args = ap.parse_args()

    want, unmapped = _wanted()
    if args.exchange:
        want = {s: r for s, r in want.items() if r.get("Exchange") == args.exchange}
    syms = sorted(want)
    have = _existing(syms)
    missing = [s for s in syms if s not in have]

    print(f"  symbol derivable : {len(syms)}")
    print(f"  already an asset : {len(have)}")
    print(f"  MISSING          : {len(missing)}")
    if unmapped and not args.exchange:
        print(f"  no symbol at all : {sum(unmapped.values())} "
              f"(exchange not in EXCHANGE_SUFFIX) -> {unmapped.most_common(4)}")
    by_ex = Counter(want[s].get("Exchange", "?") for s in missing)
    for ex, n in by_ex.most_common(8):
        print(f"    {ex:38s} {n}")

    if not args.apply:
        print(f"\n  DRY RUN - nothing written. Re-run with --apply to ingest {len(missing)}.")
        return 0

    # (!) see the module note: two Yahoo consumers is how empty answers get manufactured.
    if is_worker_active():
        print("\n  REFUSING: the asset ingest worker is live. Let it finish first.",
              file=sys.stderr)
        return 1

    todo = missing[:args.limit] if args.limit else missing
    print(f"\n  ingesting {len(todo)}...")
    made = skipped = 0
    t0 = time.perf_counter()
    for i, sym in enumerate(todo, 1):
        try:
            bars = _create(sym, want[sym])
        except yahoo.YahooThrottled as e:
            # (!) STOP. Pushing through a throttle is how empty series get written as fact.
            print(f"  [{i}/{len(todo)}] {sym}: THROTTLED ({e}) - stopping. Re-run later to resume.",
                  file=sys.stderr)
            break
        except Exception as e:  # noqa: BLE001 — one bad symbol must not end the run
            print(f"  [{i}/{len(todo)}] {sym}: {type(e).__name__}: {e}", file=sys.stderr)
            skipped += 1
            continue
        if bars:
            made += 1
            print(f"  [{i}/{len(todo)}] {sym}: {bars} bars", flush=True)
        else:
            skipped += 1
            print(f"  [{i}/{len(todo)}] {sym}: no series - not kept", flush=True)

    print(f"\n  created {made}, skipped {skipped}, in {time.perf_counter() - t0:.0f}s")
    print("  NEXT: re-link membership with "
          "`uv run python -c \"from index_universe.acwi.asset_membership import sync; "
          "print(sync())\"`")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
