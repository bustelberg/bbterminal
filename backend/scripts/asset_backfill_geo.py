"""Backfill country / continent / MSCI region onto the asset pipeline.

Three phases, runnable independently (`--only`):

  listing   ZERO Yahoo calls. Derives the listing country from data we already
            store: `asset_execution.exchange` and the analysis symbol's suffix.
            Covers ETFs and every resolved row. Seconds, not minutes.
  domicile  One Yahoo v10 assetProfile request per EQUITY symbol (no batch
            endpoint exists). ~6.5k symbols, paced by the shared throttle at
            YAHOO_RPS. ETFs/crypto/futures have no assetProfile and are skipped.
  derive    ZERO Yahoo calls. Folds (domicile, listing) into continent +
            msci_region for every analysis asset.

Re-runnable. `domicile` fetches only equities never checked before, so a symbol
Yahoo ANSWERED for is never re-fetched (even when it had no profile), while a
symbol Yahoo did NOT answer for stays unstamped and IS retried on the next run
— see `yahoo.asset_profile` for that distinction. `--refresh` re-fetches all.

    uv run python scripts/asset_backfill_geo.py                    # all phases
    uv run python scripts/asset_backfill_geo.py --only listing     # no Yahoo
    uv run python scripts/asset_backfill_geo.py --only domicile --limit 50
    uv run python scripts/asset_backfill_geo.py --refresh          # re-fetch all
    uv run python scripts/asset_backfill_geo.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

# The console is cp1252 on Windows; an accented country name arriving from Yahoo
# would otherwise raise UnicodeEncodeError mid-run.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:  # noqa: BLE001
    pass

import deps  # noqa: E402,F401
from deps import chunked, supabase  # noqa: E402

from asset_pipeline import geo, yahoo  # noqa: E402
from asset_pipeline.yahoo import YahooThrottled  # noqa: E402


def _p(msg: str = "") -> None:
    print(msg, flush=True)


def _paged(table: str, select: str) -> list[dict]:
    rows: list[dict] = []
    off = 0
    while True:
        r = supabase.table(table).select(select).range(off, off + 999).execute().data or []
        rows += r
        if len(r) < 1000:
            break
        off += 1000
    return rows


def _update_analyses(symbols: list[str], payload: dict, dry: bool) -> None:
    """Set `payload` on many analyses at once, chunked (Cloudflare 502 guard).

    Grouped `.in_()` updates rather than a bulk upsert: an upsert would have to
    satisfy asset_execution/asset_analysis NOT NULL columns on the INSERT arm
    before ever reaching ON CONFLICT."""
    if dry or not symbols:
        return
    for chunk in chunked(symbols):
        supabase.table("asset_analysis").update(payload).in_("symbol", chunk).execute()


# ---------------------------------------------------------------------------
# Phase 1 — listing country (no Yahoo)
# ---------------------------------------------------------------------------
def phase_listing(dry: bool) -> None:
    _p("=" * 72)
    _p("PHASE listing  -- listing country from stored data (0 Yahoo calls)")
    _p("=" * 72)

    # Executions: one UPDATE per distinct exchange (~58), not per row (~7.8k).
    execs = _paged("asset_execution", "exchange, listing_country")
    stored: dict[str, set] = defaultdict(set)
    for e in execs:
        stored[e.get("exchange")].add(e.get("listing_country"))
    unmapped: Counter = Counter(e["exchange"] for e in execs
                                if e.get("exchange") and not geo.country_from_exchange(e["exchange"]))

    n_ex = 0
    for exchange, seen in stored.items():
        if not exchange:
            continue
        want = geo.country_from_exchange(exchange)
        if seen == {want}:
            continue  # already correct for every row on this venue
        _p(f"  exec  {exchange:<24} -> {want or '(unmapped)'}")
        if not dry:
            supabase.table("asset_execution").update(
                {"listing_country": want}
            ).eq("exchange", exchange).execute()
        n_ex += 1
    _p(f"  asset_execution : {len(execs):,} rows across {len(stored):,} venues, {n_ex:,} venues updated")

    # Analyses: group symbols by target country, one chunked UPDATE per country.
    analyses = _paged("asset_analysis", "symbol, asset_class, listing_country")
    by_country: dict[str | None, list[str]] = defaultdict(list)
    for a in analyses:
        want = geo.country_from_symbol(a.get("symbol"), a.get("asset_class"))
        if want != a.get("listing_country"):
            by_country[want].append(a["symbol"])
    for country, syms in sorted(by_country.items(), key=lambda x: -len(x[1])):
        _p(f"  asset {str(country or '(none)'):<24} -> {len(syms):,} analyses")
        _update_analyses(syms, {"listing_country": country}, dry)
    total = sum(len(v) for v in by_country.values())
    _p(f"  asset_analysis  : {len(analyses):,} rows, {total:,} updated")

    if unmapped:
        _p("\n  !! exchanges with NO country mapping (add to geo.EXCHANGE_COUNTRY):")
        for ex, n in unmapped.most_common():
            _p(f"       {ex!r:28} {n:,} rows")
    else:
        _p("  every stored exchange resolved to a country.")
    _p("")


# ---------------------------------------------------------------------------
# Phase 2 — domicile country (one Yahoo assetProfile call per equity)
# ---------------------------------------------------------------------------
def _fetch_one(symbol: str) -> tuple[str, dict | None]:
    """(symbol, profile-or-None). None = Yahoo did not answer -> leave unstamped
    so the next run retries. A dict with country=None = answered, no profile."""
    return symbol, yahoo.asset_profile([symbol]).get(symbol)


def phase_domicile(dry: bool, refresh: bool, limit: int | None, workers: int) -> None:
    _p("=" * 72)
    _p("PHASE domicile -- Yahoo v10 assetProfile, 1 request per equity symbol")
    _p("=" * 72)

    rows = [
        a for a in _paged(
            "asset_analysis",
            "analysis_id, symbol, asset_class, domicile_country, listing_country, geo_checked_at",
        )
        if a.get("asset_class") == "equity" and a.get("symbol")
    ]
    # Never re-fetch a symbol Yahoo already answered for (stamped geo_checked_at),
    # whether or not it yielded a country.
    todo = rows if refresh else [a for a in rows if not a.get("geo_checked_at")]
    if limit:
        todo = todo[:limit]
    total = len(todo)
    _p(f"  {len(rows):,} equities, {total:,} to fetch"
       f"{' (--refresh: all)' if refresh else ' (never checked)'}"
       f"{f' [--limit {limit}]' if limit else ''}")
    if not total:
        _p("  nothing to do.\n")
        return
    _p(f"  {workers} workers, paced by the shared Yahoo throttle. Ctrl-C is safe (re-runnable).\n")

    by_symbol = {a["symbol"]: a for a in todo}
    now = datetime.now(timezone.utc).isoformat()
    done = found = no_profile = unanswered = 0
    banned = False

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_fetch_one, s) for s in by_symbol]
        try:
            for fut in as_completed(futures):
                symbol, prof = fut.result()
                done += 1
                a = by_symbol[symbol]
                tag = f"[{done:>5,}/{total:,}] {symbol:<14}"

                if prof is None:
                    unanswered += 1
                    _p(f"{tag} !! unanswered  (unstamped; next run retries)")
                    continue

                country = geo.normalize_country(prof.get("country"))
                if not country:
                    no_profile += 1
                    if not dry:  # stamp the attempt so we don't re-fetch forever
                        supabase.table("asset_analysis").update(
                            {"geo_checked_at": now}
                        ).eq("analysis_id", a["analysis_id"]).execute()
                    _p(f"{tag} -- no assetProfile country")
                    continue

                g = geo.resolve_geo(country, a.get("listing_country"))
                found += 1
                if not dry:
                    supabase.table("asset_analysis").update({
                        "domicile_country": country,
                        "continent": g["continent"],
                        "msci_region": g["msci_region"],
                        "geo_checked_at": now,
                    }).eq("analysis_id", a["analysis_id"]).execute()
                _p(f"{tag} {country:<22} {str(g['continent'] or '-'):<15} {g['msci_region'] or '-'}")
        except YahooThrottled as exc:
            banned = True
            _p(f"\n  !! {exc}")
            _p("  Stopping. Work so far is committed -- just re-run later.")
            for f in futures:
                f.cancel()
        except KeyboardInterrupt:
            _p("\n  interrupted -- committed work is kept; re-run to continue.")
            for f in futures:
                f.cancel()
            raise

    _p(f"\n  domicile found : {found:,}")
    _p(f"  no profile     : {no_profile:,}  (Yahoo answered, carries no assetProfile)")
    _p(f"  unanswered     : {unanswered:,}  (retry: re-run this script)")
    if banned:
        _p("  status         : STOPPED EARLY (Yahoo throttle)")
    _p("")


# ---------------------------------------------------------------------------
# Phase 3 — continent + MSCI region (no Yahoo)
# ---------------------------------------------------------------------------
def phase_derive(dry: bool) -> None:
    _p("=" * 72)
    _p("PHASE derive   -- continent + msci_region from (domicile, listing)")
    _p("=" * 72)

    rows = _paged("asset_analysis", "symbol, domicile_country, listing_country, continent, msci_region")
    by_pair: dict[tuple, list[str]] = defaultdict(list)
    unmapped: Counter = Counter()
    for a in rows:
        g = geo.resolve_geo(a.get("domicile_country"), a.get("listing_country"))
        if g["country"] and g["continent"] is None:
            unmapped[g["country"]] += 1
        if g["continent"] != a.get("continent") or g["msci_region"] != a.get("msci_region"):
            by_pair[(g["continent"], g["msci_region"])].append(a["symbol"])

    for (cont, region), syms in sorted(by_pair.items(), key=lambda x: -len(x[1])):
        _p(f"  {str(cont or '(none)'):<16} / {str(region or '(none)'):<18} -> {len(syms):,} analyses")
        _update_analyses(syms, {"continent": cont, "msci_region": region}, dry)
    total = sum(len(v) for v in by_pair.values())
    _p(f"  {len(rows):,} analyses, {total:,} updated")

    if unmapped:
        _p("\n  !! countries with NO continent mapping (add to geo._CONTINENT_GROUPS):")
        for c, n in unmapped.most_common():
            _p(f"       {c!r:28} {n:,} assets")
    _p("")


def _summary() -> None:
    rows = _paged("asset_analysis", "asset_class, continent, msci_region, domicile_country, listing_country")
    n = len(rows)
    _p("=" * 72)
    if not n:
        _p("COVERAGE  no assets")
        return
    have = sum(1 for r in rows if r.get("domicile_country") or r.get("listing_country"))
    _p(f"COVERAGE  {have:,}/{n:,} analyses have a country ({have / n * 100:.1f}%)")
    for field in ("continent", "msci_region"):
        counts = Counter(r.get(field) or "(none)" for r in rows)
        _p(f"\n  by {field}:")
        for k, v in counts.most_common():
            _p(f"       {k:<20} {v:>6,}")
    missing = Counter(r.get("asset_class") or "?" for r in rows if not r.get("continent"))
    if missing:
        _p("\n  no continent, by asset_class:")
        for k, v in missing.most_common():
            _p(f"       {k:<20} {v:>6,}")
    _p("=" * 72)


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill country / continent / MSCI region.")
    ap.add_argument("--only", choices=("listing", "domicile", "derive"),
                    help="run a single phase (default: all three, in order)")
    ap.add_argument("--refresh", action="store_true",
                    help="re-fetch domicile for equities already checked")
    ap.add_argument("--limit", type=int, help="cap the domicile fetch (smoke test)")
    ap.add_argument("--workers", type=int, default=4,
                    help="concurrent assetProfile fetches (default 4; the throttle still paces starts)")
    ap.add_argument("--dry-run", action="store_true", help="compute + print, write nothing")
    args = ap.parse_args()

    if args.dry_run:
        _p("DRY RUN -- no writes.\n")
    phases = [args.only] if args.only else ["listing", "domicile", "derive"]
    if "listing" in phases:
        phase_listing(args.dry_run)
    if "domicile" in phases:
        phase_domicile(args.dry_run, args.refresh, args.limit, max(1, args.workers))
    if "derive" in phases:
        phase_derive(args.dry_run)
    _summary()


if __name__ == "__main__":
    main()
