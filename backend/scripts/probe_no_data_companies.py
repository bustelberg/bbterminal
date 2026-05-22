"""Probe companies that have ZERO close_price rows in `metric_data`, and:
  - If GuruFocus serves data on the configured fallback exchange
    (`FALLBACK_EXCHANGES[exchange]`), `ensure_prices_for_company` will
    fetch + repoint the company's `exchange_id` automatically — same
    behaviour as the weekly pipeline's prices phase.
  - If `ensure_prices_for_company` explicitly returns `is_delisted=True`
    (GuruFocus returned a 403 "Delisted stocks" body), that path
    already writes `delisted_at` for us.
  - If the probe finishes with zero rows and no explicit delisted/
    forbidden signal — typically the "stock not found everywhere"
    case — this script writes `delisted_at = now()` so the row is
    excluded from future backtests and surfaced with the existing
    "delisted" badge on /companies.

Excluded from probing:
  - Rows already flagged `delisted_at`.
  - Rows on exchanges in the unsubscribed list (GuruFocus subscription
    doesn't cover them, no point retrying).

Usage (from backend/):
    uv run python scripts/probe_no_data_companies.py          # dry run
    uv run python scripts/probe_no_data_companies.py --apply  # write
    uv run python scripts/probe_no_data_companies.py --apply --limit 20

Idempotent: each run picks up wherever the prior left off (rows that
got data via fallback are no longer "no-data candidates" next time).
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from supabase import create_client


# Exchanges GuruFocus subscription does NOT cover — pre-filter them out
# instead of wasting probe calls. Keep in sync with
# `_KNOWN_UNSUBSCRIBED_EXCHANGES` in
# `routers/momentum/backtest_stream/audit.py`.
#
# LSE (UK) is excluded — the subscription covers continental Europe but
# not the UK.
_UNSUBSCRIBED_EXCHANGES = {
    "LSE", "JSE", "BSE", "NSE", "BMV", "ASX", "NZE", "MOEX",
}


def load_env() -> None:
    backend_dir = Path(__file__).resolve().parents[1]
    load_dotenv(backend_dir / ".env")
    load_dotenv(backend_dir / ".env.local", override=True)


def get_supabase():
    import os
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def find_candidates(sb, *, limit: int | None) -> list[dict]:
    """Return companies with zero `close_price` rows in metric_data.

    Strategy: ask the `company_latest_close_price_dates` RPC for the
    latest close_price target_date per company. Companies with NO
    close_price row are missing from that RPC's output (or come back
    with NULL `latest_target_date`). Cross-referenced against the
    company table to filter delisted + unsubscribed rows.

    Why not a direct metric_data query? With ~3k companies × ~7000
    close_price rows each, a naive `IN (...)` returns >>50k rows and
    runs into PostgREST's row cap — leading to spurious "no data"
    false positives. The RPC aggregates server-side, returning one
    row per cid."""
    out: list[dict] = []
    offset = 0
    page = 1000
    while True:
        resp = (
            sb.table("company")
            .select(
                "company_id, company_name, gurufocus_ticker, delisted_at, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            )
            .is_("delisted_at", "null")
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        for r in batch:
            exch = ((r.get("gurufocus_exchange") or {}).get("exchange_code")) or ""
            ticker = r.get("gurufocus_ticker") or ""
            if not ticker or not exch:
                continue
            if exch in _UNSUBSCRIBED_EXCHANGES:
                continue
            out.append({
                "company_id": int(r["company_id"]),
                "company_name": r.get("company_name") or "",
                "ticker": ticker,
                "exchange": exch,
            })
        if len(batch) < page:
            break
        offset += page

    if not out:
        return []

    # Per-company HEAD check: does ANY close_price row exist for this
    # cid? Cheaper than the `company_latest_close_price_dates` RPC,
    # which aggregates the full metric_data table and trips the
    # PostgREST statement timeout on production-sized data.
    #
    # Concurrency keeps wall time reasonable even with ~3k cids: each
    # query is small (LIMIT 1, server-side) so we can fan out.
    from concurrent.futures import ThreadPoolExecutor  # noqa: PLC0415

    def _has_data(cid: int) -> tuple[int, bool]:
        r = (
            sb.table("metric_data")
            .select("company_id")
            .eq("company_id", cid)
            .eq("metric_code", "close_price")
            .limit(1)
            .execute()
        )
        return cid, bool(r.data)

    has_data: set[int] = set()
    cids = [c["company_id"] for c in out]
    print(f"  Checking close_price coverage for {len(cids)} cids (concurrency 16)...", flush=True)
    with ThreadPoolExecutor(max_workers=16) as pool:
        for cid, ok in pool.map(_has_data, cids):
            if ok:
                has_data.add(cid)

    no_data = [c for c in out if c["company_id"] not in has_data]
    if limit is not None:
        no_data = no_data[:limit]
    return no_data


def probe_one(sb, c: dict) -> tuple[str, str]:
    """Probe one company. Returns (outcome, detail) where outcome is one
    of: 'data_fetched', 'already_delisted_by_gurufocus', 'forbidden',
    'no_data', 'error'."""
    from ingest.prices import ensure_prices_for_company  # noqa: PLC0415
    try:
        r = ensure_prices_for_company(
            sb, c["company_id"], c["ticker"], c["exchange"],
        )
    except Exception as e:
        return "error", f"{type(e).__name__}: {e}"

    if r.is_forbidden:
        return "forbidden", "unsubscribed region or 403"
    if r.is_delisted:
        # ensure_prices_for_company already marked delisted_at via its
        # own write path — nothing more for us to do.
        return "already_delisted_by_gurufocus", r.error or "delisted per GuruFocus"
    if r.total_prices > 0:
        used = getattr(r, "resolved_exchange", None) or c["exchange"]
        return "data_fetched", (
            f"{r.rows_loaded} rows loaded "
            f"(via {used}:{c['ticker']})"
        )
    return "no_data", r.error or "0 prices returned across all attempts"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true",
        help=(
            "Write delisted_at on the 'no_data' rows. Without this flag, "
            "dry-run only (still probes, doesn't write delisted_at)."
        ),
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Cap candidates to N for testing. Default: all.",
    )
    args = parser.parse_args()

    load_env()
    sb = get_supabase()

    print("Finding candidates with zero close_price rows...")
    candidates = find_candidates(sb, limit=args.limit)
    print(f"  -> {len(candidates)} candidates")
    if not candidates:
        print("Nothing to do.")
        return 0
    print()

    outcomes: dict[str, list[tuple[dict, str]]] = {
        "data_fetched": [],
        "already_delisted_by_gurufocus": [],
        "forbidden": [],
        "no_data": [],
        "error": [],
    }

    for i, c in enumerate(candidates, 1):
        label = f"{c['exchange']}:{c['ticker']}"
        print(f"  [{i}/{len(candidates)}] probing {label} ({c['company_name']})...", end="", flush=True)
        outcome, detail = probe_one(sb, c)
        outcomes.setdefault(outcome, []).append((c, detail))
        print(f" {outcome}")
        if detail and outcome in ("error", "no_data"):
            print(f"      |_ {detail[:120]}")

    print()
    print("Summary:")
    for k, v in outcomes.items():
        print(f"  {k}: {len(v)}")

    no_data_rows = outcomes.get("no_data", [])
    if not no_data_rows:
        print()
        print("No rows need delisted_at marking.")
        return 0

    print()
    print(f"{len(no_data_rows)} candidates for delisted_at marking:")
    for c, detail in no_data_rows[:20]:
        print(
            f"  cid={c['company_id']:>5} {c['exchange']}:{c['ticker']:<10} "
            f"({c['company_name'][:50]})"
        )
    if len(no_data_rows) > 20:
        print(f"  ... and {len(no_data_rows) - 20} more")

    if not args.apply:
        print()
        print("Dry run only. Re-run with --apply to set delisted_at.")
        return 0

    print()
    print(f"Setting delisted_at on {len(no_data_rows)} rows...")
    iso = _now_utc_iso()
    for c, _detail in no_data_rows:
        try:
            sb.table("company").update({"delisted_at": iso}).eq(
                "company_id", c["company_id"],
            ).is_("delisted_at", "null").execute()
        except Exception as e:
            print(
                f"  failed cid={c['company_id']}: {type(e).__name__}: {e}",
            )
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
