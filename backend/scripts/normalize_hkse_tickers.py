"""One-shot DB backfill: pad un-padded HKSE `gurufocus_ticker` rows to
5 digits.

The runtime fix in `ingest/prices.py::normalize_gurufocus_ticker` makes
GuruFocus calls work regardless of the DB state, so this script is
optional — but running it once removes the inconsistency so the DB
matches what `canonical_ticker` produces for fresh ingest.

Usage (from backend/):
    uv run python scripts/normalize_hkse_tickers.py          # dry run
    uv run python scripts/normalize_hkse_tickers.py --apply  # write

Idempotent: skips rows whose ticker is already 5 digits or non-numeric.
Refuses to write if it would create a (gurufocus_exchange_id, gurufocus_ticker)
collision with an existing row — those need manual dedupe via `ingest.dedupe`.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from supabase import create_client


def load_env() -> None:
    backend_dir = Path(__file__).resolve().parents[1]
    load_dotenv(backend_dir / ".env")
    load_dotenv(backend_dir / ".env.local", override=True)


def get_supabase():
    import os
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually write the updates. Without this flag, dry-run only.",
    )
    args = parser.parse_args()

    load_env()
    sb = get_supabase()

    hkse_resp = (
        sb.table("gurufocus_exchange")
        .select("gurufocus_exchange_id")
        .eq("exchange_code", "HKSE")
        .limit(1)
        .execute()
    )
    if not hkse_resp.data:
        print("HKSE exchange row not found in `gurufocus_exchange`; nothing to do.")
        return 0
    hkse_id = hkse_resp.data[0]["gurufocus_exchange_id"]

    rows_resp = (
        sb.table("company")
        .select("company_id, gurufocus_ticker, company_name")
        .eq("gurufocus_exchange_id", hkse_id)
        .execute()
    )
    rows = rows_resp.data or []
    print(f"Scanning {len(rows)} HKSE companies…")

    candidates: list[tuple[int, str, str, str]] = []  # (cid, old, new, name)
    for r in rows:
        cid = r["company_id"]
        old = (r.get("gurufocus_ticker") or "").strip()
        name = r.get("company_name") or ""
        if not old or not old.isdigit() or len(old) >= 5:
            continue
        new = old.zfill(5)
        candidates.append((cid, old, new, name))

    if not candidates:
        print("Nothing to do — every HKSE ticker is already canonical.")
        return 0

    # Collision check: don't overwrite if the target form already exists
    # on a DIFFERENT company_id for this exchange. Such pairs need
    # dedupe via `ingest.dedupe` first.
    target_tickers = list({c[2] for c in candidates})
    existing_resp = (
        sb.table("company")
        .select("company_id, gurufocus_ticker")
        .eq("gurufocus_exchange_id", hkse_id)
        .in_("gurufocus_ticker", target_tickers)
        .execute()
    )
    occupied: dict[str, int] = {
        r["gurufocus_ticker"]: r["company_id"] for r in (existing_resp.data or [])
    }

    safe: list[tuple[int, str, str, str]] = []
    collisions: list[tuple[int, str, str, str, int]] = []
    for cid, old, new, name in candidates:
        if new in occupied and occupied[new] != cid:
            collisions.append((cid, old, new, name, occupied[new]))
        else:
            safe.append((cid, old, new, name))

    print(f"Will pad {len(safe)} rows, {len(collisions)} have collisions.")
    print()
    print("First 20 safe updates:")
    for cid, old, new, name in safe[:20]:
        print(f"  cid={cid:>6}  {old:>4} → {new}  ({name})")
    if collisions:
        print()
        print("Collisions (need manual dedupe — old cid → existing cid):")
        for cid, old, new, name, other in collisions[:20]:
            print(f"  cid={cid:>6}  {old:>4} → {new}  ({name})  blocked by cid={other}")

    if not args.apply:
        print()
        print("Dry run only. Re-run with --apply to write.")
        return 0

    print()
    print(f"Applying {len(safe)} updates…")
    for cid, _old, new, _name in safe:
        sb.table("company").update({"gurufocus_ticker": new}).eq(
            "company_id", cid,
        ).execute()
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
