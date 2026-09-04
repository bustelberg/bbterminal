"""Is every hand-made mapping decision actually in force in THIS database?

Read-only, and safe to run against any environment — point it at prod by loading prod's env the
way the other scripts do. Exits non-zero if anything is out of place, so it can gate a deploy.

THREE CHECKS, EACH FOR A FAILURE THAT HAS HAPPENED
  1. every `asset_symbol_override` is actually applied — a pinned ISIN whose execution row names a
     DIFFERENT symbol means a resolution pass overwrote it and `apply_symbol_overrides.py` has not
     been run since. That is the whole failure mode the override table exists to prevent, and it
     is invisible from the grid.
  2. every deliberately unmapped row is still unmapped — `requeue_unmapped()` re-queues identified
     `not_found` rows, so before `store.MANUAL_UNMAP_PREFIX` was honoured a sweep could hand one
     back to the resolver and it could land on the same wrong ticker.
  3. ⚠⚠ EVERY QUOTE CURRENCY IN USE IS PRICEABLE — the check that would have caught `KWF` before
     it shipped. A currency that is neither in `fx_rate` nor in `fx.SUBUNIT` prices at nothing
     (the holding silently leaves its portfolio) or, worse, a minor unit missing its divisor
     prices 100x or 1000x high and still looks like a number. Kuwaiti fils are 1/1000 of a dinar,
     the only non-hundredth in the table.

    cd backend && uv run python scripts/verify_asset_mappings.py
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402

# ⚠⚠ AFTER `import deps`, WHICH IS THE ONLY WINDOW THEY WIN IN. The import runs
# `load_dotenv(.env.local, override=True)`, overwriting anything the shell exported — so a prod URL
# in the environment loses to the local file and a "prod" check silently reads the LOCAL database.
# The client is lazy, so setting these between the import and the first call is what sticks.
_ap = argparse.ArgumentParser(description=__doc__,
                              formatter_class=argparse.RawDescriptionHelpFormatter)
_ap.add_argument("--url", help="Supabase URL — overrides .env/.env.local")
_ap.add_argument("--key", help="service_role key for --url")
_args = _ap.parse_args()
if _args.url:
    os.environ["SUPABASE_URL"] = _args.url
if _args.key:
    os.environ["SUPABASE_SERVICE_KEY"] = _args.key

from asset_pipeline.fx import SUBUNIT  # noqa: E402
from asset_pipeline.store import MANUAL_UNMAP_PREFIX  # noqa: E402


def _page(table: str, cols: str, *, eq: tuple[str, object] | None = None) -> list[dict]:
    """⚠ PAGED. PostgREST truncates SILENTLY at 1,000 rows on cloud and 10,000 locally, so an
    unpaged read of `asset_execution` gives a DIFFERENT answer per environment — which is exactly
    the kind of check this file is supposed to be immune to."""
    out: list[dict] = []
    off = 0
    while True:
        q = deps.supabase.table(table).select(cols)
        if eq:
            q = q.eq(*eq)
        rows = q.range(off, off + 999).execute().data or []
        out += rows
        if not rows:
            break
        off += len(rows)
    return out


def main() -> int:
    bad = 0
    print(f"\n  database: {os.environ.get('SUPABASE_URL')}\n")

    # ── 1. pinned symbols are in force ────────────────────────────────────────
    pins = {r["isin"]: r["yahoo_symbol"]
            for r in _page("asset_symbol_override", "isin,yahoo_symbol")}
    ex = {r["isin"]: r for r in _page("asset_execution",
                                      "isin,yahoo_symbol,status,currency,name,reason")}
    print(f"symbol overrides: {len(pins)}")
    for isin, want in sorted(pins.items()):
        row = ex.get(isin)
        got = (row or {}).get("yahoo_symbol")
        if got != want:
            bad += 1
            print(f"   ✗ {isin} pinned to {want!r} but the row names {got!r} "
                  f"— run scripts/apply_symbol_overrides.py")
        else:
            print(f"   ✓ {isin} -> {want}")

    # ── 2. deliberate unmaps are still unmapped ───────────────────────────────
    manual = [r for r in ex.values()
              if (r.get("reason") or "").startswith(MANUAL_UNMAP_PREFIX)]
    print(f"\nhand-unmapped rows: {len(manual)}")
    for r in sorted(manual, key=lambda x: x["isin"]):
        if r.get("status") == "not_found" and not r.get("yahoo_symbol"):
            print(f"   ✓ {r['isin']} {str(r.get('name'))[:40]}")
        else:
            bad += 1
            print(f"   ✗ {r['isin']} was unmapped by hand and now reads "
                  f"status={r.get('status')!r} symbol={r.get('yahoo_symbol')!r} "
                  f"— something re-resolved it")

    # ── 3. every quote currency can be turned into EUR ────────────────────────
    have = {r["currency_code"] for r in _page("fx_rate", "currency_code")}
    used: dict[str, int] = {}
    for r in ex.values():
        c = (r.get("currency") or "").strip()
        if c and r.get("status") == "ok":
            used[c] = used.get(c, 0) + 1
    print(f"\nquote currencies in use: {len(used)}")
    for c, n in sorted(used.items(), key=lambda kv: -kv[1]):
        base, div = SUBUNIT.get(c, (c, 1.0))
        # ⚠ EUR IS THE BASE, SO IT HAS NO `fx_rate` ROW AND NEVER WILL. Without this the check
        # reports the second-largest currency in the book as unpriceable — a false alarm on 1,481
        # rows, which is how a check stops being read.
        if base == "EUR" or base in have:
            note = f"(minor unit -> {base} / {div:g})" if div != 1.0 else ""
            print(f"   ✓ {c:<5} {n:>5} row(s) {note}")
        else:
            bad += 1
            print(f"   ✗ {c:<5} {n:>5} row(s) — no `fx_rate` row for {base!r} and no SUBUNIT "
                  f"entry. Every one of these prices at NOTHING, or at the wrong scale.")

    print(f"\n{'OK — every mapping decision is in force' if not bad else f'{bad} problem(s)'}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
