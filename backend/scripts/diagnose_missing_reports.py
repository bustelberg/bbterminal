"""Which accounts wear a ⚠ report badge on /management-dashboard, and whether they should.

⚠ THE BADGE IS `reports_ok` FROM `airs_account_roster`, per account, from THAT account's own last
scan (`routers/_airs_accounts._missing_reports`). A code missing from `reports_ok` means the scan
raised on that report — `AirsNoData` does NOT count as missing, it is appended as ok, precisely so a
book that genuinely has no such report stops wearing a permanent warning.

So a badge is only honest if the report really failed. This prints, per badged account, what is
missing and whether the data behind it is nonetheless present — a row that shows holdings while
claiming Vermogensoverzicht was not retrieved is the contradiction worth explaining.

    cd backend && uv run python scripts/diagnose_missing_reports.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401  (loads .env / .env.local first)
from airs_vermogen import REPORTS  # noqa: E402

LABEL = {"att": "Rendement", "volk": "Vermogensoverzicht", "mut": "Mutaties",
         "trans": "Transacties", "model": "Model"}


def main() -> None:
    sb = deps.supabase
    roster = (sb.table("airs_account_roster")
              .select("portefeuille,reports_ok,reports_at,last_seen_at")
              .order("reports_at", desc=True).limit(2000).execute().data or [])
    print(f"roster rows: {len(roster)}")

    # The same per-account rule the page uses — newest row per account wins.
    seen: set[str] = set()
    badged: list[tuple[str, list[str], str]] = []
    complete = never = 0
    for r in roster:
        key = (r.get("portefeuille") or "").strip()
        if not key or key.lower() in seen:
            continue
        seen.add(key.lower())
        if not r.get("reports_at"):
            never += 1
            continue
        got = set(r.get("reports_ok") or ())
        gap = [c for c in REPORTS if c not in got]
        if gap:
            badged.append((key, gap, str(r.get("reports_at"))[:19]))
        else:
            complete += 1

    print(f"accounts: {len(seen)}   complete: {complete}   never scanned: {never}   "
          f"badged: {len(badged)}\n")
    if not badged:
        print("nothing badged — no ⚠ on the page")
        return

    # How many badged accounts nonetheless HOLD the thing the badge says was not retrieved.
    names = [b[0] for b in badged]
    held: dict[str, tuple[int, str]] = {}
    for i in range(0, len(names), deps.IN_CHUNK_SIZE):
        chunk = names[i:i + deps.IN_CHUNK_SIZE]
        rows = (sb.table("airs_holding")
                .select("portefeuille,as_of_date").in_("portefeuille", chunk)
                .order("as_of_date", desc=True).limit(20000).execute().data or [])
        for h in rows:
            k = h["portefeuille"]
            n, newest = held.get(k, (0, ""))
            held[k] = (n + 1, max(newest, str(h.get("as_of_date") or "")))

    by_code: dict[str, int] = {}
    for _n, gap, _at in badged:
        for c in gap:
            by_code[c] = by_code.get(c, 0) + 1
    print("badge counts by report:")
    for c in REPORTS:
        if by_code.get(c):
            print(f"  {LABEL[c]:<20} {by_code[c]}")

    print(f"\n{'account':<34} {'missing':<34} {'last scan':<20} stored holdings")
    for name, gap, at in sorted(badged):
        n, newest = held.get(name, (0, ""))
        note = f"{n} rows, newest {newest[:10]}" if n else "none"
        print(f"  {name:<32} {', '.join(LABEL[c] for c in gap):<32} {at:<20} {note}")

    # ⚠ THE CONTRADICTION THIS SCRIPT EXISTS FOR.
    bad = [(n, held.get(n, (0, ''))) for n, gap, _ in badged if 'volk' in gap and held.get(n, (0,))[0]]
    if bad:
        print(f"\n⚠ {len(bad)} account(s) badged 'Vermogensoverzicht not retrieved' while HOLDING "
              f"stored holdings — the rows are from an earlier scan, which is what the badge means, "
              f"but it is also what makes it read as wrong.")


if __name__ == "__main__":
    main()
