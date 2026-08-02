"""CLI over `ingest.refetch_history` — force a complete re-read of price + volume
history from GuruFocus and write back only the bars that moved.

The pipeline runs this automatically before each month's rebalance (see
`ingest/phases/pipeline.py`); this is the manual door onto the SAME function, so
what you debug with here is what the scheduled run does.

    cd backend && PYTHONPATH=. uv run python scripts/refetch_full_history.py \
        --universe "LEONTEQ (as of 2026-06-17)"            # dry run
    cd backend && PYTHONPATH=. uv run python scripts/refetch_full_history.py \
        --universe "LEONTEQ (as of 2026-06-17)" --apply
    ... --company-id 5608        # one name
    ... --status                 # when did a full refetch last run?
"""
from __future__ import annotations

import argparse
import sys

import deps  # noqa: F401 — loads .env/.env.local
from deps import supabase
from ingest.refetch_history import (
    last_full_refetch,
    refetch_full_history,
    refetched_this_month,
)


def _members(universe_label: str | None, company_id: int | None) -> list[int]:
    if company_id:
        return [company_id]
    if not universe_label:
        raise SystemExit("give --universe or --company-id")
    u = (supabase.table("universe").select("universe_id")
         .eq("label", universe_label).limit(1).execute()).data
    if not u:
        raise SystemExit(f"no universe labelled {universe_label!r}")
    uid = int(u[0]["universe_id"])
    out, off = [], 0
    while True:
        rows = (supabase.table("universe_membership").select("company_id")
                .eq("universe_id", uid).range(off, off + 999).execute().data or [])
        out += [int(r["company_id"]) for r in rows]
        if len(rows) < 1000:
            return sorted(set(out))
        off += 1000


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--universe")
    p.add_argument("--company-id", type=int)
    p.add_argument("--apply", action="store_true", help="write (default: dry run)")
    p.add_argument("--status", action="store_true", help="report the last refetch and exit")
    p.add_argument("--limit", type=int)
    a = p.parse_args()

    cids = _members(a.universe, a.company_id)
    if a.limit:
        cids = cids[:a.limit]

    if a.status:
        seen = last_full_refetch(cids)
        print(f"{len(cids)} companies · last full refetch: {seen or 'never'} · "
              f"already done this month: {refetched_this_month(cids)}")
        return 0

    # `flush=True`: this runs for ~20 minutes and its stdout is usually a file or
    # a pipe, where Python block-buffers — leaving the log empty until the very
    # end, which reads exactly like a hung job.
    def _say(msg: str, level: str) -> None:
        print(f"{'! ' if level in ('warn', 'error') else '  '}{msg}", flush=True)

    print(f"{len(cids)} companies · {'APPLY' if a.apply else 'DRY RUN'}", flush=True)
    res = refetch_full_history(cids, apply=a.apply, on_step=_say)
    if not a.apply and res["moved"]:
        print("\nre-run with --apply to write", flush=True)
    return 0


sys.exit(main())
