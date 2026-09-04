"""Fix asset-pipeline mis-mappings — RE-QUEUES bad rows so the running worker re-resolves them.
Fast + NO Yahoo here, so the worker stays the single Yahoo/OpenFIGI consumer, which is what avoids
throttle-corrupted resolutions.

It sweeps two kinds of bad rows:
  * wrong-company mismaps    — the stored analysis is a DIFFERENT company than the ISIN's OpenFIGI
    name (Cytokinetics stored as QCOM, the GGAL cluster, the throttle-corrupted thin-listing rows).
  * identified-but-unmapped  — OpenFIGI knows the security but yfinance came back empty (usually
    just Yahoo throttling during an earlier batch). Skips OpenFIGI bond/right/warrant types.

⚠⚠ THE MISMAP SWEEP IS NOW REPORT-ONLY UNLESS YOU NAME THE ROWS, AND THAT IS THE POINT OF THIS
    FILE (2026-09-04). It used to re-queue every row that failed `same_company`, which on the live
    grid is 110 rows of which only ~15 are genuinely wrong. The other ~95 are OpenFIGI's own
    spelling — `MUENCHENER RUECKVER AG-REG`, `IND & COMM BK OF CHINA-H`, `SAMSUNG ELECTRO-REGS GDR
    PFD`, `DHL GROUP` (a rename), `VANG FTSE JPN USDA` — i.e. CORRECT mappings on liquid names.
    Re-resolving a correct row is the one thing this pipeline must not do casually: Yahoo answers
    an overloaded caller with an EMPTY list rather than a 429, so the ranker picks from a candidate
    set missing the real listing and the row lands on a thin foreign line (Alphabet -> GOOA.VI,
    75,000x thinner). Blanket re-queueing 110 rows to fix 15 is that bet, taken 95 times.

⚠ AND NO THRESHOLD REPLACES THE HUMAN — measured against 15 hand-checked errors: the OpenFIGI-type
    allowlist catches all 15 and would re-resolve 38 correct rows; type AND a country mismatch
    leaves 11 false positives and misses 3 real ones; "a bare US ticker for a non-US ISIN" is
    structural and clean but catches only 4 of 15. So this prints the list, worst-liquidity-first,
    and takes the ISINs you decide on.

    uv run python scripts/asset_fix_mismaps.py                      # report; re-tries the unmapped
    uv run python scripts/asset_fix_mismaps.py --isin FR0004180537 --isin AU000000MFG4
    uv run python scripts/asset_fix_mismaps.py --all-suspects       # the old blanket behaviour
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402,F401  (loads env + Supabase client)
from asset_pipeline import queue  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--isin", action="append", default=None,
                    help="re-queue exactly this ISIN (repeatable) — the reviewed ones")
    ap.add_argument("--all-suspects", action="store_true",
                    help="re-queue EVERY name mismatch. Read the module docstring first.")
    ap.add_argument("--skip-unmapped", action="store_true",
                    help="do not re-try the identified-but-unmapped rows")
    a = ap.parse_args()

    s = queue.requeue_suspects(only=a.isin, apply=a.all_suspects)
    if not s["applied"]:
        print(f"wrong-company mismaps   : {s['suspects']:>5} flagged — NOT re-queued.\n"
              f"  Most of these are OpenFIGI spelling, not wrong mappings; re-resolving a correct\n"
              f"  row can only move it to a thinner listing. Review, then pass --isin for the ones\n"
              f"  that are really wrong (or --all-suspects to take the old blanket behaviour).\n",
              flush=True)
        # ⚠ WORST LIQUIDITY LAST: a genuinely wrong mapping on a liquid name is the expensive one,
        # so it ends up next to the prompt where it will actually be read.
        for r in sorted(s["rows"], key=lambda x: float(x.get("med_adv_eur") or 0)):
            print(f"    {r['isin']:<14} {str(r.get('yahoo_symbol')):<12} "
                  f"{str(r.get('name'))[:34]:<36} != {str(r.get('openfigi_name'))[:30]:<32} "
                  f"EUR {float(r.get('med_adv_eur') or 0):>14,.0f}/day", flush=True)
        print(flush=True)
    else:
        print(f"wrong-company mismaps   : {s['suspects']:>5} selected -> {s['queued']} re-queued",
              flush=True)
        if s["unknown"]:
            print(f"  ⚠ not flagged as a mismatch, so NOT queued: {', '.join(s['unknown'])}",
                  flush=True)

    if not a.skip_unmapped:
        u = queue.requeue_unmapped()
        print(f"identified-but-unmapped : {u['retryable']:>5} retryable (of {u['unmapped']}) "
              f"-> {u['queued']} re-queued", flush=True)

    st = queue.status()
    print(f"queue now: {st['pending']} pending · {st['done']} done · {st['failed']} failed",
          flush=True)
    # ⚠ ONLY WHERE SOMETHING WAS ACTUALLY QUEUED. Printed unconditionally it tells a reader who
    # just ran the report that a re-resolution is under way — the exact impression this file was
    # rewritten to remove.
    if s["applied"] or not a.skip_unmapped:
        print("The worker will re-resolve what was queued and print each result "
              "(run scripts/asset_queue_worker.py if it isn't already going).", flush=True)


if __name__ == "__main__":
    main()
