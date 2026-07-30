"""Fix asset-pipeline mis-mappings — RE-QUEUES the bad rows so the running worker
re-resolves them cleanly. Fast + NO Yahoo, so it's safe to run ANYTIME, even
while the worker is going (the worker stays the single Yahoo/OpenFIGI consumer,
which is what avoids throttle-corrupted resolutions).

It sweeps two kinds of bad rows:
  * wrong-company mismaps    — the stored analysis is a DIFFERENT company than the
    ISIN's OpenFIGI name (Cytokinetics stored as QCOM, the GGAL cluster, the
    throttle-corrupted thin-listing rows).
  * identified-but-unmapped  — OpenFIGI knows the security but yfinance came back
    empty (usually just Yahoo throttling during an earlier batch). Skips OpenFIGI
    bond/right/warrant types (genuinely no daily series).

    uv run python scripts/asset_fix_mismaps.py

Then the worker (scripts/asset_queue_worker.py) re-resolves each and prints the
result — so you SEE what got fixed.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402,F401  (loads env + Supabase client)
from asset_pipeline import queue  # noqa: E402


def main() -> None:
    s = queue.requeue_suspects()
    print(f"wrong-company mismaps   : {s['suspects']:>5} found  -> {s['queued']} re-queued", flush=True)
    u = queue.requeue_unmapped()
    print(f"identified-but-unmapped : {u['retryable']:>5} retryable (of {u['unmapped']}) -> {u['queued']} re-queued", flush=True)
    st = queue.status()
    print(f"queue now: {st['pending']} pending · {st['done']} done · {st['failed']} failed", flush=True)
    print("The worker will re-resolve these and print each result "
          "(run scripts/asset_queue_worker.py if it isn't already going).", flush=True)


if __name__ == "__main__":
    main()
