"""Standalone asset-pipeline ingest-queue worker.

Runs the queue drain as its OWN long-lived process, so it survives backend
restarts (dev `uvicorn --reload`, redeploys) — unlike the in-process scheduler
tick. It loops: pull a slice of `pending` ISINs, resolve+store them through the
THROTTLED Yahoo + OpenFIGI layers, mark done/failed, repeat; sleep briefly when
the queue is empty.

    uv run python scripts/asset_queue_worker.py

Run EXACTLY ONE worker — this OR the in-process scheduler job, never both (two
would compete for the Yahoo throttle and re-introduce throttle-corrupted
resolutions). The in-process job is OFF unless ASSET_QUEUE_INPROCESS=1, so this
standalone script is the default worker.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402,F401  (loads env + Supabase client)
from asset_pipeline import queue  # noqa: E402

IDLE_SLEEP = 10  # seconds to wait when the queue is empty (nothing to do)


def main() -> None:
    print("[asset-queue-worker] started — draining the ingest queue "
          "(Ctrl+C to stop). This is the single Yahoo/OpenFIGI consumer.", flush=True)
    while True:
        try:
            r = queue.process_slice(verbose=True)
        except KeyboardInterrupt:
            raise
        except Exception as e:  # noqa: BLE001
            print(f"[asset-queue-worker] slice error: {type(e).__name__}: {e}", flush=True)
            time.sleep(IDLE_SLEEP)
            continue
        if r.get("processed"):
            print(f"[asset-queue-worker] — slice done: {r['ok']} ok, {r['failed']} failed, "
                  f"{r['remaining']} remaining —", flush=True)
            continue  # more work likely — grab the next slice immediately
        time.sleep(IDLE_SLEEP)  # queue empty (or throttled+backed off) — idle


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[asset-queue-worker] stopped.", flush=True)
