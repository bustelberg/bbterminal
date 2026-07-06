"""Enqueue ISINs into the asset-pipeline ingest queue from the shell.

Reads ISINs from CSV/text file(s) (every ISIN-pattern match, else the first
column of each line) and/or positional ISIN args, dedupes, and adds them as
`pending`. Instant — no Yahoo. Then run `asset_queue_worker.py`, which fetches
OpenFIGI + yfinance data for each and stores it (the single throttled consumer).

    uv run python scripts/asset_enqueue.py isins.csv
    uv run python scripts/asset_enqueue.py US0378331005 US5951121038
    uv run python scripts/asset_enqueue.py isins.csv --all   # also re-queue already-stored ISINs

Then (in another terminal, and leave it running):
    uv run python scripts/asset_queue_worker.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402,F401  (loads env + Supabase client)
from asset_pipeline import queue  # noqa: E402

ISIN_RE = re.compile(r"\b[A-Z]{2}[A-Z0-9]{9}\d\b")


def _isins_from(args: list[str]) -> list[str]:
    out: list[str] = []
    for a in args:
        p = Path(a)
        if p.exists():
            text = p.read_text(encoding="utf-8", errors="replace").upper()
            found = ISIN_RE.findall(text)
            if found:
                out += found
            else:  # not ISIN-shaped — take the first column of each line
                out += [ln.split(",")[0].split(";")[0].split("\t")[0].strip()
                        for ln in text.splitlines() if ln.strip()]
        else:
            out.append(a.strip())  # a bare ISIN/symbol arg
    return out


def main() -> None:
    argv = sys.argv[1:]
    skip_existing = "--all" not in argv
    targets = [a for a in argv if a != "--all"]
    if not targets:
        print("usage: asset_enqueue.py <isins.csv | ISIN ...> [--all]")
        return
    ids = _isins_from(targets)
    r = queue.enqueue(ids, skip_existing=skip_existing)
    print(f"enqueued {r['queued']} new · {r['skipped_existing']} already stored "
          f"(of {r['input']} input)")
    print("now run:  uv run python scripts/asset_queue_worker.py")


if __name__ == "__main__":
    main()
