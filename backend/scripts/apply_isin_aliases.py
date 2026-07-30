"""Re-apply every manual ISIN alias (`asset_isin_alias`).

Idempotent, so it is safe to run after any resolution pass — and it must be run after one, because
`fast_resolve`, the repointers and the queue worker all write `asset_execution` per ISIN and would
otherwise hand an aliased ISIN a listing of its own again.

    cd backend && uv run python scripts/apply_isin_aliases.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402, F401
from asset_pipeline.isin_alias import apply_aliases, load_aliases  # noqa: E402

if __name__ == "__main__":
    aliases = load_aliases()
    changed = apply_aliases()
    print(f"{len(aliases)} alias(es); {changed} row(s) re-pointed.")
    for k, v in sorted(aliases.items()):
        print(f"  {k}  ->  {v}")
