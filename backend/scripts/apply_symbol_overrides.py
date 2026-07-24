"""Re-apply every manual symbol override (`asset_symbol_override`).

Idempotent, so it is safe to run after any resolution pass — and it must be run after one, because
`fast_resolve`, the queue worker, both repointers and the per-row Resolve action all write
`asset_execution.yahoo_symbol` and would otherwise hand an overridden ISIN a listing of its own
again, chosen BY NAME. Name is how the wrong one gets picked: iShares Global Corp Bond UCITS ETF
"EUR" (hedged) and "USD (Dist)" differ by three characters and are different share classes.

An ISIN already naming its pinned symbol costs one query and no Yahoo call, so running this often
is cheap. This is also how an override reaches another database: run it there.

    cd backend && uv run python scripts/apply_symbol_overrides.py
    cd backend && uv run python scripts/apply_symbol_overrides.py --isin IE00BJSFQW37
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402, F401  — loads SUPABASE_* before anything else
from asset_pipeline.symbol_override import (  # noqa: E402
    apply_symbol_overrides,
    load_symbol_overrides,
)

if __name__ == "__main__":
    # The module logs WHY it skipped or refused a row; uvicorn leaves the root logger at WARNING,
    # so a script that did not raise this would print "0 changed" and never say what happened.
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    # ...but not httpx's per-request line: at INFO it prints a URL for every PostgREST call and
    # buries the one message that matters (which override was applied, skipped or refused).
    logging.getLogger("httpx").setLevel(logging.WARNING)

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--isin", help="apply just this one")
    a = ap.parse_args()

    overrides = load_symbol_overrides()
    changed = apply_symbol_overrides(a.isin.strip().upper() if a.isin else None)
    print(f"\n{len(overrides)} override(s); {changed} row(s) re-pointed.")
    for k, v in sorted(overrides.items()):
        print(f"  {k}  ->  {v}")
