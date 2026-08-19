"""Prove the Analyse modal's cross-portfolio cache changes NOTHING about the answer.

⚠ A CACHE THAT CHANGES A FIGURE IS NOT A SPEED-UP, IT IS A BUG WITH A STOPWATCH ATTACHED. This
page's whole discipline is that a number is current or absent, so the only acceptable evidence for
`_analysis_cache.leg` is that the payload is IDENTICAL with the memo on and with it off — field for
field, over the real database, on portfolios that exercise every branch (a book with wrapped
certificates, a book without, an unpaired basket).

The memo is disabled by making every lookup miss, which is exactly the state the code was in before
it existed. Everything else — the same process, the same database, the same request — is held equal.

⚠ `timings_ms` IS EXCLUDED, AND IT IS THE ONLY EXCLUSION. It is a stopwatch; being faster is the
point. Every other key, including the deep structures (`axes`, `book_holdings`, `realised`), is
compared in full.

Usage (from backend/):
    uv run python scripts/verify_analysis_cache.py
    uv run python scripts/verify_analysis_cache.py --id 1932,1918
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401  (loads .env / .env.local first)


def canon(payload: dict) -> str:
    """A stable string for the whole payload, minus the stopwatch."""
    body = {k: v for k, v in payload.items() if k != "timings_ms"}
    return json.dumps(body, sort_keys=True, default=str)


def first_difference(a: dict, b: dict, path: str = "") -> str | None:
    """Where the two payloads stop agreeing, named — a diff of two 137KB blobs is unreadable."""
    if type(a) is not type(b):
        return f"{path or '<root>'}: {type(a).__name__} vs {type(b).__name__}"
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k == "timings_ms":
                continue
            if k not in a or k not in b:
                return f"{path}.{k}: present on only one side"
            d = first_difference(a[k], b[k], f"{path}.{k}")
            if d:
                return d
        return None
    if isinstance(a, list):
        if len(a) != len(b):
            return f"{path}: {len(a)} items vs {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            d = first_difference(x, y, f"{path}[{i}]")
            if d:
                return d
        return None
    return None if a == b else f"{path}: {a!r} vs {b!r}"


def run(pid: int, bench: str, memo: bool) -> dict:
    """One computation, with the leg memo on or forced to always miss."""
    from common.read_cache import read_cache

    from routers import _analysis_cache as ac
    from routers._airs_portfolio_analysis import compute_portfolio_analysis

    ac.invalidate()
    orig_leg, orig_many, orig_put = ac.leg, ac.leg_get_many, ac.leg_put_many
    if not memo:
        # ⚠ EVERY LOOKUP MISSES AND NOTHING IS FILED — the pre-2026-08-19 behaviour exactly. Not
        # "a smaller cache": a partially-filled one would still be the new code path.
        ac.leg = lambda key, compute: compute()
        ac.leg_get_many = lambda keys: ({}, list(keys))
        ac.leg_put_many = lambda values: None
    try:
        with read_cache(f"verify:{pid}"):
            return compute_portfolio_analysis(pid, bench, "book", "book")
    finally:
        ac.leg, ac.leg_get_many, ac.leg_put_many = orig_leg, orig_many, orig_put


def paired_ids(limit: int) -> list[int]:
    linked = deps.supabase.table("airs_account_model_link").select("model_portfolio_id").execute()
    return sorted({r["model_portfolio_id"] for r in (linked.data or [])})[:limit]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default=None, help="comma-separated model portfolio ids")
    ap.add_argument("--benchmark", default=None)
    ap.add_argument("--limit", type=int, default=6, help="how many paired books to check")
    args = ap.parse_args()

    from routers._airs_portfolio_analysis import SP500_LABEL

    bench = args.benchmark or SP500_LABEL
    ids = ([int(x) for x in args.id.split(",") if x.strip()] if args.id
           else paired_ids(args.limit))
    if not ids:
        raise SystemExit("No paired model portfolios in this database.")

    print(f"Comparing {len(ids)} portfolio(s) against {bench}: memo OFF vs memo ON\n")
    bad = 0
    for pid in ids:
        off = run(pid, bench, memo=False)
        on = run(pid, bench, memo=True)
        # ⚠ AND A SECOND RUN WITH THE MEMO ALREADY WARM, because that is the state a real second
        # reader hits — the first `on` run FILLED the cache, it did not read it.
        warm = run_warm(pid, bench, off)
        same = canon(off) == canon(on) == canon(warm)
        name = on.get("name") or pid
        if same:
            print(f"  OK    {pid:5}  {name}")
        else:
            bad += 1
            d = first_difference(off, on) or first_difference(off, warm)
            print(f"  DIFF  {pid:5}  {name}\n          {d}")
    print()
    if bad:
        raise SystemExit(f"{bad} portfolio(s) differ — the memo is NOT transparent.")
    print(f"All {len(ids)} identical (every key but `timings_ms`).")


def run_warm(pid: int, bench: str, _ref: dict) -> dict:
    """Compute twice in a row WITHOUT clearing in between, and return the second payload — the
    one assembled entirely out of cached legs, which is the case a `put`-then-`get` bug would hide.
    """
    from common.read_cache import read_cache

    from routers._airs_portfolio_analysis import compute_portfolio_analysis

    with read_cache(f"verify-warm-a:{pid}"):
        compute_portfolio_analysis(pid, bench, "book", "book")
    with read_cache(f"verify-warm-b:{pid}"):
        return compute_portfolio_analysis(pid, bench, "book", "book")


if __name__ == "__main__":
    main()
