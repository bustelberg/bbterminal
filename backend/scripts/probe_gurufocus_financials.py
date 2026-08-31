"""IS THE GURUFOCUS `financials` ENDPOINT BACK? One command, two API calls, an exit code.

⚠⚠ WRITTEN DURING THE OUTAGE OF 2026-08-31, when the endpoint answered EVERY symbol with its full
15.7 KB template and no values — AAPL and ASML included — while `summary`, `keyratios` and `price`
stayed healthy and the monthly quota was half spent. Nothing upstream said anything was wrong.

⚠⚠ THE CANARY IS ON THE **PATH**, NOT THE SYMBOL, AND THAT IS THE WHOLE DIAGNOSIS. This API never
404s: an unknown sub-path answers 200 with a 46-point list (~874 chars). So there are three states
and they look alike from a distance:

    endpoint GONE      -> the 874-char, 46-point list          -> our code needs repointing
    endpoint EMPTY     -> its own template, every array empty  -> wait for the vendor
    endpoint HEALTHY   -> its own template, values in it       -> nothing to do

Telling the first two apart is the difference between changing code and waiting, which is why the
canary is compared rather than merely fetched.

    uv run python scripts/probe_gurufocus_financials.py          # exit 0 = healthy, 1 = still down
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402,F401  — loads .env before anything reads it


def _values(node) -> int:
    """How many values live in every array of a payload, at any depth."""
    if isinstance(node, dict):
        return sum(_values(v) for v in node.values())
    if isinstance(node, list):
        return len(node) + sum(_values(v) for v in node)
    return 0


def main() -> int:
    from ingest.earnings._api_client import _api_request, _build_api_url  # noqa: PLC0415

    # ⚠ AAPL, DELIBERATELY: the biggest, best-covered company the vendor has. If this one is empty
    # the answer is never "that company has no data", which is exactly the doubt a thin constituent
    # leaves behind.
    real = _api_request(_build_api_url("stock/AAPL/financials", {"order": "desc"})).data
    canary = _api_request(_build_api_url("stock/AAPL/__canary__")).data

    n = _values(real)
    same_shape = json.dumps(real, sort_keys=True) == json.dumps(canary, sort_keys=True)
    print(f"financials : {len(json.dumps(real)):>7,} chars · {n:,} values in its arrays")
    print(f"__canary__ : {len(json.dumps(canary)):>7,} chars · {_values(canary):,} values")

    if same_shape:
        print("\nGONE — `financials` now answers exactly like an unknown path. The endpoint has "
              "been renamed or withdrawn; check `backend/gurufocus_api.json` and repoint.")
        return 1
    if n == 0:
        print("\nSTILL EMPTY — the endpoint is real and holds nothing. Vendor side; wait.\n"
              "Nothing here needs changing: `fetch_financials` refuses an empty payload (it will "
              "not overwrite a cached copy or stamp a company), and the benchmark fill stops after "
              f"{_limit()} of them in a row.")
        return 1
    print(f"\nHEALTHY — {n:,} values. Re-run the fills that were deferred, and re-check the two "
          "ACWI constituents with zero market caps:\n"
          "    uv run python scripts/diagnose_blend_members.py ACWI revenue")
    return 0


def _limit() -> int:
    from routers._fundamental_fill import VENDOR_EMPTY_LIMIT  # noqa: PLC0415
    return VENDOR_EMPTY_LIMIT


if __name__ == "__main__":
    raise SystemExit(main())
