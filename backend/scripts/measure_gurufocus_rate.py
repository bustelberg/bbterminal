"""How fast will GuruFocus actually answer? Measure it instead of guessing.

⚠⚠ WHY THIS EXISTS. `ingest/earnings/_api_client.py` gates every call behind a process-wide minimum
interval, and because the lock makes that a hard serializer it is THE number that sets the wall
clock of every bulk fill: an ACWI smart press is ~4,619 calls, so 1.92h at 1.5s, 1.28h at 1.0s,
0.64h at 0.5s. No amount of worker concurrency changes it. And the figure that looks like evidence
for 1.5s is not — CLAUDE.md's "6 calls, 3.4x on six threads" predates the `_RATE_LIMIT` lock, when
every thread read the same timestamp, slept the same short time and fired together. That measured a
burst. The real ceiling has never been established.

⚠ THE FAILURE MODE IS NOT A 429, WHICH IS WHY EYEBALLING IT DOES NOT WORK. An overloaded vendor here
returns an EMPTY BODY or a slow one, not a refusal — the same shape as Yahoo, and the shape that has
already put a wrong listing in this database once. So this counts empty and non-JSON bodies as
FIRST-CLASS OUTCOMES, not as noise. A run that "succeeds" 100% but returns three empty bodies has
found the ceiling.

⚠ IT SPENDS REAL QUOTA — one call per company per round, and it records them through
`track_api_call` so the usage meter stays honest. Bounded by `--companies`; nothing is written to
`metric_data` and nothing is uploaded to Storage, so it is safe to repeat.

⚠⚠ RUN IT WHEN NOTHING ELSE IS FETCHING. The limiter is per PROCESS, so this script does not share
a queue with a fill running in the backend — the two would stack, which both invalidates the
measurement and creates exactly the overload condition being tested for.

    uv run python scripts/measure_gurufocus_rate.py                       # baseline, 1.5s
    uv run python scripts/measure_gurufocus_rate.py --interval 0.75
    uv run python scripts/measure_gurufocus_rate.py --interval 0.5 --companies 40

⚠ INTERLEAVE, DO NOT BLOCK — the same rule the pytest benchmarking note in CLAUDE.md gives. Run
1.5 / 0.75 / 1.5 / 0.75, not three of each: vendor latency drifts with time of day and a blocked A/B
will hand you a stable-looking difference that is not there.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import deps  # noqa: E402  — loads .env / .env.local
from ingest.api_usage import track_api_call  # noqa: E402
from ingest.earnings._api_client import (  # noqa: E402
    _api_request, _api_request_cf, _build_api_url,
)
from ingest.earnings._common import _build_symbol  # noqa: E402
from routers._fundamental_backfill import COMPANY_SELECT, eligible  # noqa: E402


def _sample(n: int) -> list[dict]:
    """Companies we may actually call — `eligible` refuses an unsubscribed exchange, and a 403 from
    one of those would be counted as a throttle it is not."""
    rows = (deps.supabase.table("company").select(COMPANY_SELECT)
            .not_.is_("gurufocus_ticker", "null")
            # ⚠ ACTIVE ONLY. A delisted ticker answers oddly and would be scored as a throttle.
            .is_("delisted_at", "null").is_("out_of_scope_at", "null")
            .limit(n * 5).execute().data or [])
    out = [c for c in rows if eligible(c) is None]
    return out[:n]


def _url(c: dict) -> tuple[str, str]:
    exch = ((c.get("gurufocus_exchange") or {}) or {}).get("exchange_code")
    symbol = _build_symbol(c["gurufocus_ticker"], exch)
    return exch, _build_api_url(f"stock/{quote(symbol, safe=':')}/financials", {"order": "desc"})


def _raw_latency(comps: list[dict]) -> list[float]:
    """The vendor's OWN response time, with the limiter out of the way.

    ⚠⚠ THIS EXISTS BECAUSE THE OBVIOUS MEASUREMENT IS WRONG AND LOOKS RIGHT. `_api_request` SLEEPS
    INSIDE ITSELF — it takes `_RATE_LIMIT`, waits out the interval, and only then makes the request —
    so timing it end to end measures the queue, not the server. Measured that way the "latency" fell
    from 4.57s to 2.18s purely because the interval went from 1.5s to 0.75s, which reads as the
    vendor getting faster when we ask harder. It is the limiter timing itself.
    """
    out = []
    for c in comps:
        exch, url = _url(c)
        t = time.perf_counter()
        _api_request_cf(url)                 # ⚠ the UN-GATED path, called serially
        out.append(time.perf_counter() - t)
        try:
            track_api_call(deps.supabase, exch)
        except Exception:
            pass
    return out


def _one(c: dict) -> dict:
    exch, url = _url(c)
    t = time.perf_counter()
    r = _api_request(url)
    el = time.perf_counter() - t
    try:
        track_api_call(deps.supabase, exch)
    except Exception:                       # the meter must never fail the measurement
        pass
    # ⚠ THREE OUTCOMES, NOT TWO. "Empty" is the one that matters and it is not an error.
    if r.data is None:
        kind = "empty/blocked"
        if r.log and "unsubscribed region" in r.log.lower():
            kind = "unsubscribed"
        elif (r.status_code or 0) >= 400:
            kind = f"http {r.status_code}"
    elif not r.data:
        kind = "empty json"
    else:
        kind = "ok"
    return {"kind": kind, "seconds": el, "who": f"{exch}:{c['gurufocus_ticker']}",
            "log": (r.log or "")[:120]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--interval", type=float, default=None,
                    help="seconds between calls (default: whatever the app is configured for)")
    ap.add_argument("--companies", type=int, default=20, help="how many calls to spend")
    ap.add_argument("--workers", type=int, default=3,
                    help="concurrent fetchers, matching FILL_WORKERS")
    ap.add_argument("--probe", type=int, default=4,
                    help="extra un-gated serial calls used to measure the vendor's own latency")
    args = ap.parse_args()

    if args.interval is not None:
        os.environ["GURUFOCUS_MIN_INTERVAL_SECONDS"] = str(args.interval)
    from ingest.earnings._api_client import _min_interval  # noqa: PLC0415 — after the env is set
    interval = _min_interval()

    comps = _sample(args.companies + args.probe)
    if len(comps) < args.probe + 1:
        print("no eligible companies found — nothing to measure")
        return 1
    probe_set, comps = comps[:args.probe], comps[args.probe:args.probe + args.companies]

    raw = sorted(_raw_latency(probe_set)) if probe_set else []
    raw_mid = raw[len(raw) // 2] if raw else None

    floor = len(comps) * interval
    print(f"{len(comps)} calls · interval {interval:.2f}s · {args.workers} workers")
    if raw_mid:
        # ⚠⚠ THE TWO CEILINGS, SIDE BY SIDE. Sustained throughput is min(1/interval, workers/latency)
        # — so a smaller interval buys NOTHING while the second term is the smaller one, and the
        # honest fix there is more workers, not a faster gate.
        print(f"vendor latency (un-gated, serial): median {raw_mid:.2f}s · "
              f"slowest {raw[-1]:.2f}s   [n={len(raw)}]")
        print(f"ceilings: limiter {1/interval:.2f} calls/s · "
              f"{args.workers} workers at {raw_mid:.2f}s = {args.workers/raw_mid:.2f} calls/s "
              f"-> {'the LIMITER binds' if 1/interval < args.workers/raw_mid else 'CONCURRENCY binds'}")
    print(f"the limiter alone puts a floor of {floor:.1f}s on this\n", flush=True)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(_one, comps))
    wall = time.perf_counter() - t0

    by_kind: dict[str, int] = {}
    for r in results:
        by_kind[r["kind"]] = by_kind.get(r["kind"], 0) + 1

    print(f"wall clock        {wall:.1f}s   ({wall / len(comps):.2f}s per call, "
          f"{len(comps) / wall:.2f} calls/s)")
    # ⚠ NOT LABELLED "LATENCY" — this is gate wait + request, which is what a worker experiences and
    # is NOT a fact about the vendor. See `_raw_latency`.
    seen = sorted(r["seconds"] for r in results)
    print(f"per worker        median {seen[len(seen) // 2]:.2f}s in `_api_request` "
          f"(queue + request, NOT vendor latency)")
    print(f"floor overshoot   {wall - floor:+.1f}s "
          f"({'the limiter is the constraint' if wall - floor < 1 else 'the VENDOR is the constraint'})")
    print("\noutcomes:")
    for k, v in sorted(by_kind.items(), key=lambda kv: -kv[1]):
        print(f"  {k:<16} {v:>4}")

    bad = [r for r in results if r["kind"] not in ("ok", "unsubscribed")]
    if bad:
        # ⚠ NAMED, NOT COUNTED. One empty body at a given interval is the finding; a count alone
        # invites rounding it away as noise.
        print("\n⚠ non-ok responses — THIS is the signal, not the timing:")
        for r in bad[:10]:
            print(f"  {r['who']:<16} {r['kind']:<16} {r['log']}")
        print("\nAny of these at an interval faster than the current default means the ceiling was "
              "found. Do NOT lower the setting on a run with empty bodies in it.")
    else:
        print("\nno refusals and no empty bodies at this interval.")
    print("\n⚠ ONE SAMPLE IS NOT A RESULT. Interleave against the baseline (1.5 / X / 1.5 / X) "
          "before changing GURUFOCUS_MIN_INTERVAL_SECONDS — vendor latency drifts with the hour.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
