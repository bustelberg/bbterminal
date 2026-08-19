"""Profile ONE Analyse-modal request — where its wall clock and its round trips go.

The modal is a single call to `compute_portfolio_analysis`, and it already reports per-phase
milliseconds to the browser. What it has never reported is the SHAPE of the work behind a phase:
how many PostgREST round trips, against which tables, how many of those are byte-identical repeats
the per-request memo already collapses, and how many `COPY` transfers ride alongside them.

⚠ WHY BOTH NUMBERS MATTER, AND WHY THE LOCAL TIMING ALONE MISLEADS. Local Postgres answers in
single-digit milliseconds; production is eu-west-3 with the backend elsewhere, so a round trip is
~40-80ms of pure latency. A phase that is 200ms locally over 30 requests is ~2s in production, and
a phase that is 800ms locally over 2 requests is still ~900ms there. **Request COUNT is the
production cost; local wall time is not.** This script prints both, per table, so the two are never
confused again.

Usage (from backend/):
    uv run python scripts/profile_analysis_modal.py                 # slowest paired portfolio
    uv run python scripts/profile_analysis_modal.py --id 34
    uv run python scripts/profile_analysis_modal.py --id 34 --repeat 2   # 2nd run = memo/cache warm
    uv run python scripts/profile_analysis_modal.py --list
"""
from __future__ import annotations

import argparse
import collections
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401  (loads .env / .env.local before anything reads it)

# ── instrumentation ─────────────────────────────────────────────────────────────────────────
# Two transports carry every read this endpoint makes: PostgREST over HTTP, and direct-Postgres
# COPY (`common/pg.py`). Both are wrapped, so nothing gets counted twice and nothing is missed —
# the same pairing the `_analysis_cache._WATCHED` list was derived from.

REQUESTS: list[dict] = []
COPIES: list[dict] = []


def _table_of(url: str) -> str:
    """`/rest/v1/airs_holding?select=...` -> `airs_holding`."""
    path = str(url).split("?", 1)[0]
    return path.rsplit("/", 1)[-1] or "?"


def _query_of(url: str) -> str:
    s = str(url)
    return s.split("?", 1)[1] if "?" in s else ""


def install_probes():
    """Wrap both transports. Returns a callable that removes them again."""
    import httpx

    from common import pg as common_pg

    orig_send = httpx.Client.send

    def send(self, request, **kw):
        t0 = time.perf_counter()
        try:
            return orig_send(self, request, **kw)
        finally:
            REQUESTS.append({
                "table": _table_of(request.url),
                "query": _query_of(request.url),
                "method": request.method,
                "ms": (time.perf_counter() - t0) * 1000,
            })

    httpx.Client.send = send

    orig_copy = common_pg._run_copy_uncached

    def copy(sql, *a, **kw):
        t0 = time.perf_counter()
        try:
            return orig_copy(sql, *a, **kw)
        finally:
            COPIES.append({"sql": " ".join(str(sql).split())[:120],
                           "ms": (time.perf_counter() - t0) * 1000})

    common_pg._run_copy_uncached = copy

    def uninstall():
        httpx.Client.send = orig_send
        common_pg._run_copy_uncached = orig_copy

    return uninstall


# ⚠ FUNCTION-LEVEL TIMING, BECAUSE A PHASE IS NOT AN ANSWER. `returns_and_benchmark` is one
# reported phase and TWO unrelated loads inside it (this portfolio's own performance, and the whole
# benchmark index) — a 2s phase says nothing about which. Each name below is wrapped where it is
# DEFINED, so the count is calls-per-request and the time is inclusive of everything it calls.
CALLS: dict[str, list] = {}

# (module, attribute) — the loaders the modal composes. Wrapped at the definition site so a
# late `from x import y` inside a function still reaches the wrapper.
_TARGETS = [
    ("routers._airs_portfolio_perf", "compute_portfolio_performance"),
    # ⚠ PATCHED AT EVERY BINDING, NOT ONLY THE DEFINITION. `_airs_portfolio_analysis` does
    # `from routers._asset_benchmark import index_returns` at module level, so the name it calls is
    # its OWN — wrapping only the source module leaves the hottest loader invisible, which is
    # exactly how it went unmeasured in the first pass.
    ("routers._asset_benchmark", "index_returns"),
    ("routers._airs_portfolio_analysis", "index_returns"),
    ("routers._airs_portfolio_analysis", "_holding_risk"),
    ("routers._airs_portfolio_analysis", "_daily_eur"),
    ("routers._airs_portfolio_analysis", "_child_book_ledgers"),
    ("routers._airs_portfolio_analysis", "_position_ledger"),
    ("routers._airs_portfolio_analysis", "_wrapped_book_marks"),
    ("routers._airs_portfolio_analysis", "_expand_book_rows"),
    ("routers._airs_portfolio_analysis", "_reclassify_book_rows"),
    ("routers._airs_portfolio_analysis", "_book_port_items"),
    ("routers._airs_portfolio_analysis", "_basis_axes"),
    ("routers._airs_portfolio_analysis", "_realised_block"),
    ("routers._airs_portfolio_analysis", "_grid"),
    ("routers._airs_portfolio_analysis", "_members"),
    ("routers._airs_portfolio_analysis", "_bench_start_caps"),
    ("routers._airs_portfolio_analysis", "_with_results"),
    ("routers._airs_portfolio_analysis", "_book_return"),
    ("routers._airs_lookthrough", "expand_positions"),
]


# Leg-cache hits/misses by key prefix, so "the cache is warm" is a measurement rather than an
# assumption. ⚠ A CROSS-PORTFOLIO CACHE IS ONLY WORTH WHAT THE NEXT PORTFOLIO SHARES, and that is
# exactly what these counters answer: the benchmark legs are shared by every book, a holding's risk
# row only by books that hold it.
LEGS: dict[str, list] = collections.defaultdict(lambda: [0, 0])   # prefix -> [hit, miss]


def install_leg_probe():
    from routers import _analysis_cache as ac

    orig_leg, orig_many = ac.leg, ac.leg_get_many

    def leg(key, compute):
        fp = ac.fingerprint()
        had = fp is not None and ac._leg_cache.get((fp, *key)) is not None
        LEGS[str(key[0])][0 if had else 1] += 1
        return orig_leg(key, compute)

    def leg_get_many(keys):
        hits, misses = orig_many(keys)
        if keys:
            LEGS[str(keys[0][0])][0] += len(hits)
            LEGS[str(keys[0][0])][1] += len(misses)
        return hits, misses

    ac.leg, ac.leg_get_many = leg, leg_get_many

    def uninstall():
        ac.leg, ac.leg_get_many = orig_leg, orig_many
    return uninstall


def install_call_timers():
    import importlib
    undo = []
    for modname, attr in _TARGETS:
        try:
            mod = importlib.import_module(modname)
            fn = getattr(mod, attr)
        except (ImportError, AttributeError):
            continue
        CALLS.setdefault(attr, [])

        def make(fn=fn, attr=attr):
            def wrapper(*a, **kw):
                t0 = time.perf_counter()
                nreq, ncopy = len(REQUESTS), len(COPIES)
                try:
                    return fn(*a, **kw)
                finally:
                    CALLS[attr].append(((time.perf_counter() - t0) * 1000,
                                        len(REQUESTS) - nreq, len(COPIES) - ncopy))
            return wrapper

        setattr(mod, attr, make())
        undo.append((mod, attr, fn))

    def uninstall():
        for mod, attr, fn in undo:
            setattr(mod, attr, fn)
    return uninstall


# ── reporting ───────────────────────────────────────────────────────────────────────────────

# What a round trip costs in production, as measured in `_airs_ref`'s module note. Local timings
# cannot show this, and it is the number that decides what is worth fixing.
PROD_RTT_MS = 60


def bar(value: float, peak: float, width: int = 28) -> str:
    if peak <= 0:
        return ""
    return "#" * max(1, int(round(value / peak * width))) if value > 0 else ""


def report(label: str, wall_ms: float, timings: dict[str, int]) -> None:
    print()
    print("=" * 100)
    print(f"  {label}")
    print("=" * 100)

    total_req = len(REQUESTS)
    req_ms = sum(r["ms"] for r in REQUESTS)
    copy_ms = sum(c["ms"] for c in COPIES)
    other_ms = wall_ms - req_ms - copy_ms

    print(f"\n  wall {wall_ms:8.0f} ms   |   {total_req} PostgREST round trips ({req_ms:.0f} ms local)"
          f"   |   {len(COPIES)} COPY ({copy_ms:.0f} ms)"
          f"   |   compute {other_ms:.0f} ms")
    print(f"  !! the same {total_req} round trips cost ~{total_req * PROD_RTT_MS / 1000:.1f}s in "
          f"production at ~{PROD_RTT_MS}ms latency — that, not the {req_ms:.0f}ms above, is what a "
          f"user waits for.")

    # ── the endpoint's own phases (what the browser is told) ────────────────────────────────
    if timings:
        print("\n  --- server phases, as reported to the browser -------------------------------")
        peak = max(timings.values()) if timings else 0
        for name, ms in sorted(timings.items(), key=lambda kv: -kv[1]):
            print(f"    {name:28} {ms:7} ms  {bar(ms, peak)}")

    if LEGS:
        print("\n  --- cross-portfolio leg cache (hit / miss this run) ---------------------------")
        for prefix, (h, m) in sorted(LEGS.items(), key=lambda kv: -(kv[1][0] + kv[1][1])):
            print(f"    {prefix:24} {h:5} hit  {m:5} miss")

    # ── inside the phases: which LOADER the time belongs to ────────────────────────────────
    rows = [(n, len(v), sum(x[0] for x in v), sum(x[1] for x in v), sum(x[2] for x in v))
            for n, v in CALLS.items() if v]
    if rows:
        print("\n  --- by loader (inclusive; a phase is several of these) ------------------------")
        print(f"    {'loader':34} {'calls':>5} {'local ms':>9} {'reqs':>5} {'COPY':>5} {'~prod s':>8}")
        peak = max(r[2] for r in rows)
        for name, n, ms, req, cp in sorted(rows, key=lambda r: -r[2]):
            print(f"    {name:34} {n:5} {ms:9.0f} {req:5} {cp:5} {req * PROD_RTT_MS / 1000:8.1f}"
                  f"  {bar(ms, peak, 16)}")

    # ── round trips by table: the production cost centre ────────────────────────────────────
    by_table = collections.Counter(r["table"] for r in REQUESTS)
    ms_by_table: dict[str, float] = collections.defaultdict(float)
    for r in REQUESTS:
        ms_by_table[r["table"]] += r["ms"]
    print("\n  --- round trips by table (production cost = count x latency) ------------------")
    print(f"    {'table':38} {'reqs':>5} {'~prod s':>8} {'local ms':>9}")
    peak = max(by_table.values()) if by_table else 0
    for table, n in by_table.most_common():
        print(f"    {table:38} {n:5} {n * PROD_RTT_MS / 1000:8.1f} {ms_by_table[table]:9.0f}"
              f"  {bar(n, peak, 18)}")

    # ── byte-identical repeats: what a memo could still collapse ────────────────────────────
    seen = collections.Counter((r["method"], r["table"], r["query"]) for r in REQUESTS)
    dupes = {k: n for k, n in seen.items() if n > 1}
    wasted = sum(n - 1 for n in dupes.values())
    print(f"\n  --- byte-identical repeats: {wasted} of {total_req} round trips "
          f"(~{wasted * PROD_RTT_MS / 1000:.1f}s in production) --")
    if not dupes:
        print("    none — the per-request memo already collapses every repeat.")
    for (method, table, query), n in sorted(dupes.items(), key=lambda kv: -kv[1])[:15]:
        print(f"    {n:3}x  {method:5} {table:34} {query[:80]}")

    # ── distinct query shapes per table: where a canonical read would help ──────────────────
    shapes: dict[str, set] = collections.defaultdict(set)
    for r in REQUESTS:
        shapes[r["table"]].add(r["query"])
    fragmented = {t: s for t, s in shapes.items() if len(s) > 1 and by_table[t] > 2}
    if fragmented:
        print("\n  --- tables read through SEVERAL query shapes (memo cannot collapse these) ----")
        print("      the `_airs_ref` fix: ONE canonical superset read, filtered in Python.")
        for table, s in sorted(fragmented.items(), key=lambda kv: -by_table[kv[0]]):
            print(f"    {table:38} {by_table[table]:3} reqs over {len(s)} shapes")
            for q in sorted(s)[:6]:
                print(f"        {q[:110]}")

    # ── COPY transfers ─────────────────────────────────────────────────────────────────────
    if COPIES:
        print("\n  --- COPY transfers ------------------------------------------------------------")
        peak = max(c["ms"] for c in COPIES)
        for c in sorted(COPIES, key=lambda c: -c["ms"])[:12]:
            print(f"    {c['ms']:8.0f} ms  {bar(c['ms'], peak, 14):14}  {c['sql']}")

    # ── the slowest individual calls ───────────────────────────────────────────────────────
    print("\n  --- slowest individual round trips ---------------------------------------------")
    for r in sorted(REQUESTS, key=lambda r: -r["ms"])[:8]:
        print(f"    {r['ms']:8.1f} ms  {r['table']:34} {r['query'][:70]}")


def pick_portfolio(explicit: int | None, show_list: bool) -> int:
    """A PAIRED model portfolio — the modal's slow path. An unpaired one skips the whole book
    half, so profiling it would measure the cheap case and call it the cost."""
    from routers._airs_ref import models as ref_models  # noqa: PLC0415

    rows = ref_models()
    linked = deps.supabase.table("airs_account_model_link").select("model_portfolio_id").execute()
    paired = {r["model_portfolio_id"] for r in (linked.data or [])}
    if show_list:
        print(f"  {len(rows)} model portfolios, {len(paired)} paired with an AIRS book:")
        for r in sorted(rows, key=lambda r: r["id"]):
            mark = "paired" if r["id"] in paired else "      "
            print(f"    {r['id']:5}  {mark}  {r.get('display_name') or r.get('name')}")
        raise SystemExit(0)
    if explicit is not None:
        return explicit
    for r in sorted(rows, key=lambda r: r["id"]):
        if r["id"] in paired:
            return r["id"]
    if not rows:
        raise SystemExit("No model portfolios in this database — nothing to profile.")
    return rows[0]["id"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default=None,
                    help="model portfolio id, or a comma-separated list. ⚠ SEVERAL IDS IN ONE "
                         "PROCESS IS THE REALISTIC MEASUREMENT: the caches are cross-portfolio, "
                         "so profiling one id twice measures the best case and profiling one id "
                         "once measures a cold server. A reader opens books in turn.")
    ap.add_argument("--benchmark", default=None, help="benchmark label (default: the endpoint's)")
    ap.add_argument("--weight-by", default="book", choices=("model", "book"))
    ap.add_argument("--source", default="book", choices=("model", "book"))
    ap.add_argument("--repeat", type=int, default=1,
                    help="run N times; run 2+ shows what the warm caches actually save")
    ap.add_argument("--list", action="store_true", help="list portfolios and exit")
    ap.add_argument("--cold", action="store_true",
                    help="drop the cross-portfolio leg cache before EVERY run - i.e. reproduce the "
                         "behaviour before it existed, so a before/after is one flag apart")
    args = ap.parse_args()

    from routers._airs_portfolio_analysis import SP500_LABEL, compute_portfolio_analysis

    ids = ([int(x) for x in str(args.id).split(",") if x.strip()] if args.id
           else [pick_portfolio(None, args.list)])
    if args.list:
        pick_portfolio(None, True)
    bench = args.benchmark or SP500_LABEL

    uninstall = install_probes()
    uninstall_calls = install_call_timers()
    uninstall_leg = install_leg_probe()
    try:
        for i, pid in enumerate([p for p in ids for _ in range(args.repeat)]):
            REQUESTS.clear()
            COPIES.clear()
            for _v in CALLS.values():
                _v.clear()
            LEGS.clear()
            # ⚠ THE MEMO IS OPENED HERE, exactly as `compute_portfolio_analysis_async` opens it at
            # the request boundary. Profiling the bare sync function would count repeats the real
            # endpoint never makes and send the optimisation after a bug that does not exist.
            from common.read_cache import read_cache  # noqa: PLC0415

            if args.cold:
                from routers import _analysis_cache as _ac  # noqa: PLC0415
                _ac.invalidate()
            t0 = time.perf_counter()
            with read_cache(f"analysis:{pid}"):
                out = compute_portfolio_analysis(pid, bench, args.weight_by, args.source)
            wall = (time.perf_counter() - t0) * 1000
            label = (f"#{i + 1}  portfolio {pid} — {out.get('name')}   [{bench}, "
                     f"weight_by={args.weight_by}, source={args.source}]")
            report(label, wall, out.get("timings_ms") or {})
    finally:
        uninstall_leg()
        uninstall_calls()
        uninstall()


if __name__ == "__main__":
    main()
