"""Rebuild the expensive benchmark blends in the background, so no reader pays for them.

WHY
    `_blend_cache` makes the SECOND selection of an index free. The first one still costs what it
    costs, and after any fundamentals write `invalidate()` puts everybody back to first. Measured
    2026-08-19 with `scripts/profile_longequity_bench.py`, cold, local:

        ACWI (1,514 constituents)   ~20s wall for the reader; 21.8s to rebuild serially here
        SP500 (~490)                in between
        AEX (22)                    ~2s        <-- nobody has ever complained about AEX

    The cache key has NO user dimension (see `_blend_cache.cache_key`), so one rebuild serves every
    viewer of every portfolio. That is the whole argument for doing it ahead of time: the work is
    identical, it is only a question of who waits for it.

⚠⚠ TRIGGERED FROM `invalidate()`, NOT FROM THE INGEST JOB. The ask was "prewarm after the
    fundamentals ingest", and that is one of TWO writers that drop the cache — the other is the
    per-company Fetch in the modal (`benchmarks.py`), which is pressed while a reader is looking at
    the very chart it invalidates. Hanging the trigger off the thing that clears the cache covers
    both and cannot be forgotten by a third; it is the same discipline as `apiFetch` invalidating
    at the chokepoint rather than at ~15 buttons.

⚠⚠ AND THEREFORE DEBOUNCED, WHICH IS THE POINT OF THE DESIGN RATHER THAN A DETAIL. A bulk fill
    invalidates once, but a reader working through a table presses Fetch on company after company;
    without a debounce that is one ~22s rebuild per press, each one thrown away by the next. The
    thread waits for `_QUIET_SECONDS` of no further invalidation before it starts, and any
    invalidation DURING a rebuild abandons it — what it was building is stale by definition.

⚠ SERIAL, AND IT COSTS ALMOST NOTHING TO BE. The browser fires the twelve requests together and
    gets ~20s of wall clock out of ~140s of summed work, which makes serial look like a 7x
    penalty. It is not: measured at **21.8s** for ACWI, because the expensive half is the metric
    reads and `cached_metric_reads` already shares those — the first endpoint pays for them and the
    other eleven hit the cache, whether they run together or in a queue. So the concurrency was
    buying ~1.3s and costing a spike of GIL contention on a one-worker box, which is the one way a
    speed-up becomes a slow-down. Nobody is waiting on this; it queues.

⚠ ARMED EXPLICITLY BY THE APP, so a process that is not serving pages never starts the thread.
    `invalidate()` runs in unit tests (`test_fill_cancel`) where `deps.create_client` is rigged to
    raise; an always-on background rebuild there would be a thread failing against a fake Supabase
    and logging about it. `arm()` is called from ONE startup hook in `main.py`.

⚠ IT NEVER BLOCKS AND NEVER RAISES INTO ITS CALLER. A prewarm is an optimisation; a failed one
    must cost exactly the cold load it was trying to avoid. Failures are logged at WARNING (uvicorn
    leaves root at WARNING in production — an `info` line here would be invisible where it matters).
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading
import time

_log = logging.getLogger(__name__)

# What the Long Equity benchmark dropdown offers, and the cadence the tab opens on.
#
# ⚠ ANNUAL ONLY, AND AEX IS IN THE LIST ANYWAY. Quarterly is a deliberate click and doubles this
# whole budget for a view most readers never open; the cache still fills it lazily on the first
# press, exactly as before. AEX costs ~2s — it is here not because it is slow but because
# `invalidate()` dropped it too, and leaving one of three dropdown entries cold is the kind of
# asymmetry that later reads as a bug.
#
# ⚠ ORDER IS COST-DESCENDING ON PURPOSE. An invalidation mid-rebuild abandons the rest, so the
# entry that hurts most to lose is the one already finished rather than the one still queued.
_DEFAULT_TARGETS = "ACWI:annual,SP500:annual,AEX:annual"

# ⚠ THE ENV VAR IS A KILL SWITCH AS WELL AS A KNOB: `BLEND_PREWARM=` (empty) disables it entirely,
# which is what a box under memory pressure or a second replica wants. Unset = the default above.
_TARGETS_ENV = "BLEND_PREWARM"

# Long enough to swallow a reader working down a table of per-company Fetch buttons, short enough
# that a bulk fill's rebuild is ready before anyone reloads the page it finished on.
_QUIET_SECONDS = 90.0

# ⚠ A REBUILD MUST NOT RACE THE SCHEDULED PIPELINE. Both are heavy, both are in-process, and the
# pipeline is the one with a deadline. This polls rather than waits on the lock: acquiring it would
# make the prewarm a participant in the pipeline's mutual exclusion and could delay a rebalance.
_PIPELINE_POLL_SECONDS = 60.0

_armed = False
_lock = threading.Lock()
_wake = threading.Event()
# Bumped by every `notify()`. The worker snapshots it before a rebuild and abandons the rebuild if
# it has moved — the cheapest possible "is what I am building already stale?".
_generation = 0
_last_notify = 0.0


def _targets() -> list[tuple[str, str]]:
    """`[(label, cadence)]` to rebuild, from the env or the default. Empty disables the prewarm."""
    raw = os.environ.get(_TARGETS_ENV)
    spec = _DEFAULT_TARGETS if raw is None else raw
    out: list[tuple[str, str]] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        label, _, cadence = part.partition(":")
        out.append((label.strip(), (cadence.strip() or "annual")))
    return out


def arm() -> None:
    """Start the background rebuilder. Idempotent; safe to call before anything is cached."""
    global _armed
    with _lock:
        if _armed:
            return
        targets = _targets()
        if not targets:
            _log.warning("[blend-prewarm] disabled (%s is empty)", _TARGETS_ENV)
            _armed = True                      # armed-but-idle: `notify()` stays a cheap no-op
            return
        _armed = True
        threading.Thread(target=_run, name="bb-blend-prewarm", daemon=True).start()
    _log.warning("[blend-prewarm] armed for %s, %.0fs after the last invalidation",
                 ", ".join(f"{a}/{b}" for a, b in targets), _QUIET_SECONDS)


def notify() -> None:
    """Something dropped the cache — schedule a rebuild once the writes go quiet.

    ⚠ CALLED FROM `_blend_cache.invalidate()`, WHICH RUNS IN WORKER THREADS AND IN TESTS. It must
    therefore be non-blocking, loop-free and a no-op when unarmed.
    """
    global _generation, _last_notify
    if not _armed:
        return
    _generation += 1
    _last_notify = time.monotonic()
    _wake.set()


def _pipeline_busy() -> bool:
    """True while the scheduled ingest pipeline holds its lock. Never raises."""
    try:
        from ingest.phases.pipeline import _PIPELINE_LOCK  # noqa: PLC0415, PLC2701
        return _PIPELINE_LOCK.locked()
    except Exception:                                       # noqa: BLE001 — best effort by design
        return False


def _browser_request():
    """A stub ASGI request that accepts gzip.

    ⚠ NOT `None`. `cached_blend` fills its cache either way, but a request that does not accept
    gzip takes the `gzip.decompress` branch on the way out — decompressing megabytes we are about
    to throw away. Saying "gzip" hands back the stored bytes untouched.
    """
    from starlette.requests import Request  # noqa: PLC0415
    return Request({"type": "http", "method": "POST", "path": "/prewarm",
                    "headers": [(b"accept-encoding", b"gzip")], "query_string": b""})


def _endpoints() -> list[tuple[str, object]]:
    """Every `@cached_blend` endpoint the Long Equity tab fires for a benchmark.

    ⚠ IMPORTED LAZILY, INSIDE THE WORKER. `routers.earnings` imports `_blend_cache`, which imports
    this module's `notify()` — importing `earnings` at module scope would close that circle at
    startup. It is also why this file knows nothing about the endpoints until it needs them.

    ⚠ THE LIST IS EXPLICIT RATHER THAN DISCOVERED. Walking the router for decorated functions would
    silently pick up a portfolio-only endpoint, and a prewarm that spends 140s on something no
    benchmark selection asks for is invisible waste — it succeeds, it just warms the wrong thing.
    """
    from routers import earnings as E  # noqa: PLC0415

    return [
        # First, because every card blocks on it (see `useBenchInputs`) and it is the one request
        # all ten of them share.
        ("universe-period-caps", E.universe_period_caps),
        ("fundamental-blend-metrics", E.fundamental_blend_metrics),
        ("margin-inputs", E.margin_inputs),
        ("debt-ratio-inputs", E.debt_ratio_inputs),
        ("cash-return-inputs", E.cash_return_inputs),
        ("interest-burden-inputs", E.interest_burden_inputs),
        ("sbc-ocf-inputs", E.sbc_ocf_inputs),
        ("capex-margin-inputs", E.capex_margin_inputs),
        ("gross-margin-inputs", E.gross_margin_inputs),
        ("cash-conversion-inputs", E.cash_conversion_inputs),
        ("fcf-sbc-yield-inputs", E.fcf_sbc_yield_inputs),
        ("dividend-yield-inputs", E.dividend_yield_inputs),
    ]


# What `LongEquityTab.tsx` names on the growth blend. ⚠ IT MUST MATCH THE CLIENT'S LIST EXACTLY:
# `cache_key` includes the sorted metrics tuple, so a different list warms an entry the tab will
# never ask for — a prewarm that costs full price and hits nothing, with no symptom but the wait.
_BLEND_METRICS = ["eps_nri", "eps_nri_estimate", "revenue", "fcf_ps", "shares"]


async def _warm_one(label: str, cadence: str, gen: int) -> int:
    """Rebuild one (label, cadence). Returns how many endpoints were warmed."""
    from routers.earnings import FundamentalCoverageRequest  # noqa: PLC0415

    done = 0
    for name, fn in _endpoints():
        if _generation != gen:
            return done
        body = FundamentalCoverageRequest(universe=label, cadence=cadence)
        if name == "fundamental-blend-metrics":
            body.metrics = list(_BLEND_METRICS)
        t0 = time.perf_counter()
        try:
            await fn(body, _browser_request())
        except Exception as exc:                            # noqa: BLE001
            # ⚠ ONE ENDPOINT'S FAILURE IS NOT THE REBUILD'S. A label with no members 404s and
            # always will; the other twelve are still worth warming.
            _log.warning("[blend-prewarm] %s %s/%s failed: %s: %s",
                         name, label, cadence, type(exc).__name__, exc)
            continue
        done += 1
        _log.debug("[blend-prewarm] %s %s/%s in %.1fs", name, label, cadence,
                   time.perf_counter() - t0)
    return done


async def _warm_all(gen: int) -> None:
    for label, cadence in _targets():
        if _generation != gen:
            _log.warning("[blend-prewarm] abandoned at %s — the cache was invalidated again", label)
            return
        t0 = time.perf_counter()
        n = await _warm_one(label, cadence, gen)
        # ⚠ WARNING, NOT INFO. uvicorn leaves the root logger at WARNING in production, so an info
        # line here would be invisible exactly where someone is asking "did the prewarm run?".
        _log.warning("[blend-prewarm] %s/%s warmed %d endpoints in %.1fs",
                     label, cadence, n, time.perf_counter() - t0)


def _run() -> None:
    """The worker: wait for quiet, stand off the pipeline, rebuild, repeat.

    ⚠ ITS OWN EVENT LOOP, IN ITS OWN THREAD. The endpoints are coroutines that do their real work
    inside `asyncio.to_thread`, so they run correctly on any loop — and running them here rather
    than on the server's loop is what keeps a rebuild off the path of every live request.
    """
    while True:
        _wake.wait()
        # Quiet period: any `notify()` while we sleep pushes the start back.
        while True:
            wait = _QUIET_SECONDS - (time.monotonic() - _last_notify)
            if wait <= 0:
                break
            time.sleep(min(wait, 5.0))
        while _pipeline_busy():
            _log.warning("[blend-prewarm] pipeline is running — holding off %.0fs",
                         _PIPELINE_POLL_SECONDS)
            time.sleep(_PIPELINE_POLL_SECONDS)
        # ⚠ CLEARED BEFORE THE SNAPSHOT, NEVER AFTER. Between a rebuild finishing and the flag
        # being cleared, an invalidation would set an already-set event and be lost — the cache
        # would sit cold until the NEXT write. Clearing first means such a notify re-sets it and we
        # go round again, at worst rebuilding something that was already fresh.
        _wake.clear()
        gen = _generation
        try:
            asyncio.run(_warm_all(gen))
        except Exception as exc:                            # noqa: BLE001
            _log.warning("[blend-prewarm] rebuild failed: %s: %s", type(exc).__name__, exc)
