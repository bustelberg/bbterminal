"""In-process cache for the BENCHMARK blends behind the Long Equity tab.

WHY
    Selecting an index on that tab fires ~12 independent requests — every card owns its own
    `*-inputs` endpoint — and each one loads the whole constituent list. Measured on ACWI
    (1,514 constituents, local and warm): `fundamental-blend-metrics` 3.41s, `cash-return-inputs`
    2.83s, `margin-inputs` 2.17s, `debt-ratio-inputs` 2.07s, `gross-margin-inputs` 1.26s. That is
    ~25s of server work for one dropdown change, repeated for every viewer and every portfolio.

    ⚠ THE EXPENSIVE AXIS IS REQUESTS, NOT METRICS. Asking for one metric costs the same as three
    (3.41s vs 3.08s — the difference is noise), so narrowing what each card asks for wins nothing.
    Caching the whole response is the lever.

    ⚠ AND ONCE IT IS CACHED, THE REMAINING COST IS THE PAYLOAD, WHICH THE CACHE DOES NOT TOUCH.
    Re-measured 2026-08-19 with `scripts/profile_longequity_bench.py`: ACWI's responses were
    **13.21 MB** of JSON (AEX's 0.18 MB — that ratio, not the query time, is what the reader
    reports as "ACWI takes a while"), and a warm hit still re-serialised every byte of it. Two
    changes, in the order they landed: `cached_blend` now stores the GZIPPED body (13.21 -> 4.85 MB
    on the wire, and a hit is a memcpy), and `market_cap_by_period` — the same cap table repeated
    on every row of all ten card responses, 29.9% of each — moved to its own
    `/universe-period-caps` (9.34 MB decoded, **3.16 MB** on the wire).

⚠⚠ AND COLLAPSING THE FAN-OUT IS **NOT** THE OTHER LEVER — MEASURE BEFORE YOU BELIEVE IT. The
    obvious next move is one endpoint returning every card's line, on the theory that the cards
    repeat a lot of shared setup. They do not. The only thing all thirteen share is
    `_load_and_expand_members`, measured at **0.100s** — 1.3s of the 16.6s of serial work, i.e.
    **8%**. The rest is each endpoint reading its OWN metric codes for 1,514 companies, which no
    restructuring removes. (`_blend_inputs` looks like shared setup at 1.0s, but 0.914s of that is
    `coverage_for_async` and only the two blend endpoints call it — subtracting it from a
    `*-inputs` timing measures a function that is not in that call path, which is exactly the
    mistake that made the collapse look worth doing.)

    Worse, collapsing would likely make COLD WALL-CLOCK WORSE. The eleven card requests currently
    run concurrently (16.6s of work in 11.1s); serialising them inside one handler gives back that
    overlap unless the new endpoint re-implements the same fan-out internally. If cold time ever
    needs attacking, the lever is making each endpoint's own read faster — the COPY transport —
    not moving the requests around.

⚠⚠ IN-PROCESS, NEVER A `Cache-Control` HEADER. These endpoints are UI-MUTABLE: pressing Fetch or
    Fill ingests fundamentals and the benchmark line legitimately changes. A cache header hands
    the browser a copy we can no longer reach — the request never arrives, so no invalidation we
    perform can take effect, and the stale line outlives the data by the whole max-age. This repo
    already learned that on the universe member counts (`CACHE_NONE`, not `CACHE_PIPELINE`). A
    server-side entry can be dropped the instant the data changes; a header cannot.

⚠ ONLY `universe` REQUESTS. A portfolio or an explicit holdings list is per-user, changes whenever
    a holding does, and has an unbounded key space — caching it would leak memory AND serve
    someone else's book. `cache_key` returns None for those and the decorator degrades to a plain
    call.

⚠ THE CACHED VALUE IS THE FINISHED, GZIPPED RESPONSE BODY — `bytes`, therefore immutable, and
    handed to every subsequent caller by reference with nothing to copy. (It used to be the payload
    dict, which was shared by reference and had to be treated as immutable by convention.) See
    `cached_blend` for the measured reason: the transfer, not the query, is what a warm ACWI
    selection costs — 9.34 MB of JSON across twelve requests, 3.16 MB on the wire.

AND THEN REBUILT — see `_blend_prewarm`. Dropping the cache puts every viewer back on the ~20s
    cold path for ACWI; the entries have no user dimension, so rebuilding them once in the
    background serves everybody. `invalidate()` notifies it; the rebuild itself is debounced,
    serial, and stands off the scheduled pipeline.

INVALIDATION — TWO MECHANISMS, ON PURPOSE
    * EXPLICIT, from the writer. The fundamentals ingest jobs call `invalidate()` when they
      finish, so the process that changed the data serves fresh results immediately. Same
      discipline as the price phase refreshing `company_price_coverage`.
    * A TTL as backstop. Some writes are out-of-process — a script, another replica — and cannot
      call `invalidate()`. The TTL bounds how long such a write goes unnoticed. It is a safety
      net, not the primary mechanism, which is why it can be generous.
"""
from __future__ import annotations

import asyncio
import functools
import gzip
import inspect
import json
import logging
import threading
from typing import Any, Callable

from fastapi import Request, Response
from fastapi.encoders import jsonable_encoder

# Reused, not reimplemented: one LRU+TTL with one set of eviction semantics. It is generic
# (`_LruTtlCache[T]`) and already carries the thread-safety the concurrent card loads need.
from index_universe.templates._cache import _LruTtlCache  # noqa: PLC2701

_log = logging.getLogger(__name__)

# Generous because `invalidate()` is the real mechanism — fundamentals change only on a deliberate
# ingest, and that path clears this directly. The TTL only has to catch out-of-process writes.
#
# ⚠ SIX HOURS, NOT THIRTY MINUTES (2026-08-19). Half an hour was short enough that a reader who
# picked ACWI, worked through a few portfolios and came back paid the FULL cold cost again —
# measured at 15.8s wall / 94.9s of work for ACWI's eleven requests, against 1.6s for AEX. Nothing
# about a fiscal figure changes on that timescale, and the only thing the TTL protects against is
# a write this process cannot see; `DISABLE_SCHEDULER=1` on every replica but one means there is
# barely such a thing here. Both fundamentals ingest jobs still call `invalidate()`.
_TTL_SECONDS = 6 * 60 * 60
# The blends. ⚠ THIS CAP WAS 24 AND THAT WAS SMALLER THAN ONE SCREEN'S WORKING SET: fourteen
# endpoints are cached per (label, cadence), so picking ACWI filled 14 slots and flipping to
# Quarterly — or to a second index for comparison — evicted the set the reader had just paid ~20s
# for. Coming back re-paid it in full. 84 is the whole real key space (14 endpoints x 3 labels x 2
# cadences), which is affordable ONLY because the entries are now the GZIPPED BYTES rather than
# the Python payload: ACWI's biggest is 479 KB compressed where the dict behind it is 1.34 MB of
# JSON across tens of thousands of sub-dicts. See `cached_blend`.
_MAX_ENTRIES = 84
# The fundamentals GRID, in its own cache — ⚠ NOT because it needs different semantics (it is
# cleared by the same `invalidate()`), but because its entries are TEN TIMES the size of a blend's
# (ACWI is ~5.3 MB compressed). Sharing one LRU meant a single grid read could evict a third of the
# blend set, and raising the shared cap to fit the blends would have licensed six grids —
# ~32 MB — at the same time. Two caps state the two budgets separately.
_GRID_MAX_ENTRIES = 6

_cache: _LruTtlCache[Any] = _LruTtlCache(max_size=_MAX_ENTRIES, ttl_seconds=_TTL_SECONDS)
_grid_cache: _LruTtlCache[Any] = _LruTtlCache(max_size=_GRID_MAX_ENTRIES, ttl_seconds=_TTL_SECONDS)


def cache_key(endpoint: str, body: Any) -> tuple | None:
    """The key for this request, or None when it must not be cached.

    Everything that can change the answer is in the key. `_BLEND_START` is a constant and
    `_members()` takes no user input, so nothing else varies it.

    ⚠ NO USER DIMENSION, DELIBERATELY. An index's constituents are the same for every viewer —
    there is no ownership or `view_as` branch in these responses — so one entry serves everybody.
    If that ever stops being true the role has to become part of this tuple.
    """
    universe = getattr(body, "universe", None)
    if not universe:
        return None
    metrics = getattr(body, "metrics", None)
    return (endpoint, universe, getattr(body, "cadence", "annual"), tuple(sorted(metrics or ())))


def invalidate() -> int:
    """Drop every cached blend. Returns how many entries went.

    Deliberately not selective: a fundamentals ingest can touch any constituent of any index, and
    working out which labels a given company belongs to would be a second, subtler mapping to keep
    in step. Both caches are small and cost seconds to rebuild lazily.

    ⚠ THE METRIC-READ CACHE GOES TOO. It holds the very rows an ingest just rewrote, so keeping it
    would rebuild "fresh" responses on top of stale fundamentals — the one outcome worse than not
    caching at all, because it looks like it worked.
    """
    n = _cache.size() + _grid_cache.size() + _metrics_cache.size()
    _cache.clear()
    _grid_cache.clear()
    _metrics_cache.clear()
    if n:
        _log.info("[blend-cache] invalidated %d entr%s", n, "y" if n == 1 else "ies")
    # ⚠ THE REBUILD IS HUNG OFF THE CLEAR, NOT OFF THE INGEST JOB. Two writers drop this cache —
    # the bulk fill and the per-company Fetch in the modal — and the second is pressed while a
    # reader is looking at the very chart it invalidates. Notifying from here covers both and
    # cannot be forgotten by a third. It is non-blocking, needs no event loop, and is a no-op
    # until `arm()` runs, so unit tests that call `invalidate()` start no thread. Imported
    # lazily: `_blend_prewarm` reaches back into `routers.earnings`, which imports this module.
    from routers import _blend_prewarm  # noqa: PLC0415
    _blend_prewarm.notify()
    return n


# ── Metric-read dedupe: the SAME line fetched by several cards at once ──────────────────────────
#
# ⚠⚠ THIS EXISTS TO DEDUPE A BURST, NOT TO PERSIST. Measured on the Long Equity tab: it issues 27
#   metric reads but only 18 are DISTINCT — `sbc` is read by five cards (margin, cash-return,
#   sbc-ocf, cash-conversion, fcf-sbc-yield), `fcf` by four, `revenue` by three. The cards fire
#   together, so those duplicates are concurrent, not sequential. Hence a SHORT ttl and a small
#   cap: persisting across loads is the response cache's job, and holding ~1,500 companies x ~11
#   periods per metric for longer than the burst is memory for nothing.
#
# ⚠ SINGLE-FLIGHT IS THE WHOLE MECHANISM. Without it the five `sbc` callers all miss at the same
#   instant and all five compute — a plain cache would save nothing on the one load that hurts.
#   THREADING primitives, not asyncio: these reads run inside `asyncio.to_thread`, so the
#   contending callers are worker THREADS.
_metrics_cache: _LruTtlCache[Any] = _LruTtlCache(max_size=32, ttl_seconds=60.0)
_metrics_lock = threading.Lock()
_metrics_inflight: dict[tuple, threading.Event] = {}
# A read that outlives this is pathological; waiting longer would hold a worker thread hostage to
# a request that has probably already failed. On timeout the waiter just computes it itself.
_INFLIGHT_TIMEOUT = 60.0


def cached_metric_reads(company_ids: list[int], metrics: list[str], cadence: str,
                        compute_many: Callable[[list[str]], dict[str, Any]]) -> dict[str, Any]:
    """SEVERAL lines at once — cached and single-flighted per metric, but READ in one go.

    ⚠ THIS REPLACED A SINGLE-METRIC `cached_metric_read`, WHICH IS GONE RATHER THAN KEPT BESIDE IT.
    Two implementations of one cache is two sets of eviction and single-flight semantics to keep in
    step, and the one that drifts is whichever has fewer callers. A caller wanting one line passes
    a list of one.

    It exists because the two savings it has to deliver pull in opposite directions:

      * ACROSS requests, the tab's thirteen cards want 30 metrics of which only 18 are DISTINCT
        (`sbc` is wanted by five cards, `fcf` by four). Those must collapse to one read each —
        which is what the per-metric key and the in-flight map already do.
      * WITHIN a request, an endpoint wanting five lines should not open five Postgres
        connections. On the COPY transport each read is its own connect + TLS + auth, so a
        naive "one read per metric" costs 18 handshakes for the tab.

    ⚠⚠ AND THE OBVIOUS IMPLEMENTATION OF THE SECOND DESTROYS THE FIRST. Batching per endpoint —
    each card reading its own metrics together — means the five cards that want `sbc` each fetch
    it, so 18 shared reads become 30 unshared ones. That is SLOWER than what it replaces, while
    looking like an optimisation.

    So the batch is over what THIS caller is missing and nobody else is already fetching: cache
    hits are taken, metrics another thread owns are waited on, and only the remainder — the ones
    this caller claims — go into a single `compute_many`.

    ⚠ `compute_many` MUST RETURN A KEY FOR EVERY METRIC IT IS GIVEN, empty when there is nothing.
    A missing key is cached as nothing at all, so every later caller re-reads it — which converts
    a refused line (a quarterly metric with no TTM roll-up) into a permanent per-request query.

    ⚠ THE EVENTS ARE RELEASED IN A `finally`, AND THAT IS NOT BOILERPLATE. This call owns SEVERAL
    in-flight keys at once; if `compute_many` raises and they are not all set, every waiter on any
    of them blocks for the full `_INFLIGHT_TIMEOUT` (60s) — one failed read stalling the whole tab
    for a minute. The single-metric version could get away with less because it only ever held one.

    ⚠ THE COMPANY SET IS IN THE KEY AS A TUPLE, NOT A HASH. A hash collision here would serve one
    universe's fundamentals as another's — silently, and only for whoever hit the collision. It is
    ~12KB for ACWI, which against a 32-entry cap is nothing; it is now built ONCE per call rather
    than per metric (eighteen times per prefetch) purely to construct a key.
    """
    ids_key = tuple(company_ids)
    out: dict[str, Any] = {}
    pending: list[str] = []
    # `dict.fromkeys` rather than a set: a caller listing a metric twice must not make the batch
    # order (and so the failure it reports) depend on set iteration order.
    for m in dict.fromkeys(metrics):
        hit = _metrics_cache.get((m, cadence, ids_key))
        if hit is not None:
            out[m] = hit
        else:
            pending.append(m)
    if not pending:
        return out

    # ⚠ ONE PASS UNDER THE LOCK FOR ALL OF THEM. Claiming them one at a time would let two
    # callers interleave and each end up owning half of a batch the other is waiting on.
    owned: list[str] = []
    waiting: list[tuple[str, threading.Event]] = []
    with _metrics_lock:
        for m in pending:
            key = (m, cadence, ids_key)
            event = _metrics_inflight.get(key)
            if event is None:
                _metrics_inflight[key] = threading.Event()
                owned.append(m)
            else:
                waiting.append((m, event))

    if owned:
        try:
            produced = compute_many(owned)
            for m in owned:
                if m in produced:
                    _metrics_cache.put((m, cadence, ids_key), produced[m])
                    out[m] = produced[m]
        finally:
            with _metrics_lock:
                events = [_metrics_inflight.pop((m, cadence, ids_key), None) for m in owned]
            for event in events:
                if event is not None:
                    event.set()

    for m, event in waiting:
        event.wait(timeout=_INFLIGHT_TIMEOUT)
        hit = _metrics_cache.get((m, cadence, ids_key))
        # ⚠ A WAITER WHOSE OWNER FAILED RECOMPUTES — just this one line, not the whole batch.
        # Same judgement as the single-metric version: a transient fault costs the caller that hit
        # it, not every card on the tab.
        out[m] = hit if hit is not None else compute_many([m]).get(m, {})
    return out


def cached_grid(label: str, cadence: str, compute: Callable[[], Any]) -> Any:
    """The fundamentals GRID for one (label, cadence), computed once and shared.

    ⚠ IT LIVES IN A CACHE `invalidate()` CLEARS, NOT IN A `functools.lru_cache` NEXT TO THE
    ENDPOINT. Pressing Fetch or Fill ingests fundamentals and the grid legitimately changes, and
    both ingest jobs already call `invalidate()`; the failure mode of forgetting is a table that
    silently keeps showing dashes for a row that just filled in, which reads as the button not
    working. `_grid_cache` is a second STORE but not a second thing to remember — `invalidate()`
    clears it in the same breath as the blends, which is the property that mattered.

    ⚠ SINGLE-FLIGHT, FOR THE SAME REASON THE METRIC READ HAS IT. The pane fetches on mount and the
    period control fetches the other cadence; a reader who opens an index, presses Q3 and goes back
    can have two of these in flight against a cold cache. They are the most expensive read on the
    page, and computing the same one twice is the case worth spending a lock on.

    ⚠ THE KEY HAS NO USER DIMENSION — see `cache_key`. An index's constituents and their filed
    figures are the same for every viewer; nothing in this payload branches on role. The `isAdmin`
    difference is entirely in the browser (which columns render), not in what the server returns.

    ⚠ THE VALUE IS SHARED BY REFERENCE AND MUST BE TREATED AS IMMUTABLE, exactly as the blend
    responses are. The endpoint stores the FINISHED gzipped bytes, which are immutable anyway and
    are smaller than the payload object they came from — see `_encoded` in `benchmarks.py`.

    ⚠ ITS OWN CAP (`_GRID_MAX_ENTRIES`) BECAUSE ITS ENTRIES ARE THE BIGGEST HERE — ACWI is ~5.3 MB
    compressed, ten times a blend's. Sharing the blends' LRU meant one grid read could evict a
    third of the blend set; six is the real grid key space (3 labels x 2 cadences). What an
    eviction costs is a rebuild, never a wrong answer.
    """
    key = ("fundamental-grid", label, cadence)
    hit = _grid_cache.get(key)
    if hit is not None:
        return hit

    with _metrics_lock:
        event = _metrics_inflight.get(key)
        owner = event is None
        if owner:
            event = threading.Event()
            _metrics_inflight[key] = event

    if not owner:
        event.wait(timeout=_INFLIGHT_TIMEOUT)          # type: ignore[union-attr]
        hit = _grid_cache.get(key)
        # ⚠ A WAITER WHOSE OWNER FAILED RECOMPUTES rather than inheriting the failure — same
        # judgement as `cached_metric_read`. A transient fault should cost the caller that hit it.
        return hit if hit is not None else compute()

    try:
        result = compute()
        _grid_cache.put(key, result)
        return result
    finally:
        with _metrics_lock:
            _metrics_inflight.pop(key, None)
        event.set()                                    # type: ignore[union-attr]


def _encode(payload: Any) -> bytes:
    """The finished response body, gzipped — byte-for-byte what FastAPI would have sent.

    ⚠ `jsonable_encoder` + these exact `json.dumps` arguments ARE FastAPI's own `JSONResponse`
    rendering, copied rather than approximated. `allow_nan=False` is the load-bearing one: a NaN
    that leaks into a blend is a 500 today, and a hand-rolled serialiser that quietly emitted
    `NaN` would turn that into a chart with an unparseable point and no error anywhere.
    """
    return gzip.compress(
        json.dumps(jsonable_encoder(payload), ensure_ascii=False, allow_nan=False,
                   separators=(",", ":")).encode("utf-8"), 1)


def cached_blend(endpoint: str) -> Callable:
    """Decorate a `(body, request)` endpoint so benchmark requests are cached AND compressed.

    ⚠ APPLY IT BELOW `@router.post(...)`, so the router registers the wrapper rather than the bare
    function. `functools.wraps` sets `__wrapped__`, which `inspect.signature` follows — that is
    what keeps FastAPI's request-model introspection seeing the real signature through this.

    ⚠⚠ THE CACHE HOLDS THE GZIPPED BYTES, NOT THE PAYLOAD OBJECT — and that is TWO wins, not one.
    Measured 2026-08-19 on ACWI (1,514 constituents, annual), for the eleven requests one benchmark
    selection fires:

        the whole selection, decoded  9.34 MB          on the wire   3.16 MB   (3.0x)
        `margin-inputs` alone         982 KB           on the wire    336 KB
        AEX, the same requests        137 KB    <-- why ACWI feels slow and AEX does not

    1. TRANSFER. Those megabytes are shipped on EVERY load, warm cache included, and by the time
       the server work is a dict lookup the transfer IS the load time. 2.7x is the whole win on a
       warm process and no query tuning touches it.
    2. CPU ON A CACHE HIT, WHICH WAS NOT FREE AND LOOKED IT. Returning the dict meant FastAPI
       re-ran `jsonable_encoder` (111 ms) + `json.dumps` (27 ms) on every hit — ~1.5s of Python
       across the eleven, on a single-worker dyno, to reproduce bytes identical to last time's.
       Encoding once and storing the result makes a hit a memcpy.

    A third follows: an entry is a few hundred KB of bytes instead of a payload spread over tens
    of thousands of dicts, which is what makes `_MAX_ENTRIES` = 84 affordable — see there for why
    24 was too few. Measured end to end after the change: AEX 2.3s cold -> 0.01s warm, ACWI 20.5s
    cold -> 0.28s warm, with AEX STILL warm after ACWI's entries landed (exactly what 24 broke).

    ⚠ GZIPPED HERE RATHER THAN BY A `GZipMiddleware`, for the reason `/api/benchmarks/…/grid`
    records at length: this app is SSE-heavy and compression sits between a stream and its client
    and buffers. And the `Accept-Encoding` header is HONOURED, not assumed — a plain `curl` does
    not send it, and shipping gzip to a client that did not ask hands it binary.

    ⚠ ONLY THE `universe` PATH IS ENCODED. A portfolio request returns the dict exactly as before:
    it is not cached (see `cache_key`), so compressing it would be pure CPU on the request that
    can least spare it, and the payload is a book's ~40 holdings rather than an index's 1,514.

    ⚠ `request: Request` IS REQUIRED ON THE ENDPOINT, and the decorator refuses at import time
    rather than at 3am. A missing one is not a crash — the wrapper would simply never see an
    `Accept-Encoding` and would fall back to sending uncompressed — i.e. a new card would silently
    opt out of the only optimisation this file exists for, and nothing would look wrong.
    """
    def deco(fn: Callable) -> Callable:
        if "request" not in inspect.signature(fn).parameters:
            raise TypeError(
                f"@cached_blend({endpoint!r}): {fn.__name__} needs a `request: Request` parameter "
                "so the response can honour Accept-Encoding. Add it and pass nothing — the "
                "decorator reads the header itself.")

        # ⚠ `(body, request, ...)` POSITIONALLY, MIRRORING WHAT IT DEMANDS OF `fn`. FastAPI always
        # calls a route by keyword, so a keyword-only `request` would have worked in production and
        # raised "multiple values for argument 'request'" the moment anything called the endpoint
        # function directly — which the profiler and the ingest paths do.
        @functools.wraps(fn)
        async def wrapper(body, request: Request | None = None, *args, **kwargs):
            key = cache_key(endpoint, body)
            if key is None:                       # a portfolio / explicit holdings — never cached
                return await fn(body, request, *args, **kwargs)
            blob = _cache.get(key)
            if blob is None:
                # ⚠ ENCODED OFF THE EVENT LOOP. `jsonable_encoder` over ~60,000 dicts is 111 ms of
                # pure Python and gzip another 18 ms; run inline they would block every other card
                # request in this process for the duration, eleven times over on a cold selection.
                out = await fn(body, request, *args, **kwargs)
                blob = await asyncio.to_thread(_encode, out)
                _cache.put(key, blob)
            accepts = "gzip" in (
                (request.headers.get("accept-encoding") if request else "") or "").lower()
            if accepts:
                return Response(content=blob, media_type="application/json",
                                headers={"Content-Encoding": "gzip"})
            # ⚠ DECOMPRESSED ON THE WAY OUT, never stored twice — the same trade `/grid` makes.
            # This branch is a curl session, not the app.
            return Response(content=gzip.decompress(blob), media_type="application/json")
        return wrapper
    return deco
