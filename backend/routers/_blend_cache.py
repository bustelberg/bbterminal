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

⚠ THE CACHED VALUE IS SHARED AND MUST BE TREATED AS IMMUTABLE. It is handed to every subsequent
    caller by reference; a deep copy would cost more than it saves (an ACWI response carries a
    `coverage.rows` entry per constituent). Nothing downstream mutates a response after returning
    it, and nothing should start.

INVALIDATION — TWO MECHANISMS, ON PURPOSE
    * EXPLICIT, from the writer. The fundamentals ingest jobs call `invalidate()` when they
      finish, so the process that changed the data serves fresh results immediately. Same
      discipline as the price phase refreshing `company_price_coverage`.
    * A TTL as backstop. Some writes are out-of-process — a script, another replica — and cannot
      call `invalidate()`. The TTL bounds how long such a write goes unnoticed. It is a safety
      net, not the primary mechanism, which is why it can be generous.
"""
from __future__ import annotations

import functools
import logging
from typing import Any, Callable

# Reused, not reimplemented: one LRU+TTL with one set of eviction semantics. It is generic
# (`_LruTtlCache[T]`) and already carries the thread-safety the concurrent card loads need.
from index_universe.templates._cache import _LruTtlCache  # noqa: PLC2701

_log = logging.getLogger(__name__)

# Generous because `invalidate()` is the real mechanism — fundamentals change only on a deliberate
# ingest, and that path clears this directly. The TTL only has to catch out-of-process writes.
_TTL_SECONDS = 30 * 60
# ⚠ BOUNDED BECAUSE THE ENTRIES ARE BIG. Each response carries one `coverage.rows` entry per
# constituent — ~1,500 for ACWI — so an unbounded cache is a slow leak on a small dyno. The real
# key space is ~3 labels x 2 cadences x 13 endpoints; this caps the resident set well under that.
_MAX_ENTRIES = 24

_cache: _LruTtlCache[Any] = _LruTtlCache(max_size=_MAX_ENTRIES, ttl_seconds=_TTL_SECONDS)


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
    in step. The whole cache is at most `_MAX_ENTRIES` and costs seconds to rebuild lazily.
    """
    n = _cache.size()
    _cache.clear()
    if n:
        _log.info("[blend-cache] invalidated %d entr%s", n, "y" if n == 1 else "ies")
    return n


def cached_blend(endpoint: str) -> Callable:
    """Decorate a `(body: FundamentalCoverageRequest)` endpoint so benchmark requests are cached.

    ⚠ APPLY IT BELOW `@router.post(...)`, so the router registers the wrapper rather than the bare
    function. `functools.wraps` sets `__wrapped__`, which `inspect.signature` follows — that is
    what keeps FastAPI's request-model introspection seeing the real signature through this.
    """
    def deco(fn: Callable) -> Callable:
        @functools.wraps(fn)
        async def wrapper(body, *args, **kwargs):
            key = cache_key(endpoint, body)
            if key is None:                       # a portfolio / explicit holdings — never cached
                return await fn(body, *args, **kwargs)
            hit = _cache.get(key)
            if hit is not None:
                return hit
            out = await fn(body, *args, **kwargs)
            _cache.put(key, out)
            return out
        return wrapper
    return deco
