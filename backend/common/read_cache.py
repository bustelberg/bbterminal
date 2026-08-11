"""Read the same row twice in one request, pay for it once.

WHY THIS EXISTS, MEASURED ON THE /management-dashboard ANALYSE MODAL (2026-08-11)

    One press of Analyse on BUS_Neutraal_FX issued **212 database round trips**, and **103 of them
    were byte-identical repeats** -- 44 distinct queries asked between 2 and 9 times:

        x9   airs_performance?select=portefeuille,periode,beginvermogen,...
        x5   airs_model_portfolio_position?select=portfolio_id,isin,fonds
        x5   airs_model_portfolio?select=id,name,display_name,omschrijving
        x3   asset_grid?select=isin,analysis_id,yahoo_symbol,...
        x6   universe?select=universe_id&label=eq.SP500&limit=1

    Nobody wrote that loop. The endpoint is one question answered by a dozen collaborating
    modules -- look-through, the book ledger, the benchmark, attribution, the axes -- and each is
    correct on its own to load what it needs. The duplication is a property of the COMPOSITION,
    which is exactly the kind of cost that cannot be fixed in any one of them.

⚠⚠ THE ROUND-TRIP COUNT IS THE POINT, NOT THE MILLISECONDS. Locally those 103 repeats cost about
    0.8s of a 4.9s load, because a local PostgREST call is ~5ms. Production talks to Supabase over
    the network at ~50-80ms a call, where the same 103 repeats are **5-8 seconds**. A profile taken
    on a laptop will always understate this by an order of magnitude, which is why the fix is
    counted in requests rather than in a local stopwatch.

WHAT IT IS, AND WHAT IT DELIBERATELY IS NOT

    It is a per-REQUEST memo: opt in around one unit of work, and identical reads inside it are
    served from the first answer. It is NOT a TTL cache, not shared between requests, and not
    global -- there is no window in which one reader sees another reader's data, because the store
    dies with the block that made it.

    Two transports, one rule: PostgREST GETs (through the session `deps` installs) and direct
    Postgres COPY (`common.pg._run_copy`). Those are the only two ways this codebase reads.

⚠ THE HTTP RESPONSE IS CACHED, NOT THE PARSED ROWS -- and that is what makes it safe to hand the
    same answer to two callers. postgrest re-parses the body into FRESH dicts on every
    `APIResponse.from_http_request_response`, so a caller that mutates a row it got back cannot
    corrupt the next caller's copy. Caching `.data` would have needed a deep copy to be equally
    safe, and a deep copy of the 150KB airs_performance payload is not obviously cheaper than the
    query it replaces. Same rule for COPY: the BYTES are cached and each caller gets a new
    `BytesIO` over them, so nobody inherits anybody else's read cursor.

⚠ ANY WRITE EMPTIES THE STORE. A POST/PATCH/DELETE means the snapshot may no longer describe the
    database, and a memo that keeps serving reads across it is how a request comes to contradict
    itself. Read-only endpoints never trigger this; anything else pays one re-read, which is the
    correct price.

⚠ ONLY SUCCESSFUL READS ARE STORED. Caching a 500 would turn one flake into a request-long
    outage, and an error is the one answer worth asking about again.
"""
from __future__ import annotations

import io
import logging
import time
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

log = logging.getLogger(__name__)

# The active memo, or None when nothing opted in. A ContextVar rather than a module global so it
# cannot leak between concurrently-served requests -- and because `asyncio.to_thread` COPIES the
# context into the worker thread, which is how every one of these endpoints actually runs.
_ACTIVE: ContextVar[dict[str, Any] | None] = ContextVar("read_cache", default=None)


@contextmanager
def read_cache(label: str = ""):
    """Memoize identical database reads for the duration of this block.

    Yields the stats dict, so a caller can report what it saved:
    `{"hits", "misses", "writes", "saved_ms"}`.

    ⚠ NESTS BY DOING NOTHING. An inner `read_cache()` inside an outer one keeps using the OUTER
    store rather than starting a second: a nested block that shadowed it would drop every entry
    its parent had already paid for, at the exact moment the parent is mid-computation.
    """
    outer = _ACTIVE.get()
    if outer is not None:
        yield outer
        return
    state: dict[str, Any] = {"store": {}, "hits": 0, "misses": 0, "writes": 0,
                             "saved_ms": 0.0, "label": label}
    token = _ACTIVE.set(state)
    try:
        yield state
    finally:
        _ACTIVE.reset(token)
        if state["hits"]:
            log.info("[read_cache] %s: %d served from %d read(s), ~%.0fms saved%s",
                     label or "request", state["hits"], state["misses"], state["saved_ms"],
                     f", {state['writes']} write(s) cleared it" if state["writes"] else "")


def active() -> dict[str, Any] | None:
    """The live memo, or None. Transports call this; nothing else should need it."""
    return _ACTIVE.get()


def note_write() -> None:
    """A write happened -- the snapshot is no longer trustworthy, so drop it."""
    st = _ACTIVE.get()
    if st is not None and st["store"]:
        st["store"].clear()
        st["writes"] += 1


def lookup(key: Any) -> Any | None:
    st = _ACTIVE.get()
    if st is None:
        return None
    hit = st["store"].get(key)
    if hit is None:
        return None
    st["hits"] += 1
    st["saved_ms"] += hit[1]
    return hit[0]


def store(key: Any, value: Any, elapsed_ms: float) -> None:
    st = _ACTIVE.get()
    if st is None:
        return
    st["misses"] += 1
    st["store"][key] = (value, elapsed_ms)


def copy_bytes(key: Any, run, *args) -> io.BytesIO | None:
    """Run a COPY through the memo. `run(*args)` performs it and returns `BytesIO | None`.

    ⚠ A FRESH `BytesIO` PER CALLER, over the same bytes. Handing back the cached object would give
    the second caller a stream already read to EOF -- an empty result that looks exactly like "the
    database has no rows for this", which is the worst possible way to be wrong here.
    """
    if _ACTIVE.get() is None:
        return run(*args)
    cached = lookup(key)
    if cached is not None:
        return io.BytesIO(cached)
    t0 = time.perf_counter()
    out = run(*args)
    ms = (time.perf_counter() - t0) * 1000
    # ⚠ `None` IS NOT CACHED. It means the COPY path is unavailable (no SUPABASE_DB_URL, psycopg
    # missing, a connection error) and the caller must fall back -- a fallback answer is not a
    # value to remember, and the connection may well be back on the next attempt.
    if out is None:
        return None
    raw = out.getvalue()
    store(key, raw, ms)
    return io.BytesIO(raw)
