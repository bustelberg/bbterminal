"""In-process cache for the Analyse modal, keyed on a DATA VERSION rather than a clock.

WHY
    `compute_portfolio_analysis` is ~4s locally and ~7s in production (71 HTTP round trips at
    eu-west-3 latency + 16 COPYs + ~2.4s of pandas). It is ONE request — the modal fires nothing
    else — so every re-open pays all of it: toggling the benchmark and back, switching
    `weight_by`, closing and reopening the same portfolio. None of that changes any input.

⚠⚠ A TTL WOULD BE THE WRONG MECHANISM HERE, AND IT IS THE OBVIOUS ONE. This page's entire
    discipline is that a figure is either current or ABSENT — `n/a` when unpriceable, `—` when the
    window is too short, a refusal under `MIN_COVERAGE_PCT` rather than a renormalised guess. A
    time-based cache breaks exactly that: press "Refresh" or repoint a holding, reopen Analyse,
    and it shows yesterday's book for the rest of the window with nothing saying so. The staleness
    would be invisible and would look precisely like a number.

    So the cache key CONTAINS A FINGERPRINT OF THE DATA. If any input table has changed, the key
    changes and the entry is simply not found. Stale is not "unlikely" here, it is unreachable.

HOW THE FINGERPRINT IS CHEAP ENOUGH TO PAY ON EVERY HIT

    The obvious version stamp — `count(*)` + `max(updated_at)` per table — was MEASURED at
    **1,150ms warm and 14.9s cold**, because `count(*)` is a full scan in Postgres and
    `asset_price` holds 39.5M rows. Paying that on a hit would replace a 7-second recompute with a
    1.2-second floor: a poor trade, and one that looks like a win in a benchmark.

    Postgres already counts every tuple operation per table, in shared memory:

        SELECT relname, n_tup_ins + n_tup_upd + n_tup_del FROM pg_stat_user_tables

    Measured: **3.5-20ms**. It is a catalog read, it never scans a table, and — unlike anything
    in-process — it sees writes made by ANOTHER REPLICA, a script, or the scheduler. That is the
    property that makes this safe on Railway, where `invalidate()` from one worker cannot reach
    another.

⚠ THE STAMP CARRIES `pg_postmaster_start_time()` AND `stats_reset` FOR A REASON. Those counters
    are NOT durable: a restart or `pg_stat_reset()` sets them back to zero, so a fingerprint could
    go BACKWARD and match an entry computed against newer data. Folding both timestamps in means a
    reset produces a *different* stamp rather than an earlier one, so the worst case is a
    recompute.

⚠ IT COUNTS TUPLE OPERATIONS, NOT LOGICAL CHANGE, AND THAT ERRS THE SAFE WAY. An UPDATE that sets
    a column to the value it already held still bumps `n_tup_upd`, so we recompute for nothing.
    Over-invalidation costs one recompute; under-invalidation serves a wrong number on a screen
    people trade against. The asymmetry is the whole reason for this design.

⚠ `_WATCHED` MUST LIST EVERY TABLE THE ENDPOINT READS — a table missing from it is a table whose
    changes are INVISIBLE to the cache. The list below was derived by instrumenting a real call
    (wrapping `postgrest.session._c.request` and `common.pg._run_copy_uncached`), not by reading
    the code. ⚠ If you add a read to the analysis path, add its table here. Views do not appear in
    `pg_stat_user_tables`, so `asset_grid` is covered by its BASE tables (`asset_execution`,
    `asset_analysis`).

⚠ NOT A `Cache-Control` HEADER, EVER — same rule as `_blend_cache`. A header hands the browser a
    copy we can no longer reach, so no invalidation can take effect. Server-side only.

⚠ THE CACHED VALUE IS SHARED BY REFERENCE AND MUST BE TREATED AS IMMUTABLE. Nothing downstream
    mutates the payload after it is returned, and nothing should start; a deep copy of a 137KB
    dict on every hit would give back a slice of the win for a hazard that does not exist today.
"""
from __future__ import annotations

import hashlib
import logging
import threading
import time
from typing import Any, Callable

from deps import supabase
from index_universe.templates._cache import _LruTtlCache  # noqa: PLC2701

_log = logging.getLogger(__name__)

# Every table the Analyse endpoint reads, directly or through a view. See the module note.
_WATCHED = (
    "airs_model_portfolio", "airs_model_portfolio_position", "airs_model_portfolio_link",
    "airs_holding", "airs_holding_isin_override", "airs_mutatie", "airs_model_weight",
    "airs_allocation_band", "airs_account_model_link", "airs_account_display_name",
    "airs_transactie_snapshot", "airs_performance",
    "asset_price", "asset_execution", "asset_analysis", "asset_bucket_override",
    "asset_isin_alias",
    "fx_rate", "universe", "universe_membership", "universe_asset_membership", "country",
)

# 137KB per payload, so 48 entries is ~6.5MB — a portfolio has a handful of (benchmark, weight_by,
# source) combinations and there are ~56 portfolios, so this holds a working set, not all of them.
_MAX_ENTRIES = 48

# ⚠ A BACKSTOP, NOT THE MECHANISM. Correctness comes entirely from the fingerprint; this only
# bounds how long a cached payload can sit on a heap if something about the stamp ever goes wrong.
# Generous on purpose: shortening it would not make anything more correct, only slower.
_TTL_SECONDS = 60 * 60

# How long a computed fingerprint is reused before re-reading the catalog. Sub-second, so it only
# collapses the stamp reads of a burst of near-simultaneous requests (the modal + its attribution
# call), never a user action and a later one.
_STAMP_TTL_SECONDS = 2.0

_cache: _LruTtlCache[Any] = _LruTtlCache(max_size=_MAX_ENTRIES, ttl_seconds=_TTL_SECONDS)
_stamp_lock = threading.Lock()
_stamp: tuple[float, str] | None = None   # (expires_monotonic, fingerprint)

# ⚠ The two timestamps are NOT decoration — see the module note: the tuple counters reset to zero
# on a restart or `pg_stat_reset()`, and without these the fingerprint could go BACKWARD and match
# an entry built from newer data.
_STAMP_SQL = (
    "COPY ("
    " SELECT coalesce(string_agg(relname || ':' ||"
    "                 (n_tup_ins + n_tup_upd + n_tup_del)::text, '|' ORDER BY relname), '')"
    "        || '/' || (SELECT extract(epoch FROM pg_postmaster_start_time())::bigint)::text"
    "        || '/' || (SELECT coalesce(extract(epoch FROM stats_reset)::bigint, 0)::text"
    "                   FROM pg_stat_database WHERE datname = current_database())"
    " FROM pg_stat_user_tables WHERE relname = ANY(%s)"
    ") TO STDOUT WITH (FORMAT csv)"
)


def _fingerprint_uncached() -> str | None:
    """One catalog read -> a string that changes whenever any watched table is written.

    `None` means "could not determine", and every caller MUST treat that as a cache miss rather
    than as a constant — a fingerprint we could not read is not evidence that nothing changed.
    """
    from common.pg import _db_url, _run_copy  # noqa: PLC0415

    if not _db_url():
        # No direct-Postgres path: PostgREST cannot read pg_catalog, so there is no cheap
        # fingerprint available and caching is simply off. Correct, and it degrades to today's
        # behaviour rather than to a guess.
        return None
    try:
        # ⚠ `_run_copy`, not `_run_copy_uncached`: inside a `read_cache()` block the identical
        # fingerprint COPY is served from the first one, so the modal and the attribution call
        # that follows it do not each pay for it.
        buf = _run_copy(_STAMP_SQL, (list(_WATCHED),))
    except Exception as e:  # noqa: BLE001 — a cache must never be why a page fails
        _log.warning("[analysis-cache] fingerprint failed (%s: %s) — caching disabled for this "
                     "request", type(e).__name__, e)
        return None
    if buf is None:
        return None
    raw = buf.getvalue()
    if not raw:
        return None
    # Hash it: the raw string is ~1KB of counters and only its IDENTITY matters.
    return hashlib.sha256(raw).hexdigest()[:24]


def fingerprint() -> str | None:
    """The current data fingerprint, reused for `_STAMP_TTL_SECONDS`."""
    global _stamp
    now = time.monotonic()
    with _stamp_lock:
        if _stamp is not None and _stamp[0] > now:
            return _stamp[1]
    fp = _fingerprint_uncached()
    if fp is None:
        return None
    with _stamp_lock:
        _stamp = (now + _STAMP_TTL_SECONDS, fp)
    return fp


def get(key: tuple, fp: str | None) -> Any | None:
    """A cached payload, or `None`. ⚠ `fp is None` ALWAYS misses — a fingerprint we could not read
    is not evidence that nothing changed."""
    if fp is None:
        return None
    hit = _cache.get((fp, *key))
    if hit is not None:
        _log.info("[analysis-cache] HIT %s", key)
    return hit


def put(key: tuple, fp: str | None, value: Any) -> None:
    """Store a payload against the fingerprint it was computed under. No-op without one."""
    if fp is None:
        return
    _cache.put((fp, *key), value)


def cached(key: tuple, compute: Callable[[], Any]) -> Any:
    """`compute()`, memoized against the current data fingerprint.

    ⚠ WITH NO FINGERPRINT THIS IS A PLAIN CALL. That is the honest failure mode: if we cannot tell
    whether the data moved, we must not answer from a copy.
    """
    fp = fingerprint()
    if fp is None:
        return compute()
    full = (fp, *key)
    hit = _cache.get(full)
    if hit is not None:
        _log.info("[analysis-cache] HIT %s", key)
        return hit
    t0 = time.perf_counter()
    out = compute()
    _cache.put(full, out)
    _log.info("[analysis-cache] MISS %s — computed in %.0f ms", key, (time.perf_counter() - t0) * 1000)
    return out


def invalidate() -> int:
    """Drop everything. Not needed for correctness (the fingerprint handles it) — here for an
    operator who wants a cold read, and for tests."""
    n = _cache.size()
    _cache.clear()
    with _stamp_lock:
        globals()["_stamp"] = None
    return n


def stats() -> dict:
    return {"entries": _cache.size(), "max_entries": _MAX_ENTRIES,
            "watched_tables": len(_WATCHED)}
