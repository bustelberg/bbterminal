"""The Long Equity tab's benchmark load, end to end: all eleven card endpoints, concurrently.

Drives the real handlers against a local Supabase, so it measures what the tab measures rather
than one function in isolation. Read-only.

FOUR AXES, and they are not independent — which is the point of running them together:
  * TRANSPORT — PostgREST paging vs a direct `COPY` (`SUPABASE_DB_URL`).
  * DEDUPE    — whether the 30 metric requests across the cards collapse to the 18 distinct ones.
  * BATCH     — whether the metrics one card claims are read in ONE query or one query each.

⚠ INTERLEAVED (a,b,c,d, a,b,c,d, …), NEVER IN BLOCKS. Run-to-run spread here is wide and the
    machine warms under the benchmarking itself, so a blocked A/B hands you a stable-looking
    difference that does not exist. Same rule the pytest suite's notes state.

⚠ THE BATCH AXIS BARELY MOVES LOCALLY AND THAT IS EXPECTED, NOT A NULL RESULT. What it removes is
    Postgres CONNECTIONS (connect + TLS + auth per `COPY`), which cost ~2ms to a local Docker
    Postgres and 150-250ms to Supabase. Count the connections, don't read the clock — the counter
    below is the number that predicts production.
"""
import asyncio
import os
import statistics
import time

import common.pg as pg
import routers.earnings as E
from routers import _blend_cache
from routers.earnings import FundamentalCoverageRequest as R

DB = "postgresql://postgres:postgres@127.0.0.1:54322/postgres"
NAMES = ['margin_inputs', 'debt_ratio_inputs', 'cash_return_inputs', 'interest_burden_inputs',
         'sbc_ocf_inputs', 'capex_margin_inputs', 'gross_margin_inputs', 'cash_conversion_inputs',
         'fcf_sbc_yield_inputs', 'dividend_yield_inputs']
fns = [getattr(E, n) for n in NAMES]

# ⚠ CAPTURED BEFORE THE LOOP PATCHES THEM. Reading these back off the module inside the loop
# returns whatever the previous round installed, so every config after the first would measure the
# one before it.
_real = _blend_cache.cached_metric_reads
_bulk_real = E.metrics_by_company_bulk


def _nodedupe(cids, metrics, cadence, compute_many):
    """No cross-request sharing: every card reads its own lines."""
    return compute_many(list(metrics))


# ── count the direct-Postgres connections one tab load opens ────────────────────────────────────
conns = {"n": 0}
_orig_copy = pg._run_copy


def _counting_copy(sql, params):
    conns["n"] += 1
    return _orig_copy(sql, params)


pg._run_copy = _counting_copy
import routers._earnings_pg as epg  # noqa: E402

epg._run_copy = _counting_copy


def _per_metric(company_ids, ms, cadence):
    """The pre-2026-08-10 read: one query per metric, each its own connection."""
    return {m: E._metrics_by_company(company_ids, m, cadence) for m in ms}


async def one_load():
    _blend_cache._cache.clear()
    _blend_cache._metrics_cache.clear()
    conns["n"] = 0
    t = time.perf_counter()
    await asyncio.gather(
        E.fundamental_blend_metrics(R(universe='ACWI', cadence='annual',
                                      metrics=['revenue', 'fcf_ps', 'shares'])),
        *[f(R(universe='ACWI', cadence='annual')) for f in fns])
    return time.perf_counter() - t, conns["n"]


#            label                copy   dedupe  batched
CFG = [("PostgREST            ", False, False, False),
       ("PostgREST+dedupe     ", False, True, False),
       ("COPY                 ", True, False, False),
       ("COPY+dedupe          ", True, True, False),
       ("COPY+dedupe+batch    ", True, True, True)]

res = {c[0]: [] for c in CFG}
cn = {c[0]: 0 for c in CFG}
for _round in range(3):                       # INTERLEAVED: a,b,c,d,e, a,b,c,d,e, ...
    for label, copy_on, dedupe, batched in CFG:
        if copy_on:
            os.environ["SUPABASE_DB_URL"] = DB
        else:
            os.environ.pop("SUPABASE_DB_URL", None)
        _blend_cache.cached_metric_reads = _real if dedupe else _nodedupe
        E.cached_metric_reads = _blend_cache.cached_metric_reads
        E.metrics_by_company_bulk = _bulk_real if batched else _per_metric
        secs, n = asyncio.run(one_load())
        res[label].append(secs)
        cn[label] = n

base = statistics.median(res["PostgREST            "])
for label, _c, _d, _b in CFG:
    v = res[label]
    print(f"{label} median {statistics.median(v):5.2f}s   runs {[f'{x:.2f}' for x in v]}   "
          f"{base / statistics.median(v):4.2f}x   {cn[label]:>2} pg connections")
