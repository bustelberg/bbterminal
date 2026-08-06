import asyncio, os, time, statistics
import routers.earnings as E
from routers import _blend_cache
from routers.earnings import FundamentalCoverageRequest as R

DB = "postgresql://postgres:postgres@127.0.0.1:54322/postgres"
NAMES = ['margin_inputs','debt_ratio_inputs','cash_return_inputs','interest_burden_inputs',
         'sbc_ocf_inputs','capex_margin_inputs','gross_margin_inputs','cash_conversion_inputs',
         'fcf_sbc_yield_inputs','dividend_yield_inputs']
fns = [getattr(E, n) for n in NAMES]
_real = _blend_cache.cached_metric_read
def _nodedupe(cids, metric, cadence, compute): return compute()

async def one_load():
    _blend_cache._cache.clear(); _blend_cache._metrics_cache.clear()
    t = time.perf_counter()
    await asyncio.gather(
        E.fundamental_blend_metrics(R(universe='ACWI', cadence='annual',
                                      metrics=['revenue','fcf_ps','shares'])),
        *[f(R(universe='ACWI', cadence='annual')) for f in fns])
    return time.perf_counter() - t

CFG = [("PostgREST        ", False, False), ("PostgREST+dedupe ", False, True),
       ("COPY             ", True,  False), ("COPY+dedupe      ", True,  True)]
res = {c[0]: [] for c in CFG}
for _round in range(3):                       # INTERLEAVED: a,b,c,d, a,b,c,d, ...
    for label, copy_on, dedupe in CFG:
        if copy_on: os.environ["SUPABASE_DB_URL"] = DB
        else: os.environ.pop("SUPABASE_DB_URL", None)
        _blend_cache.cached_metric_read = _real if dedupe else _nodedupe
        E.cached_metric_read = _blend_cache.cached_metric_read
        res[label].append(asyncio.run(one_load()))
base = statistics.median(res["PostgREST        "])
for label, _, _ in CFG:
    v = res[label]
    print(f"{label} median {statistics.median(v):5.2f}s   runs {[f'{x:.2f}' for x in v]}   {base/statistics.median(v):4.2f}x")
