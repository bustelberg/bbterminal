"""Daily drift check — has the vendor rewritten history we already stored?

⚠ THE PIPELINE CANNOT SEE A CORRECTION TO THE PAST. `_upsert_metric_rows` writes
only `d > existing_max`, so a split, a reverse split or a free-share attribution
leaves our old bars on the old basis for ever while new ones arrive on the new
one. Measured on Leonteq 2026-08-02: 173 companies had wrong close history, 887
had wrong volume history, and Worldline sat in the live book on a **+1142%**
momentum for a stock that had fallen **69%**.

The monthly full refetch is the guarantee. This is the early warning between
them, and it exists because the undocumented `?start_date=&end_date=` filter
makes a probe **23 bytes instead of 268,703** — an 11,682× cut that turns
"re-verify the universe" from a 20-minute, 400 MB job into a few seconds.

⚠ IT DOES NOT SAVE QUOTA, WHICH IS WHY IT RUNS ON A SLICE. `api_usage` counts
REQUESTS (20,000/region/month), and a one-day probe costs the same one request as
a full history. Probing all 1,479 daily would be ~32,500 requests/month — over
the USA cap on its own, with nothing left for the price updates that share it. So
each day probes 1/`SLICE_DIVISOR` of the universe: every company is re-verified
within a week, at ~300 requests a day.

⚠ AND THE ESCALATION IS A FULL FETCH, NOT A SECOND PROBE. Two sequential
single-day probes cost two requests and can still both miss a one-bar vendor
correction. One probe that disagrees is already proof; what it cannot tell you is
HOW MUCH else moved, and only the full series answers that.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Callable

from common.pg import _run_copy
from ingest.prices import _fetch_indicator_from_api, _parse_price_series
from ingest.refetch_history import load_symbols, refetch_full_history

log = logging.getLogger(__name__)

# Each day probes `company_id % SLICE_DIVISOR == day_of_year % SLICE_DIVISOR`.
# Stateless (no cursor to lose), deterministic, and every company comes round
# within a week.
SLICE_DIVISOR = 5
WORKERS = 8
# ⚠ THE PROBE READS CLOSE PRICE ONLY, AND THAT HALVES THE BILL FOR NOTHING LOST.
# The corporate actions that rewrite history re-scale BOTH series — a 1-for-40
# multiplies price by 40 and divides volume by 40 — so the close alone detects
# them, and the escalation refetches both metrics anyway. Probing volume too
# would double a daily cost of ~1,500 requests/week to no additional detection.
_PROBE_METRIC = ("close_price", "price")
# A stored/vendor difference beyond this is drift; below it is float noise.
TOLERANCE = 1e-6
# ⚠ THE PROBE DATE IS THE OLDEST BAR WE HOLD, and that is not arbitrary: a
# re-scale multiplies the WHOLE history, so the oldest bar is the one where a
# 10/11 attribution (Air Liquide: an ordinary-looking −9.1% step) has had the
# most compounding to separate it from a real move — and it is the bar a
# truncating vendor is most likely to disagree about too.
_PROBE_WINDOW_DAYS = 7


def slice_for_day(company_ids: list[int], day: date | None = None) -> list[int]:
    """Today's share of the universe. Pure — the scheduling is testable."""
    d = day or date.today()
    bucket = d.timetuple().tm_yday % SLICE_DIVISOR
    return [c for c in company_ids if c % SLICE_DIVISOR == bucket]


def _oldest_bars(cids: list[int], metric: str) -> dict[int, tuple[str, float]]:
    """`company_id → (oldest stored date, value)` in ONE COPY."""
    buf = _run_copy(
        "COPY (SELECT DISTINCT ON (company_id) company_id, target_date::text, numeric_value "
        "FROM metric_data WHERE metric_code = %s AND source_code = 'gurufocus' "
        "AND company_id = ANY(%s::int[]) ORDER BY company_id, target_date) "
        "TO STDOUT WITH (FORMAT csv)",
        (metric, cids),
    )
    if buf is None:
        return {}
    out: dict[int, tuple[str, float]] = {}
    for line in buf.read().decode().splitlines():
        cid, d, v = line.split(",")
        out[int(cid)] = (d, float(v))
    return out


def check_drift(
    company_ids: list[int],
    *,
    escalate: bool = True,
    on_step: Callable[[str, str], None] | None = None,
) -> dict:
    """Probe each company's oldest stored bar; full-refetch the ones that moved."""
    def _say(msg: str, level: str = "info") -> None:
        if on_step:
            on_step(msg, level)
        (log.warning if level in ("warn", "error") else log.info)("[drift] %s", msg)

    cids = sorted({int(c) for c in company_ids})
    if not cids:
        return {"probed": 0, "drifted": [], "counters": {}}
    meta = load_symbols(cids)
    metric, indicator = _PROBE_METRIC
    oldest = _oldest_bars(cids, metric)
    counters: dict[str, int] = defaultdict(int)
    drifted: set[int] = set()
    detail: list[str] = []

    def _one(cid: int) -> None:
        m = meta.get(cid) or {}
        tic, exch = m.get("ticker"), m.get("exchange")
        if not tic or not exch:
            return
        anchor = oldest.get(cid)
        if anchor:
            d0, v0 = anchor
            # A short window, not a single day: the vendor may have dropped that
            # exact bar (a phantom of ours), and `[]` then reads the same as
            # "unchanged" if you only ask for one date.
            lo = date.fromisoformat(d0)
            hi = date.fromordinal(lo.toordinal() + _PROBE_WINDOW_DAYS)
            data, _log, _st = _fetch_indicator_from_api(
                tic, exch, indicator, start_date=lo, end_date=hi)
            counters["probes"] += 1
            if data is None:
                counters["probe_failed"] += 1
                return
            vendor = dict(_parse_price_series(data))
            v1 = vendor.get(lo)
            if v1 is None:
                # Our oldest bar is not in the vendor's window at all — either a
                # phantom or a truncated history. Both are answered by the full
                # series, not by guessing here.
                counters[f"{metric}_absent"] += 1
                drifted.add(cid)
                detail.append(f"{exch}:{tic} {metric}: our oldest bar {d0} is absent upstream")
            elif abs(v1 - v0) > max(TOLERANCE, abs(v0) * 1e-9):
                counters[f"{metric}_drifted"] += 1
                drifted.add(cid)
                detail.append(
                    f"{exch}:{tic} {metric}: {d0} stored {v0:.4f} vs vendor {v1:.4f} "
                    f"(×{v1 / v0:.4f})" if v0 else f"{exch}:{tic} {metric}: {d0} moved")

    with ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix="drift") as ex:
        list(ex.map(_one, cids))

    _say(
        f"Drift probe: {len(cids)} companies, {counters['probes']} request(s) "
        f"({counters.get('probe_failed', 0)} failed) — {len(drifted)} disagree with the vendor",
        "warn" if drifted else "info",
    )
    for line in detail[:25]:
        _say(f"      {line}", "warn")

    if drifted and escalate:
        _say(f"Escalating: full history refetch for {len(drifted)} company(ies)", "warn")
        refetch_full_history(sorted(drifted), apply=True, on_step=on_step)
    return {"probed": len(cids), "drifted": sorted(drifted), "counters": dict(counters),
            "detail": detail}


def daily_drift_check(
    *, day: date | None = None, on_step: Callable[[str, str], None] | None = None,
) -> dict:
    """Today's slice of every company an enabled strategy could select."""
    from datetime import datetime, timezone  # noqa: PLC0415

    from ingest.phases.planner import build_plan, collect_universe_companies  # noqa: PLC0415

    plan = build_plan(datetime.now(timezone.utc))
    universe = collect_universe_companies(plan.strategies)
    cids = [int(c["cid"]) for c in universe if c.get("cid") is not None]
    todays = slice_for_day(cids, day)
    if on_step:
        on_step(f"Daily drift check: {len(todays)} of {len(cids)} companies "
                f"(1/{SLICE_DIVISOR} slice — every name within a week)", "info")
    return check_drift(todays, on_step=on_step)
