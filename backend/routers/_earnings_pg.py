"""Direct-Postgres COPY loader for the fundamentals read behind the Long Equity benchmarks.

WHY
    `_rows_by_company` is what a benchmark selection is bounded by. Measured on ACWI (1,514
    constituents, annual): each metric is ~16,300 rows, and PostgREST serves them in pages capped
    at 1,000 — so ONE metric is ~20 round trips, and the tab issues 27 metric reads across its
    cards. That is ~540 round trips for one dropdown change.

⚠ THE COST IS ROUND TRIPS, NOT ROWS, WHICH IS WHY THIS HELPS FAR MORE IN PRODUCTION THAN LOCALLY.
    A page costs one network latency: ~2ms to a local Docker Postgres, 50-200ms to Supabase cloud.
    The same read that takes 0.47s here is several seconds there, and no amount of chunking or
    column-narrowing changes that — the pagination is forced by the SERVER's `db-max-rows`, which
    is 1,000 on cloud. A COPY streams the whole result over one connection with no cap, so it
    removes the round trips rather than making them cheaper. This repo measures ~12x on the same
    shape elsewhere (1,080ms -> 89ms).

⚠ IT RETURNS THE IDENTICAL SHAPE, OR None. `dict[company_id, list[row]]` with exactly the four
    keys the PostgREST path selects, so `_metrics_by_company` cannot tell which transport ran.
    `None` means "fall back" — unconfigured (`SUPABASE_DB_URL` absent), psycopg missing, or any
    error. That is the same contract every other COPY loader here uses, and it is what lets this
    be a pure speed-up with no behavioural surface.

⚠ `target_date` STAYS A STRING, and `numeric_value` becomes float-or-None. PostgREST returns a
    `date` as "2025-12-31" and a NULL as None; downstream (`_latest_per_year`, `_ttm_by_period`,
    `_period_label`) treats the date as an opaque, lexically-sortable label. Parsing it to a
    `datetime` here would be a silent behaviour change in the one place nobody would look.
"""
from __future__ import annotations

import csv
import io
import logging
from collections import defaultdict

from common.pg import _db_url, _run_copy

log = logging.getLogger(__name__)


def rows_by_company_via_copy(
    company_ids: list[int], codes: list[str], since: str,
) -> dict[int, list[dict]] | None:
    """`{company_id: [{company_id, metric_code, target_date, numeric_value}, ...]}` in one COPY.

    `since` is the blend window floor (`_BLEND_START`), pushed into the query exactly as the paged
    path pushes it — the rows excluded are rows nothing renders.
    """
    if not _db_url() or not company_ids or not codes:
        return None
    # ⚠ ORDERED IDENTICALLY TO THE PAGED PATH. Not for correctness of the paging (there is none
    #   here) but so the per-company lists arrive in the same order; `_latest_per_year` keeps the
    #   LAST row it sees for a period, so a different order could pick a different value.
    sql = (
        "COPY (SELECT company_id, metric_code, target_date, numeric_value "
        "FROM metric_data "
        "WHERE company_id = ANY(%s) AND metric_code = ANY(%s) AND target_date >= %s "
        "ORDER BY company_id, target_date, metric_code) TO STDOUT WITH (FORMAT csv)"
    )
    try:
        buf = _run_copy(sql, (company_ids, codes, since))
    except Exception as e:  # noqa: BLE001 — a fast path must never be the reason a page 500s
        log.warning("[earnings.pg] COPY failed, falling back to PostgREST: %s: %s",
                    type(e).__name__, e)
        return None
    if buf is None:
        return None

    raw: dict[int, list[dict]] = defaultdict(list)
    if buf.getbuffer().nbytes == 0:
        return raw
    reader = csv.reader(io.TextIOWrapper(buf, encoding="utf-8", newline=""))
    for row in reader:
        if len(row) != 4:
            continue
        cid_s, code, target_date, value = row
        cid = int(cid_s)
        raw[cid].append({
            "company_id": cid,
            "metric_code": code,
            "target_date": target_date,
            # ⚠ COPY WRITES NULL AS AN EMPTY FIELD, and PostgREST returns None. Mapping "" to 0.0
            #   would invent a reported zero — which for a fundamentals line is a real value with a
            #   real meaning, not a placeholder.
            "numeric_value": float(value) if value != "" else None,
        })
    return raw


def company_ids_with_metric_via_copy(company_ids: list[int], metric_code: str) -> set[int] | None:
    """Which of these companies carry `metric_code` at all — ONE `DISTINCT` over one connection.

    The COPY twin of `_fundamental_backfill._has`, and the reason that function stopped being the
    dominant cost of the fundamentals grid.

    ⚠⚠ THE PAGED PATH READS EVERY ROW TO ANSWER A BOOLEAN, AND ITS COST SCALES WITH THE SERIES, NOT
        WITH THE QUESTION. It chunks 20 ids and pages 1,000 rows, so proving "does this company
        have `indicator_q_forward_pe_ratio`" costs ~526 rows PER COMPANY — on ACWI's ~1,900
        constituents that is hundreds of thousands of rows and, at the 20-id chunking, **at least
        95 round trips per sentinel** before a single page of overflow. Its own docstring measures
        SP500 at ~112k rows / ~110 requests; ACWI is roughly four times that, three times over.

        `SELECT DISTINCT company_id` answers the same question in the database, where the index
        already is, and returns at most one row per company. Same shape, same semantics, one round
        trip.

    ⚠ `None` MEANS FALL BACK, exactly as `rows_by_company_via_copy` does — unconfigured, psycopg
        missing, or any error. An EMPTY SET is a real answer ("none of them have it") and must not
        be confused with it: returning `set()` on failure would mark every constituent as needing a
        feed it already has, which is the expensive-and-silent direction (`needs()` reads this to
        decide what to spend GuruFocus quota on).

    ⚠ NO DATE FLOOR. `_has` applies none either — presence is presence, and adding one here would
        make a company whose only rows predate the floor look unfetched.
    """
    if not _db_url() or not company_ids or not metric_code:
        return None
    sql = ("COPY (SELECT DISTINCT company_id FROM metric_data "
           "WHERE company_id = ANY(%s) AND metric_code = %s) TO STDOUT WITH (FORMAT csv)")
    try:
        buf = _run_copy(sql, (company_ids, metric_code))
    except Exception as e:  # noqa: BLE001 — a fast path must never be the reason a page 500s
        log.warning("[earnings.pg] DISTINCT COPY failed, falling back to PostgREST: %s: %s",
                    type(e).__name__, e)
        return None
    if buf is None:
        return None
    out: set[int] = set()
    if buf.getbuffer().nbytes == 0:
        return out
    for row in csv.reader(io.TextIOWrapper(buf, encoding="utf-8", newline="")):
        if row and row[0]:
            out.add(int(row[0]))
    return out
