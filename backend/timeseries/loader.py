"""`load_series` — one way to fetch any time series for any entity.

    from timeseries import load_series
    load_series([1, 2, 3], "gf.close", start, end)          # -> entity_id, date, close
    load_series(aids, ["yf.close", "yf.volume"])            # -> entity_id, date, close, volume

Replaces four bespoke loaders (`momentum.data.prices.load_all_{prices,volumes}`,
`asset_pipeline.alphalab._load_closes` / `_load_close_volume`) that each
hard-coded a table, a vendor and an id space.

Transport, not format, is where the speed is: the same query takes ~1,080 ms
through PostgREST's 1,000-row pagination and ~89 ms through a single `COPY`. So
COPY is the path, and PostgREST is a fallback that only `metric_data` has
(asset-pipeline callers have always degraded to "skip" instead — that behavior is
preserved via `SeriesUnavailable`).

RETURN SHAPE is canonical and vendor-agnostic:
    entity_id  int64            (company_id or analysis_id, per the series' domain)
    date       datetime64[ns]
    <alias>    float64          one column per requested series ("close", "volume")

Callers that need the legacy column names rename on the way out; that keeps the
rename in one obvious place instead of the query.
"""
from __future__ import annotations

import io
import logging
from datetime import date

import pandas as pd

from common.pg import _run_copy, copy_path_enabled

from .registry import SeriesSpec, resolve

log = logging.getLogger(__name__)

ENTITY_COL = "entity_id"
DATE_COL = "date"


class SeriesUnavailable(RuntimeError):
    """The series could not be loaded and there is no fallback for it.

    Raised when the COPY fast path is unavailable (no `SUPABASE_DB_URL`, psycopg
    missing, connection error) for a table with no PostgREST fallback. Callers
    that historically returned `None` in this situation should catch it and keep
    doing so — it is an infrastructure condition, not a data condition.
    """


def _build_copy_sql(
    specs: list[SeriesSpec],
    entity_ids: list[int],
    start: date | str | None,
    end: date | str | None,
    order: bool,
) -> tuple[str, tuple]:
    table = specs[0].table
    params: list = []

    # `= ANY(array)` rather than `IN (...)`: one parameter, no IN-chunking, and
    # it keeps the whole id list off the URL entirely (the Cloudflare 502 guard
    # that `IN_CHUNK_SIZE` exists for on the PostgREST path).
    where = [f"{table.entity_col} = ANY(%s::int[])"]
    params.append(list(entity_ids))

    for col, val in specs[0].row_filters.items():
        where.append(f"{col} = %s")
        params.append(val)

    if table.require_non_null:
        where.append(f"{table.require_non_null} IS NOT NULL")

    if start is not None:
        where.append(f"{table.date_col} >= %s")
        params.append(start.isoformat() if isinstance(start, date) else start)
    if end is not None:
        where.append(f"{table.date_col} <= %s")
        params.append(end.isoformat() if isinstance(end, date) else end)

    cols = ", ".join([table.entity_col, table.date_col, *(s.value_col for s in specs)])
    order_by = f" ORDER BY {table.entity_col}, {table.date_col}" if order else ""
    sql = (
        f"COPY (SELECT {cols} FROM {table.name} "
        f"WHERE {' AND '.join(where)}{order_by}) TO STDOUT WITH (FORMAT csv)"
    )
    return sql, tuple(params)


def _empty(specs: list[SeriesSpec]) -> pd.DataFrame:
    df = pd.DataFrame({
        ENTITY_COL: pd.Series(dtype="int64"),
        DATE_COL: pd.Series(dtype="datetime64[ns]"),
        **{s.alias: pd.Series(dtype="float64") for s in specs},
    })
    return df


def _coerce(df: pd.DataFrame, specs: list[SeriesSpec]) -> pd.DataFrame:
    df[ENTITY_COL] = df[ENTITY_COL].astype("int64")
    df[DATE_COL] = pd.to_datetime(df[DATE_COL])
    for s in specs:
        df[s.alias] = df[s.alias].astype("float64")
    return df.reset_index(drop=True)


def _via_postgrest(
    spec: SeriesSpec,
    entity_ids: list[int],
    start: date,
    end: date,
    supabase,
    on_progress,
) -> pd.DataFrame:
    """Paged fallback. Only `metric_data` has one, and only for a single EAV
    series with a bounded date range — which is exactly what the momentum
    loaders have always asked for."""
    from momentum.data._helpers import _load_metric_chunks  # noqa: PLC0415 — avoid import cycle

    rows = _load_metric_chunks(
        supabase, entity_ids, spec.row_filters["metric_code"], start, end,
        on_progress, description_prefix=f"load_series[{spec.key}]",
    )
    if not rows:
        return _empty([spec])
    df = pd.DataFrame(rows).rename(
        columns={spec.table.entity_col: ENTITY_COL,
                 spec.table.date_col: DATE_COL,
                 "numeric_value": spec.alias}
    )
    df = _coerce(df, [spec])
    # The COPY path sorts server-side; match it so downstream indexers can rely
    # on the order regardless of which path served the request.
    return df.sort_values([ENTITY_COL, DATE_COL]).reset_index(drop=True)


def load_series(
    entity_ids: list[int],
    series: str | list[str] | tuple[str, ...],
    start: date | str | None = None,
    end: date | str | None = None,
    *,
    supabase=None,
    on_progress=None,
    order: bool = True,
) -> pd.DataFrame:
    """Load one or more time series for a set of entities.

    `series` is a key (or keys) from `timeseries.registry.SERIES`. Multiple keys
    are fused into ONE query when they are columns of the same wide table;
    `resolve()` raises if they can't be (different domains, tables, or separate
    EAV rows).

    `start`/`end` are inclusive and optional. `order=False` skips the server-side
    sort for callers that immediately pivot.

    Raises `SeriesUnavailable` when COPY is unavailable and the table has no
    PostgREST fallback. Falls back silently (and returns the same shape) when it
    does — pass `supabase` to enable that.
    """
    specs = resolve(series)
    if not entity_ids:
        return _empty(specs)

    sql, params = _build_copy_sql(specs, entity_ids, start, end, order)
    buf: io.BytesIO | None = _run_copy(sql, params)

    if buf is not None:
        names = [ENTITY_COL, DATE_COL, *(s.alias for s in specs)]
        if buf.getbuffer().nbytes == 0:
            return _empty(specs)
        return _coerce(pd.read_csv(buf, names=names, header=None), specs)

    table = specs[0].table
    if not table.has_postgrest_fallback:
        raise SeriesUnavailable(
            f"{[s.key for s in specs]} needs the direct-Postgres COPY path "
            f"({'SUPABASE_DB_URL is not set' if not copy_path_enabled() else 'the COPY failed'}) "
            f"and {table.name!r} has no PostgREST fallback."
        )
    if len(specs) != 1:
        raise SeriesUnavailable(
            f"the PostgREST fallback serves one series at a time, got {[s.key for s in specs]}"
        )
    if supabase is None or start is None or end is None:
        raise SeriesUnavailable(
            f"COPY unavailable for {specs[0].key!r}; the PostgREST fallback needs "
            "`supabase` plus a bounded `start`/`end`."
        )
    return _via_postgrest(specs[0], entity_ids, start, end, supabase, on_progress)
