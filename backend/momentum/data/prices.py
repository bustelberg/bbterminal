"""Bulk price + volume loaders.

Thin adapters over `timeseries.load_series` — they exist to keep the legacy
column names (`price`, `volume`) and the `(supabase, ids, start, end)` signature
that the backtest stream and self-heal paths pass. The query, the COPY fast path
and the PostgREST fallback all live in `timeseries/`.

The returned DataFrame is sorted by `(company_id, target_date)` so the
downstream indexers in `momentum.backtest.indices` can build their per-company
Series without re-sorting.

Both series are Yahoo Finance (`yf.*`), keyed by the reviewed
`company_id → analysis_id` asset bridge. GuruFocus is not a fallback:
an unresolved Yahoo instrument is reported as missing rather than silently
mixing vendor price histories.
"""
from __future__ import annotations

from datetime import date

import pandas as pd
from supabase import Client

from deps import IN_CHUNK_SIZE
from timeseries import ENTITY_COL, SeriesUnavailable, load_series
from .universe import load_company_asset_map


def _empty(value_col: str) -> pd.DataFrame:
    return pd.DataFrame({
        "company_id": pd.Series(dtype="int64"),
        "target_date": pd.Series(dtype="datetime64[ns]"),
        value_col: pd.Series(dtype="float64"),
    })


def _yahoo_via_postgrest(supabase: Client, analysis_ids: list[int], start_date: date,
                         end_date: date, value_col: str) -> pd.DataFrame:
    """Paged fallback for Yahoo bars when the direct COPY connection is absent.

    ``asset_price`` has no generic timeseries fallback because its normal
    callers can degrade gracefully. Backtest cannot: an unavailable COPY
    transport must not masquerade as an empty market-data universe.
    """
    rows: list[dict] = []
    for start in range(0, len(analysis_ids), IN_CHUNK_SIZE):
        ids = analysis_ids[start:start + IN_CHUNK_SIZE]
        offset = 0
        while True:
            batch = (supabase.table("asset_price")
                     .select(f"analysis_id,target_date,{value_col}")
                     .in_("analysis_id", ids)
                     .not_.is_("close", "null")
                     .gte("target_date", start_date.isoformat())
                     .lte("target_date", end_date.isoformat())
                     .order("analysis_id").order("target_date")
                     .range(offset, offset + 999).execute().data or [])
            rows.extend(batch)
            if len(batch) < 1000:
                break
            offset += 1000
    if not rows:
        return pd.DataFrame(columns=[ENTITY_COL, "date", value_col])
    return pd.DataFrame(rows).rename(columns={"analysis_id": ENTITY_COL, "target_date": "date"})


def _load_yahoo(supabase: Client, company_ids: list[int], start_date: date,
                end_date: date, value_col: str) -> pd.DataFrame:
    """Load a Yahoo series and translate its analysis ids back to companies."""
    mapping = load_company_asset_map(supabase, company_ids)
    if not mapping:
        return _empty(value_col)
    analysis_to_company = {
        analysis_id: company_id
        for company_id, (analysis_id, _currency) in mapping.items()
    }
    try:
        df = load_series(list(analysis_to_company), f"yf.{value_col}", start_date, end_date)
    except SeriesUnavailable:
        df = _yahoo_via_postgrest(
            supabase, list(analysis_to_company), start_date, end_date, value_col,
        )
    if df.empty:
        return _empty(value_col)
    out = df.rename(columns={"date": "target_date"}).copy()
    out["company_id"] = out[ENTITY_COL].map(analysis_to_company)
    out = out.dropna(subset=["company_id", value_col])
    out["company_id"] = out["company_id"].astype("int64")
    return out[["company_id", "target_date", value_col]].sort_values(
        ["company_id", "target_date"]
    ).reset_index(drop=True)


def load_all_prices(
    supabase: Client,
    company_ids: list[int],
    start_date: date,
    end_date: date,
    on_progress: callable = None,
) -> pd.DataFrame:
    """Bulk-load daily closing prices for all companies.

    Args:
        on_progress: Optional callback(rows_so_far, page_num) called after
            each page. Called from worker threads; must be thread-safe.
            Only fires on the PostgREST fallback path.

    Returns DataFrame with columns: company_id, target_date, price
    sorted by (company_id, target_date).
    """
    # ``on_progress`` remains part of the stream contract. The Yahoo COPY
    # path is one query, so it has no per-page progress event to report.
    return _load_yahoo(supabase, company_ids, start_date, end_date, "close").rename(
        columns={"close": "price"}
    )


def load_all_volumes(
    supabase: Client,
    company_ids: list[int],
    start_date: date,
    end_date: date,
    on_progress: callable = None,
) -> pd.DataFrame:
    """Bulk-load daily volume for all companies.

    Args:
        on_progress: Optional callback(rows_so_far, page_num) called after
            each page. Called from worker threads; must be thread-safe.
            Only fires on the PostgREST fallback path.

    Returns DataFrame with columns: company_id, target_date, volume
    sorted by (company_id, target_date).
    """
    return _load_yahoo(supabase, company_ids, start_date, end_date, "volume")
