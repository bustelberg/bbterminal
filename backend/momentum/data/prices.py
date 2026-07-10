"""Bulk price + volume loaders.

Thin adapters over `timeseries.load_series` — they exist to keep the legacy
column names (`price`, `volume`) and the `(supabase, ids, start, end)` signature
that the backtest stream and self-heal paths pass. The query, the COPY fast path
and the PostgREST fallback all live in `timeseries/`.

The returned DataFrame is sorted by `(company_id, target_date)` so the
downstream indexers in `momentum.backtest.indices` can build their per-company
Series without re-sorting.

Both series are GuruFocus (`gf.*`). That is deliberate and load-bearing: the
scheduled /schedule strategy is priced off GuruFocus, and `yf.close` is a
different number. Swapping the vendor here would change live holdings.
"""
from __future__ import annotations

from datetime import date

import pandas as pd
from supabase import Client

from timeseries import ENTITY_COL, load_series


def _adapt(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """Canonical `[entity_id, date, <alias>]` -> legacy `[company_id, target_date, <value_col>]`."""
    return df.rename(columns={ENTITY_COL: "company_id", "date": "target_date"})[
        ["company_id", "target_date", value_col]
    ]


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
    df = load_series(
        company_ids, "gf.close", start_date, end_date,
        supabase=supabase, on_progress=on_progress,
    )
    return _adapt(df.rename(columns={"close": "price"}), "price")


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
    df = load_series(
        company_ids, "gf.volume", start_date, end_date,
        supabase=supabase, on_progress=on_progress,
    )
    return _adapt(df, "volume")
