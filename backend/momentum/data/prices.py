"""Bulk price + volume loaders.

Both call `_load_metric_chunks` with a different `metric_code`. The
returned DataFrame is sorted by `(company_id, target_date)` so the
downstream indexers in `momentum.backtest.indices` can build their
per-company Series without re-sorting."""
from __future__ import annotations

from datetime import date

import pandas as pd
from supabase import Client

from ._helpers import _load_metric_chunks


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

    Returns DataFrame with columns: company_id, target_date, price
    sorted by (company_id, target_date).
    """
    if not company_ids:
        return pd.DataFrame(columns=["company_id", "target_date", "price"])

    rows = _load_metric_chunks(
        supabase, company_ids, "close_price", start_date, end_date,
        on_progress, description_prefix="load_all_prices",
    )

    if not rows:
        return pd.DataFrame(columns=["company_id", "target_date", "price"])

    df = pd.DataFrame(rows)
    df.rename(columns={"numeric_value": "price"}, inplace=True)
    df["target_date"] = pd.to_datetime(df["target_date"])
    df["price"] = df["price"].astype(float)
    df = df.sort_values(["company_id", "target_date"]).reset_index(drop=True)
    return df


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

    Returns DataFrame with columns: company_id, target_date, volume
    sorted by (company_id, target_date).
    """
    if not company_ids:
        return pd.DataFrame(columns=["company_id", "target_date", "volume"])

    rows = _load_metric_chunks(
        supabase, company_ids, "volume", start_date, end_date,
        on_progress, description_prefix="load_all_volumes",
    )

    if not rows:
        return pd.DataFrame(columns=["company_id", "target_date", "volume"])

    df = pd.DataFrame(rows)
    df.rename(columns={"numeric_value": "volume"}, inplace=True)
    df["target_date"] = pd.to_datetime(df["target_date"])
    df["volume"] = df["volume"].astype(float)
    df = df.sort_values(["company_id", "target_date"]).reset_index(drop=True)
    return df
