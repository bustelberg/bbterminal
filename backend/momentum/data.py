"""Bulk data loaders for the momentum backtester."""
from __future__ import annotations

import logging
import time
from datetime import date

import pandas as pd
from supabase import Client

_logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRY_DELAY = 5  # seconds


def _query_with_retry(query_fn, description: str = "query"):
    """Execute a Supabase query with retry on transient errors (502, etc.)."""
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return query_fn()
        except Exception as e:
            err = str(e).lower()
            is_transient = "502" in err or "bad gateway" in err or "timeout" in err
            if is_transient and attempt < _MAX_RETRIES:
                wait = _RETRY_DELAY * attempt
                _logger.warning(f"{description}: attempt {attempt} failed ({e}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def load_universe(supabase: Client) -> pd.DataFrame:
    """Load all companies with sector info.

    Returns DataFrame with columns:
        company_id, company_name, primary_ticker, primary_exchange, sector, country
    """
    rows: list[dict] = []
    page_size = 1000
    offset = 0

    while True:
        resp = _query_with_retry(
            lambda o=offset: (
                supabase.table("company")
                .select("company_id, company_name, primary_ticker, primary_exchange, sector, country")
                .range(o, o + page_size - 1)
                .execute()
            ),
            description="load_universe",
        )
        if not resp.data:
            break
        rows.extend(resp.data)
        if len(resp.data) < page_size:
            break
        offset += page_size

    if not rows:
        return pd.DataFrame(
            columns=["company_id", "company_name", "primary_ticker", "primary_exchange", "sector", "country"]
        )

    df = pd.DataFrame(rows)
    # Drop companies without a sector (can't do sector-level selection)
    df = df.dropna(subset=["sector"]).reset_index(drop=True)
    return df


def load_all_prices(
    supabase: Client,
    company_ids: list[int],
    start_date: date,
    end_date: date,
    on_progress: callable = None,
) -> pd.DataFrame:
    """Bulk-load daily closing prices for all companies.

    Batches company_ids into chunks of 50 to avoid Cloudflare 502 errors
    from overly long query strings.

    Args:
        on_progress: Optional callback(rows_so_far, page_num) called after each page.

    Returns DataFrame with columns: company_id, target_date, price
    sorted by (company_id, target_date).
    """
    if not company_ids:
        return pd.DataFrame(columns=["company_id", "target_date", "price"])

    rows: list[dict] = []
    page_size = 1000
    page_num = 0
    chunk_size = 50  # keep .in_() URL short enough for Cloudflare

    for chunk_start in range(0, len(company_ids), chunk_size):
        chunk = company_ids[chunk_start : chunk_start + chunk_size]
        offset = 0

        while True:
            resp = _query_with_retry(
                lambda o=offset, c=chunk: (
                    supabase.table("metric_data")
                    .select("company_id, target_date, numeric_value")
                    .eq("metric_code", "close_price")
                    .eq("source_code", "gurufocus")
                    .in_("company_id", c)
                    .gte("target_date", start_date.isoformat())
                    .lte("target_date", end_date.isoformat())
                    .order("company_id")
                    .order("target_date")
                    .range(o, o + page_size - 1)
                    .execute()
                ),
                description=f"load_all_prices chunk {chunk_start // chunk_size + 1}",
            )
            if not resp.data:
                break
            rows.extend(resp.data)
            page_num += 1
            if on_progress:
                on_progress(len(rows), page_num)
            if len(resp.data) < page_size:
                break
            offset += page_size

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
) -> pd.DataFrame:
    """Bulk-load daily volume for all companies.

    Same batching approach as load_all_prices.

    Returns DataFrame with columns: company_id, target_date, volume
    sorted by (company_id, target_date).
    """
    if not company_ids:
        return pd.DataFrame(columns=["company_id", "target_date", "volume"])

    rows: list[dict] = []
    page_size = 1000
    chunk_size = 50

    for chunk_start in range(0, len(company_ids), chunk_size):
        chunk = company_ids[chunk_start : chunk_start + chunk_size]
        offset = 0

        while True:
            resp = _query_with_retry(
                lambda o=offset, c=chunk: (
                    supabase.table("metric_data")
                    .select("company_id, target_date, numeric_value")
                    .eq("metric_code", "volume")
                    .eq("source_code", "gurufocus")
                    .in_("company_id", c)
                    .gte("target_date", start_date.isoformat())
                    .lte("target_date", end_date.isoformat())
                    .order("company_id")
                    .order("target_date")
                    .range(o, o + page_size - 1)
                    .execute()
                ),
                description=f"load_all_volumes chunk {chunk_start // chunk_size + 1}",
            )
            if not resp.data:
                break
            rows.extend(resp.data)
            if len(resp.data) < page_size:
                break
            offset += page_size

    if not rows:
        return pd.DataFrame(columns=["company_id", "target_date", "volume"])

    df = pd.DataFrame(rows)
    df.rename(columns={"numeric_value": "volume"}, inplace=True)
    df["target_date"] = pd.to_datetime(df["target_date"])
    df["volume"] = df["volume"].astype(float)
    df = df.sort_values(["company_id", "target_date"]).reset_index(drop=True)
    return df
