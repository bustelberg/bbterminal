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


def load_universe(
    supabase: Client,
    *,
    universe_label: str | None = None,
    target_month: str | None = None,
) -> pd.DataFrame:
    """Load companies for backtesting.

    If universe_label and target_month are given, loads from universe_membership
    for that specific universe/month (with sector from the membership row).
    Otherwise loads all companies joined with their exchange info.

    Returns DataFrame with columns:
        company_id, company_name, gurufocus_ticker, gurufocus_exchange, sector, country
    """
    rows: list[dict] = []
    page_size = 1000
    offset = 0

    # Load companies with exchange info
    while True:
        resp = _query_with_retry(
            lambda o=offset: (
                supabase.table("company")
                .select("company_id, company_name, gurufocus_ticker, exchange_id, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))")
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
            columns=["company_id", "company_name", "gurufocus_ticker", "gurufocus_exchange", "sector", "country"]
        )

    # Flatten the nested exchange/country join
    flat_rows = []
    for r in rows:
        exchange_info = r.get("gurufocus_exchange") or {}
        country_info = exchange_info.get("country") or {}
        flat_rows.append({
            "company_id": r["company_id"],
            "company_name": r["company_name"],
            "gurufocus_ticker": r["gurufocus_ticker"],
            "gurufocus_exchange": exchange_info.get("exchange_code"),
            "country": country_info.get("country_name"),
        })

    df = pd.DataFrame(flat_rows)

    # If a universe is specified, join with universe_membership for sector
    if universe_label and target_month:
        # Get universe_id
        u_resp = supabase.table("universe").select("universe_id").eq("label", universe_label).limit(1).execute()
        if u_resp.data:
            universe_id = u_resp.data[0]["universe_id"]
            # Load membership rows
            m_rows: list[dict] = []
            m_offset = 0
            while True:
                m_resp = _query_with_retry(
                    lambda o=m_offset: (
                        supabase.table("universe_membership")
                        .select("company_id, sector, universe_ticker")
                        .eq("universe_id", universe_id)
                        .eq("target_month", target_month)
                        .range(o, o + page_size - 1)
                        .execute()
                    ),
                    description="load_universe_membership",
                )
                if not m_resp.data:
                    break
                m_rows.extend(m_resp.data)
                if len(m_resp.data) < page_size:
                    break
                m_offset += page_size

            if m_rows:
                membership_df = pd.DataFrame(m_rows)
                df = df.merge(membership_df[["company_id", "sector"]], on="company_id", how="inner")
                df = df.dropna(subset=["sector"]).reset_index(drop=True)
                return df

    # Fallback: no sector info available, return without sector filtering
    df["sector"] = None
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
