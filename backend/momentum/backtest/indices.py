"""Per-company price/volume index builders + as-of price lookups.

These pre-index the long-format DataFrame into `{company_id: Series}` so the
hot per-period and per-day loops can do O(log n) lookups instead of repeated
boolean-mask filters."""
from __future__ import annotations

import pandas as pd


def _build_price_index(prices_df: pd.DataFrame) -> dict[int, pd.Series]:
    """Pre-index prices into a dict of {company_id: Series(price, DatetimeIndex)}.

    One-time cost that eliminates repeated DataFrame filtering.
    """
    result: dict[int, pd.Series] = {}
    for cid, group in prices_df.groupby("company_id"):
        s = pd.Series(
            group["price"].values,
            index=pd.DatetimeIndex(group["target_date"]),
            dtype="float64",
        ).sort_index()
        result[int(cid)] = s
    return result


def _build_volume_index(volumes_df: pd.DataFrame) -> dict[int, pd.Series]:
    """Build a dict of {company_id: Series} from the volumes DataFrame."""
    if volumes_df.empty:
        return {}
    result: dict[int, pd.Series] = {}
    for cid, group in volumes_df.groupby("company_id"):
        s = pd.Series(
            group["volume"].values,
            index=pd.DatetimeIndex(group["target_date"]),
            dtype="float64",
        ).sort_index()
        result[int(cid)] = s
    return result


def _price_on_or_after(series: pd.Series, target: pd.Timestamp) -> float | None:
    """Get the first available price on or after target date from a pre-indexed Series."""
    subset = series[series.index >= target]
    if subset.empty:
        return None
    return float(subset.iloc[0])


def _date_on_or_after(series: pd.Series, target: pd.Timestamp) -> str | None:
    """Get the actual trading date of the first price on or after target."""
    idx = series.index[series.index >= target]
    if len(idx) == 0:
        return None
    return idx[0].strftime("%Y-%m-%d")


def _price_on_or_before(series: pd.Series, target: pd.Timestamp) -> tuple[float, pd.Timestamp] | None:
    """Get (price, trading-date) of the latest available price on or before target."""
    subset = series[series.index <= target]
    if subset.empty:
        return None
    return float(subset.iloc[-1]), subset.index[-1]
