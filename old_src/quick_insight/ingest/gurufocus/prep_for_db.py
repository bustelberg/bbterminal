# src/quick_insight/ingest/gurufocus/prep_for_db.py
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
from pandas.tseries.offsets import MonthEnd


# ---------------------------------------------------------------------------
# Canonical output columns — all loaders produce exactly this shape
# ---------------------------------------------------------------------------

LONG_DF_COLUMNS = [
    "primary_ticker",
    "primary_exchange",
    "metric_code",
    "target_date",
    "published_at",
    "imported_at",
    "source_code",
    "value",
    "is_prediction",
]


# ---------------------------------------------------------------------------
# Type coercion
# ---------------------------------------------------------------------------

def coerce_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if s.upper() in {"", "N/A", "NA", "NONE", "NULL", "-"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def yyyymm_to_month_end(yyyymm: str) -> pd.Timestamp:
    """'YYYYMM' or 'YYYY-MM' → month-end Timestamp."""
    s = str(yyyymm).strip().replace("-", "")
    return pd.Timestamp(year=int(s[:4]), month=int(s[4:6]), day=1) + MonthEnd(1)


def yyyy_mm_to_month_end(yyyy_mm: str) -> pd.Timestamp:
    """'YYYY-MM' → month-end Timestamp. Raises on 'TTM'."""
    s = str(yyyy_mm).strip()
    if s.upper() == "TTM":
        raise ValueError("TTM has no specific month-end date.")
    return yyyymm_to_month_end(s)


# ---------------------------------------------------------------------------
# Row factory — ensures every loader builds dicts in the same shape
# ---------------------------------------------------------------------------

def make_row(
    *,
    primary_ticker: str,
    primary_exchange: str,
    metric_code: str,
    target_date: date,
    published_at: date,
    imported_at: datetime,
    source_code: str,
    value: Any,
    is_prediction: bool,
) -> dict[str, Any]:
    return {
        "primary_ticker":  primary_ticker,
        "primary_exchange": primary_exchange,
        "metric_code":     metric_code,
        "target_date":     target_date,
        "published_at":    published_at,
        "imported_at":     imported_at,
        "source_code":     source_code,
        "value":           coerce_float(value),
        "is_prediction":   is_prediction,
    }


# ---------------------------------------------------------------------------
# Timestamp defaults
# ---------------------------------------------------------------------------

def resolve_timestamps(
    published_at: datetime | None,
    imported_at: datetime | None,
) -> tuple[date, datetime]:
    """
    Returns (published_at_date, imported_at_dt) with sensible defaults.
    Both default to utc-now (tz-stripped for DuckDB compatibility).
    """
    imported_at_dt = (imported_at or datetime.now(timezone.utc)).replace(tzinfo=None)
    published_at_date = (published_at or imported_at_dt).date()
    return published_at_date, imported_at_dt


# ---------------------------------------------------------------------------
# DataFrame finalisation
# ---------------------------------------------------------------------------

def finalise_long_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Build, sort, and reset a long-format DataFrame from a list of row dicts.
    Drops rows where metric_code or target_date is null.
    """
    if not rows:
        return pd.DataFrame(columns=LONG_DF_COLUMNS)

    return (
        pd.DataFrame(rows, columns=LONG_DF_COLUMNS)
        .dropna(subset=["metric_code", "target_date"])
        .sort_values(["metric_code", "target_date"])
        .reset_index(drop=True)
    )