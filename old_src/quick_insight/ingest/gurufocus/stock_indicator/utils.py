# src/quick_insight/ingest/gurufocus/stock_indicator/utils.py
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from quick_insight.ingest.gurufocus.utils import (
    GFSpec,
    cache_file_exists,
    extract_min_days_from_spec,
    resolve_cache_file_path,
)


def _parse_stock_indicator_date(value: Any) -> date | None:
    if value is None:
        return None

    s = str(value).strip()
    if not s:
        return None

    for fmt in ("%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    return None


def stock_indicator_min_days(data: list[list[Any]]) -> int | None:
    """
    Return the minimal positive number of days between successive dates
    found in a stock-indicator payload.
    """
    dates: list[date] = []

    if not isinstance(data, list):
        return None

    for row in data:
        if not isinstance(row, list) or not row:
            continue

        parsed = _parse_stock_indicator_date(row[0])
        if parsed is not None:
            dates.append(parsed)

    dates = sorted(set(dates))
    if len(dates) < 2:
        return None

    min_days: int | None = None
    for i in range(1, len(dates)):
        delta = (dates[i] - dates[i - 1]).days
        if delta > 0 and (min_days is None or delta < min_days):
            min_days = delta

    return min_days


def extract_latest_stock_indicator_date_from_spec(spec: GFSpec) -> date:
    """
    Return the most recent date found in a stock-indicator cache file.
    """
    file_path: Path = resolve_cache_file_path(spec)

    with file_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    dates: list[date] = []

    if not isinstance(data, list):
        raise ValueError(f"Stock indicator payload in {file_path} is not a list")

    for row in data:
        if not isinstance(row, list) or not row:
            continue

        parsed = _parse_stock_indicator_date(row[0])
        if parsed is not None:
            dates.append(parsed)

    if not dates:
        raise ValueError(f"No valid stock-indicator dates found in {file_path}")

    return max(dates)


def should_request_guru(spec: GFSpec) -> bool:
    """
    Decide whether we should request Guru API for this stock-indicator file.
    """
    if not cache_file_exists(spec):
        return True

    file_path = resolve_cache_file_path(spec)

    with file_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Empty payload = valid cached "no data" result
    if data == [] or data == {}:
        return False

    latest_date = extract_latest_stock_indicator_date_from_spec(spec)
    min_days = extract_min_days_from_spec(spec)

    if min_days is None:
        raise ValueError(
            f"No _min_days suffix found in filename for {file_path.name}"
        )

    today = date.today()
    next_expected_date = latest_date + timedelta(days=min_days)

    return today >= next_expected_date