# src\quick_insight\ingest\gurufocus\financials\utils.py

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from quick_insight.ingest.gurufocus.utils import (
    GFSpec,
    cache_file_exists,
    extract_min_days_from_spec,
    resolve_cache_file_path,
)


def financials_min_days(data: dict[str, Any]) -> int | None:
    """
    Return the minimal positive number of days between successive fiscal dates
    found in a GuruFocus financials payload.

    Expected input shape
    --------------------
    {
        "financials": {
            "annuals": {"Fiscal Year": [...]},
            "quarterly": {"Fiscal Year": [...]},
            ...
        }
    }

    Date format
    -----------
    Fiscal Year values are expected in YYYY-MM format, e.g. "2025-09".
    Non-date labels such as "TTM" are ignored.

    Returns
    -------
    int
        Minimal positive gap in days between consecutive unique dates.
    None
        If fewer than 2 valid dates are found.
    """
    dates: list[date] = []

    financials = data.get("financials", {})

    for section in ("annuals", "quarterly"):
        block = financials.get(section, {})
        raw_dates = block.get("Fiscal Year", [])

        for value in raw_dates:
            try:
                s = str(value).strip()
                if s.upper() == "TTM":
                    continue
                year_str, month_str = s.split("-")
                parsed = date(int(year_str), int(month_str), 1)
                dates.append(parsed)
            except Exception:
                continue

    dates = sorted(set(dates))
    if len(dates) < 2:
        return None

    min_days: int | None = None
    for i in range(1, len(dates)):
        delta = (dates[i] - dates[i - 1]).days
        if delta > 0 and (min_days is None or delta < min_days):
            min_days = delta

    return min_days


def extract_latest_financials_date_from_spec(spec: GFSpec) -> date:
    """
    Return the most recent fiscal date found in a financials cache file.

    Rules
    -----
    - Resolves the actual cache file path from the GFSpec
    - Reads the JSON and extracts the max date from:
        data["financials"]["annuals"]["Fiscal Year"]
        data["financials"]["quarterly"]["Fiscal Year"]
    - Ignores non-date labels such as "TTM"

    Returns
    -------
    datetime.date

    Raises
    ------
    FileNotFoundError
        If no cache file exists
    ValueError
        If no valid fiscal dates are found
    """
    file_path: Path = resolve_cache_file_path(spec)

    with file_path.open("r", encoding="utf-8") as f:
        data: dict[str, Any] = json.load(f)

    dates: list[date] = []
    financials = data.get("financials", {})

    for section in ("annuals", "quarterly"):
        block = financials.get(section, {})
        raw_dates = block.get("Fiscal Year", [])

        for value in raw_dates:
            try:
                s = str(value).strip()
                if s.upper() == "TTM":
                    continue
                year_str, month_str = s.split("-")
                parsed = date(int(year_str), int(month_str), 1)
                dates.append(parsed)
            except Exception:
                continue

    if not dates:
        raise ValueError(f"No valid financials fiscal dates found in {file_path}")

    return max(dates)


def should_request_guru(spec: GFSpec) -> bool:
    """
    Decide whether we should request Guru API for this financials file.

    Logic
    -----
    - If no cache file exists → return True
    - Else:
        - latest_date = max fiscal date in cache file
        - min_days    = extracted from filename suffix (_{min_days})
        - today       = current date

        Refresh if:
            today >= latest_date + min_days

    Returns
    -------
    bool
        True  -> request API
        False -> use cache

    Raises
    ------
    ValueError
        If min_days cannot be extracted from the resolved filename
    FileNotFoundError / ValueError
        Propagated from extract_latest_financials_date_from_spec
    """
    if not cache_file_exists(spec):
        return True

    latest_date = extract_latest_financials_date_from_spec(spec)
    min_days = extract_min_days_from_spec(spec)

    if min_days is None:
        resolved_path = resolve_cache_file_path(spec)
        raise ValueError(
            f"No _min_days suffix found in filename for {resolved_path.name}"
        )

    today = date.today()
    next_expected_date = latest_date + timedelta(days=min_days)

    return today >= next_expected_date