# src\quick_insight\ingest\gurufocus\analyst_estimates\utils.py

from __future__ import annotations

from datetime import date
from typing import Any
from quick_insight.ingest.gurufocus.utils import (
    GFSpec,
    extract_min_days_from_spec,
    cache_file_exists
)

def analyst_estimate_min_days(data: dict[str, Any]) -> int | None:
    """
    Return the minimal positive number of days between successive dates
    found in an analyst-estimates payload.

    Expected input shape
    --------------------
    {
        "annual": {"date": [...]},
        "quarterly": {"date": [...]},
        ...
    }

    Date format
    -----------
    Dates are expected in YYYYMM format, e.g. "202606".

    Returns
    -------
    int
        Minimal positive gap in days between consecutive unique dates.
    None
        If fewer than 2 valid dates are found.
    """
    dates: list[date] = []

    for section in ("annual", "quarterly"):
        block = data.get(section, {})
        raw_dates = block.get("date", [])

        for value in raw_dates:
            try:
                s = str(value)
                parsed = date(int(s[:4]), int(s[4:6]), 1)
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


import json
from datetime import date
from pathlib import Path
from typing import Any


def extract_latest_analyst_date_from_spec(spec: GFSpec) -> date:
    """
    Return the most recent date found in an analyst_estimate cache file.

    Rules
    -----
    - Looks for files like:
        analyst_estimate.json
        analyst_estimate_91.json
    - If multiple variants exist (e.g. _91 and _88), raises an error
    - Reads the file and extracts max date from:
        data["annual"]["date"]
        data["quarterly"]["date"]

    Returns
    -------
    datetime.date

    Raises
    ------
    FileNotFoundError
        If no analyst_estimate file exists
    ValueError
        If multiple conflicting files exist
    """

    cache_dir: Path = spec.cache_path.parent

    # Find all matching files
    files = list(cache_dir.glob("analyst_estimate*.json"))

    if not files:
        raise FileNotFoundError(f"No analyst_estimate cache file found in {cache_dir}")

    if len(files) > 1:
        raise ValueError(
            f"Multiple analyst_estimate files found: {[f.name for f in files]}. "
            "Expected exactly one."
        )

    file_path = files[0]

    # Load JSON
    with file_path.open("r", encoding="utf-8") as f:
        data: dict[str, Any] = json.load(f)

    dates: list[date] = []

    for section in ("annual", "quarterly"):
        block = data.get(section, {})
        raw_dates = block.get("date", [])

        for value in raw_dates:
            try:
                s = str(value)
                parsed = date(int(s[:4]), int(s[4:6]), 1)
                dates.append(parsed)
            except Exception:
                continue

    if not dates:
        raise ValueError(f"No valid dates found in {file_path}")

    return max(dates)

from datetime import date, timedelta


def should_request_guru(spec: GFSpec) -> bool:
    """
    Decide whether we should request Guru API for this GFSpec.

    Logic
    -----
    - If no cache file exists → return True
    - Else:
        - latest_date = max date in cache file
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
        If min_days cannot be extracted from filename
    FileNotFoundError / ValueError
        Propagated from extract_latest_analyst_date_from_spec
    """

    # 1. No cache → always fetch
    if not cache_file_exists(spec):
        return True

    # 2. Cache exists → evaluate freshness
    latest_date = extract_latest_analyst_date_from_spec(spec)
    min_days = extract_min_days_from_spec(spec)

    if min_days is None:
        raise ValueError(
            f"No _min_days suffix found in filename for {spec.cache_path.name}"
        )

    today = date.today()
    next_expected_date = latest_date + timedelta(days=min_days)

    return today >= next_expected_date