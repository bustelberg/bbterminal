"""Shared cache staleness logic for GuruFocus data.

Determines whether a cached JSON file needs to be re-fetched by looking at
the typical interval between data points. If the latest data point + the
minimum interval hasn't passed yet, no new data is expected from GuruFocus.
"""
from __future__ import annotations

from datetime import date, timedelta
import statistics


def is_cache_fresh(dates: list[date], *, today: date | None = None) -> tuple[bool, str]:
    """Check if cached data is still fresh based on its own data frequency.

    Args:
        dates: Sorted list of dates from the cached data.
        today: Override for today's date (for testing).

    Returns:
        (is_fresh, reason_string)
    """
    today = today or date.today()

    if not dates:
        return False, "no dates in cache"

    if len(dates) < 2:
        # Single data point — can't infer frequency, assume stale after 7 days
        age = (today - dates[0]).days
        if age <= 7:
            return True, f"single point, {age}d old"
        return False, f"single point, {age}d old"

    # Compute intervals between consecutive dates
    intervals = [(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)]
    # Filter out zero-day intervals (duplicates) and extreme outliers
    intervals = [i for i in intervals if i > 0]

    if not intervals:
        return False, "no valid intervals"

    # Use median interval as the typical frequency
    # This is robust against occasional gaps (holidays, missing data)
    min_interval = statistics.median(intervals)

    latest = dates[-1]
    next_expected = latest + timedelta(days=min_interval)

    # Add a small buffer (50% of the interval, min 1 day) to account for
    # data publication delays
    buffer = max(1, int(min_interval * 0.5))
    stale_after = next_expected + timedelta(days=buffer)

    if today <= stale_after:
        return True, f"latest={latest}, interval~{min_interval:.0f}d, next expected by {stale_after}"
    else:
        return False, f"latest={latest}, interval~{min_interval:.0f}d, expected by {stale_after}, now {today}"
