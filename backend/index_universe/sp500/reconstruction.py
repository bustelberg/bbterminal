"""Reconstruct monthly S&P 500 holdings from current set + change log.

Walks backwards from current holdings through the Wikipedia changes
list to build `{YYYY-MM: set[ticker]}`. We carry forward through months
that have no change events so every month in the range has a complete
membership set, then filter to >= start_month."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime

from .scraping import _month_key


_START_MONTH = "2000-01"  # Only store from Jan 2000 onwards


def reconstruct_monthly_holdings(
    current_tickers: set[str],
    changes: list[dict],
    start_month: str = _START_MONTH,
) -> tuple[dict[str, set[str]], list[dict]]:
    """Walk backwards from current holdings through changes to build
    monthly composition: {YYYY-MM: set[ticker]}.

    Only keeps months >= start_month.

    Also returns the changes list filtered to start_month onwards, with
    each change tagged with its YYYY-MM month key.

    Returns (monthly_holdings, filtered_changes).
    """
    # Group changes by month
    changes_by_month: defaultdict[str, list[dict]] = defaultdict(list)
    for c in changes:
        changes_by_month[_month_key(c["date"])].append(c)

    change_months = sorted(changes_by_month.keys(), reverse=True)
    current_month = _month_key(date.today())

    holdings = current_tickers.copy()
    all_holdings: dict[str, set[str]] = {current_month: holdings.copy()}

    for month in change_months:
        for c in changes_by_month[month]:
            if c["added"] and c["added"] in holdings:
                holdings.discard(c["added"])
            if c["removed"]:
                holdings.add(c["removed"])
        all_holdings[month] = holdings.copy()

    # Fill gaps: for every month between earliest and today, carry forward
    all_months = sorted(all_holdings.keys())
    if all_months:
        start = datetime.strptime(all_months[0], "%Y-%m").date()
        end = date.today()
        cursor = start
        prev = all_holdings[all_months[0]]
        while cursor <= end:
            mk = _month_key(cursor)
            if mk in all_holdings:
                prev = all_holdings[mk]
            else:
                all_holdings[mk] = prev.copy()
            if cursor.month == 12:
                cursor = cursor.replace(year=cursor.year + 1, month=1)
            else:
                cursor = cursor.replace(month=cursor.month + 1)

    # Filter to start_month onwards
    result = {m: t for m, t in all_holdings.items() if m >= start_month}

    # Build filtered changes with month key
    filtered_changes = []
    for c in changes:
        mk = _month_key(c["date"])
        if mk >= start_month:
            filtered_changes.append({
                "date": c["date"].isoformat(),
                "month": mk,
                "added": c["added"],
                "removed": c["removed"],
            })
    # Sort oldest first for changelog display
    filtered_changes.sort(key=lambda c: c["date"])

    return result, filtered_changes
