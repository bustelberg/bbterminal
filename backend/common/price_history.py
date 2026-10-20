"""Reviewed Yahoo listing boundaries, not statistical price-outlier filters.

Yahoo sometimes splices a predecessor security or nominal pre-listing bars into
the current listing. Those bars must never become an entry price for this equity.
Dates are inclusive and keyed by the Yahoo analysis symbol, never database ids.
"""
from __future__ import annotations

from datetime import date

# Change this when the rules change so a rerun cannot replay a contaminated result.
PRICE_HISTORY_VERSION = "listing-boundaries-2026-10-20"

LISTING_STARTS = {
    # H-share IPO; Yahoo's earlier HK$0.01 bars are not this listed security.
    # https://www.citics.com/newsite/en/investorRelations/faq/
    "6030.HK": date(2011, 10, 6),
    # Post-bankruptcy relisting. Old shares and the intervening JPY1 placeholder
    # cannot be chain-linked into the new equity's first close of JPY1915.
    # https://www.jal.com/en-jp/about/jal-history-75th/index10s.html
    "9201.T": date(2012, 9, 19),
}


def listing_start(symbol: str) -> date | None:
    return LISTING_STARTS.get(symbol.strip().upper())


def filter_listing_rows(rows: list[dict], symbol: str, *, date_key: str = "target_date") -> list[dict]:
    """Keep valid listing history, including ordinary interior no-trade bars."""
    start = listing_start(symbol)
    if start is None:
        return rows
    return [row for row in rows if str(row[date_key])[:10] >= start.isoformat()]
