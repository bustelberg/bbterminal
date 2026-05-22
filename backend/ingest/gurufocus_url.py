"""Single canonical builder for GuruFocus stock-summary URLs.

GuruFocus URL convention: US-listed names go bare (`/stock/AAPL/summary`),
everything else gets an exchange prefix (`/stock/XSWX:NESN/summary`).
Mismatched URLs (a US name with `NYSE:` prefix or a Swiss name without
prefix) produce 404s; this helper centralizes the rule so every caller
in the codebase resolves to the same URL for the same security.

The same logic lives in `frontend/lib/gurufocusUrl.ts` — keep them in
sync if you change either.
"""
from __future__ import annotations

# GuruFocus exchange codes that produce a bare URL (no prefix). Matches
# the GuruFocus-side convention; `CBOE BZX` is the iShares fund-file
# variant of `CBOE` (the canonical DB code). `US` shows up as a
# catch-all on a few legacy rows.
US_EXCHANGE_CODES = frozenset({"NYSE", "NASDAQ", "AMEX", "CBOE", "CBOE BZX", "US"})


def gurufocus_url(ticker: str | None, exchange: str | None) -> str | None:
    """Return the canonical GuruFocus summary URL or None.

    None is returned when the ticker is missing or whitespace — we'd
    otherwise synthesize broken `:TICKER`-style or `EXCH:`-style URLs.
    An empty exchange is treated as US (the bare-ticker form), matching
    the frontend's behavior so click-through links stay consistent."""
    if not ticker:
        return None
    t = ticker.strip()
    if not t:
        return None
    e = (exchange or "").strip().upper()
    if not e or e in US_EXCHANGE_CODES:
        return f"https://www.gurufocus.com/stock/{t}/summary"
    return f"https://www.gurufocus.com/stock/{e}:{t}/summary"
