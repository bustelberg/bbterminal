"""Yahoo refreshes for a company-domain caller.

Company screens identify a security by ``company_id`` while the market-data
pipeline owns ISIN -> ``asset_execution`` -> ``asset_price``.  This small
adapter is the only sanctioned way for legacy company endpoints to request a
price refresh; it never falls back to a GuruFocus ticker.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from deps import supabase


@dataclass
class YahooPriceResult:
    rows_loaded: int = 0
    total_prices: int = 0
    api_calls: int = 0
    source: str = "yfinance"
    error: str | None = None
    logs: list[str] = field(default_factory=list)
    is_forbidden: bool = False
    is_delisted: bool = False
    resolved_exchange: str | None = None
    request_url: str | None = None
    http_status: int | None = None
    response_excerpt: str | None = None


def refresh_company_prices(company_id: int, *, force_refresh: bool = False,
                           on_log=None) -> YahooPriceResult:
    """Resolve (only if necessary) then store/extend one company's Yahoo bars."""
    from asset_pipeline import price_refresh, store  # noqa: PLC0415

    row = (supabase.table("company").select("isin,company_name")
           .eq("company_id", company_id).limit(1).execute().data or [])
    if not row or not row[0].get("isin"):
        return YahooPriceResult(error="Company has no ISIN for Yahoo mapping.")
    isin = str(row[0]["isin"]).upper()
    ex = (supabase.table("asset_execution")
          .select("analysis_id,yahoo_symbol,exchange")
          .eq("isin", isin).eq("status", "ok").limit(1).execute().data or [])
    try:
        if not ex or not ex[0].get("analysis_id") or not ex[0].get("yahoo_symbol"):
            refreshed = store.refresh_row(isin)
            if refreshed.get("status") != "ok":
                return YahooPriceResult(error=refreshed.get("message") or "Yahoo mapping failed.")
            aid = int(refreshed["yfinance"]["analysis_id"])
            symbol = refreshed["yfinance"]["symbol"]
            rows = int(refreshed["yfinance"].get("rows") or 0)
        else:
            aid, symbol = int(ex[0]["analysis_id"]), str(ex[0]["yahoo_symbol"])
            latest = price_refresh.latest_close_by_analysis([aid]).get(aid)
            if force_refresh or not latest:
                rows = int(store.store_series(aid, symbol, None) or 0)
            else:
                extended = store.extend_series(aid, symbol, latest)
                rows = int(extended or 0)
                if extended is None:
                    rows = int(store.store_series(aid, symbol, None) or 0)
        total = (supabase.table("asset_price").select("target_date", count="exact")
                 .eq("analysis_id", aid).limit(1).execute().count or 0)
        msg = f"Yahoo {symbol}: {rows} bars written ({total} stored)."
        if on_log:
            on_log(msg)
        return YahooPriceResult(rows_loaded=rows, total_prices=total, logs=[msg],
                                resolved_exchange=(ex[0].get("exchange") if ex else None))
    except Exception as exc:  # endpoint callers need a result, not a provider-specific exception
        msg = f"Yahoo refresh failed: {type(exc).__name__}: {exc}"
        if on_log:
            on_log(msg)
        return YahooPriceResult(error=msg, logs=[msg])
