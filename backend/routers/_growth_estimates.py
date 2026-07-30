"""Analysts' 3–5 year growth-rate estimates, from GuruFocus's `keyratios` endpoint.

These are the numbers a reverse DCF is judged against: the model says the price implies 24%/yr, and
the only useful next question is what anyone actually forecasts.

⚠ A DIFFERENT ENDPOINT FROM EVERYTHING ELSE THE VALUATION TABS READ. `financials` and
`analyst_estimate` are already ingested into `metric_data`; these are not, and cannot easily be —
they are SCALARS with no date to sit on, the same reason `long_term_growth_rate_mean` never reaches
the database (see `ingest/earnings/analyst_estimates.py`, which only stores list-valued fields).
So this is a live fetch with its own Storage cache, not a metric read.

⚠ THE LISTING DOES NOT MATTER HERE, AND THAT IS NOT TRUE OF ITS NEIGHBOURS. GuruFocus FX-converts
the LEVELS on this endpoint per listing — ASML's estimated FY1 EPS is 37.06 in Amsterdam and 42.83
on Nasdaq — but a growth RATE is currency-free and comes back identical on both (30.08 / 36.66,
measured 2026-07-28). No listing-choice hazard, unlike every other figure in this family.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

# GuruFocus's own key -> the name we return it under. Only the forward growth rates; the endpoint
# carries ~250 more fields that nothing here reads.
#
# ⚠ `EPS` AND `EPS without NRI` ARE DIFFERENT FORECASTS AND BOTH ARE PUBLISHED. Apple: 13.14 vs
# 13.01, ASML: 36.66 vs 39.08 — the without-NRI figure is the one that equals the
# `long_term_growth_rate_mean` the analyst-estimate feed reports (Apple 13.01, exactly), so the two
# are the same consensus measured on a normalised earnings base. Returning both keeps the choice
# with the reader rather than making it silently here.
_FIELDS = {
    "Future 3-5Y EPS Growth Rate Estimate": "eps_3_5y",
    "Future 3-5Y EPS without NRI Growth Rate Estimate": "eps_nri_3_5y",
    "Future 3-5Y OCF Per Share Growth Rate Estimate": "ocf_ps_3_5y",
    "Future 3-5Y Total Revenue Growth Rate Estimate": "revenue_3_5y",
}

# Consensus moves on an earnings cadence, so a week-old copy is fine and a re-fetch per page view
# is not — this endpoint costs a GuruFocus call.
_MAX_AGE = timedelta(days=7)
_STAMP = "_bbterminal_fetched_at"


def _is_fresh(blob: dict | None) -> bool:
    """⚠ Freshness is read off OUR stamp, not off the payload. `keyratios` has no date axis at all —
    no fiscal period, no as-of — so there is nothing in it to age against."""
    if not isinstance(blob, dict):
        return False
    raw = blob.get(_STAMP)
    if not raw:
        return False
    try:
        return datetime.now(timezone.utc) - datetime.fromisoformat(raw) < _MAX_AGE
    except ValueError:
        return False


def extract(data: dict | None) -> dict[str, float | None]:
    """The four rates, as PERCENTS exactly as GuruFocus files them (36.66 means 36.66%).

    ⚠ NOT converted to decimals here. Every consumer so far wants to print them beside other
    percentages; a silent /100 in the extractor is how a 36.66% forecast becomes 0.37% on a label
    that says "%". The one caller that needs a decimal divides at the point of use.
    """
    growth = ((data or {}).get("Growth") or {}) if isinstance(data, dict) else {}
    out: dict[str, float | None] = {}
    for gf_key, name in _FIELDS.items():
        raw = growth.get(gf_key)
        try:
            # GuruFocus returns these as STRINGS ("36.66"), and an empty string for a company with
            # no coverage — `float("")` raises rather than returning a falsy number.
            out[name] = float(raw) if raw not in (None, "") else None
        except (TypeError, ValueError):
            out[name] = None
    return out


def growth_estimates_for(company: dict, *, force: bool = False) -> dict:
    """`{symbol, fields, cached}` for one company row (needs `gurufocus_ticker` + exchange code)."""
    from deps import supabase  # noqa: PLC0415
    from ingest.api_usage import track_api_call  # noqa: PLC0415
    from ingest.earnings._api_client import _api_request, _build_api_url  # noqa: PLC0415
    from ingest.earnings._common import (  # noqa: PLC0415
        _build_symbol,
        _ensure_bucket,
        _fetch_from_storage,
        _storage_path,
        _upload_to_storage,
    )

    ticker = company.get("gurufocus_ticker")
    exchange = ((company.get("gurufocus_exchange") or {}) or {}).get("exchange_code")
    if not ticker or not exchange:
        return {"symbol": None, "fields": extract(None), "cached": False,
                "detail": "no GuruFocus listing on this company row"}

    _ensure_bucket(supabase)
    path = _storage_path(ticker, exchange, "keyratios")
    symbol = _build_symbol(ticker, exchange)

    if not force:
        cached = _fetch_from_storage(supabase, path)
        if _is_fresh(cached):
            return {"symbol": symbol, "fields": extract(cached), "cached": True}

    url = _build_api_url(f"stock/{symbol}/keyratios", {})
    api = _api_request(url)
    track_api_call(supabase, exchange)
    if api.data is None:
        # ⚠ Fall back to a STALE cache rather than to nothing: a week-old consensus beats a blank
        # column, and the alternative is that one flaky call empties the panel.
        stale = _fetch_from_storage(supabase, path)
        if isinstance(stale, dict):
            return {"symbol": symbol, "fields": extract(stale), "cached": True, "stale": True}
        return {"symbol": symbol, "fields": extract(None), "cached": False, "detail": api.log}

    blob = dict(api.data)
    blob[_STAMP] = datetime.now(timezone.utc).isoformat()
    _upload_to_storage(supabase, path, blob)
    return {"symbol": symbol, "fields": extract(blob), "cached": False}
