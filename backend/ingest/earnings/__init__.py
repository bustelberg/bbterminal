"""Fetch earnings-related data from GuruFocus.

Three endpoints, each cached as raw JSON in Supabase Storage and parsed
into `metric_data` rows on every refresh:
  - financials       (historical FY/quarter financials)
  - analyst_estimate (forward-looking consensus estimates)
  - indicators       (per-key time series, e.g. forward P/E)

Data cutoff: only target dates >= 1998-01-01 are persisted.

External callers (`universe.screen`, `routers.earnings`) keep importing
from `ingest.earnings`, so this `__init__.py` re-exports the public API
they touched when this module lived in one file.

Layout:
  _common.py            — EarningsResult + storage / value parsers + constants
  _api_client.py        — ApiResult + curl/urllib request + URL builder
  financials.py         — _parse_financials + fetch_financials
  analyst_estimates.py  — _parse_analyst_estimates + fetch_analyst_estimates
  indicators.py         — _parse_single_indicator + fetch_indicators
"""
from __future__ import annotations

from ._api_client import (
    ApiResult,
    _api_request,
    _api_request_curl,
    _api_request_urllib,
    _build_api_url,
    _mask_url,
)
from ._common import (
    INDICATOR_KEYS,
    US_EXCHANGES,
    EarningsResult,
    _build_symbol,
    _coerce_float,
    _ensure_bucket,
    _fetch_from_storage,
    _storage_path,
    _upload_to_storage,
    _upsert_metric_rows,
    _yyyy_mm_to_month_end,
)
from .analyst_estimates import (
    _extract_analyst_dates,
    _parse_analyst_estimates,
    fetch_analyst_estimates,
)
from .financials import _extract_financials_dates, _parse_financials, fetch_financials
from .indicators import (
    _extract_indicator_dates,
    _extract_indicator_series,
    _parse_indicator_date,
    _parse_single_indicator,
    fetch_indicators,
)

__all__ = [
    # Constants + dataclass
    "EarningsResult", "INDICATOR_KEYS", "US_EXCHANGES",
    # API client
    "ApiResult", "_api_request", "_build_api_url", "_mask_url",
    # Public fetchers
    "fetch_financials", "fetch_analyst_estimates", "fetch_indicators",
    # Parsers (used by tests / debugging)
    "_parse_financials", "_parse_analyst_estimates",
    "_parse_single_indicator", "_extract_indicator_series",
]
