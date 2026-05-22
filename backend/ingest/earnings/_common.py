"""Shared constants, dataclass, storage helpers, and generic parsers.

Every earnings submodule (financials / analyst_estimates / indicators)
imports from here, and nothing in here imports from the submodules.
"""
from __future__ import annotations

import calendar
import json
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from supabase import Client


_BUCKET = "gurufocus-raw"
_CUTOFF = date(1998, 1, 1)

US_EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "CBOE"}

# Indicator keys we need for the earnings dashboard (quarterly variants)
# Each entry here = one GuruFocus API call per refresh. We only keep
# indicators that aren't already in the financials JSON. Everything else
# (ROE, ROIC, Gross/Net Margin, Interest Coverage, PEG, FCF Yield) is
# derived from the financials response in `_parse_financials` — those rows
# land in metric_data with `annuals__/quarterly__Ratios__...` codes and
# the dashboard reads them directly. Forward P/E stays here because it's
# forward-looking (price ÷ next-year EPS estimate) and isn't in the
# historical financials block.
INDICATOR_KEYS = [
    "forward_pe_ratio",
]


@dataclass
class EarningsResult:
    source: str = ""  # "financials", "analyst_estimates", "indicators"
    rows_loaded: int = 0
    metrics_found: int = 0
    cache_status: str = ""  # "cache_hit", "api_fresh", "api_error"
    logs: list[str] = field(default_factory=list)
    error: str | None = None
    is_forbidden: bool = False  # True if 403 / unsubscribed region
    api_calls: int = 0  # Number of GuruFocus API requests made


# ---------------------------------------------------------------------------
# Storage helpers (shared with prices.py patterns)
# ---------------------------------------------------------------------------

def _build_symbol(ticker: str, exchange: str) -> str:
    from ingest.prices import normalize_gurufocus_ticker  # noqa: PLC0415
    ticker = normalize_gurufocus_ticker(ticker, exchange)
    if exchange.upper() in US_EXCHANGES:
        return ticker
    return f"{exchange}:{ticker}"


def _storage_path(ticker: str, exchange: str, endpoint: str) -> str:
    from ingest.prices import normalize_gurufocus_ticker  # noqa: PLC0415
    ticker = normalize_gurufocus_ticker(ticker, exchange)
    return f"{exchange.upper()}_{ticker.upper()}/{endpoint}.json"


def _ensure_bucket(supabase: Client) -> None:
    try:
        supabase.storage.create_bucket(_BUCKET, options={"public": False})
    except Exception:
        pass


def _fetch_from_storage(supabase: Client, path: str) -> dict | list | None:
    try:
        raw = supabase.storage.from_(_BUCKET).download(path)
        return json.loads(raw)
    except Exception:
        return None


def _upload_to_storage(supabase: Client, path: str, data: Any) -> None:
    content = json.dumps(data, ensure_ascii=False).encode("utf-8")
    try:
        supabase.storage.from_(_BUCKET).upload(
            path, content, file_options={"content-type": "application/json"}
        )
    except Exception as e:
        msg = str(e).lower()
        if "already exists" not in msg and "duplicate" not in msg and "409" not in msg:
            raise
        try:
            supabase.storage.from_(_BUCKET).update(
                path, content, file_options={"content-type": "application/json"}
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Generic value / date parsers + DB upsert helper
# ---------------------------------------------------------------------------

def _coerce_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if s.upper() in {"", "N/A", "NA", "NONE", "NULL", "-"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _yyyy_mm_to_month_end(yyyy_mm: str) -> date | None:
    """'YYYY-MM' → last day of that month."""
    s = str(yyyy_mm).strip().replace("-", "")
    if len(s) < 6:
        return None
    try:
        year = int(s[:4])
        month = int(s[4:6])
        day = calendar.monthrange(year, month)[1]
        return date(year, month, day)
    except Exception:
        return None


def _upsert_metric_rows(supabase: Client, rows: list[dict]) -> int:
    if not rows:
        return 0
    batch_size = 500
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        resp = supabase.table("metric_data").upsert(
            batch, on_conflict="company_id,metric_code,source_code,target_date",
            ignore_duplicates=False,
        ).execute()
        total += len(resp.data)
    return total
