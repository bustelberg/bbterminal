"""GuruFocus historical analyst consensus and earnings-surprise ingest."""
from __future__ import annotations

from urllib.parse import quote

from supabase import Client

from ingest.api_usage import classify_outcome, track_api_call

from ._api_client import _api_request, _build_api_url, _mask_url
from ._common import (
    EarningsResult,
    _build_symbol,
    _coerce_float,
    _ensure_bucket,
    _fetch_from_storage,
    _storage_path,
    _upload_to_storage,
    _upsert_metric_rows,
    _yyyy_mm_to_month_end,
    refuse_unsubscribed,
)


_DISPLAY_METRICS = {
    "quarterly": frozenset({"revenue_estimate", "per_share_eps_estimate", "eps_nri_estimate"}),
    # GuruFocus supplies annual OCF surprise history too. Its `surprisemean` is the consensus
    # available before the fiscal result, which is precisely the missing denominator for the
    # historical forward P/OCF series.
    "annual": frozenset({"operating_cash_flow_estimate"}),
}
_VALUES = {
    "actual": "actual",
    "surprisemean": "consensus",
    "difference": "difference",
    "surprise_pct": "surprise_pct",
}


def _parse_estimate_history(data: dict, company_id: int) -> list[dict]:
    """Store historical consensus and surprises without overwriting filings.

    These codes are a point-in-time record supplied by ``estimate_history``;
    they are intentionally distinct from reported financial-statement metrics.
    """
    rows: list[dict] = []
    if not isinstance(data, dict):
        return rows
    for frequency, allowed_metrics in _DISPLAY_METRICS.items():
        periods_by_metric = data.get(frequency)
        if not isinstance(periods_by_metric, dict):
            continue
        for metric, periods in periods_by_metric.items():
            if metric not in allowed_metrics or not isinstance(periods, dict):
                continue
            for raw_date, observation in periods.items():
                target_date = _yyyy_mm_to_month_end(str(raw_date))
                if target_date is None or not isinstance(observation, dict):
                    continue
                for field, suffix in _VALUES.items():
                    value = _coerce_float(observation.get(field))
                    if value is None:
                        continue
                    rows.append({
                        "company_id": company_id,
                        "metric_code": f"{frequency}_estimate_history__{metric}__{suffix}",
                        "source_code": "gurufocus",
                        "target_date": target_date.isoformat(),
                        "numeric_value": value,
                        "is_prediction": False,
                    })
    return rows


def fetch_estimate_history(
    supabase: Client, company_id: int, ticker: str, exchange: str, *,
    force_refresh: bool = False, on_log: callable = None,
) -> EarningsResult:
    """Fetch the vendor's historical quarterly consensus/actual surprise feed."""
    result = EarningsResult(source="estimate_history")

    def log(message: str) -> None:
        result.logs.append(message)
        if on_log:
            on_log(message)

    refusal = refuse_unsubscribed(exchange, "analyst_estimates")
    if refusal is not None:
        return refusal
    _ensure_bucket(supabase)
    path = _storage_path(ticker, exchange, "estimate_history")
    stored = _fetch_from_storage(supabase, path)
    cached = stored
    if cached is not None and not force_refresh:
        result.cache_status = "cache_hit"
        log("Using stored historical estimate data")
    else:
        cached = None
    symbol = _build_symbol(ticker, exchange)
    if cached is None:
        url = _build_api_url(f"stock/{quote(symbol, safe=':')}/estimate_history")
        log(f"Calling {_mask_url(url)} ...")
        api = _api_request(url)
        track_api_call(supabase, exchange, job="estimate_history", outcome=classify_outcome(
            getattr(api, "status_code", None), has_data=api.data is not None))
        result.api_calls = 1
        log(api.log)
        if api.is_forbidden:
            result.cache_status = "forbidden"
            result.is_forbidden = True
            result.error = f"403 unsubscribed region for {symbol}"
            return result
        if api.data is None:
            if stored is None:
                result.cache_status = "api_error"
                result.error = api.log
                return result
            cached = stored
            result.cache_status = "cache_stale"
            log("API failed, using stored historical estimate data")
        cached = api.data
        result.cache_status = "api_fresh"
        _upload_to_storage(supabase, path, cached)
    rows = _parse_estimate_history(cached, company_id)
    result.metrics_found = len({row["metric_code"] for row in rows})
    result.rows_loaded, result.rows_unchanged = _upsert_metric_rows(supabase, rows)
    log(f"Loaded {result.rows_loaded} historical estimate rows"
        + (f", {result.rows_unchanged} unchanged" if result.rows_unchanged else ""))
    return result
