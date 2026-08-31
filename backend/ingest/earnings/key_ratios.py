"""GuruFocus `keyratios` endpoint — the CONSENSUS FREE CASH FLOW the other endpoint does not carry.

⚠⚠ THIS ENDPOINT IS UNDOCUMENTED AND WAS SITTING IN THE CATALOGUE ALL ALONG. `scripts/gurufocus_
catalog.py` recorded `stock/{sym}/keyratios` as **real** with seven sections; nobody opened the
`Fundamental` one, which holds 264 keys including:

    'Estimated Free Cash Flow for Next FY1 End (M)'   'Estimated Free Cash Flow for Next FY2 End (M)'
    'Estimated Operating Cash Flow for Next FY1 End (M)'   'Estimated EBIT/EBITDA/Net Income/Sales …'

The conclusion "GuruFocus publishes a forward FCF only in its Excel add-in" was drawn from probing
`analyst_estimate` (whose annual block genuinely has no FCF key) and from the stored metric codes.
Both were true and the conclusion did not follow: the field was one endpoint over.

⚠ IT IS THE SAME CONSENSUS AS `analyst_estimate`, VERIFIED RATHER THAN ASSUMED. On AAPL the
operating-cash-flow estimate reads 148323.41 there and 148323.411 here, and EBIT/EBITDA match to
the same precision — `keyratios` simply carries more decimals. That is what makes the pairing below
sound, and what makes `OCF_est − FCF_est` a real consensus capex (Meta FY2026: 134,330.10 −
5,412.45 = 128,917.65, inside the company's own guided range).

⚠⚠ THE PAYLOAD CARRIES NO DATES. "Next FY1 End" is an ORDINAL, and `Basic` holds two keys, neither
a fiscal year end — so a row cannot be dated from this endpoint alone. The dates come from the
`annual_*_estimate` rows `analyst_estimates` has already stored, whose `target_date`s are FY1, FY2,
FY3 in order and from the same consensus. ⚠ NO STORED ESTIMATE DATES ⇒ NO ROWS, deliberately: a
guessed fiscal year end would date a forecast to the wrong year, and the frontend already falls
back to deriving the base from the operating-cash-flow estimate.
"""
from __future__ import annotations

from datetime import date
from urllib.parse import quote

from supabase import Client

from ingest.api_usage import track_api_call
from ingest.staleness import is_cache_fresh

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
)

#: The `Fundamental` keys worth storing, and the metric code each becomes.
#:
#: ⚠ ONLY WHAT IS NOT ALREADY INGESTED. `analyst_estimate` already yields revenue, EBIT, EBITDA,
#: net income, EPS, book value and operating cash flow as `annual_*_estimate`; storing them again
#: from here would be two writers for one code, disagreeing in the last decimal for ever.
#:
#: ⚠ AND NOT THE DERIVED ONES. Consensus capex is `OCF_est − FCF_est` and forward D&A is
#: `EBITDA_est − EBIT_est`; both are arithmetic over figures already stored, and storing a
#: derivation is how the copy and its inputs come to disagree.
_ESTIMATE_KEYS = {
    "Estimated Free Cash Flow for Next FY{n} End (M)": "annual_fcf_estimate",
}

#: How many forward years the endpoint publishes.
_FY_ORDINALS = (1, 2, 3)


def _parse_key_ratios(data: dict, company_id: int,
                      fy_dates: list[date]) -> list[dict]:
    """`Fundamental`'s ordinal estimates, dated from `fy_dates` (FY1, FY2, FY3 in order).

    Pure: the dates are handed in rather than looked up, so the ordinal→date pairing — the one
    thing here that can silently be wrong — is testable without a database.

    ⚠ A MISSING DATE SKIPS THAT ORDINAL rather than shifting the rest up. `fy_dates` shorter than
    the ordinals published is the ordinary case (the endpoint carries FY3, the estimate block may
    not), and quietly assigning FY3's figure to FY2's date would be a forecast filed against a
    year it was never made for.
    """
    fund = (data or {}).get("Fundamental") or {}
    if not isinstance(fund, dict):
        return []
    rows: list[dict] = []
    for n in _FY_ORDINALS:
        if n > len(fy_dates):
            continue
        for template, metric_code in _ESTIMATE_KEYS.items():
            val = _coerce_float(fund.get(template.format(n=n)))
            if val is None:
                continue
            rows.append({
                "company_id": company_id,
                "metric_code": metric_code,
                "source_code": "gurufocus",
                "target_date": fy_dates[n - 1].isoformat(),
                "numeric_value": val,
                # ⚠ LIKE EVERY OTHER ESTIMATE. `load_company_metric_rows` reads forward rows with
                # `is_prediction=True AND metric_code LIKE 'annual_%'`, so both halves are what put
                # this figure in the payload the Deep Valuation tab already loads.
                "is_prediction": True,
            })
    return rows


def _stored_estimate_dates(supabase: Client, company_id: int) -> list[date]:
    """FY1, FY2, FY3 — the `target_date`s of the consensus rows already stored, ascending.

    ⚠ FROM THE **FUTURE** ROWS ONLY, and that is the same rule the frontend's `nextFyEstimate`
    applies: the estimate block is stored as fetched, so its early periods can already be in the
    past, and pairing "Next FY1" with a year the company has since reported would file a forecast
    against a closed year.

    ⚠ ANY `annual_*_estimate` CODE WILL DO — they share one date axis (it is one consensus, one
    `date` array) — so this reads the operating-cash-flow line and does not care which company
    happens to lack which metric.
    """
    today = date.today().isoformat()
    rows = (supabase.table("metric_data").select("target_date")
            .eq("company_id", company_id)
            .eq("metric_code", "annual_operating_cash_flow_estimate")
            .eq("is_prediction", True)
            .gt("target_date", today)
            .order("target_date").limit(len(_FY_ORDINALS)).execute().data or [])
    return [date.fromisoformat(str(r["target_date"])[:10]) for r in rows]


def fetch_key_ratios(
    supabase: Client,
    company_id: int,
    ticker: str,
    exchange: str,
    *,
    force_refresh: bool = False,
    on_log: callable = None,
) -> EarningsResult:
    """Fetch `keyratios`, cache it, and load the consensus FCF into `metric_data`.

    ⚠ RUN IT AFTER `fetch_analyst_estimates`, not before: the dates come from what that stored. Out
    of order it is not an error — it stores nothing and says so — but the figure will be missing
    until the next pass.
    """
    def _log(msg: str):
        result.logs.append(msg)
        if on_log:
            on_log(msg)

    result = EarningsResult(source="key_ratios")
    _ensure_bucket(supabase)
    path = _storage_path(ticker, exchange, "keyratios")
    symbol = _build_symbol(ticker, exchange)

    # ⚠ THE DATES DECIDE WHETHER THIS CALL IS WORTH MAKING, so they are read BEFORE the API call
    # rather than after it. A company with no stored consensus can store nothing from this payload,
    # and spending a metered GuruFocus call to parse it into zero rows is the kind of waste the
    # per-region quota guards exist to prevent.
    fy_dates = _stored_estimate_dates(supabase, company_id)
    if not fy_dates:
        result.cache_status = "skipped"
        _log("No stored consensus dates for this company — nothing to date FY1/FY2/FY3 against. "
             "Run the analyst estimates first; the panel derives its base meanwhile.")
        return result

    cached = None
    need_api = True
    if not force_refresh:
        cached = _fetch_from_storage(supabase, path)
        if cached is not None:
            # ⚠ THE CACHE IS DATED BY THE ESTIMATE AXIS, NOT BY ANYTHING IN THE PAYLOAD — see the
            # module docstring: `keyratios` carries no period of its own. The consensus is refreshed
            # on the same cadence, so its dates are the honest freshness signal available.
            fresh, reason = is_cache_fresh(fy_dates)
            if fresh:
                need_api = False
                result.cache_status = "cache_hit"
                _log(f"Cache fresh ({reason})")
            else:
                _log(f"Cache stale ({reason}), refreshing from API")

    if need_api:
        url = _build_api_url(f"stock/{quote(symbol, safe=':')}/keyratios")
        _log(f"Calling {_mask_url(url)} ...")
        api = _api_request(url)
        track_api_call(supabase, exchange)
        result.api_calls += 1
        _log(api.log)
        if api.is_forbidden:
            result.cache_status = "forbidden"
            result.is_forbidden = True
            result.error = f"403 unsubscribed region for {symbol}"
            _log(f"Forbidden — exchange {exchange} not in subscription")
            return result
        if api.data is None:
            if cached is not None:
                _log("API failed, using stale cache")
            else:
                result.cache_status = "api_error"
                result.error = api.log
                return result
        else:
            cached = api.data
            result.cache_status = "api_fresh"
            _upload_to_storage(supabase, path, api.data)
            _log("Cached to storage")

    rows = _parse_key_ratios(cached, company_id, fy_dates)
    result.metrics_found = len({r["metric_code"] for r in rows})
    _log(f"Parsed {len(rows)} rows, {result.metrics_found} metrics")
    result.rows_loaded, result.rows_unchanged = _upsert_metric_rows(supabase, rows)
    _log(f"Loaded {result.rows_loaded} rows into DB"
         + (f", {result.rows_unchanged} already identical" if result.rows_unchanged else ""))
    # ⚠ NO `_stamp_fetched` HERE, AND THAT IS A DECISION RATHER THAN AN OMISSION. The stamp exists
    # to stop the SMART REFRESH re-asking a company GuruFocus publishes nothing for; this feed is
    # on-demand (one company, from the panel that wants it) and is in no such loop, so there is no
    # decision for a stamp to improve. ⚠ Calling it with an unregistered source would have been a
    # SILENT no-op — `FETCHED_AT_COLUMN.get` returns None and it returns — which is worse than not
    # calling it: the line would read as though the feed were stamped. Add a
    # `keyratios_fetched_at` column WITH a migration on the day this joins a bulk pass.
    return result
