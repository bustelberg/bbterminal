"""Dividends-per-share for the /asset-pipeline grid.

THE BRIDGE
    The grid's rows are Yahoo assets (`asset_execution`, keyed by `execution_id`
    and ISIN). Dividends come from GuruFocus, which is keyed by `company_id`.
    No code joins those two universes — `timeseries/registry.py` refuses to mix
    them for exactly this reason — so the bridge here is ISIN, and it is explicit,
    lossy and measured:

        16,150 executions -> 2,065 (12.8%) resolve to a `company` row by ISIN
        of those, 1,974 sit on a GuruFocus-subscribed exchange
        the other 14,085 are ETFs, crypto, commodities, or equities we have
        never ingested into `company` — they get no dividend and no badge, and
        the UI must say so rather than render a misleading blank.

WHY NOT `fetch_financials(...)` AS-IS
    Its default parse writes EVERY leaf of the GuruFocus `/financials` blob:
    ~36,700 `metric_data` rows per company (263 fields x ~160 periods). Lazily
    fetching the 1,974 reachable companies that way would add ~72.5M rows — 2.8x
    the whole `metric_data` table. We pass `metric_codes=DIVIDEND_METRIC_CODES`
    so one fetch costs ~320 rows. The raw JSON still lands in Storage, so a later
    full parse needs no extra GuruFocus call.
"""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(tags=["asset-pipeline"])

ANNUAL_CODE = "annuals__Per Share Data__Dividends per Share"
QUARTERLY_CODE = "quarterly__Per Share Data__Dividends per Share"
DIVIDEND_METRIC_CODES = {ANNUAL_CODE, QUARTERLY_CODE}


class DividendCoverageEntry(BaseModel):
    company_id: int
    gurufocus_ticker: str | None = None
    exchange: str | None = None
    # True when the company's exchange is OUTSIDE our GuruFocus subscription, so a
    # missing dividend is a coverage gap, not a data gap. Same predicate the
    # /companies page badges with.
    gf_unsubscribed: bool = False
    # True when the annual series is already in `metric_data` — the UI can chart
    # without a fetch round-trip.
    has_data: bool = False


class DividendCoverageResponse(BaseModel):
    """`{ISIN: entry}` for every company we can reach. The grid holds the ISINs;
    the frontend joins on them client-side. Keyed by ISIN rather than
    execution_id so the payload is independent of the asset table's size."""

    by_isin: dict[str, DividendCoverageEntry]


class DividendPoint(BaseModel):
    date: str
    value: float
    # Converted at the rate on (or last before) THIS point's own date — what a EUR
    # investor actually received that year, FX leg included. `None` when the date
    # predates our `fx_rate` coverage; see `_fx_asof_strict`.
    value_eur: float | None = None
    fx_rate: float | None = None   # units of `currency` per 1 EUR


class DividendSeriesResponse(BaseModel):
    company_id: int
    currency: str | None = None
    annual: list[DividendPoint]
    quarterly: list[DividendPoint]
    fetched: bool = False   # True when this request hit GuruFocus
    # Earliest date we hold an FX rate for `currency`. Points before it carry
    # `value_eur=None` — the UI must say "no rate", not draw a zero.
    fx_from: str | None = None


class DividendPayment(BaseModel):
    """One declared cash payment, from `stock/{sym}/dividend`.

    GuruFocus retroactively split-adjusts these to today's share basis (NVIDIA's
    2013 records read 0.001875 = $0.075 / 40x, for the 4:1 and 10:1 splits), so
    they are directly comparable with the fiscal-year series above. Verified: the
    per-fiscal-year sum equals `annuals__Per Share Data__Dividends per Share`
    exactly, for every year of NVDA's history.
    """

    date: str                       # pay_date — when a holder actually receives it
    ex_date: str | None = None
    value: float
    currency: str
    kind: str | None = None         # GuruFocus `type`, e.g. "Cash Div."
    value_eur: float | None = None
    fx_rate: float | None = None
    # Trailing-twelve-month sum ending at this payment. `ttm_eur` is None whenever
    # any payment in the window lacks an FX rate — a partial sum would understate.
    ttm: float | None = None
    ttm_eur: float | None = None


class DividendPaymentsResponse(BaseModel):
    """The LIVE payment feed, which the fiscal-year series structurally cannot show.

    `annuals__…__Dividends per Share` only gains a point once a fiscal year closes,
    so a mid-year dividend hike is invisible for up to a year. NVIDIA raised its
    quarterly dividend from $0.01 to $0.25 with an ex-date of 2026-06-04 — inside
    FY2027, which does not close until 2027-01-31. The annual chart correctly shows
    FY2026 = $0.04; this endpoint shows the $0.25 the day it is declared.
    """

    company_id: int
    currency: str | None = None     # the dominant payment currency
    payments: list[DividendPayment]
    fetched: bool = False
    fx_from: str | None = None


def _load_coverage() -> dict[str, DividendCoverageEntry]:
    from deps import paginate, supabase  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    exchanges: dict[int, str] = {
        r["exchange_id"]: r["exchange_code"]
        for r in (supabase.table("gurufocus_exchange")
                  .select("exchange_id, exchange_code").execute().data or [])
    }

    companies = list(paginate(
        lambda lo, hi: (
            supabase.table("company")
            .select("company_id, isin, gurufocus_ticker, exchange_id")
            .not_.is_("isin", "null")
            .order("company_id")
            .range(lo, hi)
            .execute()
        )
    ))

    # Which companies already have the annual series? One query, ids only.
    with_data: set[int] = {
        r["company_id"]
        for r in paginate(
            lambda lo, hi: (
                supabase.table("metric_data")
                .select("company_id")
                .eq("metric_code", ANNUAL_CODE)
                .eq("source_code", "gurufocus")
                .order("company_id")
                .range(lo, hi)
                .execute()
            )
        )
    }

    out: dict[str, DividendCoverageEntry] = {}
    for c in companies:
        isin = (c.get("isin") or "").strip()
        if not isin:
            continue
        code = exchanges.get(c.get("exchange_id"))
        out[isin] = DividendCoverageEntry(
            company_id=c["company_id"],
            gurufocus_ticker=c.get("gurufocus_ticker"),
            exchange=code,
            gf_unsubscribed=not is_gf_subscribed_exchange(code),
            has_data=c["company_id"] in with_data,
        )
    return out


def _load_series(company_id: int) -> tuple[list[DividendPoint], list[DividendPoint]]:
    from deps import paginate, supabase  # noqa: PLC0415

    def _points(code: str) -> list[DividendPoint]:
        rows = paginate(
            lambda lo, hi: (
                supabase.table("metric_data")
                .select("target_date, numeric_value")
                .eq("company_id", company_id)
                .eq("metric_code", code)
                .eq("source_code", "gurufocus")
                .order("target_date")
                .range(lo, hi)
                .execute()
            )
        )
        return [
            DividendPoint(date=r["target_date"], value=float(r["numeric_value"]))
            for r in rows
            if r.get("numeric_value") is not None
        ]

    return _points(ANNUAL_CODE), _points(QUARTERLY_CODE)


def _company(company_id: int) -> dict:
    from deps import supabase  # noqa: PLC0415

    r = (supabase.table("company")
         .select("company_id, gurufocus_ticker, exchange_id")
         .eq("company_id", company_id).limit(1).execute())
    rows = r.data or []
    if not rows:
        raise HTTPException(404, f"company {company_id} not found")
    return rows[0]


def _exchange(exchange_id: int | None) -> tuple[str | None, str | None]:
    """`(exchange_code, currency_code)` — the listing's trading currency IS the
    dividend's currency. Reporting `5.92` with no unit is not a number."""
    from deps import supabase  # noqa: PLC0415

    if not exchange_id:
        return None, None
    r = (supabase.table("gurufocus_exchange")
         .select("exchange_code, currency_code").eq("exchange_id", exchange_id).limit(1).execute())
    rows = r.data or []
    if not rows:
        return None, None
    return rows[0].get("exchange_code"), rows[0].get("currency_code")


def _backfill_fx_history(currency: str, need_from: str) -> str | None:
    """Extend `fx_rate` BACKWARDS for `currency` to cover `need_from`.

    `momentum.data.fx.sync_fx_rates_to_db` only ever extends FORWARD — it reads the
    stored max and fetches from max+1. Nothing in the codebase fills history that
    predates the earliest stored rate, and today the currencies we actually use
    (USD, CZK, GBP, JPY, CHF) start at 2024-03-07 while ISK/THB/IDR reach back to
    2000. A dividend history running to 1998 therefore has almost no EUR line.

    One ECB call per currency, upserted, idempotent. Returns the new coverage start.
    """
    from fx_rates import fetch_history  # noqa: PLC0415

    from deps import supabase  # noqa: PLC0415

    have = _fx_coverage_start(currency)
    if have and have <= need_from:
        return have
    try:
        rates = fetch_history(currency, need_from)
    except Exception:  # noqa: BLE001 — no EUR line is better than a 500
        return have
    rows = [
        {"currency_code": currency, "rate_date": r["date"], "rate": r["rate"]}
        for r in (rates or [])
        if r.get("date") and r.get("rate")
    ]
    if not rows:
        return have
    for i in range(0, len(rows), 500):
        supabase.table("fx_rate").upsert(
            rows[i:i + 500], on_conflict="currency_code,rate_date",
        ).execute()
    return _fx_coverage_start(currency)


def _fx_coverage_start(currency: str) -> str | None:
    """The earliest date `fx_rate` actually holds a rate for `currency`.

    Needed because `momentum.data.fx.load_fx_rates` does
    `.reindex(daily).ffill().bfill()` — the BACK-fill silently extends the
    earliest rate to whatever `start_date` you ask for. That is fine for pricing a
    holding inside the covered window, and a fabrication here: our `fx_rate` table
    starts 2000-01-03 and the euro did not exist before 1999, yet asking for 1987
    would hand back the 2000 rate. Points before this date get `value_eur=None`,
    and the chart draws a gap.
    """
    from deps import supabase  # noqa: PLC0415

    r = (supabase.table("fx_rate").select("rate_date")
         .eq("currency_code", currency).order("rate_date").limit(1).execute())
    rows = r.data or []
    return rows[0]["rate_date"] if rows else None


def _fx_asof(series, day_iso: str) -> float | None:
    """Last rate on or before `day_iso`. Coverage is enforced by the caller."""
    import pandas as pd  # noqa: PLC0415

    if series is None or len(series) == 0 or not day_iso:
        return None
    sub = series.loc[series.index <= pd.Timestamp(day_iso)]
    if len(sub) == 0:
        return None
    rate = float(sub.iloc[-1])
    return rate if rate > 0 else None


def _fx_series(currency: str | None, points: list[DividendPoint]):
    """`(daily rate series, coverage_start_iso)` for the dividend dates.
    `(None, None)` for EUR (a no-op) or an unknown currency."""
    from datetime import date as _date  # noqa: PLC0415

    from deps import supabase  # noqa: PLC0415
    from momentum.data.fx import load_fx_rates  # noqa: PLC0415

    ccy = (currency or "").upper()
    if not ccy or ccy == "EUR" or not points:
        return None, None
    lo = _date.fromisoformat(min(p.date for p in points))
    hi = _date.fromisoformat(max(p.date for p in points))
    # Pull the missing early history once, so the EUR line isn't a two-point stub.
    # ECB itself starts 1999-01-04; anything older stays unconvertible.
    start = _backfill_fx_history(ccy, lo.isoformat())
    if not start:
        return None, None
    return load_fx_rates(supabase, [ccy], lo, hi).get(ccy), start


def _convert(points: list[DividendPoint], currency: str | None, series, fx_from: str | None) -> None:
    """Fill `value_eur` / `fx_rate` in place. EUR passes through at 1.0."""
    ccy = (currency or "").upper()
    for p in points:
        if not ccy or ccy == "EUR":
            p.value_eur, p.fx_rate = p.value, 1.0
            continue
        rate = None if (fx_from and p.date < fx_from) else _fx_asof(series, p.date)
        if rate is None:
            p.value_eur, p.fx_rate = None, None
        else:
            # `fx_rate` is units of `currency` per 1 EUR, so divide (matches
            # `_schedule_snapshots._to_eur`).
            p.value_eur, p.fx_rate = round(p.value / rate, 6), rate


def _series_response(company_id: int, *, fetched: bool = False) -> DividendSeriesResponse:
    company = _company(company_id)
    _, currency = _exchange(company.get("exchange_id"))
    annual, quarterly = _load_series(company_id)

    series, fx_from = _fx_series(currency, annual + quarterly)
    _convert(annual, currency, series, fx_from)
    _convert(quarterly, currency, series, fx_from)

    return DividendSeriesResponse(
        company_id=company_id, currency=currency,
        annual=annual, quarterly=quarterly, fetched=fetched, fx_from=fx_from,
    )


def _fetch_payment_records(ticker: str, exchange: str, *, force: bool) -> tuple[list[dict], bool]:
    """Raw `stock/{sym}/dividend` records. `(records, hit_the_api)`.

    Cached in the same `gurufocus-raw` bucket as `financials.json`, keyed
    `{EXCHANGE}_{TICKER}/dividend.json`. Freshness is judged by `is_cache_fresh`
    over the ex-dates, which infers the payment frequency — so a quarterly payer's
    cache survives ~a quarter rather than being refetched on every modal open.
    """
    from datetime import date as _date  # noqa: PLC0415
    from urllib.parse import quote  # noqa: PLC0415

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
    from ingest.staleness import is_cache_fresh  # noqa: PLC0415

    _ensure_bucket(supabase)
    path = _storage_path(ticker, exchange, "dividend")

    if not force:
        cached = _fetch_from_storage(supabase, path)
        if isinstance(cached, list) and cached:
            dates = sorted(
                _date.fromisoformat(r["ex_date"][:10])
                for r in cached if r.get("ex_date")
            )
            fresh, _reason = is_cache_fresh(dates) if dates else (False, "no dates")
            if fresh:
                return cached, False

    symbol = _build_symbol(ticker, exchange)
    api = _api_request(_build_api_url(f"stock/{quote(symbol, safe=':')}/dividend"))
    track_api_call(supabase, exchange)
    if api.is_forbidden:
        raise HTTPException(403, f"403 unsubscribed region for {symbol}")
    if not isinstance(api.data, list):
        raise HTTPException(502, f"GuruFocus returned no payment list for {symbol}")
    _upload_to_storage(supabase, path, api.data)
    return api.data, True


def _fx_bundle(currencies: set[str], lo: str, hi: str) -> dict[str, tuple]:
    """`{currency: (series, coverage_start)}` for every payment currency."""
    from datetime import date as _date  # noqa: PLC0415

    from deps import supabase  # noqa: PLC0415
    from momentum.data.fx import load_fx_rates  # noqa: PLC0415

    out: dict[str, tuple] = {}
    for ccy in currencies:
        if not ccy or ccy == "EUR":
            continue
        start = _backfill_fx_history(ccy, lo)
        if not start:
            continue
        series = load_fx_rates(
            supabase, [ccy], _date.fromisoformat(lo), _date.fromisoformat(hi),
        ).get(ccy)
        out[ccy] = (series, start)
    return out


def _payments_per_year(ex_dates: list[str]) -> int:
    """Inferred payment frequency: 4 for a quarterly payer, 2 semi-annual, 1 annual.

    Median gap over the most recent payments, so an old regime change (annual ->
    quarterly) doesn't skew it. Clamped to [1, 12].
    """
    from datetime import date as _date  # noqa: PLC0415
    from statistics import median  # noqa: PLC0415

    recent = [_date.fromisoformat(d) for d in ex_dates[-13:]]
    gaps = [(b - a).days for a, b in zip(recent, recent[1:]) if (b - a).days > 0]
    if not gaps:
        return 1
    return max(1, min(12, round(365 / median(gaps))))


def _trailing_12m(values: list[float | None], window_dates: list[str]) -> list[float | None]:
    """Trailing annual total at each payment: the sum of the last `k` payments,
    where `k` is the inferred annual frequency.

    NOT a strict 365-day window. Ex-dates drift forward a few days each year, so a
    365-day window catches FIVE quarterly payments as often as four and
    double-counts the anniversary quarter: Apple's five ex-dates from 2025-05-12 to
    2026-05-11 span 364 days, giving 1.31 where its real annual dividend is ~1.05.

    Windowed on EX-DATE (entitlement). Pay date drives the FX conversion elsewhere,
    because that is when the cash actually converted.

    `None` anywhere in the window makes the whole sum `None` — a trailing total
    missing one of its four quarters is not a smaller dividend, it is unknown.
    """
    if not window_dates:
        return []
    k = _payments_per_year(window_dates)
    out: list[float | None] = []
    for i in range(len(values)):
        if i + 1 < k:
            out.append(None)          # not a full year of history yet
            continue
        window = values[i - k + 1: i + 1]
        out.append(None if any(v is None for v in window) else round(sum(window), 6))
    return out


def _payments_response(company_id: int, *, force: bool = False) -> DividendPaymentsResponse:
    from collections import Counter  # noqa: PLC0415

    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    company = _company(company_id)
    ticker = company.get("gurufocus_ticker")
    exchange, listing_ccy = _exchange(company.get("exchange_id"))
    if not ticker:
        raise HTTPException(422, f"company {company_id} has no gurufocus_ticker")
    if not is_gf_subscribed_exchange(exchange):
        raise HTTPException(
            403,
            f"exchange {exchange or '?'} is outside the GuruFocus subscription — "
            "no dividend data is obtainable for this listing",
        )

    records, hit_api = _fetch_payment_records(ticker, exchange, force=force)

    rows: list[DividendPayment] = []
    for r in records:
        # `pay_date` is when the cash lands; that is the date whose FX rate a EUR
        # holder actually converted at. Fall back to `ex_date` when it's absent.
        day = (r.get("pay_date") or r.get("ex_date") or "")[:10]
        amount = _coerce(r.get("amount"))
        if not day or amount is None:
            continue
        rows.append(DividendPayment(
            date=day, ex_date=(r.get("ex_date") or None), value=amount,
            currency=(r.get("currency") or listing_ccy or "").upper(),
            kind=r.get("type"),
        ))
    rows.sort(key=lambda p: p.date)
    if not rows:
        return DividendPaymentsResponse(company_id=company_id, currency=listing_ccy,
                                        payments=[], fetched=hit_api)

    fx = _fx_bundle({p.currency for p in rows}, rows[0].date, rows[-1].date)
    for p in rows:
        if not p.currency or p.currency == "EUR":
            p.value_eur, p.fx_rate = p.value, 1.0
            continue
        series, fx_from = fx.get(p.currency, (None, None))
        rate = None if (fx_from and p.date < fx_from) else _fx_asof(series, p.date)
        if rate is None:
            p.value_eur, p.fx_rate = None, None
        else:
            p.value_eur, p.fx_rate = round(p.value / rate, 6), rate

    # Ex-date drives the TTM window; pay date drove the FX conversion above.
    window = [(p.ex_date or p.date) for p in rows]
    for p, t in zip(rows, _trailing_12m([p.value for p in rows], window)):
        p.ttm = t
    for p, t in zip(rows, _trailing_12m([p.value_eur for p in rows], window)):
        p.ttm_eur = t

    dominant = Counter(p.currency for p in rows).most_common(1)[0][0] or listing_ccy
    fx_from = fx.get(dominant, (None, None))[1]
    return DividendPaymentsResponse(
        company_id=company_id, currency=dominant, payments=rows,
        fetched=hit_api, fx_from=fx_from,
    )


def _coerce(v) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


@router.get("/api/asset-pipeline/dividends/{company_id}/payments",
            response_model=DividendPaymentsResponse)
async def dividend_payments(company_id: int, refresh: bool = False):
    """Every declared cash payment, with a trailing-twelve-month sum.

    The fiscal-year series only gains a point when a fiscal year closes, so a
    mid-year hike is invisible for up to a year (NVIDIA: $0.01 → $0.25 with an
    ex-date of 2026-06-04, inside FY2027). This endpoint shows it immediately.

    One GuruFocus call, and only when the Storage cache is stale for the payment
    frequency. `refresh=true` forces it."""
    return await asyncio.to_thread(_payments_response, company_id, force=refresh)


@router.get("/api/asset-pipeline/dividends/coverage", response_model=DividendCoverageResponse)
async def dividend_coverage():
    """`{ISIN: {company_id, exchange, gf_unsubscribed, has_data}}` for every
    company carrying an ISIN (~2.5k). The grid joins this on `isin` client-side —
    an ISIN absent from this map has no GuruFocus company behind it at all, which
    is the majority of the grid (ETFs, crypto, un-ingested equities)."""
    by_isin = await asyncio.to_thread(_load_coverage)
    return DividendCoverageResponse(by_isin=by_isin)


@router.get("/api/asset-pipeline/dividends/{company_id}", response_model=DividendSeriesResponse)
async def dividend_series(company_id: int):
    """The stored dividends-per-share series. Empty lists when nothing has been
    fetched yet — the caller then POSTs to `/fetch`."""
    return await asyncio.to_thread(_series_response, company_id)


@router.post("/api/asset-pipeline/dividends/{company_id}/fetch", response_model=DividendSeriesResponse)
async def dividend_fetch(company_id: int, force: bool = False):
    """Lazily pull this company's dividends from GuruFocus and persist ONLY the
    two dividend codes (`metric_codes=DIVIDEND_METRIC_CODES`) — see the module
    docstring for why the unrestricted parse is not an option here.

    One GuruFocus call, and only when the Storage cache is stale. 403 for an
    exchange outside the subscription, so the UI's UNSUBSCRIBED badge and this
    endpoint agree."""
    from deps import supabase  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415
    from ingest.earnings import fetch_financials  # noqa: PLC0415

    company = await asyncio.to_thread(_company, company_id)
    ticker = company.get("gurufocus_ticker")
    exchange, _currency = await asyncio.to_thread(_exchange, company.get("exchange_id"))
    if not ticker:
        raise HTTPException(422, f"company {company_id} has no gurufocus_ticker")
    if not is_gf_subscribed_exchange(exchange):
        raise HTTPException(
            403,
            f"exchange {exchange or '?'} is outside the GuruFocus subscription — "
            "no dividend data is obtainable for this listing",
        )

    def _run():
        return fetch_financials(
            supabase, company_id, ticker, exchange,
            force_refresh=force, metric_codes=DIVIDEND_METRIC_CODES,
        )

    result = await asyncio.to_thread(_run)
    if result.error and not result.rows_loaded:
        raise HTTPException(502, f"GuruFocus fetch failed: {result.error}")

    return await asyncio.to_thread(_series_response, company_id, fetched=result.api_calls > 0)
