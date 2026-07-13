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
    # NULL for a listing-backed entry: an ETF has no `company` row and never will.
    company_id: int | None = None
    gurufocus_ticker: str | None = None
    exchange: str | None = None
    # Which bridge reached GuruFocus:
    #   "company"  ISIN -> company.isin -> gurufocus_ticker   (equities we ingest)
    #   "listing"  ISIN -> GuruFocus `isin/{ISIN}`            (ETFs and everything else)
    # The two carry different data: a company has fiscal-period series in
    # `metric_data` AND a payment feed; a listing has only the payment feed (no
    # `financials` blob), so the modal must not offer annual/quarterly for it.
    kind: str = "company"
    # For a listing entry, whether it resolved: ok / not_found / unsubscribed (see
    # `_gf_listing.Resolution`). Always "ok" for a company entry. A non-ok status is
    # a NEGATIVE cache — the UI shows "—" and we never re-spend an API call on it.
    status: str = "ok"
    # False when we fell back to a listing that is NOT this row's own (different
    # ticker and currency). Its amounts are right — GuruFocus reports the
    # declaration currency on every listing — but its payment history may be
    # PARTIAL (Milan holds 35 of Apple's 91 payments). The UI must say so.
    # Always True for a company entry: that IS the row's listing.
    is_home: bool = True
    # True when the company's exchange is OUTSIDE our GuruFocus subscription, so a
    # missing dividend is a coverage gap, not a data gap. Same predicate the
    # /companies page badges with.
    gf_unsubscribed: bool = False
    # True when the annual series is already in `metric_data` — the UI can chart
    # without a fetch round-trip. Only meaningful for `kind="company"`.
    has_data: bool = False
    # THREE-valued, and it has to be:
    #   None   never fetched          -> the cell offers "Fetch"
    #   True   this listing pays      -> "View"
    #   False  fetched, pays NOTHING  -> "NO PAYOUTS" badge
    # An accumulating ETF (iShares Core MSCI World) genuinely distributes nothing;
    # collapsing that into the same blank as "we haven't looked" is the exact lie the
    # old "—" told. Listing-backed rows only.
    has_payments: bool | None = None
    # Does GuruFocus hold a financials BLOB for this listing? One blob carries every
    # income-statement line, so this is per-listing, not per-column. Rides the same
    # coverage map as the payout flag — no second request.
    has_financials: bool | None = None


class DividendCoverageResponse(BaseModel):
    """`{ISIN: entry}` for every ISIN we can reach, by EITHER bridge. The grid holds
    the ISINs; the frontend joins on them client-side. Keyed by ISIN rather than
    execution_id so the payload is independent of the asset table's size.

    An ISIN absent from this map has simply never been resolved — the UI offers a
    Fetch. It does NOT mean "no dividend"."""

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

    # NULL when the feed came from a resolved GuruFocus LISTING rather than a
    # `company` row — i.e. every ETF.
    company_id: int | None = None
    currency: str | None = None     # the dominant payment currency
    payments: list[DividendPayment]
    fetched: bool = False
    fx_from: str | None = None
    # The listing this feed came from, and whether it's the asset row's own. False
    # => the amounts are right but the HISTORY may be partial; the UI says so, and
    # `_trailing_12m` returns None rather than summing across a gap.
    symbol: str | None = None
    is_home: bool = True


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
            .select("company_id, isin, gurufocus_ticker, exchange_id, has_dividend_payments, has_financials")
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
            kind="company",
            gf_unsubscribed=not is_gf_subscribed_exchange(code),
            has_data=c["company_id"] in with_data,
            has_payments=c.get("has_dividend_payments"),
            has_financials=c.get("has_financials"),
        )

    # Listing-backed entries (ETFs etc.) — every ISIN we've resolved through
    # GuruFocus's `isin/{ISIN}`. A company row WINS if one exists: it's the richer
    # bridge (fiscal-period series + payments, vs payments alone), and it's the
    # path equities have always taken.
    for r in paginate(
        lambda lo, hi: (
            supabase.table("gurufocus_listing")
            .select("isin, gurufocus_ticker, exchange_code, status, is_home, has_payments, has_financials")
            .order("isin")
            .range(lo, hi)
            .execute()
        )
    ):
        isin = (r.get("isin") or "").strip()
        if not isin or isin in out:
            continue
        out[isin] = _coverage_entry_for_listing(r)
    return out


def _us_fallback_currency(code: str | None) -> str | None:
    """ARCA / BATS / IEXG have no `gurufocus_exchange` row — they're ETF venues, and
    that table only covers the exchanges our equity universes touch. All USD."""
    from ._gf_listing import _US_CURRENCY, _US_EXCHANGES  # noqa: PLC0415

    return _US_CURRENCY if code in _US_EXCHANGES else None


def _exchange_currencies() -> dict[str, str]:
    """Our exchange code -> trading currency. Feeds the listing picker's currency
    test without a second API call."""
    from deps import supabase  # noqa: PLC0415

    rows = (supabase.table("gurufocus_exchange")
            .select("exchange_code, currency_code").execute().data or [])
    return {
        r["exchange_code"]: (r.get("currency_code") or "")
        for r in rows if r.get("exchange_code")
    }


def _asset_row(isin: str) -> dict:
    """The grid row this ISIN belongs to (empty dict if it isn't in the grid)."""
    from deps import supabase  # noqa: PLC0415

    rows = (supabase.table("asset_grid")
            .select("yahoo_symbol, analysis_symbol, currency, leonteq_product_type")
            .eq("isin", isin).limit(1).execute().data or [])
    return rows[0] if rows else {}


def _asset_hints(isin: str) -> tuple[str | None, str | None]:
    """`(symbol, currency)` of the asset row this ISIN belongs to.

    These are what let the picker tell Apple's Nasdaq line from its Zurich line —
    we already KNOW the venue and currency of the instrument the grid row is
    about, so the resolver never has to guess. `yahoo_symbol` (the tradable
    execution) is preferred over `analysis_symbol` because `currency` describes
    that same listing.
    """
    r = _asset_row(isin)
    return (r.get("yahoo_symbol") or r.get("analysis_symbol")), r.get("currency")


# Leonteq product types whose ISIN cannot possibly name an equity listing, so
# `isin/{ISIN}` can only ever come back empty. THIRTY PERCENT of the grid is bonds —
# 4,877 rows — plus 410 futures: ~5,300 GuruFocus calls spent, on a full backfill, to
# learn a thing we already knew from the product type. And the question is meaningless
# for them anyway: a bond pays COUPONS, not a dividend per share.
#
# FUNDS is deliberately NOT here. A "fund" may well be an ETF GuruFocus carries, and
# guessing wrong would silently blank a row that has real data — the whole failure mode
# this column exists to avoid. Only skip what cannot work.
_NON_EQUITY_PRODUCTS = frozenset({"BONDS", "FUTURE", "FX", "CRYPTO_CURRENCY"})


class NoDividendData(Exception):
    """GuruFocus resolved the listing but has NO dividend payload for it.

    Its dividend endpoint answers a symbol it knows nothing about with `null`, NOT with
    `[]` — and the two are genuinely different claims:
        []    the listing exists and pays nothing   -> NO PAYOUTS (an answer)
        null  GuruFocus has no data for this symbol -> NO DATA    (a gap)
    Both showed up in a 50-row sample: dead OTC lines of acquired companies
    (OTCPK:MCFUF — Micro Focus, taken over by OpenText — and OTCPK:VGFNF). Conflating
    them would tell a user an acquired company "pays no dividend", which is not what we
    know. Previously this raised a 502, which reads as "our server broke".
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        super().__init__(f"GuruFocus has no dividend data for {symbol}")


def _resolve_listing(isin: str, *, force: bool = False) -> dict:
    """ISIN -> a GuruFocus listing row, cached in `gurufocus_listing`.

    ONE GuruFocus call per ISIN, ever — including the misses, which are cached with
    their status so an unresolvable ISIN can't be re-billed on every click.
    """
    from urllib.parse import quote  # noqa: PLC0415

    from deps import supabase  # noqa: PLC0415
    from ingest.api_usage import track_api_call  # noqa: PLC0415
    from ingest.earnings._api_client import _api_request, _build_api_url  # noqa: PLC0415

    from ._gf_listing import pick_listing  # noqa: PLC0415

    if not force:
        cached = (supabase.table("gurufocus_listing").select("*")
                  .eq("isin", isin).limit(1).execute().data or [])
        if cached:
            return cached[0]

    asset = _asset_row(isin)

    # A bond / future / FX ISIN can never name an equity listing, so don't buy the
    # answer — `isin/{ISIN}` would just return []. This is 33% of the grid.
    product = (asset.get("leonteq_product_type") or "").upper()
    if product in _NON_EQUITY_PRODUCTS:
        row = {
            "isin": isin, "gurufocus_ticker": None, "exchange_code": None,
            "status": "not_applicable", "is_home": False,
            "candidates": [], "checked_at": _now_iso(),
        }
        supabase.table("gurufocus_listing").upsert(row, on_conflict="isin").execute()
        return row

    api = _api_request(_build_api_url(f"isin/{quote(isin)}"))
    candidates = api.data if isinstance(api.data, list) else []

    symbol_hint = asset.get("yahoo_symbol") or asset.get("analysis_symbol")
    currency_hint = asset.get("currency")
    res = pick_listing(
        candidates,
        symbol_hint=symbol_hint,
        currency_hint=currency_hint,
        exchange_currency=_exchange_currencies(),
    )
    # Bill the call to the region of the listing it resolved to. The isin endpoint
    # is global, so there is no region until we've picked one; an unresolved ISIN
    # falls through `_region_for_exchange`'s default (europe). Tracked AFTER the
    # pick for exactly that reason.
    track_api_call(supabase, res.listing.exchange if res.listing else "")
    row = {
        "isin": isin,
        "gurufocus_ticker": res.listing.ticker if res.listing else None,
        "exchange_code": res.listing.exchange if res.listing else None,
        "status": res.status,
        "is_home": res.is_home,
        "candidates": candidates,
        "checked_at": _now_iso(),
    }
    supabase.table("gurufocus_listing").upsert(row, on_conflict="isin").execute()
    return row


def _now_iso() -> str:
    from datetime import datetime, timezone  # noqa: PLC0415

    return datetime.now(timezone.utc).isoformat()


def _coverage_entry_for_listing(row: dict) -> DividendCoverageEntry:
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    code = row.get("exchange_code")
    return DividendCoverageEntry(
        company_id=None,
        gurufocus_ticker=row.get("gurufocus_ticker"),
        exchange=code,
        kind="listing",
        status=row.get("status") or "ok",
        is_home=bool(row.get("is_home")),
        has_payments=row.get("has_payments"),
        has_financials=row.get("has_financials"),
        # An unresolved listing has no exchange to judge, so it can't be called a
        # coverage gap; only a resolved one can be (and by construction it never is —
        # the picker drops unsubscribed exchanges before scoring).
        gf_unsubscribed=bool(code) and not is_gf_subscribed_exchange(code),
    )


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
         .select("company_id, gurufocus_ticker, exchange_id, has_dividend_payments")
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
        # `null`, not `[]` — GuruFocus has no dividend record for this symbol at all.
        # NOT the same as "pays nothing" (see NoDividendData), and NOT a server fault:
        # this used to raise a 502, which blamed us for a dead OTC ticker.
        raise NoDividendData(f"{exchange}:{ticker}")
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


# A "trailing 12 months" window may not actually span much more than 12 months. Ex-
# dates drift, so allow slack — but a window covering 450+ days is not a year, it is
# a GAP in the feed being silently summed as though it were one.
_TTM_MAX_SPAN_DAYS = 450


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

    AND `None` when the window spans a HOLE. A non-home listing's feed can be
    partial: GuruFocus's Zurich line for Apple lists 2026-05-11, 2026-02-09, then
    jumps to 2021-02-05. Summing "the last 4 payments" there produces a confident,
    plausible, five-year-wide "annual" dividend. The span check is what makes that
    unrepresentable instead of merely unlikely — an unknown total must read as
    unknown, never as a number.
    """
    from datetime import date as _date  # noqa: PLC0415

    if not window_dates:
        return []
    k = _payments_per_year(window_dates)
    out: list[float | None] = []
    for i in range(len(values)):
        if i + 1 < k:
            out.append(None)          # not a full year of history yet
            continue
        window = values[i - k + 1: i + 1]
        if any(v is None for v in window):
            out.append(None)
            continue
        try:
            span = (_date.fromisoformat(window_dates[i])
                    - _date.fromisoformat(window_dates[i - k + 1])).days
        except ValueError:
            out.append(None)
            continue
        out.append(None if span > _TTM_MAX_SPAN_DAYS else round(sum(window), 6))
    return out


def _clean_date(v: str | None) -> str | None:
    """GuruFocus's null date, normalised away.

    ETF records carry `"0000-00-00"` — NOT an empty string — for dates they don't
    have (QQQ's oldest payment, and every ETF's `record_date`). That string is
    truthy, so `pay_date or ex_date` happily selects it, and it then reaches
    `date.fromisoformat`, which raises. Equities never expose this because their
    pay dates are always real, which is why it survived until ETFs arrived.
    """
    d = (v or "")[:10]
    return d if d and not d.startswith("0000") else None


def _payments_from_listing(
    ticker: str,
    exchange: str,
    listing_ccy: str | None,
    *,
    company_id: int | None = None,
    is_home: bool = True,
    force: bool = False,
) -> DividendPaymentsResponse:
    """The payment feed for ONE GuruFocus listing. Keyed by symbol, not company —
    so it serves an ETF (no company row) and an equity identically."""
    from collections import Counter  # noqa: PLC0415

    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

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
        # holder actually converted at. Fall back to `ex_date` when it's absent —
        # and "0000-00-00" counts as absent (see `_clean_date`).
        day = _clean_date(r.get("pay_date")) or _clean_date(r.get("ex_date"))
        amount = _coerce(r.get("amount"))
        if not day or amount is None:
            continue
        rows.append(DividendPayment(
            date=day, ex_date=_clean_date(r.get("ex_date")), value=amount,
            currency=(r.get("currency") or listing_ccy or "").upper(),
            kind=r.get("type"),
        ))
    rows.sort(key=lambda p: p.date)
    if not rows:
        return DividendPaymentsResponse(
            company_id=company_id, currency=listing_ccy, payments=[], fetched=hit_api,
            symbol=f"{exchange}:{ticker}", is_home=is_home,
        )

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
        symbol=f"{exchange}:{ticker}", is_home=is_home,
    )


def _payments_response(company_id: int, *, force: bool = False) -> DividendPaymentsResponse:
    """Company-backed payments — the SAME primitive an ETF gets: (date, cash per unit)."""
    from deps import supabase  # noqa: PLC0415

    company = _company(company_id)
    ticker = company.get("gurufocus_ticker")
    exchange, listing_ccy = _exchange(company.get("exchange_id"))
    if not ticker:
        raise HTTPException(422, f"company {company_id} has no gurufocus_ticker")
    try:
        resp = _payments_from_listing(
            ticker, exchange or "", listing_ccy, company_id=company_id, force=force,
        )
    except NoDividendData as e:
        # No payload at all — don't record has_dividend_payments=False, which would
        # claim the company pays nothing. We don't know that.
        raise HTTPException(404, _UNRESOLVED_REASON["no_data"].format(symbol=e.symbol)) from e
    # Same fact, same badge as an ETF: a company that pays nothing gets NO PAYOUTS
    # rather than a "Fetch" that would keep finding nothing. Three-valued — NULL
    # still means "never looked".
    has = bool(resp.payments)
    if company.get("has_dividend_payments") is not has:
        supabase.table("company").update(
            {"has_dividend_payments": has}
        ).eq("company_id", company_id).execute()
    return resp


# A resolution status -> why the UI can't chart it. Each is a dead end we have
# already PAID for once and cached, so the message explains rather than invites a
# retry.
_UNRESOLVED_REASON = {
    "not_found": "GuruFocus does not know this ISIN — no listing to price.",
    "unsubscribed": (
        "GuruFocus lists this ISIN, but only on exchanges outside our "
        "subscription — no dividend data is obtainable."
    ),
    "not_applicable": (
        "This ISIN is a bond / future / FX instrument, not an equity listing. It pays "
        "coupons or nothing at all, never a dividend per share — so GuruFocus's "
        "equity ISIN lookup cannot resolve it, and we don't spend a call asking."
    ),
    "no_data": (
        "GuruFocus resolved this ISIN to {symbol} but holds no dividend record for "
        "that listing — typically a dead OTC line of an acquired or delisted company. "
        "That is a GAP, not a statement that it pays nothing."
    ),
}


def _payments_response_for_isin(isin: str, *, force: bool = False) -> DividendPaymentsResponse:
    """Payments for an ISIN via EITHER bridge.

    A `company` row wins when one exists — it's the path equities already take, and
    it carries the fiscal-period series too. Otherwise we go through the resolved
    GuruFocus listing, which is how an ETF gets here at all.
    """
    from deps import supabase  # noqa: PLC0415

    co = (supabase.table("company").select("company_id")
          .eq("isin", isin).limit(1).execute().data or [])
    if co:
        return _payments_response(co[0]["company_id"], force=force)

    row = _resolve_listing(isin)
    status = row.get("status") or "ok"
    if status != "ok" or not row.get("gurufocus_ticker"):
        symbol = f"{row.get('exchange_code')}:{row.get('gurufocus_ticker')}"
        reason = _UNRESOLVED_REASON.get(status, f"unresolved ISIN ({status})")
        raise HTTPException(404, reason.format(symbol=symbol) if "{symbol}" in reason else reason)

    _, listing_ccy = _exchange_by_code(row["exchange_code"])
    try:
        resp = _payments_from_listing(
            row["gurufocus_ticker"], row["exchange_code"], listing_ccy,
            is_home=bool(row.get("is_home")), force=force,
        )
    except NoDividendData as e:
        # Negative-cache it on the listing row: the ticker stays visible in the grid's
        # Exchange/Ticker columns (it IS the right listing), but the Div/share cell now
        # says NO DATA instead of re-billing a symbol GuruFocus has nothing for.
        supabase.table("gurufocus_listing").update(
            {"status": "no_data"}
        ).eq("isin", isin).execute()
        raise HTTPException(404, _UNRESOLVED_REASON["no_data"].format(symbol=e.symbol)) from e
    # Remember whether this listing pays anything AT ALL, so the grid can say
    # "NO PAYOUTS" instead of a blank — and so we don't re-ask GuruFocus about an
    # accumulating fund on every render. Three-valued: NULL means "never looked".
    has = bool(resp.payments)
    if row.get("has_payments") is not has:
        supabase.table("gurufocus_listing").update(
            {"has_payments": has}
        ).eq("isin", isin).execute()
    return resp


def _exchange_by_code(code: str | None) -> tuple[str | None, str | None]:
    """`(exchange_code, currency_code)` looked up by CODE rather than id — a listing
    resolves to a code, never to a `gurufocus_exchange.exchange_id`."""
    from deps import supabase  # noqa: PLC0415

    if not code:
        return None, None
    rows = (supabase.table("gurufocus_exchange")
            .select("exchange_code, currency_code")
            .eq("exchange_code", code).limit(1).execute().data or [])
    if not rows:
        # Not in `gurufocus_exchange` — an ETF venue (ARCA/BATS/IEXG). USD.
        return code, _us_fallback_currency(code)
    return rows[0].get("exchange_code"), rows[0].get("currency_code")


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


@router.post("/api/asset-pipeline/dividends/isin/{isin}/resolve",
             response_model=DividendCoverageEntry)
async def dividend_resolve_isin(isin: str, refresh: bool = False):
    """Resolve an ISIN to a GuruFocus listing WITHOUT a `company` row.

    This is the second bridge, and the only one an ETF can cross: GuruFocus's
    `isin/{ISIN}` -> [{symbol, exchange}] -> the one listing that IS this asset
    (see `_gf_listing.pick_listing` for why choosing is the hard part).

    ONE API call, cached in `gurufocus_listing` — including the misses, so an
    ISIN GuruFocus can't resolve is never billed twice. `refresh=true` re-asks.

    A company-backed ISIN short-circuits to its company entry: that bridge is
    richer (fiscal-period series + payments) and costs nothing.
    """
    from deps import supabase  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    def _run() -> DividendCoverageEntry:
        co = (supabase.table("company")
              .select("company_id, gurufocus_ticker, exchange_id")
              .eq("isin", isin).limit(1).execute().data or [])
        if co:
            code, _ccy = _exchange(co[0].get("exchange_id"))
            return DividendCoverageEntry(
                company_id=co[0]["company_id"],
                gurufocus_ticker=co[0].get("gurufocus_ticker"),
                exchange=code, kind="company",
                gf_unsubscribed=not is_gf_subscribed_exchange(code),
            )
        return _coverage_entry_for_listing(_resolve_listing(isin, force=refresh))

    return await asyncio.to_thread(_run)


@router.get("/api/asset-pipeline/dividends/isin/{isin}/payments",
            response_model=DividendPaymentsResponse)
async def dividend_payments_by_isin(isin: str, refresh: bool = False):
    """Payments for an ISIN through whichever bridge reaches it.

    An ETF has no fiscal-period series (no `financials` blob), so this feed IS its
    dividend history — there is no annual/quarterly cadence to fall back to."""
    return await asyncio.to_thread(_payments_response_for_isin, isin, force=refresh)


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
