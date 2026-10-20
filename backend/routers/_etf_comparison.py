"""Independent ETF/benchmark windows, using Yahoo's dividend-adjusted daily closes."""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone

from pydantic import BaseModel


class EtfComparisonPoint(BaseModel):
    date: str
    etf_pct: float
    benchmark_pct: float


class EtfComparisonResult(BaseModel):
    ticker: str
    benchmark: str
    requested_start: str
    start: str
    end: str
    currency: str | None
    benchmark_currency: str | None
    total_return_pct: float
    benchmark_return_pct: float
    excess_pp: float
    points: list[EtfComparisonPoint]


def adjusted_closes(payload: dict) -> dict[str, float]:
    """Never silently substitute raw closes: that would drop an ETF's distributions."""
    from asset_pipeline.yahoo import is_closed_bar, utc_dt

    adjusted = ((payload.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose") or []
    result = {}
    for timestamp, value in zip(payload.get("timestamp") or [], adjusted):
        if timestamp is None or not is_closed_bar(timestamp):
            continue
        try:
            price = float(value)
        except (TypeError, ValueError):
            continue
        if price > 0 and math.isfinite(price):
            result[utc_dt(timestamp).date().isoformat()] = price
    return result


def compare(ticker: str, benchmark: str, start: date, etf: dict, index: dict,
            fx: dict[str, dict[str, float]] | None = None) -> EtfComparisonResult:
    from routers._benchmark_index import _rate

    own, bench = adjusted_closes(etf), adjusted_closes(index)
    days = sorted(d for d in own.keys() & bench.keys() if d >= start.isoformat())
    if len(days) < 2:
        raise ValueError("Fewer than two shared completed trading days since this start date. Check the ticker and listing history.")
    # Convert each historical price BEFORE rebasing. Today's spot FX would cancel out of a
    # return ratio and silently leave the result in native currency.
    for payload, prices in ((etf, own), (index, bench)):
        currency = (payload.get("meta") or {}).get("currency")
        if not currency:
            raise ValueError("Yahoo returned no quote currency; cannot safely convert this series to EUR.")
        for day in days:
            rate = _rate(fx or {}, currency, day)
            if rate is None or not math.isfinite(rate) or rate <= 0:
                raise ValueError(f"Missing historical {currency}/EUR FX rate on or before {day}. EUR comparison is unavailable; refresh FX rates.")
            prices[day] /= rate
    first = days[0]
    points = [EtfComparisonPoint(date=d, etf_pct=(own[d] / own[first] - 1) * 100,
                                 benchmark_pct=(bench[d] / bench[first] - 1) * 100) for d in days]
    last = points[-1]
    return EtfComparisonResult(
        ticker=ticker, benchmark=benchmark, requested_start=start.isoformat(), start=first, end=last.date,
        currency="EUR",
        benchmark_currency="EUR",
        total_return_pct=last.etf_pct, benchmark_return_pct=last.benchmark_pct,
        excess_pp=last.etf_pct - last.benchmark_pct, points=points,
    )


def load(ticker: str, benchmark: str, start: date) -> EtfComparisonResult:
    from concurrent.futures import ThreadPoolExecutor
    from asset_pipeline.yahoo import chart_window

    begin = int(datetime.combine(start, datetime.min.time(), timezone.utc).timestamp())
    end = int(datetime.combine(date.today() + timedelta(days=1), datetime.min.time(), timezone.utc).timestamp())
    with ThreadPoolExecutor(max_workers=2) as pool:
        own = pool.submit(chart_window, ticker, begin, end)
        index = pool.submit(chart_window, benchmark, begin, end)
        etf, bench = own.result(), index.result()
    if not etf or not bench:
        raise ValueError("Yahoo Finance returned no history for the ETF or benchmark. Use an exact Yahoo ticker, including its exchange suffix.")
    from common.fx_load import load_fx_to_eur

    currencies = {(payload.get("meta") or {}).get("currency") or "" for payload in (etf, bench)}
    fx = load_fx_to_eur(currencies, (start - timedelta(days=45)).isoformat(), date.today().isoformat())
    return compare(ticker, benchmark, start, etf, bench, fx)
