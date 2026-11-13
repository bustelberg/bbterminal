"""Dated EUR total-return factsheets, using strictly adjusted completed Yahoo closes."""
from bisect import bisect_right
from datetime import date, datetime, timedelta, timezone
from math import isfinite, sqrt
from statistics import stdev
from threading import RLock

from deps import supabase

TABLE = 'benchmark_factsheet'
PERIOD_YEARS = 15
KEY = f'eur-total-return-{PERIOD_YEARS}y-acwi-agg-qai-v4'
ASSETS = (
    ('ACWI', 'iShares MSCI ACWI ETF', 'USD'),
    ('AGG', 'iShares Core U.S. Aggregate Bond ETF', 'USD'),
    ('QAI', 'NYLIM Hedge Multi-Strategy Tracker ETF · Liquid alternatives proxy', 'USD'),
)
_lock = RLock()


def years_before(day):
    try:
        return day.replace(year=day.year - PERIOD_YEARS)
    except ValueError:
        return day.replace(year=day.year - PERIOD_YEARS, day=28)


def adjusted_prices(chart, ticker, currency, today):
    from asset_pipeline.yahoo import is_completed_daily_bar, utc_dt
    meta = chart.get('meta') or {}
    if meta.get('currency') != currency:
        raise ValueError(f'{ticker}: expected {currency} quotes, received {meta.get("currency")!r}.')
    adjusted = ((chart.get('indicators') or {}).get('adjclose') or [{}])[0].get('adjclose') or []
    result = {}
    for timestamp, raw in zip(chart.get('timestamp') or [], adjusted):
        if timestamp is None or raw is None or not is_completed_daily_bar(timestamp, meta):
            continue
        value = float(raw)
        day = utc_dt(timestamp).date().isoformat()
        if value > 0 and isfinite(value) and day <= today.isoformat():
            result[day] = value
    if not result:
        raise ValueError(f'{ticker}: no completed dividend-adjusted closes. Raw price returns are not substituted.')
    if (today - date.fromisoformat(max(result))).days > 7:
        raise ValueError(f'{ticker}: latest adjusted close is {max(result)}, more than seven days old.')
    return result


def eur_prices(prices, currency, fx):
    if currency == 'EUR':
        return dict(prices)
    rates = fx.get(currency) or {}
    dates = sorted(rates)
    result = {}
    for day, value in prices.items():
        index = bisect_right(dates, day) - 1
        rate = float(rates[dates[index]]) if index >= 0 else 0
        if (index < 0 or not isfinite(rate) or rate <= 0
                or (date.fromisoformat(day) - date.fromisoformat(dates[index])).days > 7):
            raise ValueError(f'Missing recent historical {currency}/EUR FX rate for {day}; refresh FX history.')
        result[day] = value / rate  # Stored rate is units of source currency per EUR.
    return result


def statistics(prices, start, end):
    days = sorted(day for day in prices if start <= day <= end)
    if len(days) < 2:
        raise ValueError('Fewer than two completed prices in the requested period.')
    elapsed = (date.fromisoformat(days[-1]) - date.fromisoformat(days[0])).days
    years = elapsed / 365.2425
    if years <= 0:
        raise ValueError('The return window has zero length.')
    gaps = [(date.fromisoformat(b) - date.fromisoformat(a)).days for a, b in zip(days, days[1:])]
    if max(gaps) > 10 or len(days) - 1 < max(2, int(years * 200)):
        raise ValueError('Daily price history has gaps or too few observations for reliable volatility.')
    returns = [prices[b] / prices[a] - 1 for a, b in zip(days, days[1:])]
    ratio = prices[days[-1]] / prices[days[0]]
    return {'start': days[0], 'end': days[-1], 'years': years, 'observations': len(returns),
            'total_return_pct': (ratio - 1) * 100, 'cagr_pct': (ratio ** (1 / years) - 1) * 100,
            'volatility_pct': stdev(returns) * sqrt(252) * 100}


def calculate(series):
    # Same closing date and same opening date wherever all funds have full history.
    common = set.intersection(*(set(series[ticker]) for ticker, _, _ in ASSETS))
    if not common:
        raise ValueError('ACWI, AGG and QAI have no shared completed trading date.')
    end = max(common)
    anchor = years_before(date.fromisoformat(end)).isoformat()
    shared_open = max((day for day in common if day <= anchor), default=None)
    rows = []
    for ticker, name, _ in ASSETS:
        prices = series[ticker]
        prior = sorted(day for day in prices if day <= anchor)
        opening = shared_open or (prior[-1] if prior else None)
        full = None
        reason = None
        if opening is None or (date.fromisoformat(anchor) - date.fromisoformat(opening)).days > 7:
            reason = f'Fifteen years of fund history are not available. Saved history starts {min(prices)}.'
        else:
            try:
                full = statistics(prices, opening, end)
            except ValueError as exc:
                reason = str(exc)
        available = None
        if full is None:
            try:
                available = statistics(prices, min(prices), end)
            except ValueError as exc:
                reason = f'{reason or ""} Available-history calculation: {exc}'.strip()
        rows.append({'ticker': ticker, 'name': name, 'currency': 'EUR', 'requested_start': anchor,
                     'end': end, 'period': full, 'available_history': available, 'reason': reason,
                     'source_prices_eur': prices})
    return rows


def read():
    from fastapi import HTTPException
    try:
        rows = supabase.table(TABLE).select('payload,prepared_at').eq('key', KEY).limit(1).execute().data or []
    except Exception as exc:
        raise HTTPException(503, 'Factsheet storage is unavailable. Apply migration '
                            '20261113200000_benchmark_factsheet.sql. '
                            f'Database error: {type(exc).__name__}') from exc
    if not rows:
        return {'ready': False, 'rows': [], 'currency': 'EUR', 'period_years': PERIOD_YEARS}
    row = rows[0]
    return {**row['payload'], 'ready': True, 'prepared_at': row['prepared_at'],
            'stale': str(row['prepared_at'])[:10] != date.today().isoformat()}


def prepare(*, force=False, today=None):
    from asset_pipeline.yahoo import chart_window
    from common.fx_load import load_fx_to_eur
    today = today or date.today()
    with _lock:
        saved = read()
        if saved.get('ready') and not saved.get('stale') and not force:
            return saved
        start = years_before(today) - timedelta(days=45)
        begin = int(datetime.combine(start, datetime.min.time(), timezone.utc).timestamp())
        finish = int(datetime.combine(today + timedelta(days=1), datetime.min.time(), timezone.utc).timestamp())
        series = {}
        fx = load_fx_to_eur({'USD'}, (start - timedelta(days=10)).isoformat(), today.isoformat())
        for ticker, _name, currency in ASSETS:
            chart = chart_window(ticker, begin, finish, '1d')
            if not chart:
                raise ValueError(f'{ticker}: Yahoo did not return history; previous Factsheet remains saved.')
            series[ticker] = eur_prices(adjusted_prices(chart, ticker, currency, today), currency, fx)
        payload = {'currency': 'EUR', 'period_years': PERIOD_YEARS, 'calculation_date': today.isoformat(), 'rows': calculate(series),
                   'method': 'Yahoo dividend-adjusted completed daily closes, converted to EUR at historical FX. '
                             'CAGR uses elapsed calendar years; volatility is sample standard deviation of daily '
                             'arithmetic returns × sqrt(252). Fifteen-year windows use the last close on/before the anniversary '
                             '(maximum seven days earlier). Shorter history is labelled separately.'}
        stamp = datetime.now(timezone.utc).isoformat()
        supabase.table(TABLE).upsert({'key': KEY, 'prepared_at': stamp, 'payload': payload}, on_conflict='key').execute()
        return {**payload, 'prepared_at': stamp, 'ready': True, 'stale': False}
