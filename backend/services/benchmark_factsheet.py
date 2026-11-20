"""Dated EUR total-return factsheets, using strictly adjusted completed Yahoo closes."""
from bisect import bisect_left, bisect_right
from datetime import date, datetime, timedelta, timezone
from math import ceil, isfinite, sqrt
from statistics import correlation, mean, StatisticsError, stdev
from threading import RLock

from deps import supabase

TABLE = 'benchmark_factsheet'
PERIOD_YEARS = 15
KEY = f'eur-total-return-{PERIOD_YEARS}y-acwi-syba-qai-hdg-v6'
ASSETS = (
    ('ACWI', 'iShares MSCI ACWI ETF', 'USD'),
    ('SYBA.DE', 'State Street SPDR Bloomberg Euro Aggregate Bond UCITS ETF', 'EUR'),
    ('QAI', 'NYLIM Hedge Multi-Strategy Tracker ETF · Liquid alternatives proxy', 'USD'),
    ('HDG', 'ProShares Hedge Replication ETF · HFRI-oriented replication proxy', 'USD'),
)
BOND_ISIN = 'IE00B41RYL63'
BOND_SOURCE = f'https://www.justetf.com/en/etf-profile.html?isin={BOND_ISIN}#overview'
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
            'volatility_pct': stdev(returns) * sqrt(252) * 100,
            'cvar_95_pct': historical_cvar(returns)}


def historical_cvar(returns):
    """Average loss in the worst 5%, with fractional weight at the boundary."""
    tail_size = len(returns) / 20
    whole = int(tail_size)
    ordered = sorted(returns)
    total = sum(ordered[:whole]) + (tail_size - whole) * ordered[whole]
    return -100 * total / tail_size


def with_saved_cvar(rows):
    result = []
    for row in rows:
        updated = dict(row)
        prices = row.get('source_prices_eur') or {}
        for key in ('period', 'available_history'):
            stats = row.get(key)
            if not stats or stats.get('cvar_95_pct') is not None or not prices:
                continue
            days = sorted(day for day in prices if stats['start'] <= day <= stats['end'])
            if len(days) >= 2:
                returns = [prices[b] / prices[a] - 1 for a, b in zip(days, days[1:])]
                updated[key] = {**stats, 'cvar_95_pct': historical_cvar(returns)}
        result.append(updated)
    return result


def calculate(series):
    # Same closing date and same opening date wherever all funds have full history.
    common = set.intersection(*(set(series[ticker]) for ticker, _, _ in ASSETS))
    if not common:
        raise ValueError('ACWI, SYBA.DE, QAI and HDG have no shared completed trading date.')
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
                     'isin': BOND_ISIN if ticker == 'SYBA.DE' else None,
                     'source_url': BOND_SOURCE if ticker == 'SYBA.DE' else None,
                     'end': end, 'period': full, 'available_history': available, 'reason': reason,
                     'source_prices_eur': prices})
    return rows


def benchmark_correlations(rows):
    tickers = ('ACWI', 'SYBA.DE', 'HDG')
    series = {row['ticker']: row.get('source_prices_eur') or {} for row in rows}
    if any(not series.get(ticker) for ticker in tickers):
        return None
    end = min(row['end'] for row in rows if row['ticker'] in tickers)
    anchor = years_before(date.fromisoformat(end)).isoformat()
    common = sorted(set.intersection(*(set(series[ticker]) for ticker in tickers)))
    opening = max((day for day in common if day <= anchor), default=anchor)
    if (date.fromisoformat(anchor) - date.fromisoformat(opening)).days > 7:
        opening = anchor
    days = [day for day in common if opening <= day <= end]
    if len(days) < 3:
        return None
    returns = {ticker: [series[ticker][b] / series[ticker][a] - 1 for a, b in zip(days, days[1:])]
               for ticker in tickers}
    def coefficient(left, right):
        try:
            return max(-1.0, min(1.0, correlation(returns[left], returns[right])))
        except StatisticsError:
            return None
    return {'tickers': tickers, 'start': days[0], 'end': days[-1], 'observations': len(days) - 1,
            'matrix': [[coefficient(left, right) for right in tickers] for left in tickers]}


def downside_comparison(rows):
    selected = {row['ticker']: row for row in rows if row['ticker'] in ('ACWI', 'HDG')}
    acwi = selected.get('ACWI', {}).get('source_prices_eur') or {}
    hdg = selected.get('HDG', {}).get('source_prices_eur') or {}
    if not acwi or not hdg:
        return []
    days = sorted(acwi.keys() & hdg.keys())
    if len(days) < 2:
        return []
    end = days[-1]
    start = years_before(date.fromisoformat(end)).isoformat()
    pairs = [(day, acwi[day] / acwi[prior] - 1, hdg[day] / hdg[prior] - 1)
             for prior, day in zip(days, days[1:]) if day >= start]
    negative = [pair for pair in pairs if pair[1] < 0]
    def summarize(key, first, last, points):
        equity = [point[1] for point in points]
        hedge = [point[2] for point in points]
        try:
            coefficient = max(-1.0, min(1.0, correlation(equity, hedge))) if len(points) >= 3 else None
        except StatisticsError:
            coefficient = None
        return {'key': key, 'start': first, 'end': last, 'observations': len(points),
                'correlation': coefficient, 'acwi_mean_pct': mean(equity) * 100 if equity else None,
                'hdg_mean_pct': mean(hedge) * 100 if hedge else None,
                'hdg_positive': sum(value > 0 for value in hedge)}
    worst = sorted(negative, key=lambda point: point[1])[:ceil(len(pairs) * .05)]
    return [summarize('worst', max(start, days[0]), end, worst),
            *(summarize(key, first, last, [point for point in negative if first <= point[0] <= last])
              for key, first, last in (('covid', '2020-02-20', '2020-03-23'),
                                       ('tariffs', '2025-04-03', '2025-04-08')))]


def largest_drawdowns(values):
    """Return the depths of the three worst distinct drawdown episodes."""
    peak = values[0]
    depth = 0.0
    episodes = []
    for value in values[1:]:
        if value >= peak:
            if depth < 0:
                episodes.append(depth)
            peak = value
            depth = 0.0
        else:
            depth = min(depth, float(value / peak - 1))
    if depth < 0:
        episodes.append(depth)
    return [100 * depth for depth in sorted(episodes)[:3]]


def allocation_backtest(rows):
    """Fixed weights, restored at the last shared close of each calendar quarter."""
    import numpy as np

    tickers = ('ACWI', 'SYBA.DE', 'HDG')
    series = {row['ticker']: row.get('source_prices_eur') or {} for row in rows}
    if any(not series.get(ticker) for ticker in tickers):
        return None
    common = sorted(set.intersection(*(set(series[ticker]) for ticker in tickers)))
    if not common:
        return None
    anchor = years_before(date.fromisoformat(common[-1])).isoformat()
    start = max((day for day in common if day <= anchor), default=common[0])
    if (date.fromisoformat(anchor) - date.fromisoformat(start)).days > 7:
        start = anchor
    days = [day for day in common if day >= start]
    if len(days) < 3:
        return None
    prices = np.array([[series[ticker][day] for ticker in tickers] for day in days])
    quarters = [(day[:4], (int(day[5:7]) - 1) // 3) for day in days]
    stops = [index for index in range(1, len(days)) if quarters[index] != quarters[index - 1]]
    boundaries = [0, *(index - 1 for index in stops if index > 1), len(days) - 1]
    windows = []
    for index, day in enumerate(days):
        if index and days[index - 1][:7] == day[:7]:
            continue
        first = date.fromisoformat(day)
        try:
            anniversary = first.replace(year=first.year + 5)
        except ValueError:
            anniversary = first.replace(year=first.year + 5, day=28)
        last = bisect_left(days, anniversary.isoformat())
        if last < len(days):
            windows.append((index, last, (date.fromisoformat(days[last]) - first).days / 365.2425))
    years = (date.fromisoformat(days[-1]) - date.fromisoformat(days[0])).days / 365.2425
    allocations = [(acwi, 100 - acwi - hdg, hdg)
                   for acwi in range(0, 101, 5)
                   for hdg in range(0, 101 - acwi, 5)]
    results = []
    for allocation in allocations:
        weights = np.array(allocation) / 100
        values = np.ones(len(days))
        for left, right in zip(boundaries, boundaries[1:]):
            values[left + 1:right + 1] = values[left] * (prices[left + 1:right + 1] / prices[left]) @ weights
        returns = values[1:] / values[:-1] - 1
        volatility = returns.std(ddof=1)
        five_year = []
        for first, last, elapsed in windows:
            next_rebalance = boundaries[bisect_right(boundaries, first)]
            bridge = min(last, next_rebalance)
            growth = ((prices[bridge] / prices[first]) @ weights) * values[last] / values[bridge]
            five_year.append((growth ** (1 / elapsed) - 1) * 100)
        results.append({'weights': allocation, 'cagr_pct': (values[-1] ** (1 / years) - 1) * 100,
                        'sharpe': returns.mean() / volatility * sqrt(252) if volatility else None,
                        'volatility_pct': volatility * sqrt(252) * 100,
                        'cvar_95_pct': historical_cvar(returns),
                        'drawdowns_pct': largest_drawdowns(values),
                        'lower_quartile_pct': float(np.quantile(five_year, .25)) if five_year else None,
                        'worst_pct': min(five_year) if five_year else None})
    return {'start': days[0], 'end': days[-1], 'windows': len(windows), 'rows': results}


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
    return {**row['payload'], 'rows': with_saved_cvar(row['payload']['rows']),
            'correlations': benchmark_correlations(row['payload']['rows']),
            'downside': downside_comparison(row['payload']['rows']),
            'allocations': allocation_backtest(row['payload']['rows']),
            'ready': True, 'prepared_at': row['prepared_at'],
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
                   'method': 'Yahoo dividend-adjusted completed daily closes, converted to EUR at historical FX where needed. '
                             'The bond fund is State Street SPDR Bloomberg Euro Aggregate Bond UCITS ETF, ISIN IE00B41RYL63 (justETF reference). '
                             'CAGR uses elapsed calendar years; volatility is sample standard deviation of daily '
                             'arithmetic returns × sqrt(252). Fifteen-year windows use the last close on/before the anniversary '
                             '(maximum seven days earlier). Shorter history is labelled separately.'}
        stamp = datetime.now(timezone.utc).isoformat()
        supabase.table(TABLE).upsert({'key': KEY, 'prepared_at': stamp, 'payload': payload}, on_conflict='key').execute()
        return {**payload, 'correlations': benchmark_correlations(payload['rows']),
                'downside': downside_comparison(payload['rows']),
                'allocations': allocation_backtest(payload['rows']),
                'prepared_at': stamp, 'ready': True, 'stale': False}
