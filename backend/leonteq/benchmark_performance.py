"""Saved Leonteq monthly returns beside the existing EUR benchmark ETF series."""
import calendar
import math
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from deps import supabase
from leonteq.monthly_performance import TABLE
from leonteq.portfolio_cache import _account_key


def monthly_rows(source, year, series, today):
    values = {row['month']: row.get('return_pct') for row in source.get('months', [])}
    points = {row['date']: row for row in series.get('points', [])}
    try:
        fetched = datetime.fromisoformat(source['retrieved_at'].replace('Z', '+00:00')).astimezone(ZoneInfo('Europe/Amsterdam')).date()
    except (KeyError, ValueError, TypeError):
        fetched = None
    rows = []
    for number in range(1, 13):
        start = date(year, number, 1)
        end = date(year, number, calendar.monthrange(year, number)[1])
        opening = (start - timedelta(days=1)).isoformat()
        closed = end < today
        mtd = start <= today <= end
        boundary = today if mtd else end
        # A cached MTD return does not become a full-month return when the calendar advances.
        reported = fetched and ((closed and end < fetched) or (mtd and start <= fetched <= today))
        left = values.get(start.strftime('%Y-%m')) if reported else None
        if not isinstance(left, (int, float)) or not math.isfinite(left):
            left = None
        before, after = points.get(opening), points.get(boundary.isoformat())
        right = None
        # Never carry an ageing listing through subsequent months as a fabricated 0% return.
        def usable(point, boundary):
            if not point or not isinstance(point.get('cum_pct'), (int, float)) or not math.isfinite(point['cum_pct']):
                return False
            try:
                return 0 <= (boundary - date.fromisoformat(point['price_date'])).days <= 7
            except (ValueError, KeyError, TypeError):
                return False
        if (closed or mtd) and usable(before, start - timedelta(days=1)) and usable(after, boundary) and before['cum_pct'] > -100:
            right = ((1 + after['cum_pct'] / 100) / (1 + before['cum_pct'] / 100) - 1) * 100
        rows.append({'month': start.strftime('%Y-%m'), 'leonteq_pct': left, 'benchmark_pct': right,
                     'difference_pp': left - right if left is not None and right is not None else None,
                     'completed': closed, 'mtd': mtd, 'benchmark_price_date': after.get('price_date') if right is not None else None})
    return rows


def comparison(name, year, benchmark='ACWI'):
    from routers._benchmark_etf import etf_return_series
    saved = (supabase.table(TABLE).select('payload').eq('account_key', _account_key())
             .eq('portfolio_name', name).limit(1).execute().data or [])
    source = saved[0]['payload'] if saved else {}
    today = datetime.now(ZoneInfo('Europe/Amsterdam')).date()
    anchor = date(year - 1, 12, 31)
    ends = [date(year, month, calendar.monthrange(year, month)[1]) for month in range(1, 13)]
    dates = [anchor.isoformat(), *(min(end, today).isoformat() for end in ends if end.replace(day=1) <= today)]
    series = etf_return_series(benchmark, anchor.isoformat(), dates, read_only=True) if len(dates) > 1 else {}
    years = sorted({int(row['month'][:4]) for row in source.get('months', [])}, reverse=True)
    return {'portfolio': name, 'benchmark': benchmark, 'year': year, 'years': years,
            'months': monthly_rows(source, year, series, today), 'retrieved_at': source.get('retrieved_at'),
            'benchmark_fetched_at': series.get('fetched_at'),
            'benchmark_ticker': series.get('ticker'), 'currency': 'EUR', 'saved_returns': bool(saved),
            'benchmark_available': bool(series.get('points'))}
