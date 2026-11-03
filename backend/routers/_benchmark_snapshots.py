"""Durable, compact benchmark chart results. Chart reads never load constituents.

The existing background prewarmer supplies its calculated input payloads once per refresh.
Publication is atomic; an interrupted rebuild leaves the previous complete snapshot intact.
"""
from __future__ import annotations

from datetime import datetime, timezone
import math
import re

from deps import supabase

VERSION = 2
TABLE = 'benchmark_chart_snapshot'
INPUTS = ('margin-inputs', 'debt-ratio-inputs', 'cash-return-inputs', 'interest-burden-inputs',
          'sbc-ocf-inputs', 'capex-margin-inputs', 'gross-margin-inputs', 'cash-conversion-inputs',
          'fcf-sbc-yield-inputs', 'dividend-yield-inputs')


def period_x(period: str) -> float | None:
    if re.fullmatch(r'\d{4}', period):
        return float(period)
    match = re.fullmatch(r'(\d{4})-Q([1-4])', period)
    return int(match[1]) + (int(match[2]) - 1) / 4 if match else None


def _value(row, field, period):
    value = (row.get(field) or {}).get(period)
    return float(value) if value is not None and math.isfinite(float(value)) else None


def _ratio(row, period, numerator, denominator, *, correct=False, absolute=False):
    num = _value(row, numerator, period)
    den = _value(row, denominator, period)
    if num is None or den is None or den <= 0:
        return None
    if correct:
        num -= _value(row, 'sbc', period) or 0
    return 100 * (abs(num) if absolute else num) / den


def _capital(row, period):
    ncl, equity = _value(row, 'noncurrent_liabilities', period), _value(row, 'total_equity', period)
    return ncl + equity if ncl is not None and equity is not None else None


def _croic(row, period, correct):
    capital, fcf = _capital(row, period), _value(row, 'fcf', period)
    if capital is None or capital <= 0 or fcf is None:
        return None
    return 100 * (fcf - ((_value(row, 'sbc', period) or 0) if correct else 0)) / capital


def _debt(row, period):
    assets, debt = _value(row, 'total_assets', period), _value(row, 'long_term_debt', period)
    base = assets - (_value(row, 'goodwill', period) or 0) if assets is not None else None
    return 100 * debt / base if debt is not None and base is not None and base > 0 else None


def weighted_series(rows, caps, label, year_field, value_of):
    """Twin of marginData.weightedByYear, including bounded carry and reported-only coverage."""
    from routers._fundamental_blend import _weight_at

    prepared = []
    axis = {}
    for row in rows:
        own = {}
        for period in (row.get(year_field) or {}):
            x = period_x(period)
            value = value_of(row, period)
            if x is not None and value is not None and math.isfinite(value):
                own[x] = value
                axis[x] = period
        prepared.append((row, own, {'weight': row['weight_pct'],
                                    **({'weights': caps.get(row.get('isin'), {})} if caps else {})}))
    num, den, reported, covered = {}, {}, {}, {}
    total = sum(row['weight_pct'] for row in rows)
    if total <= 0:
        return {'points': [], 'coverage': []}
    for row, own, weighted in prepared:
        last = None
        for x in sorted(axis):
            if x in own:
                last = (x, own[x])
            if last is None or x - last[0] > 1.05:
                continue
            weight = _weight_at(weighted, axis[x])
            if not weight:
                continue
            num[x] = num.get(x, 0) + weight * last[1]
            den[x] = den.get(x, 0) + weight
            if x in own:
                reported[x] = reported.get(x, 0) + 1
                covered[x] = covered.get(x, 0) + row['weight_pct']
    points, coverage = [], []
    for x in sorted(axis):
        count = reported.get(x, 0)
        weight_pct = 100 * covered.get(x, 0) / total if total > 0 else 0
        clears = count >= 500 if label == 'ACWI' else count >= len(rows) * .5 and weight_pct >= 50
        coverage.append({'period': axis[x], 'companies': count, 'weight_pct': round(weight_pct, 2)})
        if clears and den.get(x, 0) > 0:
            points.append([x, num[x] / den[x]])
    return {'points': points, 'coverage': coverage}


def compact(label, cadence, payloads):
    """Convert background-only constituent inputs into chart-sized series for every toggle."""
    from routers._fundamental_blend import blend_series, quarter_bucket, year_bucket, period_end

    caps = payloads['universe-period-caps'].get('caps', {})
    specs = {
        'margin-inputs': ('revenue', {mode: lambda r, p, c=c: _ratio(r, p, 'fcf', 'revenue', correct=c)
                                    for mode, c in [('reported', False), ('corrected', True)]}),
        'debt-ratio-inputs': ('total_assets', {'default': _debt}),
        'cash-return-inputs': ('total_equity', {
            'croic_reported': lambda r, p: _croic(r, p, False),
            'croic_corrected': lambda r, p: _croic(r, p, True)}),
        'interest-burden-inputs': ('operating_income', {'default': lambda r, p: _ratio(r, p, 'interest_expense', 'operating_income', absolute=True)}),
        'sbc-ocf-inputs': ('ocf', {'default': lambda r, p: _ratio(r, p, 'sbc', 'ocf', absolute=True)}),
        'capex-margin-inputs': ('revenue', {'default': lambda r, p: _ratio(r, p, 'capex', 'revenue', absolute=True)}),
        'gross-margin-inputs': ('revenue', {'default': lambda r, p: _ratio(r, p, 'gross_profit', 'revenue')}),
        'cash-conversion-inputs': ('net_income', {mode: lambda r, p, c=c: _ratio(r, p, 'fcf', 'net_income', correct=c)
                                             for mode, c in [('reported', False), ('corrected', True)]}),
        'fcf-sbc-yield-inputs': ('market_cap', {mode: lambda r, p, c=c: _ratio(r, p, 'fcf', 'market_cap', correct=c)
                                            for mode, c in [('reported', False), ('corrected', True)]}),
        'dividend-yield-inputs': ('price_ps', {'default': lambda r, p: _ratio(r, p, 'div_ps', 'price_ps')}),
    }
    series = {}
    for path, (year_field, variants) in specs.items():
        rows = payloads[path]['rows']
        series[path] = {name: weighted_series(rows, caps, label, year_field, fn) for name, fn in variants.items()}
    rows = payloads['cash-return-inputs']['rows']
    series['cash-return-inputs']['roic'] = weighted_series(rows, caps, label, 'roic', lambda r, p: _value(r, 'roic', p))
    members = []
    for row in rows:
        points = {period_end(p): value for p in set(row.get('total_equity', {})) | set(row.get('noncurrent_liabilities', {}))
                  if period_x(p) is not None and (value := _capital(row, p)) is not None}
        members.append({'weight': row['weight_pct'], 'points': points,
                        **({'weights': caps.get(row.get('isin'), {})} if caps else {}),
                        **({'min_reporting_companies': 500} if label == 'ACWI' else {})})
    capital = blend_series(members, 'annuals__Income Statement__Revenue',
                           quarter_bucket if cadence == 'quarterly' else year_bucket)
    series['cash-return-inputs']['invested_capital'] = {
        'points': [[period_x(p['period']), p['value']] for p in capital['points']],
        'coverage': [{'period': p['period'], 'companies': p['reporting_companies'], 'weight_pct': p['covered_pct']}
                     for p in capital['points']]}
    growth = dict(payloads['fundamental-blend-metrics'])
    from routers._chart_statistics import precalculate
    growth['chart_statistics'] = precalculate(growth.get('metrics', []), cadence)
    growth['coverage'] = {k: v for k, v in growth.get('coverage', {}).items() if k != 'rows'}
    return {'version': VERSION, 'label': label, 'cadence': cadence, 'growth': growth, 'series': series,
            'policy': {'minimum_companies': 500 if label == 'ACWI' else None, 'minimum_percentage': None if label == 'ACWI' else 50}}


def publish(label, cadence, payloads, started_at):
    required = {'universe-period-caps', 'fundamental-blend-metrics', *INPUTS}
    if not required.issubset(payloads):
        raise ValueError('Incomplete chart rebuild; keeping the previous snapshot')
    payload = compact(label, cadence, payloads)
    built_at = datetime.now(timezone.utc).isoformat()
    payload['built_at'] = built_at
    supabase.rpc('publish_benchmark_chart_snapshot', {'p_label': label, 'p_cadence': cadence,
                 'p_version': VERSION, 'p_started_at': started_at, 'p_built_at': built_at, 'p_payload': payload}).execute()
    return payload


def read(label, cadence, *, compact=False):
    if compact:
        return supabase.rpc('read_benchmark_chart_snapshot', {
            'p_label': label, 'p_cadence': cadence, 'p_version': VERSION}).execute().data
    rows = (supabase.table(TABLE).select('payload').eq('label', label).eq('cadence', cadence)
            .eq('version', VERSION).limit(1).execute().data or [])
    return rows[0]['payload'] if rows else None


def read_comparison(label, cadence, own_rows):
    from routers._chart_statistics import METRICS, reported_points
    periods = {metric: [p['year'] for p in reported_points(own_rows, metric, cadence)] for metric in METRICS}
    return supabase.rpc('read_benchmark_chart_snapshot', {
        'p_label': label, 'p_cadence': cadence, 'p_version': VERSION,
        'p_own_periods': periods}).execute().data
