"""Statistics shipped with chart lines, with identical windows for a comparison pair."""
from __future__ import annotations

import math

METRICS = ('price_ps', 'eps_nri', 'revenue', 'fcf_ps', 'shares')


def reported_points(rows, metric, cadence):
    from routers.earnings import _metric_codes
    codes = set(_metric_codes(metric))
    points = {}
    for row in rows:
        value = row.get('numeric_value')
        if row.get('metric_code') not in codes or value is None or not math.isfinite(float(value)):
            continue
        date = str(row['target_date'])[:10]
        year = int(date[:4])
        if year < 2017:
            continue
        x = year + ((int(date[5:7]) - 1) // 3) / 4 if cadence == 'quarterly' else year
        if x not in points or date > points[x]['date']:
            points[x] = {'year': x, 'value': float(value), 'date': date}
    return [points[x] for x in sorted(points)]


def _label(x):
    year = int(x)
    quarter = round((x - year) * 4)
    return str(year) if not quarter else f'{year} Q{quarter + 1}'


def statistics(points):
    used = [p for p in points if p['value'] > 0 and math.isfinite(p['value'])]
    n = len(used)
    fit = {'r2': None, 'cagr': None, 'n': n, 'dropped': len(points) - n,
           'slope': None, 'intercept': None}
    if n >= 2:
        mx = sum(p['year'] for p in used) / n
        my = sum(math.log(p['value']) for p in used) / n
        xx = sum((p['year'] - mx) ** 2 for p in used)
        if xx > 0:
            slope = sum((p['year'] - mx) * (math.log(p['value']) - my) for p in used) / xx
            intercept = my - slope * mx
            residual = sum((math.log(p['value']) - intercept - slope * p['year']) ** 2 for p in used)
            total = sum((math.log(p['value']) - my) ** 2 for p in used)
            fit.update(r2=1 if total == 0 else max(0, 1 - residual / total),
                       cagr=math.expm1(slope), slope=slope, intercept=intercept)
    cagr = {'pct': None, 'reason': 'fewer than two reported periods'}
    if len(points) >= 2:
        a, b = points[0], points[-1]
        years = b['year'] - a['year']
        if years > 0 and a['value'] > 0 and b['value'] > 0:
            cagr = {'pct': 100 * ((b['value'] / a['value']) ** (1 / years) - 1),
                    'from': _label(a['year']), 'to': _label(b['year']), 'years': years,
                    'fromValue': a['value'], 'toValue': b['value']}
        else:
            cagr = {'pct': None, 'reason': 'the series is not positive at both ends, so a compound growth rate is undefined'}
    return {'fit': fit, 'cagr': cagr,
            'fromX': points[0]['year'] if points else None, 'toX': points[-1]['year'] if points else None}


def window_key(start, end):
    return f'{start:g}:{end:g}'


def precalculate(rows, cadence):
    """All reported windows are saved, so a later company comparison needs no benchmark regression."""
    result = {}
    for metric in METRICS:
        points = reported_points(rows, metric, cadence)
        windows = {}
        for i, start in enumerate(points):
            for j in range(i, len(points)):
                windows[window_key(start['year'], points[j]['year'])] = statistics(points[i:j + 1])
        result[metric] = {'points': points, 'windows': windows, 'full': statistics(points)}
    return result


def paired_statistics(own_rows, benchmark_rows, cadence, saved=None):
    own, benchmark = {}, {}
    for metric in METRICS:
        a = reported_points(own_rows, metric, cadence)
        b = reported_points(benchmark_rows, metric, cadence) if benchmark_rows is not None else []
        shared = sorted({p['year'] for p in a} & {p['year'] for p in b})
        if shared:
            lo, hi = shared[0], shared[-1]
            a = [p for p in a if lo <= p['year'] <= hi]
            b = [p for p in b if lo <= p['year'] <= hi]
        own[metric] = statistics(a)
        if benchmark_rows is not None:
            windows = (saved or {}).get(metric, {})
            key = window_key(b[0]['year'], b[-1]['year']) if b else None
            if saved is not None:
                prepared = windows if 'fit' in windows else (windows.get('windows', {}).get(key) if b else windows.get('full'))
                if prepared is None:
                    raise ValueError(f'Saved {metric} statistics are missing for the chart window')
                if b and (prepared['fromX'] != b[0]['year'] or prepared['toX'] != b[-1]['year']):
                    raise ValueError(f'Saved {metric} statistics do not match the chart window')
                benchmark[metric] = prepared
            else:
                benchmark[metric] = statistics(b)
    return own, benchmark
