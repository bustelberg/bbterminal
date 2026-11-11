"""Bounded, reproducible allocation research over saved AIRS weekly observations."""
from datetime import date, timedelta
from math import isfinite, sqrt

BUCKETS = ('Equity', 'Bonds', 'Alternatives', 'Cash')


def metrics(values, annual_target=2.0, annual_risk_free=0.0):
    """Annualised Sharpe/sample deviation and Sortino/all-week target semideviation."""
    if not values:
        return None
    target = (1 + annual_target / 100) ** (1 / 52) - 1
    downside = sqrt(sum(min(value - target, 0) ** 2 for value in values) / len(values))
    average = sum(values) / len(values)
    deviation = sqrt(sum((value - average) ** 2 for value in values) / (len(values) - 1)) if len(values) > 1 else 0
    risk_free = (1 + annual_risk_free / 100) ** (1 / 52) - 1
    wealth = peak = 1.0
    drawdown = 0.0
    for value in values:
        wealth *= 1 + value
        peak = max(peak, wealth)
        drawdown = min(drawdown, wealth / peak - 1)
    return {'sortino': sqrt(52) * (average - target) / downside if downside > 1e-12 else None,
            'sharpe': sqrt(52) * (average - risk_free) / deviation if deviation > 1e-12 else None,
            'annual_return_pct': (wealth ** (52 / len(values)) - 1) * 100,
            'max_drawdown_pct': drawdown * 100, 'weeks': len(values)}


def statistical_check(candidate, baseline, annual_target, objective='sortino', annual_risk_free=0.0):
    """Paired moving-block bootstrap on the training winner's untouched holdout."""
    import random

    n = len(candidate)
    if n < 26:
        return {'supported': False, 'reason': f'Not enough evidence yet: {n} validation weeks; at least 26 needed.'}
    target = (1 + annual_target / 100) ** (1 / 52) - 1
    if objective == 'sortino' and any(sum(value < target for value in values) < 8 for values in (candidate, baseline)):
        return {'supported': False, 'reason': 'Too few downside observations to assess the improvement reliably.'}
    rng = random.Random(2026)
    improvements = []
    for _ in range(999):
        indices = []
        while len(indices) < n:
            start = rng.randrange(n - 3)
            indices.extend(range(start, start + 4))
        a = metrics([candidate[index] for index in indices[:n]], annual_target, annual_risk_free)[objective]
        b = metrics([baseline[index] for index in indices[:n]], annual_target, annual_risk_free)[objective]
        if a is not None and b is not None:
            improvements.append(a - b)
    if len(improvements) < 950:
        return {'supported': False, 'reason': 'The validation sample is too unstable to support an allocation.'}
    improvements.sort()
    lower = improvements[int(.05 * len(improvements))]
    return {'supported': lower > 0, f'lower_{objective}_improvement': lower,
            'reason': 'Held-out improvement supported.' if lower > 0 else 'No statistically supported improvement over the current allocation.',
            'method': f'One-sided 95% paired moving-block bootstrap of {objective}; 4-week blocks; 999 resamples. Only the training winner is tested.'}


def search(matrix, current, annual_target=2.0, check=lambda: None, min_weeks=52, bounds=None, discontinuous=False,
           objective='sortino', annual_risk_free=0.0):
    """Fit chronologically; reserve 26 validation weeks when 26 training weeks remain."""
    n = len(matrix)
    if objective not in ('sharpe', 'sortino'):
        raise ValueError('Unknown search objective')
    if n < min_weeks:
        return {'available': False, 'reason': f'Only {n} consecutive common weeks are available; at least {min_weeks} are required.'}
    split = min(n * 3 // 4, n - 26) if n >= 52 else n * 3 // 4
    candidates = []
    tested = 0
    for equity in range(21):
        check()
        for bonds in range(21 - equity):
            for alternatives in range(21 - equity - bonds):
                units = (equity, bonds, alternatives, 20 - equity - bonds - alternatives)
                if any(units[i] and not current[i] for i in range(3)):
                    continue  # Do not invent a return series for an absent sleeve.
                weights = [value / 20 for value in units]
                if bounds and any(not bounds[bucket][0] - 1e-9 <= weights[i] * 100 <= bounds[bucket][1] + 1e-9
                                  for i, bucket in enumerate(BUCKETS)):
                    continue
                values = [sum(w * r for w, r in zip(weights, row)) for row in matrix]
                fit = metrics(values[:split], annual_target, annual_risk_free)
                tested += 1
                if fit[objective] is None:
                    continue
                candidates.append((fit[objective], weights))
    # Deterministic ties prefer diversification, then less cash.
    candidates.sort(key=lambda item: (-round(item[0], 10), sum(w * w for w in item[1]), item[1][3]))
    if not candidates:
        return {'available': False, 'reason': f'No allocation satisfies the profile bounds with a defined {objective} ratio. Check bounds and asset-class history.'}

    def result(weights, values):
        answer = {'weights': dict(zip(BUCKETS, [round(w * 100, 6) for w in weights])),
                'training': metrics(values[:split], annual_target, annual_risk_free), 'validation': metrics(values[split:], annual_target, annual_risk_free),
                'full_history': metrics(values, annual_target, annual_risk_free)}
        if discontinuous:
            for key in ('training', 'validation', 'full_history'):
                answer[key]['annual_return_pct'] = answer[key]['max_drawdown_pct'] = None
        return answer

    current_values = [sum(w * r for w, r in zip(current, row)) for row in matrix]
    winner = candidates[0][1]
    winner_values = [sum(w * r for w, r in zip(winner, row)) for row in matrix]
    validation = ({'supported': False, 'reason': 'Missing weeks prevent reliable validation.'}
                  if discontinuous else statistical_check(winner_values[split:], current_values[split:], annual_target, objective, annual_risk_free))
    if not validation['supported']:
        validation = {**validation, 'reason': f'Calculated from {n} usable weeks. {validation["reason"]}'}
    return {'available': True, 'objective': objective, 'candidates_tested': tested, 'training_weeks': split, 'validation_weeks': n - split,
            'statistical_check': validation,
            'current': result(current, current_values),
            'candidates': [result(weights, [sum(w * r for w, r in zip(weights, row)) for row in matrix])
                           for _, weights in candidates[:5]]}


def weekly_point(points, target):
    point = points.get(target)
    if not point:
        return False
    end = date.fromisoformat(target)
    opening, closing = date.fromisoformat(point[1]), date.fromisoformat(point[2])
    return (0 <= (end - closing).days <= 6 and 0 <= (end - timedelta(days=7) - opening).days <= 6
            and 4 <= (closing - opening).days <= 10)


def instrument_breakdown(sleeves, labels, dates, matrix, split, annual_target, annual_cash, proxy):
    """Same temporal split and exact basket membership as the optimisation."""
    blocks = []
    for column, bucket in enumerate(BUCKETS):
        totals = [sum(value for value, points in sleeves[bucket]
                      if bucket == 'Cash' or (weekly_point(points, target) if proxy else target in points)) for target in dates]
        instruments = []
        for value, points in sleeves[bucket]:
            values, weights = [[], []], [[], []]
            observed_periods = []
            for index, target in enumerate(dates):
                segment = int(index >= split)
                included = bucket == 'Cash' or (weekly_point(points, target) if proxy else target in points)
                weights[segment].append(value / totals[index] * 100 if included and totals[index] else 0)
                if included:
                    values[segment].append(matrix[index][column] if bucket == 'Cash' else points[target][0])
                    observed_periods.append(((date.fromisoformat(target) - timedelta(days=7)).isoformat(), target)
                                            if bucket == 'Cash' else points[target][1:])
            overall = metrics(values[0] + values[1], annual_target, annual_cash)
            if overall:
                overall = {**overall, 'start': min(period[0] for period in observed_periods),
                           'end': max(period[1] for period in observed_periods)}
            instruments.append({'name': labels[id(points)],
                                'overall': overall,
                                'training_weight_pct': sum(weights[0]) / len(weights[0]) if weights[0] else 0,
                                'test_weight_pct': sum(weights[1]) / len(weights[1]) if weights[1] else 0,
                                'training': metrics(values[0], annual_target, annual_cash),
                                'validation': metrics(values[1], annual_target, annual_cash)})
        defined = [row['overall']['sharpe'] for row in instruments if row['overall'] and row['overall']['sharpe'] is not None]
        periods = [row['overall'] for row in instruments if row['overall']]
        blocks.append({'bucket': bucket, 'instruments': instruments,
                       'average_sharpe': sum(defined) / len(defined) if defined else None,
                       'period': {'start': min(row['start'] for row in periods), 'end': max(row['end'] for row in periods)} if periods else None,
                       'training': metrics([row[column] for row in matrix[:split]], annual_target, annual_cash) if sleeves[bucket] or bucket == 'Cash' else None,
                       'validation': metrics([row[column] for row in matrix[split:]], annual_target, annual_cash) if sleeves[bucket] or bucket == 'Cash' else None})
    return blocks


def period_range(dates, sleeves, proxy):
    if not dates:
        return None
    opening = [points[dates[0]][1] for bucket in BUCKETS[:3] for _, points in sleeves[bucket]
               if (weekly_point(points, dates[0]) if proxy else dates[0] in points)]
    closing = [points[dates[-1]][2] for bucket in BUCKETS[:3] for _, points in sleeves[bucket]
               if (weekly_point(points, dates[-1]) if proxy else dates[-1] in points)]
    return {'start': min(opening) if opening else (date.fromisoformat(dates[0]) - timedelta(days=7)).isoformat(),
            'end': max(closing) if closing else dates[-1]}


def weekly_proxy(sleeves):
    """Reweight available instruments per class/week; missing returns never become zero."""
    union = sorted({target for bucket in BUCKETS[:3] for _, points in sleeves[bucket] for target in points})
    dates, rows, membership, coverage, skipped = [], [], {}, {}, []
    for target in union:
        eligible = {}
        for bucket in BUCKETS[:3]:
            eligible[bucket] = []
            for value, points in sleeves[bucket]:
                if weekly_point(points, target):
                    eligible[bucket].append((value, points))
        if any(sleeves[bucket] and not eligible[bucket] for bucket in BUCKETS[:3]):
            skipped.append(target)
            continue
        dates.append(target)
        row = []
        for bucket in BUCKETS[:3]:
            used = sum(value for value, _ in eligible[bucket])
            total = sum(value for value, _ in sleeves[bucket])
            row.append(sum(value * points[target][0] for value, points in eligible[bucket]) / used if used else 0)
            coverage.setdefault(bucket, []).append(used / total * 100 if total else None)
            for _, points in eligible[bucket]:
                membership[id(points)] = membership.get(id(points), 0) + 1
        rows.append(row)
    return dates, rows, membership, coverage, skipped


def policy(account, profile=None):
    from routers._airs_allocation_bands import load_bands
    from routers._airs_portfolio_variant import VARIANTS, portfolio_variant
    all_cells = load_bands()
    chosen = profile or portfolio_variant(account)
    if chosen not in VARIANTS:
        return {'profiles': list(VARIANTS), 'profile': None, 'bounds': [], 'all_bounds': all_cells, 'reason': 'Choose a risk profile first.'}
    cells = [cell for cell in all_cells if cell['variant'] == chosen]
    return {'profiles': list(VARIANTS), 'profile': chosen, 'bounds': cells, 'all_bounds': all_cells}


def run(account, start_date, annual_target=2.0, annual_cash=0.0, available_history=False, profile=None, objective='sharpe'):
    selected_policy = policy(account, profile)
    cells = {row['bucket']: row for row in selected_policy['bounds']}
    if any(bucket not in cells or any(cells[bucket].get(field) is None for field in ('min_pct', 'max_pct')) for bucket in BUCKETS):
        return {'available': False, 'reason': selected_policy.get('reason', 'Configure all profile minimum and maximum weights first.'), **selected_policy}
    bounds = {bucket: (cells[bucket]['min_pct'], cells[bucket]['max_pct']) for bucket in BUCKETS}
    if any(not (isfinite(lo) and isfinite(hi) and 0 <= lo <= hi <= 100) for lo, hi in bounds.values()):
        return {'available': False, 'reason': 'Profile bounds are invalid. Update the allocation bands.', **selected_policy}
    if sum(lo for lo, _ in bounds.values()) > 100 or sum(hi for _, hi in bounds.values()) < 100:
        return {'available': False, 'reason': 'Profile bounds cannot sum to 100%. Update the allocation bands.', **selected_policy}
    result = _run(account, start_date, annual_target, annual_cash, available_history, bounds, objective)
    return {**result, **selected_policy}


def _run(account, start_date, annual_target=2.0, annual_cash=0.0, available_history=False, bounds=None, objective='sharpe'):
    from routers import _airs_instrument_returns as history
    from routers._airs_holding_isin import resolve_account_isins
    from routers._airs_portfolio_analysis import classify_account_holdings
    from routers._benchmark_index import _fx_to_eur, _rate
    from collections import defaultdict
    from bisect import bisect_right
    from asset_pipeline.fx import SUBUNIT

    through, snapshots = history._snapshots(account)
    options, _, aliases, named = history._catalogue(snapshots)
    rows = resolve_account_isins(account, freshen=False, price_checks=False).get('rows') or []
    rows = classify_account_holdings(account, rows)
    positive = [row for row in rows if float(row.get('current_value_eur') or 0) > 0]
    if any(float(row.get('current_value_eur') or 0) < 0 for row in rows):
        return {'available': False, 'reason': 'Short positions or negative cash require a separate leverage model.'}
    if not positive:
        return {'available': False, 'reason': 'No current AIRS holdings with positive EUR values are saved.'}
    individual = defaultdict(list)
    for row in history._pages(history.supabase.table(history.TABLE).select('instrument_id,target_date,as_of_date,holdings,status')
                             .eq('portefeuille', account).lte('target_date', through.isoformat())
                             .order('target_date').order('instrument_id')):
        individual[aliases.get(row['instrument_id'], row['instrument_id'])].append(row)
    lookup = {(option['isin'], option['currency']): option for option in options}
    fx = _fx_to_eur({row.get('currency') for row in positive if row.get('currency') and row.get('currency') != 'EUR'},
                    (start_date - timedelta(days=14)).isoformat(), through.isoformat())
    fx_dates = {currency: sorted(values) for currency, values in fx.items()}

    def rate(currency, when):
        if currency == 'EUR':
            return 1.0
        base, _ = SUBUNIT.get(currency, (currency, 1))
        days = fx_dates.get(base, [])
        index = bisect_right(days, when) - 1
        if index < 0 or (date.fromisoformat(when) - date.fromisoformat(days[index])).days > 7:
            return None
        return _rate(fx, currency, when)
    sleeves = defaultdict(list)
    labels = {}
    issues = []
    for row in positive:
        bucket = row.get('bucket')
        if bucket not in BUCKETS:
            issues.append(f"{row.get('holding_name')}: asset class is not classified.")
            continue
        value = float(row['current_value_eur'])
        if bucket == 'Cash':
            points = {}
            sleeves[bucket].append((value, points))
            labels[id(points)] = row.get('holding_name') or 'Cash'
            continue
        option = lookup.get((row.get('isin'), row.get('currency')))
        if not option:
            if not available_history:
                issues.append(f"{row.get('holding_name')}: no saved weekly instrument history.")
            points = {}
            sleeves[bucket].append((value, points))
            labels[id(points)] = row.get('holding_name') or row.get('isin') or 'Unknown instrument'
            continue
        instrument = history._with_aliases(option, named)
        series = history._series(account, through, snapshots, individual[option['id']], instrument, rolling=True)
        points = {}
        for period, cell in zip(series['periods'], series['selected']['weeks']):
            opening, closing = period.get('from_date'), period.get('as_of_date')
            if (cell['status'] != 'priced' or not opening or not closing or opening < start_date.isoformat()
                    or not cell.get('from_price') or not cell.get('price')):
                continue
            a, b = rate(option['currency'], opening), rate(option['currency'], closing)
            if a and b:
                value_return = cell['price'] / cell['from_price'] * a / b - 1
                if isfinite(value_return) and value_return > -1:
                    points[period['target_date']] = (value_return, opening, closing)
        sleeves[bucket].append((value, points))
        labels[id(points)] = row.get('holding_name') or option['name']
    if issues:
        return {'available': False, 'reason': 'Some current instruments cannot be included. Fetch all histories or fix classifications first.', 'issues': issues}
    risky = [points for bucket in BUCKETS[:3] for _, points in sleeves[bucket]]
    if not risky:
        return {'available': False, 'reason': 'No non-cash instruments have saved history.'}
    common = set.intersection(*(set(points) for points in risky))
    common = {target for target in common if len({points[target][1:] for points in risky}) == 1}
    # Never compress missing weeks into a fictitious continuous investment path.
    dates = sorted(common)
    continuous = []
    for target in dates:
        if continuous and date.fromisoformat(target) - date.fromisoformat(continuous[-1]) != timedelta(days=7):
            continuous = []
        continuous.append(target)
    dates = continuous
    totals = [sum(value for value, _ in sleeves[bucket]) for bucket in BUCKETS]
    selected = {bucket: sleeves[bucket] for bucket in BUCKETS[:3]}
    proxy = available_history
    dynamic_rows = None
    skipped = []
    weekly_coverage = {}
    membership = {}
    if proxy:
        dates, dynamic_rows, membership, weekly_coverage, skipped = weekly_proxy(sleeves)
        selected = {bucket: [(value, points) for value, points in sleeves[bucket] if id(points) in membership] for bucket in BUCKETS[:3]}
        if len(dates) < 4:
            return {'available': False, 'reason': f'Only {len(dates)} usable weekly baskets represent every held asset class; at least 4 needed for an exploratory calculation.',
                    'issues': [f"{bucket} · {labels[id(points)]}: {len(points)} usable EUR weeks"
                               for bucket in BUCKETS[:3] for _, points in sleeves[bucket]]}
    coverage = []
    for i, bucket in enumerate(BUCKETS[:3]):
        selected_ids = {id(points) for _, points in selected[bucket]}
        covered = sum(value for value, _ in selected[bucket])
        samples = [value for value in weekly_coverage.get(bucket, []) if value is not None]
        coverage.append({'bucket': bucket, 'coverage_pct': sum(samples) / len(samples) if samples else covered / totals[i] * 100 if totals[i] else None,
                         'minimum_coverage_pct': min(samples) if samples else None,
                         'included': [labels[id(points)] for _, points in selected[bucket]],
                         'excluded': [labels[id(points)] for _, points in sleeves[bucket] if id(points) not in selected_ids]})
    total = sum(totals)
    current = [value / total for value in totals]
    cash = (1 + annual_cash / 100) ** (1 / 52) - 1
    represented = {bucket: sum(value for value, _ in selected[bucket]) for bucket in BUCKETS[:3]}
    matrix = ([row + [cash] for row in dynamic_rows] if dynamic_rows is not None else
              [[sum(value * points[target][0] for value, points in selected[bucket]) / represented[bucket] if represented[bucket] else 0
                for bucket in BUCKETS[:3]] + [cash] for target in dates])
    discontinuous = any(date.fromisoformat(b) - date.fromisoformat(a) != timedelta(days=7) for a, b in zip(dates, dates[1:]))
    result = search(matrix, current, annual_target, min_weeks=4 if proxy else 52, bounds=bounds, discontinuous=discontinuous,
                    objective=objective, annual_risk_free=annual_cash)
    breakdown = instrument_breakdown(sleeves, labels, dates, matrix, result['training_weeks'], annual_target, annual_cash, proxy) if result['available'] else []
    split = result.get('training_weeks', 0)
    return {**result, 'from': dates[0] if dates else None, 'through': dates[-1] if dates else None,
            'training_period': period_range(dates[:split], sleeves, proxy) if result['available'] else None,
            'test_period': period_range(dates[split:], sleeves, proxy) if result['available'] else None,
            'instrument_breakdown': breakdown,
            'annual_target_pct': annual_target, 'annual_cash_pct': annual_cash,
            'proxy_history': proxy, 'coverage': coverage, 'skipped_weeks': skipped,
            'method': '5% grid; long-only; weekly rebalancing; available instruments reweighted within each class per weekly timestep in proxy mode; costs excluded.',
            'limitations': ['Historical EUR price-return research; dividends and bond coupons are excluded.',
                            *(['Proxy composition and coverage vary by week; missing returns are excluded and available instruments reweighted.',
                               'Quotes are grouped into weekly timesteps; actual valuation dates can differ within the week.',
                               'Weeks with a wholly unavailable held class are skipped; gapped samples cannot pass the validation check.'] if proxy else []),
                            'Unadjusted stock splits or stale quotes can distort the results; review unusual price moves.',
                            'Current holdings are tested retrospectively; survivorship and selection bias remain.',
                            'Cash uses the stated constant annual return assumption.',
                            'Highest training Sortino is not a validated long-term strategic allocation.']}
