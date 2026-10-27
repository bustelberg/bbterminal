"""One EUR simulation for CAGR, active return and monthly tracking error.

Use a fixed cohort of direct equities with complete Yahoo history. Weights drift between
annual anniversary rebalances. Exclusions and the covered share of the original book are explicit.
"""
from bisect import bisect_right
from calendar import monthrange
from datetime import date
import math
import re
import time
import unicodedata

from routers._tracking_error import _risk_prices
from routers._risk_history_window import history_years, history_marks


def _month_end(year: int, month: int) -> str:
    return date(year, month, monthrange(year, month)[1]).isoformat()


def _simulation_dates(years: int, today: date) -> list[str]:
    last_month = today.year * 12 + today.month - 2
    result = []
    for index in range(last_month - years * 12, last_month + 1):
        year, month = divmod(index, 12)
        result.append(_month_end(year, month + 1))
    return result


def _yahoo_mappings(isins: list[str]) -> dict[str, dict]:
    from deps import IN_CHUNK_SIZE, supabase
    from common.pg import load_rows_via_copy
    columns = 'isin,analysis_id,yahoo_symbol,asset_class,name,currency'
    rows = load_rows_via_copy('asset_execution', columns, 'isin', isins)
    if rows is None:
        rows = []
        for offset in range(0, len(isins), IN_CHUNK_SIZE):
            rows += supabase.table('asset_execution').select(columns).in_('isin', isins[offset:offset + IN_CHUNK_SIZE]).execute().data or []
    return {row['isin']: row for row in rows}


def _equity_name_key(name: str | None) -> str:
    """Exact issuer spelling after legal/share-class suffixes and reviewed historical names."""
    from routers._active_share import _issuer_key
    text = unicodedata.normalize('NFKD', name or '').encode('ascii', 'ignore').decode().lower()
    text = text.replace('`', "'")
    text = re.sub(r'\bsociete en commandite par actions\b', '', text)
    text = _issuer_key(text)
    text = _issuer_key(re.sub(r'[\s,.]+[abc]\.?$', '', text))
    text = {'adobe systems': 'adobe', 'hermes internationale': 'hermes international'}.get(text, text)
    return ''.join(re.findall(r'[a-z0-9]+', text))


def simulate(inputs: dict, years: int = 5, today: date | None = None,
             start_date: str | None = None) -> dict:
    """Pure calculation over daily EUR marks; only complete calendar months enter TE."""
    today = today or date.today()
    series, weights, names = inputs['series'], inputs['weight'], inputs['names']
    legs = inputs['benchmark_component_weights']
    leg_isins = inputs['benchmark_component_isins']
    # Last fully completed month, never the unfinished current month.
    dates = _simulation_dates(years, today)
    if start_date:
        latest = [day for name, isin in leg_isins.items() for day, price in series.get(isin, [])
                  if day <= today.isoformat() and price is not None and price > 0]
        dates = history_marks(start_date, max(latest) if latest else today.isoformat())
    requested_start, requested_end = start_date or dates[0], dates[-1]
    required = {**{key: {'name': names.get(key, key), 'weight_pct': weight * 100, 'kind': 'holding'}
                   for key, weight in weights.items()},
                **{f'benchmark:{name}': {'name': 'AGGH.AS' if name == 'BONDS' else name,
                    'weight_pct': weight, 'kind': 'benchmark', 'isin': leg_isins[name]}
                   for name, weight in legs.items()}}
    marks, issues = {}, []
    for key, info in required.items():
        isin = info.get('isin', key)
        rows = sorted((day, float(value)) for day, value in series.get(isin, [])
                      if value is not None and math.isfinite(value) and value > 0)
        row_dates = [day for day, _ in rows]
        values = {}
        for day in dates:
            index = bisect_right(row_dates, day) - 1
            fresh = index >= 0 and (date.fromisoformat(day) - date.fromisoformat(row_dates[index])).days <= 7
            if fresh:
                values[day] = rows[index][1]
        marks[key] = values
        missing = len(dates) - len(values)
        if missing:
            issues.append({**info, 'isin': isin, 'first_price_date': row_dates[0] if rows else None,
                           'last_price_date': row_dates[-1] if rows else None,
                           'missing_months': missing,
                           'reason': 'No usable EUR price history' if not rows else 'Missing or stale month-end EUR prices'})
    # Keep the latest uninterrupted run with EVERY holding and benchmark priced.
    common, run = [], []
    for day in dates:
        if all(day in values for values in marks.values()):
            run.append(day)
            common = run.copy()
        else:
            run = []
    base = {'available': False, 'benchmark': inputs['benchmark_label'], 'frequency': 'monthly',
            'periods_per_year': 12, 'years': years, 'observations': max(0, len(common) - 1),
            'requested_window_from': requested_start, 'requested_window_to': requested_end,
            'window_from': start_date or (common[0] if common else None), 'window_to': common[-1] if common else None,
            'history_limited': len(common) != len(dates), 'history_issues': issues,
            'simulation_method': 'Annual anniversary rebalancing to current weights; completed monthly EUR returns',
            'preliminary': False, 'total_holdings': len(weights), 'priced_holdings': sum(bool(marks[key]) for key in weights)}
    if len(common) < 13:
        return {**base, 'reason': f'Only {max(0, len(common) - 1)} complete monthly returns cover every holding and benchmark. At least 12 are required; no daily fallback or holdings substitution is used.'}
    if start_date and len(common) != len(dates):
        return {**base, 'reason': f'Complete genuine price history from {start_date} is required; the requested window is not shortened.'}
    preserve_weights = inputs.get('preserve_weights', False)
    target = {key: weight / (1 if preserve_weights else sum(weights.values())) for key, weight in weights.items()}
    benchmark_target = {f'benchmark:{name}': weight / (100 if preserve_weights else sum(legs.values())) for name, weight in legs.items()}

    def curve(allocation):
        values = dict(allocation)
        remainder = max(0.0, 1 - sum(allocation.values()))
        wealth = 1.0
        returns, rebalances = [], []
        for index, (prev, cur) in enumerate(zip(common, common[1:]), 1):
            values = {key: value * marks[key][cur] / marks[key][prev] for key, value in values.items()}
            new_wealth = sum(values.values()) + remainder
            returns.append(new_wealth / wealth - 1)
            wealth = new_wealth
            if index % 12 == 0 and cur != common[-1]:
                values = {key: wealth * weight for key, weight in allocation.items()}
                remainder = wealth * max(0.0, 1 - sum(allocation.values()))
                rebalances.append(cur)
        return wealth, returns, rebalances

    port, port_returns, rebalances = curve(target)
    bench, bench_returns, _ = curve(benchmark_target)
    active = [p - b for p, b in zip(port_returns, bench_returns)]
    completed = [(day, value) for day, value in zip(common[1:], active)
                 if not start_date or day == _month_end(date.fromisoformat(day).year, date.fromisoformat(day).month)]
    count = len(completed)
    if count < 12:
        return {**base, 'reason': 'At least 12 completed monthly returns are required.'}
    mean = sum(value for _, value in completed) / count
    sample_std = math.sqrt(sum((value - mean) ** 2 for _, value in completed) / (count - 1))
    te = sample_std * math.sqrt(12) * 100
    elapsed = (date.fromisoformat(common[-1]) - date.fromisoformat(common[0])).days / 365.25
    factor = 1 / elapsed
    pcagr, bcagr = (port ** factor - 1) * 100, (bench ** factor - 1) * 100
    components = [{'name': 'AGGH.AS' if name == 'BONDS' else name, 'isin': leg_isins[name],
                   'weight_pct': weight, 'start_date': common[0], 'end_date': common[-1],
                   'start_price_eur': marks[f'benchmark:{name}'][common[0]],
                   'end_price_eur': marks[f'benchmark:{name}'][common[-1]],
                   'window_return_pct': (marks[f'benchmark:{name}'][common[-1]] / marks[f'benchmark:{name}'][common[0]] - 1) * 100,
                   'annualized_return_pct': ((marks[f'benchmark:{name}'][common[-1]] / marks[f'benchmark:{name}'][common[0]]) ** factor - 1) * 100}
                  for name, weight in legs.items()]
    return {**base, 'available': True, 'reason': None, 'rebalance_dates': rebalances,
            'observations': count, 'tracking_window_to': completed[-1][0],
            'portfolio_return_total_pct': (port - 1) * 100, 'benchmark_return_total_pct': (bench - 1) * 100,
            'portfolio_return_ann_pct': pcagr, 'benchmark_return_ann_pct': bcagr,
            'active_return_ann_pct': pcagr - bcagr, 'tracking_error_pct': te,
            'mean_active_per_period_pct': mean * 100, 'active_return_std_per_period_pct': sample_std * 100,
            'annualization_factor': factor, 'information_ratio': (pcagr - bcagr) / te if te > 1e-9 else None,
            'avg_weight_covered_pct': 100, 'benchmark_components': components,
            'benchmark_history': {'components': components, 'cagr_pct': bcagr,
                                  'start_date': start_date or common[0], 'end_date': common[-1], 'complete': not base['history_limited']},
            'historical_comparison': {'available': True, 'source': base['simulation_method'],
                'start_date': start_date or common[0], 'end_date': common[-1], 'years_elapsed': elapsed,
                'total_return_pct': (port - 1) * 100, 'portfolio_cagr_pct': pcagr, 'benchmark_cagr_pct': bcagr,
                'active_return_pp': pcagr - bcagr, 'min_weight_covered_pct': 100, 'avg_weight_covered_pct': 100,
                'holdings_history': [{'isin': key, 'name': names.get(key, key), 'weight_pct': value * 100,
                                      'first_used': common[0]} for key, value in target.items()]},
            'monthly_returns': [{'date': day, 'portfolio_return_pct': p * 100, 'benchmark_return_pct': b * 100,
                                 'active_return_pp': (p - b) * 100}
                                for day, p, b in zip(common[1:], port_returns, bench_returns)]}


def compute_simulation(holdings: list[dict], benchmark: str, years: int = 5,
                       benchmark_weights: dict | None = None,
                       start_date: str | None = None) -> dict:
    from routers._airs_portfolio_analysis import _daily_eur, _instrument_names_match
    from routers._asset_financials import _BENCHMARK_RISK_ETF
    from routers._tracking_error import is_direct_equity_stock

    try:
        years = history_years(start_date, years)
    except ValueError as exc:
        return {'available': False, 'benchmark': benchmark, 'reason': str(exc)}
    if not 1 <= years <= 30:
        return {'available': False, 'benchmark': benchmark, 'reason': 'The simulation window must be between 1 and 20 years.'}
    policy = benchmark_weights or {'Stocks': 100, 'Bonds': 0}
    stocks, bonds = float(policy.get('Stocks', 0)), float(policy.get('Bonds', 0))
    if not all(math.isfinite(w) for w in (stocks, bonds)) or min(stocks, bonds) < 0 or abs(stocks + bonds - 100) > .01:
        return {'available': False, 'benchmark': benchmark, 'reason': 'Benchmark weights must total 100%.'}
    equity_only = bool(policy.get('EquityOnly'))
    if equity_only:
        bonds = 0
    legs = {'ACWI': stocks, 'BONDS': bonds} if benchmark_weights else {benchmark.upper(): 100}
    legs = {name: weight for name, weight in legs.items() if weight > 0}
    if any(name not in _BENCHMARK_RISK_ETF for name in legs):
        return {'available': False, 'benchmark': benchmark, 'reason': 'No approved benchmark price source.'}

    excluded, candidates, names = [], {}, {}
    total = 0.0
    non_equity_name = re.compile(r'\b(etf|etp|etn|ucits|fund|fonds|certificate|certificaten|index)\b', re.I)
    for holding in holdings:
        weight = float(holding.get('weight_pct') or 0)
        if not math.isfinite(weight) or weight < 0:
            return {'available': False, 'benchmark': benchmark, 'reason': 'The simulation requires finite, non-negative weights.'}
        if weight == 0:
            continue
        total += weight
        isin = (holding.get('isin') or '').strip().upper()
        name = holding.get('name') or isin or 'Holding'
        if not is_direct_equity_stock(holding) or non_equity_name.search(name) or not isin:
            excluded.append({'isin': isin or None, 'name': name, 'weight_pct': weight,
                             'reason': 'Excluded: ETF, fund, bond, certificate, cash or non-equity holding'})
            continue
        candidates[isin] = candidates.get(isin, 0) + weight
        names[isin] = name
    ids = {name: _BENCHMARK_RISK_ETF[name] for name in legs}
    dates = _simulation_dates(years, date.today())
    if start_date:
        dates = history_marks(start_date, date.today().isoformat())
    issues = []
    base = {'available': False, 'benchmark': ' + '.join(f"{weight:g}% {'AGGH.AS' if name == 'BONDS' else name}" for name, weight in legs.items()),
            'frequency': 'monthly', 'simulation_scope': 'Yahoo-only direct equities',
            'requested_window_from': dates[0], 'requested_window_to': dates[-1],
            'covered_portfolio_pct': 0, 'excluded_holdings': excluded, 'history_issues': issues}
    try:
        mappings = _yahoo_mappings(sorted(set(candidates) | set(ids.values())))
        verified = {}
        for isin, weight in candidates.items():
            source = mappings.get(isin) or {}
            symbol = source.get('yahoo_symbol') or ''
            if source and (source.get('asset_class') not in (None, 'equity')
                           or non_equity_name.search(source.get('name') or '')):
                excluded.append({'isin': isin, 'name': names[isin], 'weight_pct': weight,
                                 'reason': 'Excluded: Yahoo instrument is a fund or non-equity'})
                continue
            same_issuer = bool(_equity_name_key(names[isin])) and (
                _equity_name_key(names[isin]) == _equity_name_key(source.get('name'))
                or _instrument_names_match(names[isin], source.get('name') or ''))
            if (not symbol or ':' in symbol or not source.get('analysis_id')
                    or source.get('asset_class') != 'equity'
                    or non_equity_name.search(source.get('name') or '') or not same_issuer):
                excluded.append({'isin': isin, 'name': names[isin], 'weight_pct': weight,
                                 'reason': 'No verified Yahoo equity mapping'})
                issues.append({'isin': isin, 'name': names[isin], 'kind': 'holding', 'weight_pct': weight,
                               'missing_months': len(dates), 'reason': 'No verified Yahoo equity mapping'})
            else:
                verified[isin] = weight
        for name, isin in ids.items():
            source = mappings.get(isin) or {}
            if not source.get('yahoo_symbol') or not source.get('analysis_id'):
                return {**base, 'reason': f"Yahoo history is unavailable for benchmark {name}.",
                        'history_issues': issues + [{'isin': isin, 'name': name, 'kind': 'benchmark',
                        'weight_pct': legs[name], 'missing_months': len(dates), 'reason': 'No Yahoo benchmark mapping'}]}
        # No NAV fallback, synthetic cash or AIRS valuation prices enter this load.
        series = dict(_risk_prices(tuple(sorted(set(verified) | set(ids.values()))), years + 1,
                                   date.today().isoformat(), int(time.monotonic() // 300), _daily_eur))
        if start_date:
            latest = [day for isin in ids.values() for day, price in series.get(isin, [])
                      if day <= date.today().isoformat() and price is not None and price > 0]
            dates = history_marks(start_date, max(latest) if latest else date.today().isoformat())
            base['requested_window_from'], base['requested_window_to'] = start_date, dates[-1]
        eligible = {}
        for isin, weight in verified.items():
            rows = sorted(series.get(isin) or [])
            row_dates = [day for day, _ in rows]
            missing = 0
            for day in dates:
                index = bisect_right(row_dates, day) - 1
                if (index < 0 or (date.fromisoformat(day) - date.fromisoformat(row_dates[index])).days > 7
                        or not math.isfinite(rows[index][1]) or rows[index][1] <= 0):
                    missing += 1
            if missing:
                excluded.append({'isin': isin, 'name': names[isin], 'weight_pct': weight,
                                 'reason': 'Incomplete Yahoo price/FX history for the requested period'})
                issues.append({'isin': isin, 'name': names[isin], 'kind': 'holding', 'weight_pct': weight,
                               'first_price_date': row_dates[0] if rows else None,
                               'last_price_date': row_dates[-1] if rows else None,
                               'missing_months': missing, 'reason': 'Incomplete Yahoo price/FX history for the requested period'})
            else:
                eligible[isin] = weight
        if not eligible:
            return {**base, 'reason': 'No direct equities have verified Yahoo prices throughout the requested period.'}
        included = sum(eligible.values())
        result = simulate({'series': series, 'weight': {isin: weight / (100 if equity_only else included) for isin, weight in eligible.items()},
                           'preserve_weights': equity_only,
                           'names': names, 'benchmark_component_weights': legs,
                           'benchmark_component_isins': ids, 'benchmark_label': base['benchmark']}, years,
                          start_date=start_date)
        return {**result, 'simulation_scope': base['simulation_scope'], 'excluded_holdings': excluded,
                'benchmark_isin': ', '.join(f"{'AGGH.AS' if name == 'BONDS' else name}: {isin}" for name, isin in ids.items()),
                'covered_portfolio_pct': included if equity_only else (included / total * 100 if total else 0),
                'history_issues': [*issues, *result.get('history_issues', [])]}
    except Exception as exc:
        return {**base, 'reason': f'Yahoo equity history could not be loaded ({type(exc).__name__}).'}
