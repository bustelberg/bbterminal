"""Toppenberg basket views: AIRS allocation weights and saved Leonteq source data."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
import logging
from math import fsum, isfinite
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from deps import supabase
from leonteq import management as leonteq
from management_portfolios import MANAGEMENT_GROUPS, key
from postgrest.exceptions import APIError


ALTERNATIVES = {'display_name': 'AlternatievenTopSelectie', 'leonteq_name': 'AlternatievenTopSelectie'}
CASH = 'Cash'
ALLOCATION_BUCKETS = ('Equity', 'Bonds', 'Alternatives', CASH)
ALTERNATIVE_SLEEVES = {ALTERNATIVES['display_name'], 'VastgoedTopSelectie'}
SLEEVE_BUCKETS = {'ObligatieTopSelectie': 'Bonds', **dict.fromkeys(ALTERNATIVE_SLEEVES, 'Alternatives')}
LEONTEQ_CATEGORIES = {'share': 'Equity', 'bond': 'Bonds', 'option': 'Alternatives'}
log = logging.getLogger(__name__)


def _position_bucket(sleeve, category, fallback):
    category = str(category or '').strip().casefold()
    if category == 'cash':
        return CASH
    if sleeve in ALTERNATIVE_SLEEVES:
        return 'Alternatives'
    if category == 'fund' and sleeve == 'ObligatieTopSelectie':
        return 'Bonds'
    return LEONTEQ_CATEGORIES.get(category) or fallback()


def configured_entries():
    return [*leonteq.entries(), ALTERNATIVES]


def _sleeve_name(name):
    if key(name) == 'effectenrekening':
        return CASH
    if key(name) == key(ALTERNATIVES['display_name']):
        return ALTERNATIVES['display_name']
    entry = leonteq.entry_for_identity(name)
    return entry['display_name'] if entry else name


def _weights(accounts):
    if not accounts:
        return {}
    rows = (supabase.table('airs_holding').select('portefeuille,as_of_date,holding_name,weight')
            .in_('portefeuille', list(accounts)).in_('as_of_date', sorted(set(accounts.values())))
            .execute().data or [])
    weights = defaultdict(lambda: defaultdict(float))
    for row in rows:
        account = row['portefeuille']
        if row['as_of_date'] != accounts.get(account):
            continue
        weight = float(row.get('weight') or 0)
        if not isfinite(weight) or weight <= 0:
            continue
        weights[account][_sleeve_name(row['holding_name'])] += weight
    return weights


def _return_window(weights, sources):
    """Fixed saved weights applied to each sleeve's return from a common start."""
    active = {name: weight for name, weight in weights.items() if name != CASH}
    total = sum(weights.values())
    marks = {}
    for name in active:
        source = leonteq.return_windows.for_monthly_report(sources.get(name, {}))
        marks[name] = {point['date']: 1 + point['cum_pct'] / 100 for point in (source or {}).get('returns', [])}
    missing = [name for name, points in marks.items() if len(points) < 2]
    covered = sum(weight for name, weight in active.items() if name not in missing)
    if total:
        covered_pct = 100 * covered / total
    else:
        covered_pct = 0
    if not active or missing or abs(total - 1) > .02:
        return None, missing, covered_pct
    days = sorted(set.intersection(*(set(points) for points in marks.values())))
    if len(days) < 2 or any(points[days[0]] <= 0 for points in marks.values()):
        return None, missing, covered_pct
    # Cash has zero return. Keep that assumption separate from sourced fund returns.
    points = [{'date': day, 'cum_pct': 100 * sum(
        active[name] * (values[day] / values[days[0]] - 1) for name, values in marks.items()) / total}
        for day in days]
    sleeve_returns = {name: 100 * (values[days[-1]] / values[days[0]] - 1)
                      for name, values in marks.items()}
    if CASH in weights:
        sleeve_returns[CASH] = 0
    return {'from': days[0], 'to': days[-1], 'ytd_pct': points[-1]['cum_pct'],
            'returns': points, 'sleeve_returns': sleeve_returns, 'sleeve_marks': marks}, [], covered_pct


def _sleeve_holdings(weights, window):
    total = sum(weights.values())
    sleeve_returns = window['sleeve_returns'] if window else {}
    rows = []
    for name, weight in sorted(weights.items(), key=lambda item: (-item[1], item[0])):
        sleeve_return = sleeve_returns.get(name)
        rows.append({'name': name, 'weight_pct': 100 * weight,
                     'bucket': SLEEVE_BUCKETS.get(name, CASH if name == CASH else 'Equity'),
                     'source': 'leonteq',
                     'return_pct': sleeve_return,
                     'contribution_pct': weight / total * sleeve_return if sleeve_return is not None else None})
    return rows


def _return_start_details(window):
    marks = window['sleeve_marks']
    first_dates = {name: min(points) for name, points in marks.items()}
    latest_first = max(first_dates.values())
    anchor = window['from']
    limiting_sleeves = [{'name': name, 'source': 'Leonteq'}
                        for name in sorted(first_dates) if first_dates[name] == latest_first]
    previous = max((day for points in marks.values() for day in points if latest_first <= day < anchor),
                   default=None)
    return {'first_possible_date': latest_first, 'limiting_sleeves': limiting_sleeves,
            'previous_observation_date': previous,
            'missing_sleeves': [name for name in sorted(marks) if previous and previous not in marks[name]]}


def _position_identity(row):
    return row.isin or (f'cash:{row.currency}' if row.currency and row.category.casefold() == 'cash' else None)


def _saved_report(source):
    ledger = source.get('accounting') or {}
    report = ledger.get('result') or {}
    return report if ledger.get('success') and report.get('available') else {}


def _aligned_reports(window, sources):
    """Read only saved ledgers and rebase them to the dates used by the portfolio chart."""
    if not window:
        return {}
    from leonteq import accounting, portfolio_cache

    start, end = window['from'], window['to']
    reports = {}
    pending = defaultdict(list)
    for sleeve in window['sleeve_marks']:
        source = sources.get(sleeve, {})
        report = _saved_report(source)
        if (report.get('from'), report.get('to')) == (start, end) and abs(
                report['reported_ytd_pct'] - window['sleeve_returns'][sleeve]) <= .0051:
            reports[sleeve] = report
        elif source.get('leonteq_name'):
            pending[source['leonteq_name']].append(sleeve)
    account_key = portfolio_cache._account_key()
    if not pending or not account_key:
        return reports
    try:
        rows = (supabase.table(accounting.TABLE)
                .select('portfolio_name,valuations:payload->valuations,transactions:payload->transactions,'
                        'cash_history:payload->certificate_cash_history')
                .eq('account_key', account_key).eq('payload->>version', str(accounting.VERSION))
                .in_('portfolio_name', list(pending))
                .filter(f'payload->valuations->{start}', 'not.is', 'null')
                .filter(f'payload->valuations->{end}', 'not.is', 'null').execute().data or [])
    except APIError as exc:
        log.warning('Saved Leonteq period attribution inputs unavailable (%s)', exc.code)
        return reports
    for row in rows:
        for sleeve in pending[row['portfolio_name']]:
            report = _calculate_aligned_report(row, window, sleeve)
            if report:
                reports[sleeve] = report
    return reports


def _calculate_aligned_report(raw, window, sleeve):
    from leonteq import accounting
    from leonteq.portal_api import ApiUnavailable, _number

    valuations = raw.get('valuations') or {}
    records = raw.get('transactions')
    start, end = window['from'], window['to']
    marks = [(day, Decimal(str(value))) for day, value in sorted(window['sleeve_marks'][sleeve].items())
             if start <= day <= end]
    if records is None or any(day not in valuations for day, _ in marks):
        return None
    cash_history = raw.get('cash_history') or {}
    try:
        cash_flows = None
        if cash_history.get('complete'):
            cash_flows = [{'date': flow['dateTime'][:10], 'cash_eur': str(_number(flow['netFlows']))}
                          for certificate in cash_history['certificates'] for flow in certificate['cash_movements']]
        target = window['sleeve_returns'][sleeve]
        try:
            boundaries = [accounting.valuation(valuations[day], day) for day, _ in marks]
            return accounting.calculate(boundaries, records, marks, target, cash_flows=cash_flows)
        except ApiUnavailable:
            if sleeve == ALTERNATIVES['display_name']:
                # Keep full NAV as the denominator while attributing identifiable non-option
                # positions. The excluded option activity stays in the sleeve residual.
                boundaries = [accounting.valuation(valuations[day], day, exclude_options=True)
                              for day, _ in marks]
                records = [record for record in records if str(record.get('category') or '').casefold() != 'option']
                return {**accounting.calculate(boundaries, records, marks, target, allow_estimates=True,
                                               positions_only=True, cash_flows=cash_flows), 'estimated': True}
            boundaries = [accounting.valuation(valuations[day], day) for day, _ in marks]
            return accounting.calculate(boundaries, records, marks, target, allow_estimates=True,
                                        positions_only=True, cash_flows=cash_flows)
    except (ApiUnavailable, KeyError, TypeError, ValueError, ArithmeticError):
        return None


def _position_performance(row, source, sleeve_share, *, report=None, unique=True):
    identity = _position_identity(row)
    report = report or {}
    position = (report.get('positions') or {}).get(identity) if unique else None
    if report and unique and identity == 'cash:EUR':
        position = {'opening': report['cash']['opening'], 'return_pct': 0, 'contribution_pct': 0}
    if position is None:
        # The dated sleeve result can still supply an instrument return on its own dates.
        saved = _saved_report(source)
        position = (saved.get('positions') or {}).get(identity) if unique else None
        if position is not None and position.get('return_pct') is not None:
            return {'own_return_pct': position['return_pct'], 'own_return_source': 'leonteq_sleeve_position',
                    'own_return_from': saved['from'], 'own_return_as_of': saved['to'],
                    'own_return_estimated': bool(saved.get('estimated')), 'contribution_pct': None}
        return {'own_return_pct': row.source_return, 'own_return_source': 'leonteq_average', 'contribution_pct': None}
    basis = report['basis_eur']
    return {'own_return_pct': position.get('return_pct'), 'own_return_source': 'leonteq_sleeve_position',
            'own_return_from': report.get('from'), 'own_return_as_of': report.get('to'),
            'own_return_estimated': bool(report.get('estimated')),
            'weight_start_pct': 100 * sleeve_share * position['opening'] / basis,
            'contribution_pct': sleeve_share * position['contribution_pct']}


def _attribution_summary(weights, window, reports, holdings):
    if not window:
        return {'available': False}
    total = sum(weights.values())
    matched = {(row['via_names'][0], row['accounting_identity']) for row in holdings
               if row['via_names'] and row['contribution_pct'] is not None}
    allocated = defaultdict(float)
    for row in holdings:
        if row['via_names'] and row['contribution_pct'] is not None:
            allocated[row['via_names'][0]] += row['contribution_pct']
    sold, adjustments = [], []
    for sleeve, report in reports.items():
        for identity, item in (report.get('positions') or {}).items():
            if (sleeve, identity) in matched or item.get('contribution_pct') is None:
                continue
            contribution = item['contribution_pct'] * weights[sleeve] / total
            if item.get('isin') and item.get('closing') == 0:
                sold.append({'name': f"{item['name']} · {sleeve}", 'isin': item['isin'], 'closed_out': True,
                             'return_pct': item.get('return_pct'), 'contribution_pct': contribution})
            else:
                continue
            allocated[sleeve] += contribution
    residuals = [row['contribution_pct'] - allocated[row['name']] for row in _sleeve_holdings(weights, window)]
    if any(abs(residual) >= 1e-9 for residual in residuals):
        adjustments.append({'name': 'Unallocated', 'label': 'Total unallocated',
                            'contribution_pct': fsum(residuals)})
    return {'available': True, 'source': 'Leonteq', 'positions': sold,
            'certificate_adjustments': adjustments, 'book_ytd_pct': window['ytd_pct'],
            'book_as_of': window['to'], 'dates_aligned': True, 'comparable': True,
            'estimated': any(report.get('estimated') for report in reports.values()),
            'note': 'Contributions use the common Leonteq return window and normalized AIRS sleeve weights. '
                    'Total unallocated combines sleeve contributions without matched position attribution.'}


def _monthly_performance(weights, window, series):
    if not window:
        return {}
    points = window['returns']
    month_ends = {point['date'][:7]: point for point in points}
    benchmark_marks = {point['date']: 1 + point['cum_pct'] / 100
                       for point in (series or {}).get('benchmark_returns', [])}
    start = points[0]
    total = sum(weights.values())
    months, attribution, benchmarks = [], {}, {}
    for month, end in month_ends.items():
        start_date, end_date = start['date'], end['date']
        basis = 1 + start['cum_pct'] / 100
        if start_date == end_date or basis <= 0:
            start = end
            continue
        period_return = (end['cum_pct'] - start['cum_pct']) / basis
        months.append({'month': month, 'return_pct': period_return, 'as_of': end_date})
        positions = []
        for name, weight in weights.items():
            marks = window['sleeve_marks'].get(name)
            opening, closing = (1, 1) if marks is None else (
                marks[start_date] / marks[window['from']], marks[end_date] / marks[window['from']])
            share = weight / total / basis
            positions.append({'name': name, 'isin': None, 'is_cash': name == CASH, 'status': 'held',
                              'opening_weight_pct': 100 * share * opening,
                              'return_pct': 100 * (closing / opening - 1) if opening > 0 else None,
                              'contribution_pp': 100 * share * (closing - opening),
                              'start_date': start_date, 'end_date': end_date})
        attribution[month] = {'positions': positions, 'opening_weight_date': start_date,
                              'positions_through': end_date, 'positions_return_pct': period_return}
        opening_benchmark = benchmark_marks.get(start_date)
        closing_benchmark = benchmark_marks.get(end_date)
        if opening_benchmark and closing_benchmark is not None:
            benchmarks[month] = {'return_pct': 100 * (closing_benchmark / opening_benchmark - 1),
                                 'start_date': start_date, 'end_date': end_date,
                                 'source': (series or {}).get('benchmark_source') or 'saved_prices'}
        start = end
    return {'performance': {'months': months}, 'monthly_attribution': attribution,
            'monthly_benchmark': benchmarks}


def overlay_overview(rows):
    accounts = {row['dynamic_portefeuille']: row['as_of'] for row in rows
                if row['management_group'] == 'toppenberg' and row.get('as_of')}
    if not any(row['management_group'] == 'toppenberg' for row in rows):
        return rows
    weights = _weights(accounts)
    sources = leonteq._dashboard_snapshots(configured_entries()) if weights else {}
    month = datetime.now(ZoneInfo('Europe/Amsterdam')).strftime('%Y-%m')
    result = []
    for row in rows:
        if row['management_group'] != 'toppenberg':
            result.append(row)
            continue
        account_weights = weights.get(row['dynamic_portefeuille'], {})
        window, missing, covered = _return_window(account_weights, sources)
        mtd = leonteq._month_return(window, month)
        positions = [position for sleeve in account_weights
                     for group in (sources.get(sleeve, {}).get('composition') or {}).get('groups') or []
                     for position in group.get('positions') or []]
        note = ('Calculated from Leonteq returns over a common window using saved AIRS weights. '
                'Cash assumes zero return.')
        if missing:
            note += f" Missing source history for {', '.join(missing)}."
        elif not window:
            note += ' A complete allocation or shared return window is unavailable.'
        result.append({**row, 'ytd_pct': window['ytd_pct'] if window else None,
                       'ytd_from': window['from'] if window else None,
                       'as_of': window['to'] if window else None, 'latest_month_pct': mtd,
                       'periode': window['to'] if window else None, 'returns_fetched_at': None,
                       'holdings': len(positions) or None,
                       'isins': len({p['isin'] for p in positions if p.get('isin')}) if positions else None,
                       'missing_reports': [], 'saved_price_check': None,
                       'months': None, 'price_result_eur': None, 'income_eur': None,
                       'investment_result_eur': None, 'deposits_eur': None, 'withdrawals_eur': None,
                       'begin_value_eur': None, 'end_value_eur': None, 'reconciles': None, 'residual_eur': None,
                       'returns_note': note, 'returns_coverage_pct': covered})
    return result


def analysis(account, benchmark, include_holding_risk=False):
    from fastapi import HTTPException
    from routers._airs_holding_isin import classify_bucket
    from routers import _airs_portfolio_analysis as pa
    from management_portfolios import dashboard_progress_label

    if MANAGEMENT_GROUPS.get(key(account)) != 'toppenberg':
        raise HTTPException(404, 'Unknown Toppenberg portfolio.')
    latest = (supabase.table('airs_holding').select('as_of_date').eq('portefeuille', account)
              .order('as_of_date', desc=True).limit(1).execute().data or [])
    weights = _weights({account: latest[0]['as_of_date']}).get(account, {}) if latest else {}
    if not weights:
        raise HTTPException(409, 'Saved AIRS portfolio weights are unavailable.')
    sources = leonteq._dashboard_snapshots(configured_entries())
    window, missing, coverage = _return_window(weights, sources)
    sleeve_rows = {row['name']: row for row in _sleeve_holdings(weights, window)}
    reports = _aligned_reports(window, sources)
    name = dashboard_progress_label(account).split(' · ')[-1]
    profile = pa._variant_bands(account, name)
    series = leonteq._return_series(window, benchmark, policy_weights=profile['risk_profile_policy'])
    if series and window:
        series['return_start_details'] = _return_start_details(window)
    holdings = []
    missing_holdings = []
    allocation = defaultdict(lambda: {'pct': 0, 'holdings': 0})
    for bucket in ALLOCATION_BUCKETS:
        allocation[bucket] = {'pct': 0, 'holdings': 0}
    for sleeve, weight in weights.items():
        groups = (sources.get(sleeve, {}).get('composition') or {}).get('groups') or []
        if sleeve == CASH or not groups:
            if sleeve != CASH:
                missing_holdings.append(sleeve)
            allocation['Cash' if sleeve == CASH else 'Unclassified']['pct'] += weight * 100
            continue
        for group in groups:
            for position in group['positions']:
                child_weight = leonteq._number(position.get('weight'))
                if child_weight is None or not isfinite(child_weight):
                    continue
                actual_weight = weight * child_weight
                holdings.append(SimpleNamespace(isin=position.get('isin'), name=position['name'], weight=actual_weight,
                                                category=group['name'], sleeve=sleeve, currency=position.get('currency'),
                                                source_return=leonteq._number(position.get('performance'))))
    if not holdings:
        raise HTTPException(409, 'Saved Leonteq holdings are unavailable for the Toppenberg allocation.')
    comparison = series or {}
    benchmark_ytd = comparison.get('benchmark_return_pct')
    returns = {'source': 'leonteq', 'book_available': True, 'ytd_from': window['from'] if window else None,
               'portfolio_as_of': window['to'] if window else None, 'portfolio_ytd_pct': window['ytd_pct'] if window else None,
               'benchmark_ytd_pct': benchmark_ytd,
               'benchmark_source': comparison.get('benchmark_source'),
               'benchmark_as_of': comparison.get('benchmark_as_of'),
               'ytd_excess_pct': window['ytd_pct'] - benchmark_ytd if window and benchmark_ytd is not None else None,
               'book_reason': None if window else f'Shared Leonteq return history unavailable. Coverage {coverage:.1f}%.'}
    result = pa.compute_basket_analysis(holdings, benchmark, name, portfolio_returns=returns)
    grid = pa._grid([row.isin for row in holdings if row.isin])
    codes = pa._country_by_code()
    book_holdings = []
    total_weight = sum(weights.values())
    position_counts = Counter((row.sleeve, _position_identity(row)) for row in holdings)
    if CASH in weights:
        book_holdings.append({'name': CASH, 'isin': None, 'bucket': CASH,
                              'weight_now_pct': weights[CASH] * 100, 'own_return_pct': 0,
                              'weight_start_pct': weights[CASH] / total_weight * 100 if window else None,
                              'contribution_pct': 0 if window else None,
                              'own_return_source': 'leonteq_sleeve', 'via_names': []})
        allocation[CASH]['holdings'] += 1
    for row in holdings:
        instrument = grid.get(row.isin) or {}
        fund = pa._is_fund(instrument, row.name)
        bucket = _position_bucket(row.sleeve, row.category, lambda:
            pa._class_from_categorie(row.category)
            or classify_bucket(row.category, fund, row.isin, row.name, instrument))
        allocation[bucket]['pct'] += row.weight
        allocation[bucket]['holdings'] += 1
        sector, region, _ = pa._buckets(instrument, is_cash=bucket == CASH, isin=row.isin, codes=codes)
        performance = _position_performance(row, sources.get(row.sleeve, {}), weights[row.sleeve] / total_weight,
            report=reports.get(row.sleeve), unique=position_counts[row.sleeve, _position_identity(row)] == 1)
        book_holdings.append({'name': instrument.get('name') or row.name, 'isin': row.isin,
                              'bucket': bucket, 'is_fund': fund, 'sector': sector, 'region': region,
                              'currency': row.currency, 'company_id': instrument.get('company_id'),
                              'accounting_identity': _position_identity(row),
                              'weight_now_pct': row.weight, 'via_names': [row.sleeve],
                              **performance})
    for bucket, stats in allocation.items():
        members = [row for row in book_holdings if row['bucket'] == bucket]
        contributions = [row['contribution_pct'] for row in members if row['contribution_pct'] is not None]
        priced = [row for row in members if (row.get('weight_start_pct') or 0) > 0 and row.get('own_return_pct') is not None]
        basis = sum(row['weight_start_pct'] for row in priced)
        stats['contribution_pct'] = sum(contributions) if contributions else None
        stats['return_pct'] = sum(row['weight_start_pct'] * row['own_return_pct'] for row in priced) / basis if basis > 0 else None
    components = {item['name']: item for item in comparison.get('benchmark_components', [])}
    returns['block_returns'] = [
        {'bucket': label, 'weight_pct': policy_weight, 'portfolio_return_pct': allocation[bucket]['return_pct'],
         'benchmark_return_pct': components.get(label, {}).get('return_pct'),
         'benchmark_ticker': components.get(label, {}).get('ticker'),
         'benchmark_start_date': components.get(label, {}).get('start_date'),
         'benchmark_end_date': components.get(label, {}).get('end_date'),
         'benchmark_start_value': components.get(label, {}).get('start_price'),
         'benchmark_end_value': components.get(label, {}).get('end_price'),
         'benchmark_currency': components.get(label, {}).get('currency'),
         'benchmark_fx_start': components.get(label, {}).get('fx_start'),
         'benchmark_fx_end': components.get(label, {}).get('fx_end')}
        for label, bucket in (('Stocks', 'Equity'), ('Bonds', 'Bonds'))
        if (policy_weight := (profile['risk_profile_policy'] or {}).get(label)) is not None]
    realised = _attribution_summary(weights, window, reports, book_holdings)
    if window:
        realised['held_pct'] = sum(stats['contribution_pct'] or 0 for stats in allocation.values())
    for row in book_holdings:
        row.pop('accounting_identity', None)
    if include_holding_risk:
        risks = pa._holding_risk([row.isin for row in holdings if row.isin], benchmark)
        for row in book_holdings:
            row.update(risks.get(row['isin']) or {})
    note = 'AIRS supplies sleeve weights. Leonteq supplies all sleeve holdings and returns. Cash assumes zero return.'
    if missing or missing_holdings:
        note += ' Missing source data for ' + ', '.join(sorted(set(missing + missing_holdings))) + '.'
    return {**result, **profile, 'returns': returns, 'realised': realised,
            'source': 'Leonteq', 'weight_basis': 'book', 'as_of': latest[0]['as_of_date'],
            'holdings_as_of': latest[0]['as_of_date'],
            'allocation': [{'bucket': bucket, **stats} for bucket, stats in allocation.items()],
            'book_holdings': book_holdings, 'book_note': note,
            'sleeve_holdings': list(sleeve_rows.values()),
            'monthly_performance': _monthly_performance(weights, window, series),
            'return_series': series, 'details_pending': False}
