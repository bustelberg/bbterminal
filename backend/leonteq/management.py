"""Management TopSelecties read model. Portfolio numbers never come from AIRS."""
from __future__ import annotations

import json
import logging
import math
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from types import SimpleNamespace

from fastapi import HTTPException

from deps import supabase
from leonteq import composition_cache, portfolio_cache
from leonteq import return_windows

log = logging.getLogger(__name__)
_DASHBOARD_TABLE = 'leonteq_dashboard_bundle'


def key(value):
    normalized = unicodedata.normalize('NFKD', (value or '').casefold())
    return re.sub(r'[^a-z0-9]', '', normalized)


def entries():
    path = Path(__file__).resolve().parent.parent / 'config' / 'management_topselecties.json'
    return json.loads(path.read_text(encoding='utf-8'))['topselecties']


def resolve(entry, catalogue):
    """Exact normalized identities only; ambiguous names fail closed."""
    name = entry['leonteq_name']
    preferred = [row for row in catalogue if key(row['name']) == key(name)]
    if preferred:
        return preferred[0] if len(preferred) == 1 else None
    aliases = {key(entry['display_name']), key(name), key(re.sub(r'\s+index$', '', name, flags=re.I))}
    matches = [row for row in catalogue if key(row['name']) in aliases]
    return matches[0] if len(matches) == 1 else None


def entry_for_identity(name):
    from routers._airs_strategy_map import strategies
    wanted = key(name)
    for entry in entries():
        identities = [entry['display_name'], entry['dynamic_portefeuille'], entry['leonteq_name']]
        for strategy in strategies():
            if key(entry['dynamic_portefeuille']) in {key(n) for n in strategy['airs_names']}:
                identities += strategy['airs_names'] + strategy.get('holding_aliases', [])
        if wanted in {key(n) for n in identities}:
            return entry
    return None


def positions_for_identity(name):
    """None = another strategy; [] = TopSelectie with no Leonteq holdings. Never AIRS."""
    entry = entry_for_identity(name)
    if not entry:
        return None
    catalogue = portfolio_cache.saved_portfolios()
    matched = resolve(entry, catalogue.get('portfolios') or [])
    if not matched:
        return []
    source = composition_cache.composition(matched['name'], saved_only=True)
    if not source.get('success'):
        return []
    rows = []
    for group in source.get('groups') or []:
        for position in group.get('positions') or []:
            raw = str(position.get('weight') or '').replace('’', '').replace('%', '').strip()
            try:
                weight = float(raw)
            except ValueError:
                return []
            if not math.isfinite(weight) or weight < 0:
                return []
            rows.append({'isin': position.get('isin'), 'fonds': position['name'],
                         'percentage': weight, 'categorie': group['name'],
                         'currency': position.get('currency'),
                         'datum': None, 'source': 'Leonteq', 'fetched_at': source.get('retrieved_at')})
    return rows


def performance(name, refresh=False):
    from leonteq.portal_login import check_performance
    account = portfolio_cache._account_key()
    if not account:
        return {'success': False, 'status': 'missing_credentials'}
    try:
        rows = (supabase.table('leonteq_performance_snapshot').select('payload')
                .eq('account_key', account).eq('portfolio_name', name).limit(1).execute().data or [])
        saved = rows[0]['payload'] if rows else None
        if saved and not saved.get('success'):
            saved = None
    except Exception:
        saved = None
    if not refresh:
        return {**saved, 'cached': True} if saved else {'success': False, 'status': 'not_cached'}
    fresh = check_performance(name)
    if fresh.get('success'):
        try:
            supabase.table('leonteq_performance_snapshot').upsert({
                'account_key': account, 'portfolio_name': name, 'payload': fresh,
                'retrieved_at': fresh['retrieved_at'],
            }, on_conflict='account_key,portfolio_name').execute()
        except Exception:
            return {**fresh, 'refresh_error': 'snapshot_save_failed'}
        return fresh
    return {**saved, 'refresh_error': fresh.get('status')} if saved else fresh


def overview():
    catalogue = portfolio_cache.saved_portfolios()
    rows = []
    for entry in entries():
        matched = resolve(entry, catalogue.get('portfolios') or [])
        rows.append({'name': entry['display_name'], 'source': 'Leonteq',
                     'leonteq_name': matched['name'] if matched else None,
                     'status': 'success' if matched else catalogue.get('status') if not catalogue.get('success') else 'portfolio_not_found',
                     'summary': matched, 'retrieved_at': catalogue.get('retrieved_at')})
    return rows


def detail(display_name, refresh=False, progress=None, benchmark=None, *, compact_accounting=False):
    from leonteq.progress import report

    entry = next((row for row in entries() if row['display_name'] == display_name), None)
    if not entry:
        raise HTTPException(404, 'Unknown TopSelectie')
    if refresh:
        report('Refreshing the Leonteq portfolio catalogue…')
    catalogue = portfolio_cache.portfolios(refresh=True) if refresh else portfolio_cache.saved_portfolios()
    matched = resolve(entry, catalogue.get('portfolios') or [])
    if not matched:
        return {'success': False, 'source': 'Leonteq', 'status': catalogue.get('status') if not catalogue.get('success') else 'portfolio_not_found',
                'catalogue_error': catalogue.get('refresh_error')}
    name = matched['name']
    if refresh:
        if catalogue.get('refresh_error'):
            report('The catalogue refresh failed. Using the saved portfolio match to read fresh data.')
        report('Refreshing Leonteq holdings…')
    holdings = composition_cache.composition(name, refresh=refresh, saved_only=not refresh)
    if refresh:
        report('Refreshing Leonteq monthly returns…')
    returns = performance(name, refresh)
    if refresh and holdings.get('success') and not holdings.get('refresh_error'):
        window, _ = return_windows.ensure({'leonteq_name': name,
            'catalogue_retrieved_at': catalogue.get('retrieved_at'), 'performance': returns})
        if window:
            returns = {**returns, 'return_window': window}
    from leonteq import accounting
    if refresh and holdings.get('success') and not holdings.get('refresh_error'):
        ledger = accounting.refresh(name, holdings.get('as_of') or matched.get('as_of'), progress)
    elif compact_accounting:
        try:
            rows = supabase.rpc('leonteq_dashboard_accounting_inputs', {
                'p_account_key': portfolio_cache._account_key(), 'p_names': [name],
            }).execute().data or []
            ledger = rows[0]['payload'] if rows else None
            if ledger and ledger.get('version') != accounting.VERSION:
                ledger = None
        except Exception:
            ledger = accounting.saved(name)
    else:
        ledger = accounting.saved(name)
    response = _snapshot_detail(display_name, catalogue, matched, holdings, returns, ledger,
                                prefer_accounting_snapshot=compact_accounting)
    if benchmark:
        response['monthly_benchmark'] = _monthly_benchmark(response, benchmark)
        response['monthly_attribution'] = _monthly_attribution(response)
    return response


def _snapshot_detail(display_name, catalogue, matched, holdings, returns, ledger, *, prefer_accounting_snapshot=False):
    name = matched['name']
    ledger = ledger or {}
    result = ledger.get('result') or {}
    verified_window = return_windows.cached({'leonteq_name': name, 'accounting': ledger})
    reference_ytd = verified_window['ytd_pct'] if verified_window else _number(matched.get('ytd'))
    closing = (ledger.get('valuations') or {}).get(result.get('to'))
    # Analyse uses the ledger's own closing holdings for attribution. A later
    # catalogue/holdings read must not invalidate contributions for that dated book.
    dated_accounting = bool(prefer_accounting_snapshot and closing and verified_window
                            and verified_window['to'] == result.get('to')
                            and (matched.get('as_of') or result['to'])[:4] == result['to'][:4])
    # Do not blend a previous accounting run into newer holdings or returns. Only a
    # source-consistent reconciled ledger can replace the current display boundary.
    eligible = bool(ledger.get('success') and result.get('available')
        and closing
        and (dated_accounting or (ledger.get('retrieved_at') or '') >= (holdings.get('retrieved_at') or '')
             or (verified_window and holdings.get('as_of') == result.get('to')))
        and (not verified_window or verified_window['to'] == result.get('to'))
        and reference_ytd is not None and abs(reference_ytd - result['reported_ytd_pct']) <= .0051)
    if eligible and (dated_accounting or (not holdings.get('refresh_error') and holdings.get('snapshot_saved') is not False)):
        from leonteq.portal_api import composition as shape_composition
        holdings = {**shape_composition(closing, name), 'success': True,
                    'retrieved_at': ledger['retrieved_at']}
        if (matched.get('as_of') or '') <= result['to']:
            matched = {**matched, 'as_of': result['to'], 'ytd': f"{result['reported_ytd_pct']}%"}
    elif result.get('available'):
        ledger = {**ledger, 'success': False, 'reason': 'Leonteq accounting predates the current holdings/return snapshot', 'result': {'available': False}}
    return {'success': bool(holdings.get('success')), 'source': 'Leonteq', 'name': display_name,
            'leonteq_name': name, 'summary': matched, 'catalogue_retrieved_at': catalogue.get('retrieved_at'),
            'catalogue_error': catalogue.get('refresh_error'),
            'catalogue_saved': catalogue.get('snapshot_saved'), 'composition': holdings, 'performance': returns,
            'accounting': ledger}


def _monthly_attribution(source):
    from leonteq.accounting import monthly_cash_verified

    window = return_windows.for_monthly_report(source)
    ledger = source.get('accounting') or {}
    saved = ledger.get('monthly_attribution') or {}
    if not window:
        return {row['month']: {'reason': 'Verified Leonteq return dates are unavailable for this report.'}
                for row in (source.get('performance') or {}).get('months') or []}
    details = {}
    dates = [row['date'] for row in window['returns']]
    for row in (source.get('performance') or {}).get('months') or []:
        month = row['month']
        report = saved.get(month)
        if not report:
            details[month] = {'reason': (source.get('accounting') or {}).get('reason')
                              or 'Monthly positions have not been prepared for this period.'}
            continue
        if not report.get('available'):
            details[month] = {'reason': report.get('reason')}
            continue
        end = max((stamp for stamp in dates if stamp[:7] == month), default=None)
        start = max((stamp for stamp in dates if stamp[:7] < month), default=window['from'])
        partial = bool(end and report['from'] == start and start < report['to'] < end
                       and report['to'][:7] == month)
        if not partial and (report['from'] != start or report['to'] != end or row.get('return_pct') is None
                            or abs(report['reported_ytd_pct'] - row['return_pct']) > .01):
            details[month] = {'reason': 'Saved position results do not match the current Leonteq monthly return.'}
            continue
        end = report['to']
        estimated = bool(report.get('estimated') and monthly_cash_verified(ledger, report))
        first_buy = {}
        for trade in ledger.get('transactions') or []:
            stamp, isin = trade.get('date'), trade.get('isin')
            if trade.get('booked') and trade.get('event') == 'Buy' and isin and stamp and start < stamp <= end:
                first_buy[isin] = min(stamp, first_buy.get(isin, stamp))
        details[month] = {'opening_weight_date': start, 'reason': report.get('reason'),
            'positions_through': end if partial else None,
            'positions_return_pct': report['reported_ytd_pct'],
            'estimated_contribution_gap_pp': report['difference_pct'] if estimated else None, 'positions': [
            {'name': item['name'], 'isin': item['isin'], 'return_pct': item['return_pct'],
             'contribution_pp': item['contribution_pct'] if estimated or not report.get('estimated') else None,
             'start_date': (first_buy.get(identity) or start) if not item['opening'] else start, 'end_date': end,
             'opening_weight_pct': item['opening'] / report['basis_eur'] * 100 if report['basis_eur'] else None,
             'is_cash': identity.startswith('cash:'),
             'status': 'bought' if not item['opening'] and identity in first_buy else 'held'}
            for identity, item in report['positions'].items()]}
    positions = [position for month in details.values() for position in month.get('positions', [])
                 if position.get('isin')]
    if positions:
        from routers._airs_portfolio_analysis import _grid

        instruments = _grid(sorted({position['isin'] for position in positions}))
        for position in positions:
            internal = instruments.get(position['isin']) or {}
            if internal.get('company_id') and internal.get('name'):
                position['name'] = internal['name']
    return details


def _monthly_benchmark(source, benchmark):
    from routers._benchmark_etf import etf_return_series
    window = return_windows.for_monthly_report(source)
    points = (window or {}).get('returns') or []
    if len(points) < 2:
        return {}
    comparison = etf_return_series(benchmark, window['from'], [p['date'] for p in points], read_only=True)
    observations = {row['date']: row for row in comparison.get('points') or []}
    initial_price = comparison.get('start_price')
    initial_fx = comparison.get('fx_start')
    initial_eur = initial_price / initial_fx if initial_price is not None and initial_fx else None
    result = {}
    for row in (source.get('performance') or {}).get('months') or []:
        month = row['month']
        dates = sorted(p['date'] for p in points if p['date'][:7] == month)
        if row.get('return_pct') is None or not dates:
            continue
        previous = sorted(p['date'] for p in points if p['date'] < month + '-01')
        start = previous[-1] if previous else window['from']
        opened, closed = observations.get(start), observations.get(dates[-1])
        if not opened or not closed or opened['cum_pct'] <= -100 or start >= dates[-1]:
            continue
        value = ((1 + closed['cum_pct'] / 100) / (1 + opened['cum_pct'] / 100) - 1) * 100
        component = {'ticker': comparison.get('ticker') or benchmark, 'weight_pct': 100,
            'return_pct': value, 'start_date': opened.get('price_date') or start,
            'end_date': closed.get('price_date') or dates[-1]}
        if initial_eur is not None:
            component.update(start_price_eur=initial_eur * (1 + opened['cum_pct'] / 100),
                end_price_eur=initial_eur * (1 + closed['cum_pct'] / 100))
        result[month] = {'return_pct': value, 'start_date': start, 'end_date': dates[-1],
            'source': 'saved ETF prices, aligned to Leonteq', 'components': [component]}
    return result


def _number(value):
    text = str(value if value is not None else '').replace('’', '').replace("'", '').replace('−', '-').replace('%', '').strip()
    try:
        result = float(text)
        return result if math.isfinite(result) else None
    except ValueError:
        return None


def _month_return(window, month):
    """MTD from the source's flow-aware index curve, never from portfolio NAV."""
    if not window:
        return None
    points = sorted(window.get('returns') or [], key=lambda point: point['date'])
    current = [point for point in points if point['date'][:7] == month]
    opening = [point for point in points if point['date'] < month + '-01']
    if not current:
        return None
    if not opening:
        # A portfolio launched this month starts at its verified inception mark.
        if window.get('from') != current[0]['date'] or current[0]['cum_pct'] != 0:
            return None
        opening = current[:1]
    basis = 1 + opening[-1]['cum_pct'] / 100
    return ((1 + current[-1]['cum_pct'] / 100) / basis - 1) * 100 if basis > 0 else None


def refresh_returns(context, *, on_saved=None, configured=None):
    """Refresh source returns independently of expensive position accounting."""
    from jobs import JobCancelled
    from scheduler_runtime.execution import Cancelled
    context.check()
    catalogue = portfolio_cache.portfolios(refresh=True)
    if not catalogue.get('success') or catalogue.get('refresh_error'):
        return {'saved': [], 'errors': {'catalogue': 'Leonteq catalogue refresh failed.'}}
    result = {'saved': [], 'errors': {}}
    selected = entries() if configured is None else configured
    for index, entry in enumerate(selected):
        context.check()
        label = entry['display_name']
        context.progress(index, len(selected), f'{label} — refreshing Leonteq MTD and YTD')
        matched = resolve(entry, catalogue.get('portfolios') or [])
        if not matched:
            result['errors'][label] = 'Leonteq portfolio identity is unavailable.'
            continue
        try:
            monthly = performance(matched['name'], refresh=True)
            monthly_failed = not monthly.get('success') or monthly.get('refresh_error')
            window, reason = return_windows.ensure({'leonteq_name': matched['name'],
                'catalogue_retrieved_at': catalogue.get('retrieved_at'),
                'performance': {**monthly, 'return_window': None}})
            if not window:
                result['errors'][label] = reason
            else:
                result['saved'].append(label)
            if monthly_failed:
                monthly_error = 'Leonteq monthly returns could not be refreshed and saved.'
                result['errors'][label] = ' '.join(filter(None, (result['errors'].get(label), monthly_error)))
        except (JobCancelled, Cancelled):
            raise
        except Exception as exc:
            result['errors'][label] = f'Leonteq return refresh failed ({type(exc).__name__}).'
        if on_saved and label in result['saved']:
            on_saved(index + 1, len(selected), label)
    return result


def prepare_contributions(context):
    """Prepare every reviewed Topselectie, retaining independent portfolio outcomes."""
    from leonteq import accounting
    from jobs import JobCancelled
    from scheduler_runtime.execution import Cancelled
    from services.toppenberg import ALTERNATIVES
    catalogue = portfolio_cache.saved_portfolios()
    result = {'saved': [], 'estimated': [], 'errors': {}}
    configured = entries()
    if resolve(ALTERNATIVES, catalogue.get('portfolios') or []):
        configured = [*configured, ALTERNATIVES]
    for entry in configured:
        context.check()
        label = entry['display_name']
        matched = resolve(entry, catalogue.get('portfolios') or [])
        if not matched:
            result['errors'][label] = 'Leonteq portfolio identity is unavailable.'
            continue
        def progress(done, total, message):
            context.check()
            context.progress(done, total, f'{label} — {message}')
        try:
            progress(0, 0, 'preparing contributions from Leonteq')
            payload = accounting.refresh(matched['name'], matched.get('as_of'), progress)
            if payload.get('success') and payload.get('result', {}).get('available'):
                result['saved'].append(label)
                if payload['result'].get('estimated'):
                    result['estimated'].append(label)
            else:
                result['errors'][label] = payload.get('reason') or 'Leonteq accounting could not be prepared.'
        except (JobCancelled, Cancelled):
            raise
        except Exception as exc:
            result['errors'][label] = f'Leonteq accounting failed ({type(exc).__name__}).'
    return result


def _dashboard_snapshots(configured, *, strict=False):
    from leonteq import accounting

    catalogue = portfolio_cache.saved_portfolios()
    matched = {entry['display_name']: resolve(entry, catalogue.get('portfolios') or [])
               for entry in configured}
    names = sorted({row['name'] for row in matched.values() if row})
    account = portfolio_cache._account_key()
    if not names or not account:
        return {}

    def load(table):
        try:
            rows = (supabase.table(table).select('portfolio_name,payload').eq('account_key', account)
                    .in_('portfolio_name', names).execute().data or [])
            return {row['portfolio_name']: row['payload'] for row in rows if row.get('payload')}
        except Exception:
            if strict:
                raise
            return {}

    compositions = load(composition_cache.TABLE)
    performances = load('leonteq_performance_snapshot')
    try:
        ledger_rows = supabase.rpc('leonteq_dashboard_accounting_inputs',
            {'p_account_key': account, 'p_names': names}).execute().data or []
        ledgers = {row['portfolio_name']: row['payload'] for row in ledger_rows}
    except Exception:
        # Rolling deployments still return correct data before the migration arrives.
        log.warning('Compact dashboard accounting RPC unavailable; using full saved snapshots')
        ledgers = load(accounting.TABLE)
    snapshots = {}
    for label, summary in matched.items():
        if not summary:
            continue
        name = summary['name']
        holdings = compositions.get(name, {})
        returns = performances.get(name, {})
        ledger = ledgers.get(name, {})
        snapshots[label] = _snapshot_detail(label, catalogue, summary,
            holdings if holdings.get('success') and holdings.get('groups') else {},
            returns if returns.get('success') else {},
            ledger if ledger.get('version') == accounting.VERSION else {})
    return snapshots


def dashboard_overview():
    """Read the prepared table in one database request."""
    current_month = datetime.now(ZoneInfo('Europe/Amsterdam')).strftime('%Y-%m')
    account = portfolio_cache._account_key()
    if account:
        try:
            rows = (supabase.table(_DASHBOARD_TABLE).select('payload').eq('account_key', account)
                    .eq('report_month', current_month).limit(1).execute().data or [])
            if rows:
                return rows[0]['payload']
        except Exception:
            log.warning('Prepared TopSelecties dashboard could not be read')
    # Existing installations and month rollover can still read saved source data.
    return _build_dashboard_overview(current_month)


def prepare_dashboard_overview():
    account = portfolio_cache._account_key()
    if not account:
        return False
    current_month = datetime.now(ZoneInfo('Europe/Amsterdam')).strftime('%Y-%m')
    try:
        payload = _build_dashboard_overview(current_month, strict=True)
        if not any(row['fetched_at'] for row in payload):
            return False
        supabase.table(_DASHBOARD_TABLE).upsert({'account_key': account,
            'report_month': current_month, 'payload': payload}, on_conflict='account_key').execute()
        return True
    except Exception:
        log.warning('Prepared TopSelecties dashboard could not be saved')
        return False


def _build_dashboard_overview(current_month, *, strict=False):
    rows = []
    configured = entries()
    snapshots = _dashboard_snapshots(configured, strict=strict)
    for entry in configured:
        source = snapshots.get(entry['display_name'], {})
        return_window = return_windows.for_monthly_report(source)
        summary = source.get('summary') or {}
        composition = source.get('composition') or {}
        returns = source.get('performance') or {}
        positions = [p for g in composition.get('groups') or [] for p in g.get('positions') or []]
        month = next((r for r in returns.get('months') or [] if r['month'] == current_month), {})
        month_pct = _month_return(return_window, current_month)
        month_from_history = month_pct is not None
        if month_pct is None:
            month_pct = month.get('return_pct')
        inception = summary.get('inception_date')
        if not inception and summary.get('start_date'):
            try:
                inception = datetime.strptime(summary['start_date'], '%d %b %Y').date().isoformat()
            except ValueError:
                pass
        fetched = source.get('catalogue_retrieved_at')
        year = (fetched or current_month)[:4]
        since = return_window['from'] if return_window else max(inception, f'{year}-01-01') if inception else None
        as_of = return_window['to'] if return_window else summary.get('as_of') or composition.get('as_of')
        rows.append({
            'name': entry['display_name'], 'management_group': 'topselecties',
            'dynamic_portefeuille': entry['display_name'], 'link_source': 'leonteq',
            'fixed_portfolio_id': None, 'holdings': len(positions),
            'isins': sum(bool(p.get('isin')) for p in positions),
            'ytd_pct': return_window['ytd_pct'] if return_window else _number(summary.get('ytd')), 'ytd_from': since,
            'latest_month_pct': month_pct,
            'periode': f'{current_month}-01' if month_pct is not None else None,
            'as_of': as_of, 'fetched_at': return_window.get('retrieved_at') if return_window else fetched, 'missing_reports': [],
            'month_as_of': as_of if month_from_history else None,
            'month_fetched_at': return_window.get('retrieved_at') if month_from_history else returns.get('retrieved_at'),
        })
    return rows


def _return_series(window, benchmark, *, policy_weights=None):
    if not window or not window.get('returns'):
        return None
    from routers._benchmark_etf import etf_return_series
    points = window['returns']
    dates = [point['date'] for point in points]
    if policy_weights:
        from routers._airs_value_series import _weighted_benchmark_series
        comparison = _weighted_benchmark_series(policy_weights, window['from'], dates, read_only=True)
    else:
        comparison = etf_return_series(benchmark, window['from'], dates, read_only=True)
    return {'returns': points, 'return_from': window['from'], 'return_pct': points[-1]['cum_pct'],
        'first_date': window['from'], 'last_date': window['to'], 'points': [], 'flows': [],
        'benchmark': comparison.get('label') or benchmark, 'benchmark_ticker': comparison.get('ticker'),
        'benchmark_components': comparison.get('components') or [],
        'benchmark_source': comparison.get('source'), 'benchmark_returns': comparison.get('points') or [],
        'benchmark_return_pct': comparison.get('return_pct'), 'benchmark_as_of': comparison.get('as_of')}


def _held_group_return(members):
    """Same profit / (opening value + buys) basis as Leonteq position returns."""
    if not members or any(row.get('result_eur') is None or row.get('avg_capital_eur') is None
                          for row in members):
        return None
    capital = sum(row['avg_capital_eur'] for row in members)
    return 100 * sum(row['result_eur'] for row in members) / capital if capital > 0 else None


def analysis(display_name, benchmark='ACWI', include_holding_risk=False):
    """Full valued Leonteq portfolio in the shared Analyse contract; no AIRS book needed."""
    from routers import _airs_portfolio_analysis as pa
    from routers._airs_holding_isin import classify_bucket
    from routers._benchmark_etf import PROXY, etf_returns

    source = detail(display_name, compact_accounting=True)
    composition = source.get('composition') or {}
    if not source.get('success') or not composition.get('groups'):
        raise HTTPException(409, 'Leonteq holdings are unavailable. Refresh this portfolio.')
    summary = source.get('summary') or {}
    as_of = composition.get('as_of') or summary.get('as_of')
    inception = summary.get('inception_date')
    if not inception and summary.get('start_date'):
        try:
            inception = datetime.strptime(summary['start_date'], '%d %b %Y').date().isoformat()
        except ValueError:
            pass
    # Manual and scheduled preparation save the verified dates before Analyse is opened.
    return_window = return_windows.for_monthly_report(source)
    window_error = None if return_window else 'Verified Leonteq return dates have not been saved. Refresh this portfolio.'
    since = return_window['from'] if return_window else None
    return_as_of = return_window['to'] if return_window else None
    portfolio_ytd = return_window['ytd_pct'] if return_window else _number(summary.get('ytd'))
    market = {}
    if return_window:
        if benchmark in PROXY:
            market = etf_returns(benchmark, [since], end=return_as_of, read_only=True)
        else:
            market = pa._index_returns(benchmark, [since], end=return_as_of)
    window = market.get(since) or {}
    benchmark_ytd = window.get('eur_pct')
    if return_window and benchmark_ytd is None:
        window_error = f'Saved {benchmark} prices or EUR exchange rates do not cover the verified Leonteq return dates.'
    returns = {**pa._bench_prov(window), 'source': 'leonteq', 'book_available': True,
               'ytd_from': since, 'since_from': inception, 'portfolio_ytd_pct': portfolio_ytd,
               'strategy_ytd_pct': portfolio_ytd, 'portfolio_as_of': return_as_of,
               'benchmark_ytd_pct': benchmark_ytd,
               'ytd_excess_pct': portfolio_ytd - benchmark_ytd if return_window and portfolio_ytd is not None and benchmark_ytd is not None else None,
               'book_reason': window_error}
    raw = [(group['name'], position) for group in composition['groups'] for position in group['positions']]
    holdings = []
    for category, position in raw:
        weight = _number(position.get('weight'))
        if weight is None or weight < 0:
            raise HTTPException(409, 'Leonteq contains an invalid position weight. Refresh this portfolio.')
        holdings.append(SimpleNamespace(isin=position.get('isin'), weight=weight, name=position['name']))
    # This shared builder supplies market classifications and benchmark axes only. Passing
    # authoritative returns explicitly prevents the generic basket return reconstruction.
    result = pa.compute_basket_analysis(holdings, benchmark, display_name, portfolio_returns=returns)
    grid = pa._grid([h.isin for h in holdings if h.isin])
    codes = pa._country_by_code()
    valued = []
    allocation = {}
    for (category, position), holding in zip(raw, holdings):
        instrument = grid.get(holding.isin) or {}
        fund = pa._is_fund(instrument, position['name'])
        bucket = pa._class_from_categorie(category) or classify_bucket(category, fund, holding.isin, position['name'], instrument)
        sector, region, _ = pa._buckets(instrument, is_cash=bucket == 'Cash', isin=holding.isin, codes=codes)
        value = str(position.get('total_value') or '')
        # Source position values are in the AMC's base currency, not the quote currency.
        # Keep non-EUR values unavailable in EUR columns rather than mislabelling them.
        value_eur = _number(value[:-4]) if value.endswith(' EUR') else None
        source_return = _number(position.get('performance'))
        valued.append({'name': instrument.get('name') or position['name'], 'isin': holding.isin, 'bucket': bucket,
                       'is_fund': fund, 'currency': position.get('currency'),
                       'sector': sector, 'region': region,
                       'weight_now_pct': holding.weight, 'weight_start_pct': None,
                       'current_value_eur': value_eur, 'start_value_eur': None,
                       'return_pct': None, 'own_return_pct': source_return,
                       'own_return_source': 'leonteq_average' if source_return is not None else 'leonteq',
                       'contribution_pct': None,
                       'current_value_as_of': position.get('price_date') or as_of,
                       'via_names': [], 'sources': []})
        group = allocation.setdefault(bucket, {'bucket': bucket, 'pct': 0, 'holdings': 0})
        group['pct'] += holding.weight
        group['holdings'] += 1
    if include_holding_risk:
        risk = pa._holding_risk([h.isin for h in holdings if h.isin], benchmark)
        for row in valued:
            row.update(risk.get(row.get('isin')) or {})
    for bucket in ('Equity', 'Bonds', 'Alternatives', 'Cash'):
        allocation.setdefault(bucket, {'bucket': bucket, 'pct': 0, 'holdings': 0})
    ledger = source.get('accounting') or {}
    measured = ledger.get('result') or {}
    realised = {'available': False, 'note': ledger.get('reason') or 'Refresh Leonteq to collect opening valuations and net transactions.'}
    if ledger.get('success') and measured.get('available'):
        lines = measured['positions']
        held = {row['isin'] for row in valued if row.get('isin')}
        for row in valued:
            identity = row.get('isin') or ('cash:' + str(row.get('currency')) if row['bucket'] == 'Cash' else None)
            item = lines.get(identity) if identity != 'cash:EUR' else None
            if item:
                capital = item['opening'] + item['buys']
                row.update(start_value_eur=item['opening'], current_value_eur=item['closing'],
                    weight_start_pct=item['opening'] / measured['basis_eur'] * 100,
                    result_eur=item['profit'], income_eur=item['dividends'],
                    own_return_pct=item['return_pct'], return_pct=item['return_pct'],
                    own_return_source='leonteq_transactions', own_return_from=measured['from'],
                    own_return_as_of=measured['to'], avg_capital_eur=capital,
                    money_weighted_return_pct=item['return_pct'], contribution_pct=item['contribution_pct'])
            elif identity == 'cash:EUR':
                cash = measured.get('cash') or {}
                row.update(result_eur=0, contribution_pct=0, own_return_pct=0,
                    own_return_source='leonteq_transactions', start_value_eur=cash.get('opening'),
                    weight_start_pct=cash['opening'] / measured['basis_eur'] * 100 if 'opening' in cash else None,
                    avg_capital_eur=cash.get('opening'), money_weighted_return_pct=0)
        sold = [{'name': item['name'], 'isin': identity, 'closed_out': True,
                 'opening_eur': item['opening'], 'avg_capital_eur': item['opening'] + item['buys'],
                 'held_result_eur': 0, 'realised_result_eur': item['profit'] - item['dividends'],
                 'income_eur': item['dividends'], 'result_eur': item['profit'],
                 'money_weighted_return_pct': item['return_pct'], 'contribution_pct': item['contribution_pct']}
                for identity, item in lines.items() if identity not in held and item['isin']]
        fee = lines.get('fee:AMC')
        adjustments = [{'name': fee['name'], 'label': 'Leonteq AMC fees',
                        'unrealised_eur': 0, 'realised_result_eur': fee['profit'],
                        'result_eur': fee['profit'], 'contribution_pct': fee['contribution_pct']}] if fee else []
        estimated = measured.get('estimated', False)
        realised = {'available': True, 'estimated': estimated, 'source': 'Leonteq', 'positions': sold,
            'certificate_adjustments': adjustments, 'basis_eur': measured['basis_eur'],
            'book_ytd_pct': measured['reported_ytd_pct'], 'book_as_of': measured['to'],
            'reconciles': not estimated, 'comparable': True, 'dates_aligned': True,
            'has_external_flows': measured.get('has_external_flows', False),
            'note': ('Estimated contributions using daily opening capital where cash-flow timing is unresolved. '
                     'The unallocated source difference is shown separately.' if estimated else
                     'Leonteq net transactions and daily valuations; contributions are geometrically linked.')
                    + f" Contributions cover {measured['from']} to {measured['to']}."
                    + (f" AMC fees: EUR {abs(fee['profit']):.2f} ({fee['contribution_pct']:+.4f} pp), included separately." if fee else '')}
        for bucket, group in allocation.items():
            members = [row for row in valued if row['bucket'] == bucket]
            contributions = [row['contribution_pct'] for row in members if row['contribution_pct'] is not None]
            group['contribution_pct'] = sum(contributions) if len(contributions) == len(members) else None
            group['return_pct'] = _held_group_return(members)
        for axis in result.get('axes', []):
            if axis['axis'] == 'sector':
                for sector in axis['rows']:
                    stocks = [row for row in valued if row['bucket'] == 'Equity' and not row['is_fund']
                              and row['sector'] == sector['bucket']]
                    sector['held_return_pct'] = _held_group_return(stocks)
    accounting_note = realised['note']
    return {**result, 'source': 'Leonteq', 'weight_basis': 'book', 'as_of': as_of,
            'holdings_as_of': as_of, 'holdings_fetched_at': composition.get('retrieved_at'),
            'allocation': list(allocation.values()), 'book_holdings': valued,
            'book_note': accounting_note, 'realised': realised,
            'returns': returns, 'return_series': _return_series(return_window, benchmark), 'details_pending': False}
