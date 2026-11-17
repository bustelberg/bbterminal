"""Management TopSelecties read model. Portfolio numbers never come from AIRS."""
from __future__ import annotations

import json
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


def detail(display_name, refresh=False, progress=None):
    entry = next((row for row in entries() if row['display_name'] == display_name), None)
    if not entry:
        raise HTTPException(404, 'Unknown TopSelectie')
    catalogue = portfolio_cache.portfolios(refresh=True) if refresh else portfolio_cache.saved_portfolios()
    matched = resolve(entry, catalogue.get('portfolios') or [])
    if not matched:
        return {'success': False, 'source': 'Leonteq', 'status': catalogue.get('status') if not catalogue.get('success') else 'portfolio_not_found'}
    name = matched['name']
    holdings = composition_cache.composition(name, refresh=refresh, saved_only=not refresh)
    returns = performance(name, refresh)
    from leonteq import accounting
    ledger = accounting.refresh(name, holdings.get('as_of') or matched.get('as_of'), progress) if refresh and holdings.get('success') and not holdings.get('refresh_error') else accounting.saved(name)
    ledger = ledger or {}
    result = ledger.get('result') or {}
    # Do not blend a previous accounting run into newer holdings or returns. Only a
    # source-consistent reconciled ledger can replace the current display boundary.
    eligible = bool(ledger.get('success') and result.get('available')
        and (ledger.get('retrieved_at') or '') >= (holdings.get('retrieved_at') or '')
        and _number(matched.get('ytd')) is not None
        and abs(_number(matched['ytd']) - result['reported_ytd_pct']) <= .0051)
    if eligible:
        from leonteq.portal_api import composition as shape_composition
        closing = ledger['valuations'][result['to']]
        holdings = {**shape_composition(closing, name), 'success': True,
                    'retrieved_at': ledger['retrieved_at']}
        matched = {**matched, 'as_of': result['to']}
    elif result.get('available'):
        ledger = {**ledger, 'success': False, 'reason': 'Leonteq accounting predates the current holdings/return snapshot', 'result': {'available': False}}
    return {'success': bool(holdings.get('success')), 'source': 'Leonteq', 'name': display_name,
            'leonteq_name': name, 'summary': matched, 'catalogue_retrieved_at': catalogue.get('retrieved_at'),
            'catalogue_error': catalogue.get('refresh_error'), 'composition': holdings, 'performance': returns,
            'accounting': ledger}


def _number(value):
    text = str(value if value is not None else '').replace('’', '').replace("'", '').replace('−', '-').replace('%', '').strip()
    try:
        result = float(text)
        return result if math.isfinite(result) else None
    except ValueError:
        return None


def dashboard_overview():
    """Adapt Leonteq snapshots to the existing management table contract, without AIRS."""
    current_month = datetime.now(ZoneInfo('Europe/Amsterdam')).strftime('%Y-%m')
    rows = []
    for entry in entries():
        source = detail(entry['display_name'])
        return_window = return_windows.cached(source)
        summary = source.get('summary') or {}
        composition = source.get('composition') or {}
        returns = source.get('performance') or {}
        positions = [p for g in composition.get('groups') or [] for p in g.get('positions') or []]
        month = next((r for r in returns.get('months') or [] if r['month'] == current_month), {})
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
            'latest_month_pct': month.get('return_pct'),
            'periode': f'{current_month}-01' if month.get('return_pct') is not None else None,
            'as_of': as_of, 'fetched_at': fetched, 'missing_reports': [],
            'month_fetched_at': returns.get('retrieved_at'),
        })
    return rows


def analysis(display_name, benchmark='ACWI', include_holding_risk=False):
    """Full valued Leonteq portfolio in the shared Analyse contract; no AIRS book needed."""
    from routers import _airs_portfolio_analysis as pa
    from routers._airs_holding_isin import classify_bucket

    source = detail(display_name)
    composition = source.get('composition') or {}
    if not source.get('success') or not composition.get('groups'):
        raise HTTPException(409, 'Leonteq holdings are unavailable. Refresh this portfolio.')
    summary = source.get('summary') or {}
    as_of = summary.get('as_of') or composition.get('as_of')
    inception = summary.get('inception_date')
    if not inception and summary.get('start_date'):
        try:
            inception = datetime.strptime(summary['start_date'], '%d %b %Y').date().isoformat()
        except ValueError:
            pass
    # A return window needs index marks, not opening position valuations. Acquire
    # that lightweight source contract independently and use its SAME dates for ACWI.
    return_window, window_error = return_windows.ensure(source)
    since = return_window['from'] if return_window else None
    return_as_of = return_window['to'] if return_window else None
    portfolio_ytd = return_window['ytd_pct'] if return_window else _number(summary.get('ytd'))
    market = pa._index_returns(benchmark, [since], end=return_as_of) if return_window else {}
    window = market.get(since) or {}
    benchmark_ytd = window.get('eur_pct')
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
        valued.append({'name': position['name'], 'isin': holding.isin, 'bucket': bucket,
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
            item = lines.get(row.get('isin'))
            if item:
                capital = item['opening'] + item['buys']
                row.update(start_value_eur=item['opening'], current_value_eur=item['closing'],
                    weight_start_pct=item['opening'] / measured['basis_eur'] * 100,
                    result_eur=item['profit'], income_eur=item['dividends'],
                    own_return_pct=item['return_pct'], return_pct=item['return_pct'],
                    own_return_source='leonteq_transactions', own_return_from=measured['from'],
                    own_return_as_of=measured['to'], avg_capital_eur=capital,
                    money_weighted_return_pct=item['return_pct'], contribution_pct=item['contribution_pct'])
            elif row['bucket'] == 'Cash':
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
        realised = {'available': True, 'source': 'Leonteq', 'positions': sold,
            'certificate_adjustments': adjustments, 'basis_eur': measured['basis_eur'],
            'book_ytd_pct': measured['reported_ytd_pct'], 'book_as_of': measured['to'],
            'reconciles': True, 'comparable': True, 'dates_aligned': True,
            'has_external_flows': measured.get('has_external_flows', False),
            'note': 'Leonteq net transactions and daily valuations; contributions are geometrically linked.'
                    + (f" AMC fees: EUR {abs(fee['profit']):.2f} ({fee['contribution_pct']:+.4f} pp), included separately." if fee else '')}
        for bucket, group in allocation.items():
            members = [row for row in valued if row['bucket'] == bucket]
            group['contribution_pct'] = sum(row.get('contribution_pct') or 0 for row in members)
    accounting_note = realised['note']
    return {**result, 'source': 'Leonteq', 'weight_basis': 'book', 'as_of': as_of,
            'holdings_as_of': as_of, 'holdings_fetched_at': composition.get('retrieved_at'),
            'allocation': list(allocation.values()), 'book_holdings': valued,
            'book_note': accounting_note, 'realised': realised,
            'returns': returns, 'details_pending': False}
