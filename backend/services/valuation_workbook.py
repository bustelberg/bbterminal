"""Workbook assumptions and saved GuruFocus consensus; no provider requests."""
import json
import math
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

from fastapi import HTTPException
from deps import chunked, paginate, supabase
from services.gurufocus_listing_overrides import reviewed_listings
from services.portfolio_membership import read as read_memberships

DEFAULTS = Path(__file__).resolve().parents[1] / 'config' / 'valuation_workbook_defaults.json'
TABLE = 'portfolio_valuation_assumptions'
FORECAST_CODES = ('annual_eps_nri_estimate', 'annual_per_share_eps_estimate')


def defaults():
    return json.loads(DEFAULTS.read_text(encoding='utf-8'))


def normalize_assumptions(ticker, assumptions):
    """Repair the old empty Tesla SOTP template; preserve entered manual bases."""
    if (ticker == 'TSLA' and assumptions.get('basis_kind') == 'manual'
            and assumptions.get('manual_basis') is None
            and assumptions.get('basis_label') == 'SOTP / optie-scenario vereist'):
        return {**assumptions, 'basis_kind': 'forward_eps', 'basis_label': ''}
    return assumptions


def attach_fx(workbook):
    """Bundle saved units-per-EUR observations; never fetch providers on a read."""
    overrides = reviewed_listings()
    workbook = {**workbook, 'rows': [
        {**row, 'ticker': source_ticker({}, row.get('isin'))}
        if not row.get('ticker') and row.get('isin') in overrides else row
        for row in workbook['rows']]}
    dates = [row.get('quote', {}).get('price_date') for row in workbook['rows']]
    dates = [date.fromisoformat(stamp[:10]) for stamp in dates if stamp]
    if not dates:
        return workbook
    start, end = min(dates) - timedelta(days=7), max(dates)
    observations = list(paginate(lambda lo, hi: supabase.table('fx_rate')
        .select('currency_code,rate_date,rate').gte('rate_date', start.isoformat())
        .lte('rate_date', end.isoformat()).order('rate_date').order('currency_code')
        .range(lo, hi).execute()))
    rates = {}
    for item in observations:
        rate = item.get('rate')
        if isinstance(rate, (int, float)) and math.isfinite(rate) and rate > 0:
            rates.setdefault(str(item['rate_date'])[:10], {})[item['currency_code']] = rate
    return {**workbook, 'rows': [{**row, 'fx_rates': rates} for row in workbook['rows']]}


def source_ticker(company, isin=None):
    ticker = (company.get('gurufocus_ticker') or '').strip()
    exchange = company.get('gurufocus_exchange') or {}
    if ':' in ticker or exchange.get('is_us'):
        return ticker
    if exchange.get('exchange_code') and ticker:
        return f"{exchange['exchange_code']}:{ticker}"
    reviewed = reviewed_listings().get(isin)
    if not reviewed:
        return ''
    return reviewed['ticker'] if reviewed.get('is_us') else f"{reviewed['exchange']}:{reviewed['ticker']}"


def consensus(rows, after):
    """One coherent EPS series, with adjacent future annual fiscal targets."""
    for code in FORECAST_CODES:
        values = {}
        for row in rows:
            stamp = str(row.get('target_date') or '')[:10]
            value = row.get('numeric_value')
            if row.get('metric_code') != code or not stamp or stamp < after or value is None:
                continue
            if stamp in values and values[stamp] != value:
                return {'reason': 'Conflicting GuruFocus annual EPS estimates.'}
            values[stamp] = value
        dates = sorted(values)
        if len(dates) >= 2:
            first, second = dates[:2]
            if not 330 <= (date.fromisoformat(second) - date.fromisoformat(first)).days <= 400:
                return {'reason': 'GuruFocus forecast fiscal years are not consecutive.'}
            return {'fy1': values[first], 'fy2': values[second], 'fy1_date': first, 'fy2_date': second,
                    'metric_code': code, 'basis_label': 'EPS without NRI (consensus)' if code == FORECAST_CODES[0]
                    else 'EPS (consensus; reported basis)', 'source': 'GuruFocus'}
    return {'reason': 'Two consecutive future annual GuruFocus EPS forecasts are not saved.'}


def read(portfolio_id, *, only_isin=None, include_membership=True, prepared_bundle=None):
    from services import portfolio_valuation as valuation
    # An internal refresh caller already validated scope and saved this exact bundle.
    bundle = prepared_bundle if prepared_bundle is not None else valuation.read(portfolio_id)
    if only_isin is not None:
        bundle = {**bundle, 'rows': [row for row in bundle['rows'] if row.get('isin') == only_isin]}
    template = defaults()
    from services.portfolio_membership import DEFINITIONS
    membership = (read_memberships() if only_isin is None and include_membership else
                  {'memberships': {}, 'legend': [{**{k: v for k, v in item.items() if k != 'accounts'},
                    'status': 'unavailable', 'portfolios': [], 'source': 'AIRS' if 'accounts' in item else 'Leonteq'}
                    for item in DEFINITIONS]})
    seed = {row['ticker']: row for row in template['companies']}
    try:
        saved = (supabase.table(TABLE).select('payload,updated_at').eq('portfolio_id', portfolio_id)
                 .limit(1).execute().data or [])
    except Exception:
        raise HTTPException(503, 'Valuation assumptions storage is unavailable. Apply migration '
                            '20261116140000_valuation_workbook_assumptions.sql.') from None
    settings = saved[0]['payload'] if saved else {}
    isins = sorted({row['isin'] for row in bundle['rows'] if row.get('isin')})
    def lookup(keys, query):
        return [row for group in chunked(keys)
                for row in paginate(lambda lo, hi: query(group).range(lo, hi).execute())]

    company_columns = 'company_id,isin,gurufocus_ticker,gurufocus_exchange:gurufocus_exchange(currency_code,exchange_code,is_us)'
    def company_lookup(column, keys):
        return lookup(keys, lambda group: supabase.table('company').select(company_columns)
                      .in_(column, group).is_('out_of_scope_at', 'null').order('company_id'))

    selected_ids = {row['valuation_company_id'] for row in bundle['rows'] if row.get('valuation_company_id')}
    records = company_lookup('isin', sorted({valuation.reference_isin(isin) for isin in isins}))
    records += company_lookup('company_id', sorted(selected_ids - {row['company_id'] for row in records}))
    companies_by_id = {row['company_id']: row for row in records}
    by_isin = {}
    for company in companies_by_id.values():
        by_isin.setdefault(company['isin'], []).append(company)
    ids = sorted(companies_by_id)
    metrics = lookup(ids, lambda group: supabase.table('metric_data')
        .select('company_id,target_date,numeric_value,metric_code').in_('company_id', group)
        .eq('source_code', 'gurufocus').in_('metric_code', FORECAST_CODES)
        .gte('target_date', date.today().isoformat()).order('company_id').order('target_date')
        .order('metric_code'))
    by_company = {}
    for metric in metrics:
        by_company.setdefault(metric['company_id'], []).append(metric)
    # Expose the untouched Yahoo observation separately from unit normalization.
    # Read recent closes independently of historical P/E availability. A missing
    # historical valuation must not hide a valid current quote or forecast.
    quote_start = (date.today() - timedelta(days=14)).isoformat()
    quotes = lookup(ids, lambda group: supabase.table('company_yahoo_price')
        .select('company_id,analysis_id,target_date,close,currency').in_('company_id', group)
        .gte('target_date', quote_start).lte('target_date', date.today().isoformat()).order('company_id').order('target_date')
        .order('analysis_id'))
    executions = lookup(sorted(isin for isin in by_isin if isin), lambda group: supabase.table('asset_execution')
        .select('isin,analysis_id,yahoo_symbol').in_('isin', group)
        .eq('status', 'ok').order('isin'))
    symbols = {(item['isin'], item.get('analysis_id')): item.get('yahoo_symbol') for item in executions}
    by_quote = {}
    for quote in quotes:
        by_quote.setdefault(quote['company_id'], []).append(quote)
    rows = []
    incomplete_codes = [item['code'] for item in membership['legend'] if item['status'] != 'complete']
    for holding in bundle['rows']:
        matches = by_isin.get(valuation.reference_isin(holding.get('isin')), [])
        company = companies_by_id.get(holding.get('valuation_company_id')) or (matches[0] if len(matches) == 1 else {})
        ticker = source_ticker(company, valuation.reference_isin(holding.get('isin')))
        initial = seed.get(ticker, {})
        assumptions = {k: v for k, v in initial.items() if k not in ('name', 'ticker', 'selections')}
        assumptions.update((settings.get('companies') or {}).get(holding.get('isin'), {}))
        assumptions = normalize_assumptions(ticker, assumptions)
        forecast = consensus(by_company.get(company.get('company_id'), []), date.today().isoformat())
        if valuation.reference_isin(holding.get('isin')) != holding.get('isin'):
            forecast['basis_label'] = (forecast.get('basis_label') or 'EPS') + ' - ordinary-share reference valuation'
        available_quotes = by_quote.get(company.get('company_id'), [])
        latest_date = max((str(item['target_date'])[:10] for item in available_quotes), default=None)
        observations = [item for item in available_quotes if str(item['target_date'])[:10] == latest_date]
        unique = {(item.get('close'), item.get('currency')) for item in observations}
        source_quote = observations[0] if len(unique) == 1 else {}
        quote_symbols = {symbols.get((company.get('isin'), item.get('analysis_id'))) for item in observations}
        yahoo_symbol = next(iter(quote_symbols)) if len(quote_symbols) == 1 and source_quote else None
        confirmed = {}
        for member_isin in holding.get('member_isins') or [holding.get('isin')]:
            for code, profiles in membership['memberships'].get(member_isin, {}).items():
                confirmed[code] = sorted(set(confirmed.get(code, [])) | set(profiles))
        # Membership in a family is proved by one positive holding, even if
        # another profile in that family is unavailable. Only unconfirmed codes
        # can still be affected by missing sources.
        unknown_codes = [code for code in incomplete_codes if code not in confirmed]
        rows.append({'isin': holding.get('isin'), 'ticker': ticker or None,
            'selections': list(confirmed), 'membership_profiles': confirmed,
            'membership_complete': bool(holding.get('isin')) and not unknown_codes,
            'membership_unknown_codes': unknown_codes, 'assumptions': assumptions,
            'forecast': {**forecast, 'currency': (company.get('gurufocus_exchange') or {}).get('currency_code')},
            'quote': {'price': source_quote.get('close'), 'currency': source_quote.get('currency'),
                      'price_date': str(source_quote['target_date'])[:10] if source_quote else None,
                      'source': 'Yahoo Finance' if source_quote else None, 'symbol': yahoo_symbol},
            'seed_source': 'test.xlsx' if initial else None})
    return attach_fx({'portfolio_id': portfolio_id, 'as_of': date.today().isoformat(), 'parameters': {**template['parameters'], **settings.get('parameters', {})},
            'rows': rows, 'membership_legend': membership['legend'],
            'updated_at': saved[0]['updated_at'] if saved else None})


def save(portfolio_id, payload, *, prepared_bundle=None):
    from services import portfolio_valuation as valuation
    bundle = valuation.read(portfolio_id) if prepared_bundle is None else prepared_bundle
    allowed = {row['isin'] for row in bundle['rows'] if row.get('isin')}
    if set(payload.get('companies', {})) - allowed:
        raise HTTPException(422, 'Assumptions must belong to companies in this portfolio.')
    stamp = datetime.now(timezone.utc).isoformat()
    supabase.table(TABLE).upsert({'portfolio_id': portfolio_id, 'payload': payload,
        'updated_at': stamp}, on_conflict='portfolio_id').execute()
    # A save confirms the write only. Source facts and cross-portfolio membership
    # checks are unrelated to an assumption edit and must not delay its acknowledgement.
    return {'portfolio_id': portfolio_id, **payload, 'updated_at': stamp}
