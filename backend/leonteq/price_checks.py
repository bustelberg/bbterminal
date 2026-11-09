"""One batch of source-versus-Yahoo close checks for every Dynamic connection."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextvars import copy_context
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import time

from deps import supabase
from jobs import JobCancelled
from asset_pipeline.fx import SUBUNIT

THRESHOLD = Decimal('5')
_CASH = {'cash', 'eur', 'euro', 'liquiditeiten', 'liquide middelen'}


def positive(value):
    try:
        text = str(value).replace('’', '').replace("'", '').replace(' ', '')
        text = text.replace(',', '.') if ',' in text and '.' not in text else text.replace(',', '')
        result = Decimal(text)
        return result if result.is_finite() and result > 0 else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def day(value):
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value[:10]).date().isoformat()
    except ValueError:
        return None


def compare(row, quote):
    """Compare raw closes in the same currency and quote units; never use adjusted close."""
    source = positive(row.get('source_price'))
    out = {**row, 'source_price': float(source) if source is not None else None,
           'symbol': (quote or {}).get('symbol'), 'yahoo_price': None,
           'yahoo_currency': (quote or {}).get('currency'), 'yahoo_date': None,
           'difference_pct': None, 'alert': False, 'status': 'unavailable', 'reason': ''}
    if row.get('cash'):
        return {**out, 'status': 'cash', 'reason': 'Cash is excluded.'}
    if not quote or not quote.get('bars'):
        return {**out, 'reason': 'No completed Yahoo close in the fetched three-month history.'}
    expected = day(row.get('source_date'))
    candidates = [(d, p) for d, p in quote['bars'] if not expected or d <= expected]
    if not candidates:
        return {**out, 'reason': 'No close in the fetched history on or before the source valuation date.'}
    stamp, price = max(candidates)
    out.update(yahoo_price=float(price), yahoo_date=stamp)
    yahoo = positive(price)
    if source is None or yahoo is None:
        return {**out, 'reason': 'A source or Yahoo price is missing, zero or invalid.'}
    source_unit = (row.get('source_currency') or '').strip()
    yahoo_unit = (quote.get('currency') or '').strip()
    source_ccy, source_div = SUBUNIT.get(source_unit, (source_unit.upper() or None, 1))
    yahoo_ccy, yahoo_div = SUBUNIT.get(yahoo_unit, (yahoo_unit.upper() or None, 1))
    source, yahoo = source / Decimal(str(source_div)), yahoo / Decimal(str(yahoo_div))
    out.update(source_price_original=row.get('source_price'), source_currency_original=row.get('source_currency'),
               yahoo_price_original=float(price), yahoo_currency_original=quote.get('currency'),
               source_price=float(source), source_currency=source_ccy,
               yahoo_price=float(yahoo), yahoo_currency=yahoo_ccy)
    if not source_ccy or not yahoo_ccy or source_ccy != yahoo_ccy:
        return {**out, 'reason': 'Quote currencies differ or are unknown; no FX conversion was assumed.'}
    if not expected:
        return {**out, 'reason': 'The source price has no verified valuation date.'}
    if stamp != expected:
        return {**out, 'reason': 'Yahoo and source valuation dates differ; review the dated prices.'}
    difference = (source / yahoo - 1) * 100
    alert = abs(difference) > THRESHOLD
    return {**out, 'difference_pct': float(difference), 'alert': alert,
            'status': 'alert' if alert else 'ok',
            'reason': 'Difference exceeds 5%.' if alert else 'Within 5%.'}


def fetch_yahoo(symbol):
    from asset_pipeline import yahoo

    data = yahoo.chart(symbol, rng='3mo', interval='1d')
    if not data:
        return {'symbol': symbol, 'currency': None, 'bars': []}
    meta = data.get('meta') or {}
    if meta.get('symbol') and meta['symbol'].upper() != symbol.upper():
        return {'symbol': symbol, 'currency': meta.get('currency'), 'bars': []}
    closes = (((data.get('indicators') or {}).get('quote') or [{}])[0]).get('close') or []
    bars = {}
    now = time.time()
    today = yahoo.session_date(now, meta)
    end = (((meta.get('currentTradingPeriod') or {}).get('regular') or {}).get('end'))
    for timestamp, close in zip(data.get('timestamp') or [], closes):
        value = positive(close)
        if value is not None and timestamp is not None and yahoo.is_closed_bar(timestamp):
            stamp = yahoo.session_date(timestamp, meta)
            if stamp == today and (not end or now < end):
                continue
            bars[stamp] = float(value)
    return {'symbol': symbol, 'currency': meta.get('currency'), 'bars': sorted(bars.items())}


def _executions(isins):
    from asset_pipeline.symbol_override import load_symbol_overrides
    result = {}
    for offset in range(0, len(isins), 100):
        rows = (supabase.table('asset_execution').select('isin,yahoo_symbol,currency,status')
                .in_('isin', isins[offset:offset + 100]).execute().data or [])
        for row in rows:
            if row.get('yahoo_symbol') and row.get('status') not in ('unmapped', 'ambiguous', 'delisted'):
                result[row['isin']] = row['yahoo_symbol'].strip().upper()
    overrides = load_symbol_overrides() if isins else {}
    for isin in isins:
        if overrides.get(isin):
            result[isin] = overrides[isin].strip().upper()
    return result


def _read_source(link, source):
    from leonteq import composition_cache, airs_models

    if source == 'Leonteq':
        payload = composition_cache.composition(link['leonteq_name'], refresh=True)
        if not payload.get('success') or payload.get('refresh_error') or not payload.get('groups'):
            raise ValueError('Leonteq did not return a fresh composition')
        return [{'source': source, 'name': position['name'], 'isin': position.get('isin'),
                 'source_price': position.get('price'), 'source_currency': position.get('currency'),
                 'source_date': day(position.get('price_date')),
                 'cash': group['name'].casefold() == 'cash'
                 or position['name'].strip().casefold() == 'effectenrekening'}
                for group in payload['groups'] for position in group['positions']]
    payload = airs_models.composition(link['airs_portfolio_id'], refresh=True)
    if payload.get('no_fixed_model') or not payload.get('rows'):
        raise ValueError('AIRS did not return Dynamic holdings')
    return [{'source': source, 'name': position.get('Fonds'), 'isin': position.get('ISINCode'),
             'source_price': position.get('price'), 'source_currency': position.get('valuta'),
             'source_date': day(position.get('price_date') or payload.get('datum')),
             'cash': (position.get('Fonds') or '').strip().casefold() == 'effectenrekening'
             or (not position.get('ISINCode') and (position.get('Fonds') or '').strip().casefold() in _CASH)}
            for position in payload['rows']]


def run(ctx, *, name=None, portfolio_id=None, source=None, isin=None, holding_name=None):
    from leonteq.portfolio_links import options

    ctx.check()
    ctx.progress(0, 0, 'Loading all Leonteq to AIRS Dynamic connections…')
    links = options('dynamic')['links']
    single = source is not None
    requested_source = source
    if single:
        links = [link for link in links if link['leonteq_name'] == name and link['airs_portfolio_id'] == portfolio_id]
        if not links:
            raise ValueError('This Dynamic connection no longer exists')
    portfolios, sources, errors = [], {}, []
    source_names = (requested_source,) if single else ('Leonteq', 'AIRS')
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix='price-sources') as pool:
        for index, link in enumerate(links):
            ctx.check()
            ctx.progress(index, len(links), f'Reading source prices · {link["leonteq_name"]} → {link["airs_name"]}')
            pending = {}
            for source in source_names:
                key = (source, link['leonteq_name'] if source == 'Leonteq' else link['airs_portfolio_id'])
                if key not in sources:
                    pending[key] = pool.submit(copy_context().run, _read_source, link, source)
            for key, future in pending.items():
                try:
                    sources[key] = future.result()
                except JobCancelled:
                    raise
                except Exception:
                    sources[key] = []
                    errors.append(f'{key[0]} holdings could not be refreshed for {link["leonteq_name"]}.')
            rows = [row for source in source_names
                    for row in sources[(source, link['leonteq_name'] if source == 'Leonteq' else link['airs_portfolio_id'])]]
            portfolios.append({**link, 'rows': rows})
    if single:
        # Match a server-read holding, never trust client-submitted prices or currencies.
        for portfolio in portfolios:
            portfolio['rows'] = [row for row in portfolio['rows']
                                 if row['source'] == requested_source
                                 and ((row.get('isin') or '').strip().upper() == (isin or '').strip().upper())
                                 and row['name'] == holding_name]
        if not any(p['rows'] for p in portfolios):
            raise ValueError('The holding is no longer present. Run Check all linked portfolios.')
    isins = sorted({row['isin'].strip().upper() for p in portfolios for row in p['rows'] if row.get('isin') and not row.get('cash')})
    mappings = _executions(isins)
    from leonteq import price_symbols
    existing_quotes = {}
    def fetch(symbol):
        ctx.check()
        return fetch_yahoo(symbol)

    with ThreadPoolExecutor(max_workers=4, thread_name_prefix='price-existing') as pool:
        pending = {pool.submit(copy_context().run, fetch, symbol): symbol for symbol in sorted(set(mappings.values()))}
        for future in as_completed(pending):
            try:
                existing_quotes[pending[future]] = future.result()
            except JobCancelled:
                raise
            except Exception:
                existing_quotes[pending[future]] = {}
    resolved, mapping_errors, warnings = {}, {}, set()
    for portfolio in portfolios:
        for row in portfolio['rows']:
            key = ((row.get('isin') or '').strip().upper(), price_symbols.currency(row.get('source_currency')))
            if row.get('cash') or not all(key) or key in resolved:
                continue
            existing = mappings.get(key[0])
            if not single and existing and price_symbols.currency(existing_quotes.get(existing, {}).get('currency')) == key[1]:
                resolved[key] = existing
                continue
            ctx.check()
            ctx.progress(0, 0, f'Checking Yahoo listing · {row["name"]}')
            try:
                # Resolve missing or currency-mismatched listings without repointing analysis.
                resolved[key] = price_symbols.resolve(*key, force=single, check=ctx.check, warn=warnings.add)
            except JobCancelled:
                raise
            except Exception:
                resolved[key] = None
                mapping_errors[key] = 'The Yahoo listing could not be resolved right now. Recheck this row.'
            if not resolved[key]:
                resolved[key] = mappings.get(key[0])
    symbols = sorted({symbol for symbol in resolved.values() if symbol})
    quotes = {symbol: quote for symbol, quote in existing_quotes.items() if symbol in symbols}
    def fetch(symbol):
        ctx.check()  # Queued reads must not start after the user cancels the batch.
        return fetch_yahoo(symbol)

    # Only known symbols are priced here; the shared Yahoo governor still limits all calls.
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix='price-yahoo') as pool:
        pending = {pool.submit(copy_context().run, fetch, symbol): symbol for symbol in symbols if symbol not in quotes}
        for index, future in enumerate(as_completed(pending), 1):
            ctx.check()
            symbol = pending[future]
            try:
                quotes[symbol] = future.result()
            except JobCancelled:
                raise
            except Exception:
                quotes[symbol] = {'symbol': symbol, 'currency': None, 'bars': []}
            ctx.progress(index, len(symbols), f'Yahoo Finance · {index}/{len(symbols)} checked · {symbol}')
    summary = {'portfolios': len(links), 'yahoo_symbols': len(symbols), 'checked': 0, 'alerts': 0, 'unavailable': 0, 'cash': 0}
    for portfolio in portfolios:
        checked = []
        for row in portfolio['rows']:
            isin = (row.get('isin') or '').strip().upper()
            key = (isin, price_symbols.currency(row.get('source_currency')))
            symbol = resolved.get(key)
            comparison = compare({**row, 'isin': isin or None}, quotes.get(symbol))
            if not row.get('cash') and not symbol:
                comparison['reason'] = mapping_errors.get(key, 'No verified Yahoo listing in the source currency could be resolved for this ISIN.')
            checked.append(comparison)
            key = {'ok': 'checked', 'alert': 'alerts', 'cash': 'cash'}.get(comparison['status'], 'unavailable')
            summary[key] += 1
        portfolio['rows'] = checked
    summary['checked'] += summary['alerts']
    ctx.progress(len(links), len(links), f'Finished: {summary["alerts"]} differences over 5%; {summary["unavailable"]} prices uncheckable.')
    return {'success': True, 'checked_at': datetime.now(timezone.utc).isoformat(),
            'threshold_pct': 5, 'summary': summary, 'portfolios': portfolios, 'errors': errors, 'warnings': sorted(warnings),
            'refresh_error': 'incomplete_price_checks' if errors or summary['unavailable'] else None}
