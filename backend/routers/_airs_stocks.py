"""Prepared Stocks cards and individual exposure breakdowns."""
from concurrent.futures import ThreadPoolExecutor


def key(portfolio_id, benchmark, look_through):
    from routers._prepared_analyse import holdings_key
    return f'stocks:{holdings_key(portfolio_id, benchmark, look_through)}'


def fund_allocations(holdings):
    from routers import _etf_currency_allocation as currency
    from routers import _etf_region_allocation as region
    from routers import _etf_sector_allocation as sector

    readers = {'sector': (sector.supported, sector.fetch_sector_allocation),
               'region': (region.supported, region.fetch_region_allocation),
               'currency': (currency.supported, currency.fetch_currency_allocation)}
    isins = sorted({row['isin'].strip().upper() for row in holdings
                    if row.get('isin') and row.get('bucket') == 'Equity' and row.get('is_fund')
                    and (row.get('weight_now_pct') or 0) > 0})
    result = {axis: {'allocations': {}, 'errors': {}} for axis in readers}
    with ThreadPoolExecutor(max_workers=4) as pool:
        work = [(axis, isin, pool.submit(read, isin)) for axis, (supported, read) in readers.items()
                for isin in isins if supported(isin)]
        for axis, isin, future in work:
            try:
                result[axis]['allocations'][isin] = future.result().model_dump(mode='json')
            except Exception:
                result[axis]['errors'][isin] = 'The provider did not return a usable ETF allocation.'
    return result


def cards(payload):
    axes = []
    for axis in payload.get('axes') or []:
        rows = []
        for row in axis['rows']:
            summary = {k: v for k, v in row.items() if k != 'benchmark_holdings'}
            summary['has_benchmark_holdings'] = any((holding.get('weight_pct') or 0) > 0
                                                     for holding in row.get('benchmark_holdings') or [])
            rows.append(summary)
        axes.append({**axis, 'rows': rows})
    return {**payload, 'axes': axes}


def save(scope, cache_key, payload, version, code):
    from routers import _prepared_analyse as prepared

    breakdown_axes = [
        {'axis': axis['axis'], 'rows': [
            {'bucket': row['bucket'], 'benchmark_holdings': row.get('benchmark_holdings') or []}
            for row in axis['rows']
        ]}
        for axis in payload.get('axes') or []
    ]
    if prepared.save(scope, f'{cache_key}:breakdowns', {'axes': breakdown_axes}, version, code):
        prepared.save(scope, cache_key, cards(payload), version, code)


async def snapshot(portfolio_id, benchmark, look_through, *, source='book', weight_by='book', overview_only=True,
                   include_holding_risk=False, breakdown_only=False):
    import asyncio
    from routers import _prepared_analyse as prepared
    from routers._airs_portfolio_analysis import compute_portfolio_analysis_async

    cache_key = key(portfolio_id, benchmark, look_through)
    use_prepared = overview_only and source == weight_by == 'book'
    read_key = f'{cache_key}:breakdowns' if breakdown_only else cache_key
    version, existing = await asyncio.to_thread(prepared.read, 'holdings', read_key) if use_prepared else (None, None)
    if existing is not None:
        return existing
    code = prepared.code_version()
    payload = await compute_portfolio_analysis_async(portfolio_id, benchmark, weight_by, source, 'Equity',
        look_through, True, overview_only=overview_only, include_holding_risk=include_holding_risk)
    if breakdown_only:
        return payload
    allocations = await asyncio.to_thread(fund_allocations, payload.get('book_holdings') or [])
    payload = {**payload, 'stock_etfs': allocations}
    if version is not None:
        try:
            await asyncio.to_thread(save, 'holdings', cache_key, payload, version, code)
        except Exception as exc:
            prepared._unavailable(exc)
    return cards(payload)
