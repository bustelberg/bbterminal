"""Refresh only the AIRS model catalogue, without downloading compositions."""
from __future__ import annotations

from datetime import datetime, timezone
import math

from deps import supabase


def _security_holdings(rows):
    """Exclude explicit interest and conservatively recognise older untagged EUR rows."""
    rows = [row for row in rows if row.get('valuation_component') != 'accrued_interest']
    excluded = set()
    for index, row in enumerate(rows):
        if row.get('valuation_component') or not row.get('isin') or row.get('currency') != 'EUR':
            continue
        quantity, price, value = row.get('quantity'), row.get('current_price_local'), row.get('current_value_eur')
        if not all(isinstance(v, (int, float)) and math.isfinite(v) and v > 0 for v in (quantity, price, value)):
            continue
        verified = [other for other in rows if other.get('isin') == row['isin']
                    and other.get('currency') == 'EUR' and other.get('quantity') == quantity
                    and other.get('current_price_local') == price
                    and isinstance(other.get('current_value_eur'), (int, float))
                    and any(math.isclose(other['current_value_eur'], quantity * price * factor,
                                         rel_tol=0, abs_tol=.51 if float(other['current_value_eur']).is_integer() else .011)
                            for factor in (1, .01))]
        # Never pick the larger line alone: verify the principal against quantity × quote.
        if (len(verified) == 1 and verified[0] is not row and row.get('start_value_eur') == 0
                and value < verified[0]['current_value_eur'] * .1):
            excluded.add(index)
    return [row for index, row in enumerate(rows) if index not in excluded]


def saved_compositions() -> dict:
    """Bulk database-only preload; never acquire the AIRS session or launch a browser."""
    from collections import defaultdict
    from routers import _airs_portfolio_store as store
    from airs_scanner import has_fixed_model

    def pages(query):
        rows, offset = [], 0
        while True:
            page = query.range(offset, offset + 499).execute().data or []
            rows.extend(page)
            if len(page) < 500:
                return rows
            offset += len(page)

    models = pages(supabase.table('airs_model_portfolio')
        .select('id,name,portfolio_type,positions_datum,positions_dates,positions_scanned_at,positions_schema_version')
        .gte('positions_schema_version', 1).not_.is_('positions_scanned_at', 'null').order('id'))
    models = [model for model in models if has_fixed_model(model.get('portfolio_type'))]
    positions = defaultdict(list)
    if models:
        rows = pages(supabase.table('airs_model_portfolio_position').select('*')
            .in_('portfolio_id', [model['id'] for model in models]).order('id'))
        for row in rows:
            positions[row['portfolio_id']].append(row)
    return {'compositions': [{**store.load_positions(model['id'], model=model,
        stored_rows=positions[model['id']]), 'cache_upgrade_available': model['positions_schema_version'] < 3}
        for model in models]}


def refresh_models(ctx) -> str:
    from airs_scanner import fetch_model_portfolios_sync
    from airs_vermogen import _LOCK
    from routers import _airs_portfolio_store as store

    if not _LOCK.acquire(blocking=False):
        raise RuntimeError('AIRS is busy with another refresh. Please try again shortly.')
    try:
        ctx.check()
        ctx.progress(0, 0, 'Signing in to AIRS and reading Model portefeuilles…')

        def progress(kind, **event):
            ctx.check()
            if event.get('message'):
                ctx.progress(0, 0, event['message'])

        rows = fetch_model_portfolios_sync(progress)
        ctx.check()
        if not rows:
            raise RuntimeError('AIRS returned no model portfolios; the saved list was kept.')
        store.save_portfolios(rows)
        ctx.progress(len(rows), len(rows), f'{len(rows)} model portfolios saved')
        return f'{len(rows)} model portfolios loaded from AIRS.'
    finally:
        _LOCK.release()


def _refresh_dynamic_holdings(name: str) -> None:
    """The inspector needs VOLK only, not the five-report account scan."""
    from airs_vermogen import (_acquire_session, _LOCK, _reset_valuation_memo,
                               _vermogen_most_recent, _save_holdings, latest_completed_market_date)
    from portfolio import parse_airs_excel
    from leonteq.progress import report
    from fastapi import HTTPException

    if not _acquire_session(wait=30):
        raise HTTPException(409, 'AIRS is busy with another refresh')
    try:
        _reset_valuation_memo()
        end = latest_completed_market_date()
        report(f'Downloading the AIRS holdings report for {name}…', f'AIRS-positierapport downloaden voor {name}…')
        mark, raw = _vermogen_most_recent(name, f'{end[:4]}-01-01', through=end)
        holdings = parse_airs_excel(raw)
        if not holdings:
            raise HTTPException(502, 'AIRS returned no holdings; the saved snapshot was kept')
        report(f'Saving {len(holdings)} AIRS holdings as of {mark}…',
               f'{len(holdings)} AIRS-posities opslaan per {mark}…')
        if not _save_holdings(name, mark, holdings):
            raise HTTPException(502, 'AIRS returned no usable holdings; the saved snapshot was kept')
    finally:
        _LOCK.release()


def _dynamic_composition(model: dict, datum: str | None, refresh: bool) -> dict:
    """Dynamic models use dated account holdings, never the Fixed composition export."""
    from fastapi import HTTPException

    name = model['name']
    if refresh:
        _refresh_dynamic_holdings(name)
    latest = (supabase.table('airs_holding').select('as_of_date').eq('portefeuille', name))
    if datum:
        latest = latest.eq('as_of_date', datum)
    dates = latest.order('as_of_date', desc=True).limit(1).execute().data or []
    if not dates:
        raise HTTPException(409, 'No saved AIRS account holdings. Select Refresh both.')
    mark = str(dates[0]['as_of_date'])[:10]
    holdings, offset = [], 0
    while True:
        batch = (supabase.table('airs_holding').select('holding_name,isin,weight,currency,current_price_local,valuation_component,quantity,current_value_eur,start_value_eur')
                 .eq('portefeuille', name).eq('as_of_date', mark).order('id')
                 .range(offset, offset + 499).execute().data or [])
        # Accrued interest is an accounting component, not a second bond position.
        # Keep it in AIRS accounting storage, but exclude it from connection/price checks.
        holdings.extend(batch)
        if len(batch) < 500:
            break
        offset += len(batch)
    return {'portfolio_id': model['id'], 'portfolio': name, 'datum': mark, 'dates': [mark],
            'rows': [{'Fonds': row['holding_name'], 'ISINCode': row.get('isin'),
                      'Percentage': float(row['weight']) * 100 if row.get('weight') is not None else None,
                      'valuta': row.get('currency'), 'price': row.get('current_price_local'),
                      'price_date': mark} for row in _security_holdings(holdings)]}


def composition(portfolio_id: int, datum: str | None = None, refresh: bool = False, saved_only: bool = False) -> dict:
    from jobs import JobCancelled
    from airs_scanner import fetch_portfolio_positions_sync, has_fixed_model
    from airs_vermogen import _LOCK, _acquire_session
    from fastapi import HTTPException
    from routers import _airs_portfolio_store as store

    models = (supabase.table('airs_model_portfolio').select('id,name,portfolio_type')
              .eq('id', portfolio_id).limit(1).execute().data or [])
    if not models:
        raise HTTPException(404, 'Unknown AIRS model portfolio')
    model = models[0]
    if (model.get('portfolio_type') or '').strip().lower() == 'normaal':
        return _dynamic_composition(model, datum, refresh and not saved_only)
    if not has_fixed_model(model.get('portfolio_type')):
        return {'portfolio_id': portfolio_id, 'portfolio': model['name'], 'rows': [],
                'dates': [], 'datum': None, 'no_fixed_model': True}
    if not refresh:
        saved = store.load_positions(portfolio_id)
        if saved and saved.get('schema_version', 1) >= 1 and (datum is None or datum == saved.get('datum')):
            return {**saved, 'cache_upgrade_available': saved.get('schema_version', 1) < 3}
    if saved_only:
        raise HTTPException(409, 'No saved AIRS composition. Select Refresh compositions.')
    if not _acquire_session(wait=30):
        raise HTTPException(409, 'AIRS is busy with another refresh')
    try:
        from leonteq.progress import report
        report(f'Downloading the dated AIRS composition for {model["name"]}…',
               f'Gedateerde AIRS-samenstelling downloaden voor {model["name"]}…')
        raw = fetch_portfolio_positions_sync(portfolio_id, datum=datum)
        if datum is None:
            report(f'Saving {len(raw["rows"])} AIRS holdings…', f'{len(raw["rows"])} AIRS-posities opslaan…')
            store.save_positions(portfolio_id, raw['datum'], raw['rows'], raw.get('dates'))
        return {**raw, 'retrieved_at': datetime.now(timezone.utc).isoformat(), 'schema_version': 3}
    except (HTTPException, JobCancelled):
        raise
    except Exception:
        raise HTTPException(502, 'The AIRS composition could not be loaded') from None
    finally:
        _LOCK.release()
