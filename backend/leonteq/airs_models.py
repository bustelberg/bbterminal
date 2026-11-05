"""Refresh only the AIRS model catalogue, without downloading compositions."""
from __future__ import annotations

from datetime import datetime, timezone

from deps import supabase


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


def composition(portfolio_id: int, datum: str | None = None, refresh: bool = False, saved_only: bool = False) -> dict:
    from airs_scanner import fetch_portfolio_positions_sync, has_fixed_model
    from airs_vermogen import _LOCK, _acquire_session
    from fastapi import HTTPException
    from routers import _airs_portfolio_store as store

    models = (supabase.table('airs_model_portfolio').select('id,name,portfolio_type')
              .eq('id', portfolio_id).limit(1).execute().data or [])
    if not models:
        raise HTTPException(404, 'Unknown AIRS model portfolio')
    model = models[0]
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
        raw = fetch_portfolio_positions_sync(portfolio_id, datum=datum)
        if datum is None:
            store.save_positions(portfolio_id, raw['datum'], raw['rows'], raw.get('dates'))
        return {**raw, 'retrieved_at': datetime.now(timezone.utc).isoformat(), 'schema_version': 3}
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(502, 'The AIRS composition could not be loaded') from None
    finally:
        _LOCK.release()
