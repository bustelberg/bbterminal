"""Explicit links between saved Leonteq portfolios and fixed AIRS models."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import HTTPException

from deps import supabase
from leonteq.portfolio_cache import _account_key, saved_portfolios

TABLE = 'leonteq_airs_portfolio_link'


def _account() -> str:
    key = _account_key()
    if not key:
        raise HTTPException(400, 'missing_credentials')
    return key


def options(kind: str = 'fixed') -> dict:
    from airs_scanner import has_fixed_model
    from routers import _airs_portfolio_store as store

    key = _account()
    saved = saved_portfolios()
    models = store.load_portfolios()
    if kind not in ('fixed', 'dynamic'):
        raise HTTPException(400, 'invalid_portfolio_kind')
    models = [row for row in models if (has_fixed_model(row.get('portfolio_type')) if kind == 'fixed'
              else (row.get('portfolio_type') or '').strip().lower() == 'normaal')]
    names = {row['id']: row['name'] for row in models}
    links = (supabase.table(TABLE).select('leonteq_name,airs_portfolio_id,updated_at')
             .eq('account_key', key).order('leonteq_name').execute().data or [])
    return {
        'leonteq_portfolios': [{'name': row['name'], 'sponsor': row.get('sponsor')}
                              for row in saved.get('portfolios', [])],
        'airs_portfolios': [{'id': row['id'], 'name': row['name'], 'omschrijving': row.get('omschrijving'),
                             'portfolio_type': row.get('portfolio_type')}
                            for row in models],
        'links': [{**row, 'airs_name': names.get(row['airs_portfolio_id'])} for row in links if row['airs_portfolio_id'] in names],
    }


def connect(leonteq_name: str, airs_portfolio_id: int, kind: str = 'fixed') -> dict:
    key = _account()
    available = options(kind)
    if not any(row['name'] == leonteq_name for row in available['leonteq_portfolios']):
        raise HTTPException(400, 'unknown_leonteq_portfolio')
    target = next((row for row in available['airs_portfolios'] if row['id'] == airs_portfolio_id), None)
    if target is None:
        raise HTTPException(400, f'{kind}_airs_portfolio_required')
    link = {'leonteq_name': leonteq_name, 'airs_portfolio_id': airs_portfolio_id,
            'updated_at': datetime.now(timezone.utc).isoformat()}
    supabase.table(TABLE).upsert({'account_key': key, **link},
        on_conflict='account_key,leonteq_name,airs_portfolio_id').execute()
    return {**link, 'airs_name': target['name']}


def disconnect(leonteq_name: str, airs_portfolio_id: int) -> dict:
    (supabase.table(TABLE).delete().eq('account_key', _account())
     .eq('leonteq_name', leonteq_name).eq('airs_portfolio_id', airs_portfolio_id).execute())
    return {'deleted': True}
