"""Manual matching overrides per portfolio connection; no source weights change."""
from datetime import datetime, timezone

from fastapi import HTTPException

from deps import supabase
from leonteq.portfolio_links import _account

TABLE = 'leonteq_holding_connection'


def _scope(name, portfolio_id):
    account = _account()
    existing = (supabase.table('leonteq_airs_portfolio_link').select('leonteq_name')
                .eq('account_key', account).eq('leonteq_name', name).eq('airs_portfolio_id', portfolio_id)
                .limit(1).execute().data or [])
    if not existing:
        raise HTTPException(404, 'portfolio_connection_not_found')
    return account


def _read(account, name, portfolio_id):
    rows = (supabase.table(TABLE).select('leonteq_key,airs_key').eq('account_key', account)
            .eq('leonteq_name', name).eq('airs_portfolio_id', portfolio_id).execute().data or [])
    return {'rules': rows}


def read(name, portfolio_id):
    return _read(_scope(name, portfolio_id), name, portfolio_id)


def save(name, portfolio_id, left, right):
    account = _scope(name, portfolio_id)
    if right is not None:
        rows = _read(account, name, portfolio_id)['rules']
        if any(row['airs_key'] == right and row['leonteq_key'] != left for row in rows):
            raise HTTPException(409, 'holding_already_connected')
    try:
        supabase.table(TABLE).upsert({'account_key': account, 'leonteq_name': name,
            'airs_portfolio_id': portfolio_id, 'leonteq_key': left, 'airs_key': right,
            'updated_at': datetime.now(timezone.utc).isoformat()},
            on_conflict='account_key,leonteq_name,airs_portfolio_id,leonteq_key').execute()
    except Exception as error:
        if getattr(error, 'code', None) == '23505':
            raise HTTPException(409, 'holding_already_connected') from None
        raise
    return _read(account, name, portfolio_id)
