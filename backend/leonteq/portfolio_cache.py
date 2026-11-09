"""Saved AMC catalogue: reading it never starts a browser or logs in."""
from __future__ import annotations

import hashlib
import logging
import os

from deps import supabase
from leonteq.portal_login import check_login

log = logging.getLogger(__name__)
TABLE = 'leonteq_portfolio_snapshot'


def _account_key() -> str | None:
    user = os.environ.get('LEONTEQ_USER') or os.environ.get('LYNQS_USER', '')
    password = os.environ.get('LEONTEQ_PASSWORD') or os.environ.get('LYNQS_PASS', '')
    return hashlib.sha256(user.encode()).hexdigest() if user and password else None


def saved_portfolios() -> dict:
    account_key = _account_key()
    if not account_key:
        return {'success': False, 'status': 'missing_credentials'}
    try:
        rows = supabase.table(TABLE).select('payload').eq('account_key', account_key).limit(1).execute().data or []
        payload = rows[0]['payload'] if rows else None
        if payload and payload.get('success') and payload.get('portfolios'):
            return {**payload, 'cached': True}
    except Exception:
        log.warning('Leonteq portfolio list snapshot could not be read')
    return {'success': False, 'status': 'not_cached'}


def portfolios(refresh: bool = False) -> dict:
    saved = saved_portfolios()
    if saved['status'] == 'missing_credentials' or (saved['success'] and not refresh):
        return saved
    result = check_login()
    if result.get('success'):
        from leonteq.progress import report
        report(f'Saving {len(result.get("portfolios") or [])} portfolios…',
               f'{len(result.get("portfolios") or [])} portefeuilles opslaan…')
        persisted = False
        try:
            supabase.table(TABLE).upsert({'account_key': _account_key(), 'payload': result,
                'retrieved_at': result['retrieved_at']}, on_conflict='account_key').execute()
            persisted = True
        except Exception:
            log.warning('Leonteq portfolio list snapshot could not be saved')
        return {**result, 'cached': False, 'snapshot_saved': persisted}
    if saved['success']:
        return {**saved, 'refresh_error': result['status']}
    return result
