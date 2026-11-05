"""Durable last-successful compositions, isolated by configured LYNQS account."""
from __future__ import annotations

import hashlib
import logging
import os

from deps import supabase
from leonteq.portal_login import check_login

log = logging.getLogger(__name__)
TABLE = 'leonteq_composition_snapshot'


def _read(account_key: str, name: str) -> dict | None:
    try:
        rows = (supabase.table(TABLE).select('payload').eq('account_key', account_key)
                .eq('portfolio_name', name).limit(1).execute().data or [])
        payload = rows[0]['payload'] if rows else None
        return payload if payload and payload.get('success') and payload.get('groups') else None
    except Exception:
        log.warning('Leonteq composition snapshot could not be read')
        return None


def _write(account_key: str, name: str, payload: dict) -> bool:
    try:
        supabase.table(TABLE).upsert({'account_key': account_key, 'portfolio_name': name,
            'payload': payload, 'retrieved_at': payload['retrieved_at']},
            on_conflict='account_key,portfolio_name').execute()
        return True
    except Exception:
        log.warning('Leonteq composition snapshot could not be saved')
        return False


def composition(name: str, refresh: bool = False, saved_only: bool = False) -> dict:
    user = os.environ.get('LEONTEQ_USER') or os.environ.get('LYNQS_USER', '')
    password = os.environ.get('LEONTEQ_PASSWORD') or os.environ.get('LYNQS_PASS', '')
    if not user or not password:
        return {'success': False, 'status': 'missing_credentials'}
    account_key = hashlib.sha256(user.encode()).hexdigest()
    saved = _read(account_key, name)
    if saved and not refresh:
        return {**saved, 'cached': True}
    if saved_only:
        return {'success': False, 'status': 'not_cached'}
    result = check_login(name)
    if result.get('success'):
        persisted = _write(account_key, name, result)
        return {**result, 'cached': False, 'snapshot_saved': persisted}
    if saved:
        return {**saved, 'cached': True, 'refresh_error': result['status']}
    return result
