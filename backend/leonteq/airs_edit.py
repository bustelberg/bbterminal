"""Percentage-only AIRS updates, tied to a fixed model, date and remote row id."""
from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from time import perf_counter
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import urlencode

from fastapi import HTTPException

from deps import supabase

_log = logging.getLogger(__name__)


@contextmanager
def _timed(stage, portfolio_id, row_id):
    started = perf_counter()
    try:
        yield
    finally:
        _log.warning('[airs_edit] portfolio=%s row=%s stage=%s seconds=%.2f',
                     portfolio_id, row_id, stage, perf_counter() - started)


def _remote_update(name, datum, row_id, expected, percentage):
    from airs_scanner import BASE_URL, AirsHttpResponse, _session

    def job(page):
        page.goto(f'{BASE_URL}/modelportefeuillefixedList.php?' + urlencode(
            {'Portefeuille': name, 'Datum': datum, 'type': 1}), wait_until='domcontentloaded')
        row = page.locator(f'tr[data-lineid="{row_id}"]')
        if row.count() != 1:
            raise HTTPException(409, 'row_changed')
        current = Decimal(row.locator('.PercentageVal').inner_text().strip().replace(',', '.'))
        if current != expected:
            raise HTTPException(409, 'percentage_changed')
        row.locator('.editInline').click()
        row.locator('input[name="Percentage"]').fill(str(percentage))
        endpoint = f'{BASE_URL}/modelportefeuillefixedEdit.php?action=update&id={row_id}'
        with page.expect_response(lambda response: response.url == endpoint and response.request.method == 'POST', timeout=20000) as receipt:
            row.locator('.saveInline').click()
        response = receipt.value
        result = response.json()
        if response.status != 200 or result.get('success') is not True or result.get('saved') is False:
            raise HTTPException(422, 'airs_rejected')
        return AirsHttpResponse(json.dumps({'saved': True}).encode(), 200, 'application/json', endpoint)

    # An uncertain POST outcome must never be retried automatically.
    job.retry_safe = False
    _session._submit(job)


def update_percentage(portfolio_id: int, row_id: int, datum: str, expected: Decimal, percentage: Decimal) -> dict:
    from airs_scanner import fetch_portfolio_positions_sync, has_fixed_model
    from airs_vermogen import _LOCK, _acquire_session
    from routers import _airs_portfolio_store as store

    models = (supabase.table('airs_model_portfolio').select('id,name,portfolio_type')
              .eq('id', portfolio_id).limit(1).execute().data or [])
    if not models or not has_fixed_model(models[0].get('portfolio_type')):
        raise HTTPException(400, 'fixed_model_required')
    with _timed('session_wait', portfolio_id, row_id):
        acquired = _acquire_session(wait=30)
    if not acquired:
        raise HTTPException(409, 'airs_busy')
    submitted = False
    try:
        with _timed('read_before', portfolio_id, row_id):
            before = fetch_portfolio_positions_sync(portfolio_id, datum=datum)
        matches = [row for row in before['rows'] if row.get('id') is not None and int(row['id']) == row_id]
        if len(matches) != 1:
            raise HTTPException(409, 'row_changed')
        original = matches[0]
        if original.get('Percentage') is None or Decimal(str(original['Percentage'])) != expected:
            raise HTTPException(409, 'percentage_changed')
        # Invalidate before writing AIRS: even a failed cache repair cannot serve
        # the pre-edit weights as a current, complete snapshot afterwards.
        supabase.table('airs_model_portfolio').update({'positions_schema_version': 0}).eq('id', portfolio_id).execute()
        submitted = True
        with _timed('native_save', portfolio_id, row_id):
            _remote_update(before['portfolio'], datum, row_id, expected, percentage)
        with _timed('verify_export', portfolio_id, row_id):
            after = fetch_portfolio_positions_sync(portfolio_id, datum=datum, metadata=before)
        changed = [row for row in after['rows'] if row.get('id') is not None and int(row['id']) == row_id]
        if (len(changed) != 1 or Decimal(str(changed[0].get('Percentage'))) != percentage
                or changed[0].get('Fonds') != original.get('Fonds')
                or changed[0].get('ISINCode') != original.get('ISINCode')):
            raise HTTPException(502, 'save_unverified')
        # Refresh the canonical newest snapshot, even when an older date was edited.
        persisted = False
        try:
            # The verified newest composition is already the canonical snapshot.
            # Historical edits still need a separate latest-date read.
            with _timed('cache_refresh', portfolio_id, row_id):
                latest = (after if before.get('dates') and datum == max(before['dates'])
                          else fetch_portfolio_positions_sync(portfolio_id))
                store.save_positions(portfolio_id, latest['datum'], latest['rows'], latest.get('dates'))
            persisted = True
        except Exception:
            pass  # The invalidated cache will be repaired by the next read.
        return {**after, 'schema_version': 3, 'snapshot_saved': persisted,
                'retrieved_at': datetime.now(timezone.utc).isoformat()}
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(502, 'save_unverified' if submitted else 'read_failed') from None
    finally:
        _LOCK.release()
