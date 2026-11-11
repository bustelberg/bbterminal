"""Percentage-only AIRS updates, tied to a fixed model, date and remote row id."""
from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from time import perf_counter
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import urlencode, urlparse, parse_qs
import re

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


def _registered_instrument(payload, isin):
    if not isinstance(payload, list):
        raise HTTPException(502, 'AIRS instrument lookup returned an unreadable response.')
    matches = {}
    for row in payload:
        data = row.get('data') if isinstance(row, dict) else None
        if not isinstance(data, dict) or str(data.get('ISINCode') or '').strip().upper() != isin:
            continue
        if (not isinstance(row.get('value'), str) or not row['value'] or not row.get('field_value')
                or data.get('Fonds') != row['value']):
            raise HTTPException(502, 'AIRS instrument lookup returned incomplete selection data.')
        matches[row['value']] = {'value': row['value'], 'field_value': row['field_value'], 'isin': isin}
    if not matches:
        raise HTTPException(422, f'Instrument {isin} is not registered in AIRS. Register it manually, then fetch transactions again.')
    if len(matches) != 1:
        raise HTTPException(409, f'Multiple AIRS instruments match {isin}. Review the instrument registration manually.')
    return next(iter(matches.values()))


def _lookup_registered(isin, name, datum):
    from airs_scanner import BASE_URL, AirsHttpResponse, _session
    def job(page):
        # AIRS resolves "virtual/Fonds" using the current form's session context.
        # Looking up directly after login can return [] even for a registered instrument.
        page.goto(f'{BASE_URL}/modelportefeuillefixedList.php?' + urlencode(
            {'Portefeuille': name, 'Datum': datum, 'type': 1}), wait_until='domcontentloaded')
        if page.locator('#addNewRecord').count() != 1:
            raise HTTPException(502, 'AIRS Fixed lookup context is unavailable. Retry later.')
        response = page.request.get(f'{BASE_URL}/lookups/autocomplete.php?' + urlencode(
            {'object': 'virtual', 'field': 'Fonds', 'term': isin}))
        return AirsHttpResponse(response.body(), response.status, 'application/json', response.url)
    response = _session._submit(job)
    if response.status != 200:
        raise HTTPException(502, 'AIRS instrument lookup is unavailable. Retry later.')
    try:
        payload = json.loads(response.body)
    except (ValueError, TypeError):
        raise HTTPException(502, 'AIRS instrument lookup returned an unreadable response.') from None
    return _registered_instrument(payload, isin)


def _remote_create(name, datum, isin, percentage, instrument):
    from airs_scanner import BASE_URL, AirsHttpResponse, _session

    def is_save(response):
        parsed = urlparse(response.url)
        query = parse_qs(parsed.query)
        return (response.request.method == 'POST' and parsed.netloc == urlparse(BASE_URL).netloc
                and parsed.path.endswith('/modelportefeuillefixedEdit.php')
                and query.get('action') == ['update'] and query.get('saveType') == ['new']
                and query.get('Portefeuille') == [name] and query.get('Datum') == [datum]
                and query.get('type') == ['1'])

    def job(page):
        page.goto(f'{BASE_URL}/modelportefeuillefixedList.php?' + urlencode(
            {'Portefeuille': name, 'Datum': datum, 'type': 1}), wait_until='domcontentloaded')
        if page.locator('tr[data-lineid]').filter(has_text=re.compile(r'(?<![A-Z0-9])' + re.escape(isin) + r'(?![A-Z0-9])')).count():
            raise HTTPException(409, 'The instrument is already in AIRS Fixed. Fetch transactions again.')
        page.locator('#addNewRecord').click()
        form = page.locator('#addNewRecordHolder')
        with page.expect_response(lambda response: '/lookups/autocomplete.php' in response.url
                                  and parse_qs(urlparse(response.url).query).get('term') == [isin], timeout=15000) as receipt:
            form.locator('input[name="FondsLookup"]').fill(isin)
        lookup = receipt.value
        if lookup.status != 200:
            raise HTTPException(502, 'AIRS instrument lookup is unavailable. Retry later.')
        selected = _registered_instrument(lookup.json(), isin)
        if selected != instrument:
            raise HTTPException(409, 'AIRS instrument registration changed. Fetch transactions again.')
        option = page.locator('.ui-autocomplete:visible .ui-menu-item').filter(has_text=isin)
        option.first.wait_for(state='visible', timeout=5000)
        if option.count() != 1:
            raise HTTPException(409, 'AIRS could not uniquely select this ISIN. Review registration manually.')
        option.click()
        if form.locator('input[name="Fonds"]').input_value() != instrument['value']:
            raise HTTPException(409, 'AIRS selected a different instrument. No position was added.')
        form.locator('input[name="Percentage"]').fill(str(percentage))
        with page.expect_response(is_save, timeout=20000) as receipt:
            form.locator('#newInLine').click()
        response = receipt.value
        result = response.json()
        if response.status != 200 or result.get('success') is not True or result.get('saved') is False:
            raise HTTPException(422, 'AIRS did not confirm the new position. Fetch transactions before retrying.')
        return AirsHttpResponse(b'{"saved":true}', 200, 'application/json', response.url)

    job.retry_safe = False
    _session._submit(job)


def _persist_verified(portfolio_id, before, after):
    from airs_scanner import fetch_portfolio_positions_sync
    from routers import _airs_portfolio_store as store
    persisted = False
    try:
        latest = (after if before.get('dates') and after['datum'] == max(before['dates'])
                  else fetch_portfolio_positions_sync(portfolio_id))
        store.save_positions(portfolio_id, latest['datum'], latest['rows'], latest.get('dates'))
        persisted = True
    except Exception:
        pass  # AIRS was verified; the invalidated local snapshot will be repaired by its next read.
    return {**after, 'schema_version': 3, 'snapshot_saved': persisted,
            'retrieved_at': datetime.now(timezone.utc).isoformat()}


def add_position(portfolio_id: int, datum: str, isin: str, percentage: Decimal, leonteq_name: str) -> dict:
    from airs_scanner import fetch_portfolio_positions_sync, has_fixed_model
    from airs_vermogen import _LOCK, _acquire_session
    from leonteq import portfolio_links, composition_cache, holding_connections
    models = (supabase.table('airs_model_portfolio').select('id,name,portfolio_type')
              .eq('id', portfolio_id).limit(1).execute().data or [])
    if not models or not has_fixed_model(models[0].get('portfolio_type')):
        raise HTTPException(400, 'fixed_model_required')
    links = portfolio_links.options('fixed')['links']
    if not any(row['leonteq_name'] == leonteq_name and row['airs_portfolio_id'] == portfolio_id for row in links):
        raise HTTPException(409, 'The Fixed portfolio connection changed. Fetch transactions again.')
    rules = holding_connections.read(leonteq_name, portfolio_id)['rules']
    if any(row['leonteq_key'] == f'isin:{isin}' for row in rules):
        raise HTTPException(409, 'This holding has a manual connection rule. Review Connections before adding it.')
    source = composition_cache.composition(leonteq_name, refresh=True)
    if not source.get('success') or source.get('refresh_error') or not source.get('groups'):
        raise HTTPException(502, 'Fresh Leonteq weights are unavailable. No position was added.')
    positions = [p for group in source['groups'] for p in group['positions'] if (p.get('isin') or '').strip().upper() == isin]
    try:
        target = Decimal(str(positions[0]['weight']).strip().removesuffix('%').replace(',', '.')) if len(positions) == 1 else None
    except Exception:
        target = None
    if target is None or not target.is_finite() or target != percentage:
        raise HTTPException(409, 'The Leonteq weight changed or is ambiguous. Fetch transactions and review again.')
    if not _acquire_session(wait=30):
        raise HTTPException(409, 'airs_busy')
    submitted = False
    try:
        before = fetch_portfolio_positions_sync(portfolio_id, datum=datum)
        if before['datum'] != datum:
            raise HTTPException(409, 'AIRS composition date changed. Fetch transactions again.')
        if any((row.get('ISINCode') or '').strip().upper() == isin for row in before['rows']):
            raise HTTPException(409, 'The instrument is already in AIRS Fixed. Fetch transactions again.')
        instrument = _lookup_registered(isin, before['portfolio'], datum)
        supabase.table('airs_model_portfolio').update({'positions_schema_version': 0}).eq('id', portfolio_id).execute()
        submitted = True
        _remote_create(before['portfolio'], datum, isin, percentage, instrument)
        after = fetch_portfolio_positions_sync(portfolio_id, datum=datum, metadata=before)
        created = [row for row in after['rows'] if (row.get('ISINCode') or '').strip().upper() == isin]
        old_ids = {row.get('id') for row in before['rows']}
        if (len(created) != 1 or not created[0].get('id') or created[0]['id'] in old_ids
                or Decimal(str(created[0].get('Percentage'))) != percentage
                or created[0].get('Fonds') != instrument['value'] or after['datum'] != datum):
            raise HTTPException(502, 'New position save is unverified. Fetch transactions before retrying.')
        return _persist_verified(portfolio_id, before, after)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(502, 'New position save is unverified. Fetch transactions before retrying.'
                            if submitted else 'Could not read AIRS. No position was added.') from None
    finally:
        _LOCK.release()


def update_percentage(portfolio_id: int, row_id: int, datum: str, expected: Decimal, percentage: Decimal) -> dict:
    from airs_scanner import fetch_portfolio_positions_sync, has_fixed_model
    from airs_vermogen import _LOCK, _acquire_session

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
        if original.get('Percentage') is not None and Decimal(str(original['Percentage'])) == percentage:
            # A manual restore (or another completed request) already reached the requested
            # value. Verify from the fresh export and do not submit another native save.
            return {**_persist_verified(portfolio_id, before, before), 'already_at_target': True}
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
        with _timed('cache_refresh', portfolio_id, row_id):
            return _persist_verified(portfolio_id, before, after)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(502, 'save_unverified' if submitted else 'read_failed') from None
    finally:
        _LOCK.release()


def _cash_plan(composition, displayed_total):
    """Use full export precision; the native footer rounds the same total to two decimals."""
    rows = composition['rows']
    try:
        weights = [Decimal(str(row.get('Percentage'))) for row in rows]
        if not weights or not all(value.is_finite() for value in weights):
            raise ValueError
        total = sum(weights, Decimal(0))
        if abs(total - displayed_total) > Decimal('0.005'):
            raise ValueError
    except Exception:
        raise HTTPException(409, 'AIRS total and exported weights disagree or are incomplete. Read the latest total again.') from None
    cash = [row for row in rows if (row.get('Fonds') or '').strip().casefold() in
            {'liquiditeiten', 'cash', 'effectenrekening', 'liquide middelen'}]
    if len(cash) != 1 or not cash[0].get('id'):
        raise HTTPException(409, 'A single editable cash holding is required. Review cash in AIRS manually.')
    current = Decimal(str(cash[0]['Percentage']))
    target = current + Decimal(100) - total
    if not 0 <= target <= 100 or target != target.quantize(Decimal('0.000001')):
        raise HTTPException(409, 'Cash cannot bring this portfolio to 100%. Review the holdings in AIRS manually.')
    return {'total': total, 'displayed_total': displayed_total, 'row_id': int(cash[0]['id']),
            'cash_name': cash[0]['Fonds'], 'current': current, 'target': target,
            'datum': composition['datum'], 'balanced': total == 100}


def _native_total(name, datum):
    from lxml import html as lxml_html
    from airs_scanner import BASE_URL, _session, _decode_html
    html = _decode_html(_session.get_response(f'{BASE_URL}/modelportefeuillefixedList.php?' + urlencode(
        {'Portefeuille': name, 'Datum': datum, 'type': 1})))
    try:
        cells = lxml_html.fromstring(html).xpath('//*[contains(concat(" ", normalize-space(@class), " "), " tableTotal ")]')
        if len(cells) != 1:
            raise ValueError
        total = Decimal(cells[0].text_content().strip().replace(',', '.'))
        if not total.is_finite():
            raise ValueError
        return total
    except Exception:
        raise HTTPException(502, 'Could not read the AIRS Totaal footer. No cash change was sent.') from None


def balance_cash(portfolio_id, expected=None):
    from airs_scanner import fetch_portfolio_positions_sync, has_fixed_model
    from airs_vermogen import _LOCK, _acquire_session
    models = (supabase.table('airs_model_portfolio').select('id,name,portfolio_type')
              .eq('id', portfolio_id).limit(1).execute().data or [])
    if not models or not has_fixed_model(models[0].get('portfolio_type')):
        raise HTTPException(400, 'fixed_model_required')
    if not _acquire_session(wait=30):
        raise HTTPException(409, 'airs_busy')
    submitted = False
    try:
        before = fetch_portfolio_positions_sync(portfolio_id)
        plan = _cash_plan(before, _native_total(before['portfolio'], before['datum']))
        if expected is None:
            return {**plan, 'composition': before}
        if any(plan[key] != expected[key] for key in ('datum', 'row_id', 'total', 'current', 'target')):
            raise HTTPException(409, 'The latest AIRS weights changed. Read the total again before executing.')
        if plan['balanced']:
            return {**plan, 'composition': before}
        supabase.table('airs_model_portfolio').update({'positions_schema_version': 0}).eq('id', portfolio_id).execute()
        submitted = True
        _remote_update(before['portfolio'], before['datum'], plan['row_id'], plan['current'], plan['target'])
        after = fetch_portfolio_positions_sync(portfolio_id, datum=before['datum'], metadata=before)
        verified = _cash_plan(after, _native_total(after['portfolio'], after['datum']))
        def unchanged(data):
            return [row for row in data['rows'] if row.get('id') != plan['row_id']]
        def cash_identity(data):
            row = next(row for row in data['rows'] if row.get('id') == plan['row_id'])
            return {key: value for key, value in row.items() if key != 'Percentage'}
        if (not verified['balanced'] or verified['row_id'] != plan['row_id'] or verified['current'] != plan['target']
                or verified['cash_name'] != plan['cash_name'] or after['datum'] != before['datum']
                or unchanged(before) != unchanged(after) or cash_identity(before) != cash_identity(after)):
            raise HTTPException(502, 'Cash save is unverified. Read the latest total before retrying.')
        return {**verified, 'composition': _persist_verified(portfolio_id, before, after)}
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(502, 'Cash save is unverified. Read the latest total before retrying.'
                            if submitted else 'Could not read AIRS. No cash change was sent.') from None
    finally:
        _LOCK.release()
