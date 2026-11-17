"""One selected-portfolio payload and one streamed, forced source refresh."""
from datetime import date

from fastapi import HTTPException

from deps import supabase
from services import portfolio_valuation as V, valuation_workbook as W


def selection(portfolio_id):
    matches = [row for row in V.portfolios() if row.get('id') == portfolio_id]
    if len(matches) != 1:
        raise HTTPException(404, 'Selected dashboard portfolio is missing or ambiguous.')
    return matches[0]


def _leonteq_identity(selected, *, refresh=False):
    from leonteq import management, portfolio_cache
    entry = next((item for item in management.entries()
                  if management.key(item['display_name']) == management.key(selected['name'])), None)
    catalogue = portfolio_cache.portfolios(refresh=True) if refresh else portfolio_cache.saved_portfolios()
    if refresh and (not catalogue.get('success') or catalogue.get('refresh_error') or not catalogue.get('snapshot_saved')):
        raise HTTPException(502, 'Leonteq catalogue refresh failed. No AIRS fallback was used.')
    matched = management.resolve(entry, catalogue.get('portfolios') or []) if entry else None
    if not matched:
        raise HTTPException(503, 'Selected Topselectie is unavailable in Leonteq. No AIRS fallback was used.')
    return matched['name']


def holdings(selected):
    if selected.get('management_group') != 'topselecties':
        rows, evidence = V.company_holdings(selected['id'])
        return rows, {**evidence, 'source': 'AIRS'}
    from leonteq import composition_cache
    snapshot = composition_cache.composition(_leonteq_identity(selected), saved_only=True)
    if not snapshot.get('success') or not snapshot.get('groups'):
        raise HTTPException(503, 'Leonteq holdings are not saved. Refresh this portfolio; no AIRS fallback is used.')
    rows = {}
    for group in snapshot['groups']:
        # Valuation covers companies, not cash, bonds or funds.
        if str(group.get('name') or '').casefold() not in ('stock', 'stocks', 'share', 'shares', 'equities', 'equity'):
            continue
        for position in group.get('positions') or []:
            isin = position.get('isin')
            weight = V.number(str(position.get('weight') or '').replace('%', ''))
            if not weight or weight <= 0:
                continue
            identity = isin or f'missing:{len(rows)}'
            row = rows.setdefault(identity, {'isin': isin, 'name': position.get('name') or isin, 'weight_pct': 0})
            row['weight_pct'] += weight
    return list(rows.values()), {'composition_date': snapshot.get('as_of'),
        'retrieved_at': snapshot.get('retrieved_at'), 'source': 'Leonteq'}


def _saved_bundle(selected, current):
    rows, evidence = current
    saved = (supabase.table(V.TABLE).select('payload,prepared_at').eq('portfolio_id', selected['id'])
             .limit(1).execute().data or [])
    valid = bool(saved and saved[0]['payload'].get('method', {}).get('version') == V.MODEL_VERSION)
    payload = saved[0]['payload'] if valid else {}
    previous = {row.get('isin'): row for row in payload.get('rows', [])}
    current_rows = [{**previous.get(row.get('isin'), {'status': 'pending',
                     'reason': 'Valuation has not been prepared yet.'}), **row} for row in rows]
    return {**payload, 'portfolio_id': selected['id'], 'method': V.METHOD, 'evidence': evidence,
        'ready': valid, 'rows': current_rows, 'prepared_at': saved[0]['prepared_at'] if valid else None,
        'stale': not valid or bool(payload.get('errors')) or payload.get('calculation_date') != date.today().isoformat()
            or payload.get('evidence', {}).get('composition_date') != evidence.get('composition_date')
            or {row.get('isin') for row in payload.get('rows', [])} != {row.get('isin') for row in rows}}


def read(portfolio_id):
    selected = selection(portfolio_id)
    saved = (supabase.table(V.TABLE).select('payload,prepared_at').eq('portfolio_id', portfolio_id).limit(1).execute().data or [])
    if saved and saved[0]['payload'].get('workbook') and saved[0]['payload'].get('method', {}).get('version') == V.MODEL_VERSION:
        payload = saved[0]['payload']
        expected = 'Leonteq' if selected.get('management_group') == 'topselecties' else 'AIRS'
        if payload.get('evidence', {}).get('source') == expected:
            settings = (supabase.table(W.TABLE).select('payload,updated_at').eq('portfolio_id', portfolio_id).limit(1).execute().data or [])
            workbook = payload['workbook']
            if settings:
                overrides = settings[0]['payload']
                workbook = {**workbook, 'parameters': {**workbook['parameters'], **overrides.get('parameters', {})},
                    'updated_at': settings[0]['updated_at'], 'rows': [{**row, 'assumptions': {**row['assumptions'],
                        **overrides.get('companies', {}).get(row.get('isin'), {})}} for row in workbook['rows']]}
            return {**payload, 'workbook': workbook, 'ready': True, 'prepared_at': saved[0]['prepared_at'],
                'stale': bool(payload.get('errors')) or payload.get('calculation_date') != date.today().isoformat()}
    bundle = _saved_bundle(selected, holdings(selected))
    return {**bundle, 'workbook': W.read(portfolio_id, prepared_bundle=bundle, include_membership=True)}


def _refresh_holdings(selected, check):
    if selected.get('management_group') == 'topselecties':
        from leonteq import composition_cache
        name = _leonteq_identity(selected, refresh=True)
        check()
        snapshot = composition_cache.composition(name, refresh=True)
        if not snapshot.get('success') or snapshot.get('refresh_error') or not snapshot.get('snapshot_saved'):
            raise HTTPException(502, 'Leonteq holdings could not be refreshed and saved. No AIRS fallback was used.')
    else:
        from services.airs_refresh import collect_model_compositions
        result = collect_model_compositions(on_event=lambda *_args, **_kwargs: None, force=True,
            portfolio_ids={selected['id']}, should_stop=lambda: _cancelled(check))
        check()
        if result is None:
            raise HTTPException(409, 'Another AIRS scan is running. Try again after it finishes.')
        row = next((item for item in result['selected'] if item['id'] == selected['id']), None)
        if result['errors'] or not row or row.get('holdings') is None or row.get('no_snapshot'):
            raise HTTPException(502, 'AIRS did not supply a refreshed, dated composition for this portfolio.')


def _cancelled(check):
    from jobs import JobCancelled
    try:
        check()
        return False
    except JobCancelled:
        return True


def refresh(portfolio_id, emit, check, *, refresh_holdings=True, source_cache=None):
    """One request owns the whole operation; each successful row is still saved."""
    from jobs import JobCancelled
    selected = selection(portfolio_id)
    lock = V._publication_lock(portfolio_id)
    if not lock.acquire(blocking=False):
        raise HTTPException(409, 'This portfolio is already being refreshed.')
    try:
        check()
        source = 'Leonteq' if selected.get('management_group') == 'topselecties' else 'AIRS'
        emit({'type': 'progress', 'done': 0, 'total': 0, 'message': f"{selected['name']} — refreshing holdings from {source}"})
        if refresh_holdings:
            _refresh_holdings(selected, check)
        current = holdings(selected)
        bundle = _saved_bundle(selected, current)
        emit({'type': 'holdings', 'rows': bundle['rows']})
        failures = {}
        for index, row in enumerate(current[0]):
            check()
            emit({'type': 'progress', 'done': index, 'total': len(current[0]), 'isin': row['isin'],
                  'message': f"{index + 1}/{len(current[0])} · {row['name']}"})
            try:
                if not row.get('isin'):
                    raise HTTPException(422, 'Holding has no verified ISIN. Source data could not be refreshed.')
                bundle = V.refresh_row(portfolio_id, row['isin'], existing=bundle, current_holdings=current, source_cache=source_cache)
                emit({'type': 'row', 'row': next(item for item in bundle['rows'] if item.get('isin') == row['isin'])})
            except JobCancelled:
                raise
            except Exception as exc:
                reason = exc.detail if isinstance(exc, HTTPException) and isinstance(exc.detail, str) else 'Source refresh failed. Previous saved data was kept.'
                identity = row.get('isin') or row['name']
                failures[identity] = f"{row['name']}: {reason}"
                emit({'type': 'row_error', 'message': failures[identity]})
            emit({'type': 'progress', 'done': index + 1, 'total': len(current[0]), 'message': row['name']})
        check()
        bundle = {**bundle, 'errors': failures, 'stale': bool(failures) or bundle.get('stale', False)}
        # Persist failures as well, so a later cached read cannot call a partial run current.
        if bundle.get('prepared_at'):
            supabase.table(V.TABLE).upsert({'portfolio_id': portfolio_id, 'prepared_at': bundle['prepared_at'],
                'payload': {key: value for key, value in bundle.items() if key not in ('prepared_at', 'ready', 'stale', 'workbook')}}).execute()
        final = {**bundle, 'workbook': W.read(portfolio_id, prepared_bundle=bundle, include_membership=True)}
        if final.get('prepared_at'):
            supabase.table(V.TABLE).upsert({'portfolio_id': portfolio_id, 'prepared_at': final['prepared_at'],
                'payload': {key: value for key, value in final.items() if key not in ('prepared_at', 'ready', 'stale')}}).execute()
        emit({'type': 'complete', 'data': final, 'failures': list(failures.values())})
        return final
    finally:
        lock.release()



def save_settings(portfolio_id, payload):
    selected = selection(portfolio_id)
    rows, _evidence = holdings(selected)
    return W.save(portfolio_id, payload, prepared_bundle={'rows': rows})
