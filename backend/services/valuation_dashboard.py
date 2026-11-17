"""One selected-portfolio payload and one streamed, forced source refresh."""
from datetime import date, datetime, timezone, timedelta
from threading import Lock

from fastapi import HTTPException

from deps import supabase
from services import portfolio_valuation as V, valuation_workbook as W

_preparation_lock = Lock()


class NotPrepared(HTTPException):
    """A valid portfolio whose initial source snapshot has not been fetched."""

    def __init__(self):
        super().__init__(503, 'Portfolio holdings have not been prepared yet.')


def selection(portfolio_id):
    matches = [row for row in V.portfolios() if row.get('id') == portfolio_id]
    if len(matches) != 1:
        raise HTTPException(404, 'Selected dashboard portfolio is missing or ambiguous.')
    return matches[0]


def _leonteq_identity(selected, *, refresh=False):
    from leonteq import management, portfolio_cache
    entry = next((item for item in management.entries()
                  if management.key(item['display_name']) == management.key(selected['name'])), None)
    # Identity is stable configuration, not a price or holdings snapshot. Do not
    # require a second browser login before refreshing a known composition.
    catalogue = portfolio_cache.saved_portfolios()
    matched = management.resolve(entry, catalogue.get('portfolios') or []) if entry else None
    if refresh and not matched:
        catalogue = portfolio_cache.portfolios(refresh=True)
        if not catalogue.get('success') or catalogue.get('refresh_error') or not catalogue.get('snapshot_saved'):
            raise HTTPException(502, _leonteq_failure('catalogue', catalogue, selected['name']))
        matched = management.resolve(entry, catalogue.get('portfolios') or []) if entry else None
    if not matched:
        if not refresh and catalogue.get('status') == 'not_cached':
            raise NotPrepared()
        raise HTTPException(503, 'Selected Topselectie is unavailable in Leonteq. No AIRS fallback was used.')
    return matched['name']


def _leonteq_failure(resource, result, portfolio_name=None):
    # Only expose known status codes; browser exception text can contain secrets.
    reasons = {
        'missing_credentials': 'Leonteq credentials are not configured on the server',
        'busy': 'another Leonteq operation is running; retry after it finishes',
        'login_rejected': 'Leonteq rejected the configured login',
        'verification_required': 'Leonteq requires login verification',
        'timeout': 'the Leonteq request timed out',
        'browser_unavailable': 'the server browser runtime is unavailable',
        'browser_error': 'the server browser could not complete the Leonteq request',
        'session_failed': 'the Leonteq session could not be established',
        'portfolios_failed': 'Leonteq did not return its portfolio catalogue',
        'portfolio_not_found': 'the selected portfolio was not found in Leonteq',
        'composition_failed': 'Leonteq did not return a complete composition',
    }
    status = result.get('refresh_error') or result.get('status')
    reason = reasons.get(status, 'Leonteq did not return a successful response')
    if status == 'timeout':
        stage = (result.get('refresh_diagnostics') or result.get('diagnostics') or {}).get('stage')
        activity = {
            'browser_start': 'opening the Leonteq connection',
            'login_navigation': 'opening the Leonteq sign-in page',
            'login_form': 'loading the Leonteq sign-in form',
            'authentication': 'waiting for Leonteq sign-in to finish',
            'indices_navigation': 'loading the Leonteq portfolio list',
            'composition': 'loading the portfolio holdings from Leonteq',
        }.get(stage, 'loading the portfolio holdings from Leonteq' if resource == 'holdings' else 'loading the Leonteq portfolio list')
        reason = f'Leonteq timed out while {activity}'
    if result.get('success') and not result.get('refresh_error') and not result.get('snapshot_saved'):
        reason = 'the refreshed snapshot could not be saved'
    prefix = f'{portfolio_name}: ' if portfolio_name else ''
    return (f'{prefix}{reason[0].upper() + reason[1:]}. '
            'Company price and forecast updates have not started. Any previously saved valuation remains unchanged. '
            'Try Refresh again in a minute. If this keeps happening, check the Leonteq connection.')


def holdings(selected):
    if selected.get('management_group') != 'topselecties':
        rows, evidence = V.company_holdings(selected['id'])
        return rows, {**evidence, 'source': 'AIRS'}
    from leonteq import composition_cache
    snapshot = composition_cache.composition(_leonteq_identity(selected), saved_only=True)
    if snapshot.get('status') == 'not_cached':
        raise NotPrepared()
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
            # A complete portfolio bundle can outlive a separately repaired or
            # refreshed company feed. Re-read saved inputs when forecast years
            # or quotes are missing/expired, without making provider requests.
            if any('forecast' in row and (row['forecast'].get('fy1') is None
                   or row['forecast'].get('fy2') is None
                   or str(row['forecast'].get('fy1_date') or '') < date.today().isoformat())
                   or ('quote' in row and (row['quote'].get('price') is None
                       or str(row['quote'].get('price_date') or '') < (date.today() - timedelta(days=7)).isoformat()))
                   for row in workbook['rows']):
                current = W.read(portfolio_id, prepared_bundle=payload, include_membership=False)
                sources = {row.get('isin'): row for row in current['rows']}
                workbook = {**workbook, 'as_of': current.get('as_of', date.today().isoformat()),
                    'rows': [{**row, **{key: value for key, value in
                    sources.get(row.get('isin'), {}).items() if key in ('forecast', 'quote', 'ticker')}}
                    for row in workbook['rows']]}
            if settings:
                overrides = settings[0]['payload']
                workbook = {**workbook, 'parameters': {**workbook['parameters'], **overrides.get('parameters', {})},
                    'updated_at': settings[0]['updated_at'], 'rows': [{**row, 'assumptions': {**row['assumptions'],
                        **overrides.get('companies', {}).get(row.get('isin'), {})}} for row in workbook['rows']]}
            workbook = {**workbook, 'rows': [{**row, 'assumptions':
                W.normalize_assumptions(row.get('ticker'), row.get('assumptions', {}))} for row in workbook['rows']]}
            return {**payload, 'workbook': W.attach_fx(workbook), 'ready': True, 'prepared_at': saved[0]['prepared_at'],
                'stale': bool(payload.get('errors')) or payload.get('calculation_date') != date.today().isoformat()}
    try:
        current = holdings(selected)
    except NotPrepared:
        return _unprepared_bundle(portfolio_id)
    except ValueError as exc:
        if str(exc).startswith('No saved composition for portfolio '):
            return _unprepared_bundle(portfolio_id)
        raise
    bundle = _saved_bundle(selected, current)
    return {**bundle, 'workbook': W.read(portfolio_id, prepared_bundle=bundle, include_membership=True)}


def _unprepared_bundle(portfolio_id):
    return {'portfolio_id': portfolio_id, 'ready': False, 'stale': True,
            'rows': [], 'method': V.METHOD, 'detail': 'Portfolio data has not been prepared yet. Press Refresh to fetch it.',
            'workbook': {'portfolio_id': portfolio_id, 'as_of': None, 'rows': [],
                         'parameters': W.defaults()['parameters'], 'membership_legend': []}}


def _refresh_holdings(selected, check):
    if selected.get('management_group') == 'topselecties':
        from leonteq import composition_cache
        name = _leonteq_identity(selected, refresh=True)
        check()
        snapshot = composition_cache.composition(name, refresh=True)
        if not snapshot.get('success') or snapshot.get('refresh_error') or not snapshot.get('snapshot_saved'):
            raise HTTPException(502, _leonteq_failure('holdings', snapshot, selected['name']))
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
        succeeded = failed = 0
        for index, row in enumerate(current[0]):
            check()
            emit({'type': 'progress', 'done': index, 'total': len(current[0]), 'isin': row['isin'],
                  'message': f"{index + 1}/{len(current[0])} · {row['name']}"})
            try:
                if not row.get('isin'):
                    raise HTTPException(422, 'Holding has no verified ISIN. Source data could not be refreshed.')
                bundle = V.refresh_row(portfolio_id, row['isin'], existing=bundle, current_holdings=current, source_cache=source_cache)
                workbook_row = W.read(portfolio_id, only_isin=row['isin'], prepared_bundle=bundle, include_membership=False)
                succeeded += 1
                emit({'type': 'row', 'row': next(item for item in bundle['rows'] if item.get('isin') == row['isin']),
                      'workbook': workbook_row})
            except JobCancelled:
                raise
            except Exception as exc:
                reason = exc.detail if isinstance(exc, HTTPException) and isinstance(exc.detail, str) else 'Source refresh failed. Previous saved data was kept.'
                identity = row.get('isin') or row['name']
                failures[identity] = f"{row['name']}: {reason}"
                failed += 1
                emit({'type': 'row_error', 'message': failures[identity]})
            emit({'type': 'progress', 'done': index + 1, 'total': len(current[0]), 'message': row['name']})
        check()
        bundle = {**bundle, 'errors': failures, 'stale': bool(failures) or bundle.get('stale', False)}
        # Persist failures as well, so a later cached read cannot call a partial run current.
        if bundle.get('prepared_at'):
            supabase.table(V.TABLE).upsert({'portfolio_id': portfolio_id, 'prepared_at': bundle['prepared_at'],
                'payload': {key: value for key, value in bundle.items() if key not in ('prepared_at', 'ready', 'stale', 'workbook')}},
                on_conflict='portfolio_id').execute()
        final = {**bundle, 'workbook': W.read(portfolio_id, prepared_bundle=bundle, include_membership=True)}
        gaps = [item['code'] for item in final['workbook'].get('membership_legend', []) if item['status'] != 'complete']
        if gaps and refresh_holdings:
            from services import portfolio_membership
            from scheduler_runtime.context import LogContext
            class Context(LogContext):
                def check(self):
                    check()

                @property
                def cancelled(self):
                    return _cancelled(check)
            context = Context(step=lambda done, total, message: emit({
                'type': 'progress', 'done': done, 'total': total, 'message': message}))
            try:
                portfolio_membership.refresh_holdings(context, source='all')
            except JobCancelled:
                raise
            except Exception:
                failures['memberships'] = 'Portfolio membership sources could not all be refreshed. Completed data was saved.'
            check()
            final['workbook'] = W.read(portfolio_id, prepared_bundle=bundle, include_membership=True)
            gaps = [item['code'] for item in final['workbook'].get('membership_legend', []) if item['status'] != 'complete']
        if gaps:
            failures['memberships'] = 'Membership coverage is incomplete for: ' + ', '.join(gaps)
        final.update(errors=failures, stale=bool(failures) or final.get('stale', False))
        if not current[0] and not failures:
            # A valid all-cash/bond portfolio needs a prepared empty workbook;
            # no company row exists to create its publication timestamp.
            final.update(prepared_at=datetime.now(timezone.utc).isoformat(), ready=True,
                         calculation_date=date.today().isoformat(), stale=False)
        if final.get('prepared_at'):
            supabase.table(V.TABLE).upsert({'portfolio_id': portfolio_id, 'prepared_at': final['prepared_at'],
                'payload': {key: value for key, value in final.items() if key not in ('prepared_at', 'ready', 'stale')}},
                on_conflict='portfolio_id').execute()
        emit({'type': 'complete', 'data': final, 'failures': list(failures.values()),
              'counts': {'succeeded': succeeded, 'failed': failed, 'total': len(current[0])}})
        return final
    finally:
        lock.release()



def save_settings(portfolio_id, payload):
    selected = selection(portfolio_id)
    rows, _evidence = holdings(selected)
    return W.save(portfolio_id, payload, prepared_bundle={'rows': rows})


def prepare_all(context):
    """Bootstrap source snapshots and publish complete bundles, also on a cold DB."""
    if not _preparation_lock.acquire(blocking=False):
        raise HTTPException(409, 'Portfolio preparation is already running.')
    try:
        return _prepare_all(context)
    finally:
        _preparation_lock.release()


def _prepare_all(context):
    from services import airs_refresh, portfolio_membership
    from scheduler_runtime.execution import Cancelled
    from jobs import JobCancelled
    summary = {'saved': [], 'errors': {}}
    roster = V.portfolios()
    if not roster or any(row.get('id') is None for row in roster):
        # Account discovery creates the dashboard roster; model discovery makes
        # reviewed strategy-map pairings possible without manual seeding.
        for label, collect in (('accounts', airs_refresh.refresh_accounts), ('models', airs_refresh.scan_models)):
            context.check()
            context.progress(0, 0, f'Discovering AIRS {label}…')
            try:
                collect(context, force=True)
            except (Cancelled, JobCancelled):
                raise
            except Exception as exc:
                summary['errors'][label] = f'AIRS {label} discovery failed ({type(exc).__name__}).'
    context.check()
    try:
        portfolio_membership.refresh_holdings(context, source='all', all_portfolios=True)
    except (Cancelled, JobCancelled):
        raise
    except Exception as exc:
        summary['errors']['holdings'] = str(exc)
    from leonteq.management import prepare_contributions
    contributions = prepare_contributions(context)
    summary['leonteq_contributions'] = contributions
    if contributions['errors']:
        summary['errors']['leonteq_contributions'] = contributions['errors']
    roster = V.portfolios()
    selected = {row['id']: row for row in roster if row.get('id') is not None}
    summary['unavailable_portfolios'] = [row for row in roster if row.get('id') is None]
    for row in summary['unavailable_portfolios']:
        summary['errors'][row.get('account') or row['name']] = row.get('unavailable_reason') or 'Portfolio has no linked model.'
    source_cache = {}
    for index, (portfolio_id, portfolio) in enumerate(selected.items()):
        context.check()
        context.progress(index, len(selected), f"Preparing portfolio: {portfolio['name']}")
        try:
            payload = refresh(portfolio_id,
                lambda event: context.progress(event.get('done', 0), event.get('total', 0), event.get('message', '')),
                context.check, refresh_holdings='holdings' in summary['errors'], source_cache=source_cache)
            summary['saved'].append(portfolio_id)
            if payload.get('errors'):
                summary['errors'][str(portfolio_id)] = payload['errors']
        except (Cancelled, JobCancelled):
            raise
        except Exception as exc:
            summary['errors'][str(portfolio_id)] = (exc.detail if isinstance(exc, HTTPException) else type(exc).__name__)
    if not selected:
        summary['errors']['roster'] = 'No linked dashboard portfolios found after source discovery.'
    return summary
