"""Audit actual stored chart inputs before automatically fetching missing vendor feeds."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import threading
import time

from deps import supabase

# Missing bank gross profit, goodwill or a zero dividend is not proof of a broken feed.
# Audit these too, but only unambiguously missing core lines justify a statements fetch.
CORE = ('revenue', 'total_assets', 'shares', 'eps_nri', 'fcf_ps')
_lock = threading.Lock()
_recent: dict[tuple[str, str], float] = {}
_failed: dict[str, tuple[float, str]] = {}
_running: dict[str, threading.Lock] = {}


def audit(label: str, cadence: str, report=None) -> dict:
    from routers._benchmark_index import _members
    from routers._benchmark_fundamentals import _KEYS
    from routers._fundamental_backfill import company_rows, eligible
    from routers.earnings import _metric_codes

    profile = {}
    started = time.monotonic()
    members = _members(label, require_market_cap=False)
    profile['membership_ms'] = round((time.monotonic() - started) * 1000, 2)
    ids = [row['company_id'] for row in members]
    if not ids:
        raise ValueError(f'No companies were found for {label}')
    keys = tuple(dict.fromkeys((*_KEYS, *CORE, 'eps_nri_estimate')))
    codes = {key: tuple(_metric_codes(key)) for key in keys}
    if cadence == 'quarterly':
        codes = {key: tuple(code.replace('annuals__', 'quarterly__') for code in values)
                 if key != 'eps_nri_estimate' else () for key, values in codes.items()}
    started = time.monotonic()
    # Keep each metric's aliases together so period counts dedupe companies correctly.
    # A wide all-code request makes PostgreSQL scan the much larger metric history;
    # bounded per-metric reads use its code index and are cheap even for all of ACWI.
    stored_audit = {'presence': {}, 'periods': []}
    batches = []
    for key, values in codes.items():
        if not values:
            continue
        if report:
            report(f'Checking stored {key} values', {'metric': key, 'aliases': len(values)})
        began = time.monotonic()
        part = supabase.rpc('research_metric_presence', {
            'company_ids': ids, 'metric_codes': list(values),
            'code_keys': {code: key for code in values},
        }).execute().data
        if not isinstance(part, dict) or not isinstance(part.get('presence'), dict):
            raise RuntimeError('Could not verify stored benchmark data; no automatic fetch was started')
        for cid, found in part['presence'].items():
            stored_audit['presence'].setdefault(cid, []).extend(found)
        stored_audit['periods'].extend(part.get('periods', []))
        batches.append({'metric': key, 'aliases': len(values), 'ms': round((time.monotonic() - began) * 1000, 2)})
        if report:
            report(f'Stored {key} check finished', batches[-1])
    profile['metric_presence_rpc_ms'] = round((time.monotonic() - started) * 1000, 2)
    profile['metric_batches'] = batches
    if not isinstance(stored_audit, dict) or not isinstance(stored_audit.get('presence'), dict):
        raise RuntimeError('Could not verify stored benchmark data; no automatic fetch was started')
    presence = stored_audit['presence']
    started = time.monotonic()
    comps = company_rows(ids)
    profile['company_metadata_ms'] = round((time.monotonic() - started) * 1000, 2)
    counts = Counter()
    missing_fin, missing_est, unavailable = [], [], []
    cutoff = datetime.now(timezone.utc) - timedelta(days=1)

    def recently_checked(company, field):
        raw = company.get(field)
        if not raw:
            return False
        try:
            stamp = datetime.fromisoformat(str(raw).replace('Z', '+00:00'))
            return stamp.tzinfo is not None and stamp >= cutoff
        except ValueError:
            return False

    missing = []
    selection = []
    for cid in ids:
        stored = set(presence.get(str(cid), ()))
        absent = [key for key, values in codes.items() if values and not stored.intersection(values)]
        for key in keys:
            if codes[key] and key not in absent:
                counts[key] += 1
        if not absent:
            continue
        company = comps.get(cid)
        reason = eligible(company) if company else 'Company identity is missing'
        identity = {'company_id': cid, 'company': (company or {}).get('company_name'),
                    'ticker': (company or {}).get('gurufocus_ticker'),
                    'exchange': ((company or {}).get('gurufocus_exchange') or {}).get('exchange_code')}
        missing.append({**identity, 'metrics': absent})
        if reason:
            unavailable.append({**identity, 'reason': reason})
            continue
        if any(key in absent for key in CORE) and not recently_checked(company, 'financials_fetched_at'):
            missing_fin.append(cid)
        if 'eps_nri_estimate' in absent and not recently_checked(company, 'estimates_fetched_at'):
            missing_est.append(cid)
        selection.append({**identity, 'missing_metrics': absent,
                          'statements': 'request' if cid in missing_fin else 'present or checked within 24h',
                          'estimates': 'request' if cid in missing_est else 'present or checked within 24h'})
    return {'label': label, 'cadence': cadence, 'companies': len(ids),
            'companies_checked': [{'company_id': cid, 'company': (comps.get(cid) or {}).get('company_name'),
                                   'ticker': (comps.get(cid) or {}).get('gurufocus_ticker'),
                                   'exchange': ((comps.get(cid) or {}).get('gurufocus_exchange') or {}).get('exchange_code')}
                                  for cid in ids],
            'present_companies_by_metric': {key: counts[key] for key in keys if codes[key]}, 'missing': missing,
            'raw_reporting_by_year': stored_audit.get('periods', []),
            'profile': profile,
            'unavailable': unavailable, 'statements_to_fetch': missing_fin, 'estimates_to_fetch': missing_est,
            'selection': selection,
            'display_policy': 'ACWI: at least 500 usable companies per metric and period; other portfolios retain their percentage coverage rules.',
            'note': 'Presence includes all stored sources. Year counts describe supplier data, not per-period chart coverage. '
                    'Existing zero/negative values, reporting dates, '
                    'missing weights and unsupported company templates can still prevent a chart. Counts of any historical value are not chart-period coverage.'}


def _repair(ctx, label: str, cadence: str) -> str:
    from jobs import UserFacingJobError
    from routers._fundamental_fill import fill_company_ids
    start = time.monotonic()
    timings = {}
    key = (label, cadence)
    with _lock:
        failure = _failed.get(label)
        if failure and time.monotonic() - failure[0] < 900:
            ctx.progress(0, 0, failure[1], data_changed=False)
            raise UserFacingJobError(failure[1])
        if time.monotonic() - _recent.get(key, -float('inf')) < 86400:
            ctx.emit('diagnostic', 'This benchmark was already checked in the last day; repeated vendor work was skipped.')
            ctx.progress(1, 1, f'{label} was checked recently.', data_changed=False)
            return f'{label} was checked recently. Available data is being used.'
    def stage(name, fn):
        ctx.check()
        began = time.monotonic()
        try:
            return fn()
        finally:
            timings[name] = round((time.monotonic() - began) * 1000, 2)
            ctx.emit('diagnostic', f'{name}: {timings[name]:.2f} ms', profile=dict(timings))
    ctx.progress(0, 0, f'Checking {label} data…')
    receipts = []
    try:
        def report(message, profile):
            return ctx.emit('diagnostic', message, profile=profile)
        before = stage('stored_data_audit', lambda: audit(label, cadence, report=report))
        ctx.emit('diagnostic', 'Verified stored chart inputs', details=before)
        changed = bool(before['statements_to_fetch'] or before['estimates_to_fetch'])
        class QuietContext:
            check = ctx.check
            spent = ctx.spent
            @property
            def cancelled(self):
                return ctx.cancelled
            def emit(self, kind, message, **data):
                ctx.emit('diagnostic', message, details={'original_kind': kind, **data})
            def progress(self, done, total, message, **data):
                ctx.emit('diagnostic', message, details={'done': done, 'total': total, **data})
        for feed, ids in [('statements', before['statements_to_fetch']), ('estimates', before['estimates_to_fetch'])]:
            if ids:
                ctx.progress(0, 0, f'Loading missing {label} company data…')
                receipt = {}
                receipts.append(receipt)
                try:
                    stage(f'fetch_{feed}', lambda: fill_company_ids(QuietContext(), label, ids,
                          feeds=feed, force=True, only_due=False, receipt=receipt, best_effort=True))
                finally:
                    ctx.emit('diagnostic', f'{feed} request results', details={'feed': feed, 'receipt': receipt})
        after = stage('verification', lambda: audit(label, cadence, report=report)) if changed else before
        ctx.emit('diagnostic', 'Coverage after checking/fetching', details=after)
        if any(r.get('rows', 0) > 0 for r in receipts):
            import asyncio
            from routers import _blend_prewarm
            ctx.progress(0, 0, f'{label}: saving updated charts…')
            stage('save_chart_snapshot', lambda: asyncio.run(
                _blend_prewarm._warm_one(label, cadence, _blend_prewarm._generation)))
        with _lock:
            _recent[key] = time.monotonic()
        ctx.progress(1, 1, f'{label} check finished. Available data is ready.',
                     data_changed=any(r.get('rows', 0) > 0 for r in receipts))
        return (f'{label} data checked. Some company figures are still unavailable.' if after['missing']
                else f'{label} data is ready.')
    except Exception as error:
        import traceback
        ctx.emit('diagnostic', 'Repair stopped before completion',
                 details={'error_type': type(error).__name__, 'traceback': traceback.format_exc(),
                          'receipts': receipts})
        ctx.progress(0, 0, 'Available data has been kept.',
                     data_changed=any(not r or r.get('rows', 0) > 0 for r in receipts))
        if isinstance(error, UserFacingJobError):
            message = ('Some company data could not be downloaded. Existing data is still available. '
                       'Automatic updates will try again after 15 minutes.')
            if 'did not provide financial statements' in str(error):
                message = ('The data provider returned no financial statements for the companies checked. '
                           'Existing data is still available. Automatic updates will try again after 15 minutes.')
            with _lock:
                _failed[label] = (time.monotonic(), message)
            raise UserFacingJobError(message) from error
        raise
    finally:
        timings['total'] = round((time.monotonic() - start) * 1000, 2)
        ctx.emit('diagnostic', 'Total repair profile', profile=timings)


def repair(ctx, label: str, cadence: str) -> str:
    # Financials feeds contain both cadences. Do not refetch the same missing
    # companies concurrently when someone switches Annual/Quarterly mid-repair.
    with _lock:
        gate = _running.setdefault(label, threading.Lock())
    while not gate.acquire(timeout=1):
        ctx.check()
        ctx.progress(0, 0, f'{label} data is already being updated…')
    try:
        return _repair(ctx, label, cadence)
    finally:
        gate.release()
