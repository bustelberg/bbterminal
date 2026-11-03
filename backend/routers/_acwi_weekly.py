"""Weekly best-effort ACWI acquisition and atomic annual/quarterly chart publication."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone


def run(ctx, *, reserve):
    from deps import supabase
    from ingest.api_usage import remaining_budget, _region_for_exchange
    from routers._benchmark_index import _members
    from routers._fundamental_backfill import company_rows, eligible
    from routers._fundamental_fill import fill_company_ids, order_work
    from routers._benchmark_repair import audit
    from routers import _blend_prewarm as warm
    from routers._benchmark_snapshots import read

    ctx.check()
    # Publish the data we already hold before any discovery or slow vendor work.
    # A first-time user must not wait for thousands of company requests to finish.
    for cadence in ('annual', 'quarterly'):
        ctx.check()
        ctx.progress(0, 0, f'Preparing saved ACWI {cadence} charts…')
        asyncio.run(warm.ensure_saved('ACWI', cadence))
    ctx.progress(0, 0, 'Checking ACWI company coverage…')
    members = _members('ACWI', require_market_cap=False)
    companies = company_rows([m['company_id'] for m in members])
    budget = remaining_budget(supabase)
    # Four possible calls: statements, estimates, estimate history, and key ratios.
    room = {region: max(0, remaining - reserve) for region, remaining in budget.items()}
    # A six-day cooldown avoids skipping next Saturday merely because last week's
    # company request completed a few minutes after the scheduled start time.
    cutoff = datetime.now(timezone.utc) - timedelta(days=6)
    selected, deferred = [], []
    skipped = [{'company_id': m['company_id'], 'company': m.get('company_name'),
                'reason': 'Company identity is missing', 'status': 'unavailable'}
               for m in members if m['company_id'] not in companies]

    def recent(value):
        if not value:
            return False
        try:
            date = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            return date.tzinfo is not None and date > cutoff
        except ValueError:
            return False

    for company in order_work(list(companies.values())):
        reason = eligible(company)
        status = 'unavailable'
        if not reason and recent(company.get('financials_fetched_at')) and recent(company.get('estimates_fetched_at')):
            reason = 'Both feeds checked within the last six days'
            status = 'recent'
        if reason:
            skipped.append({'company_id': company['company_id'], 'company': company.get('company_name'), 'reason': reason, 'status': status})
            continue
        region = _region_for_exchange(company['gurufocus_exchange']['exchange_code'])
        if room.get(region, 0) < 4:
            deferred.append(company['company_id'])
            continue
        room[region] -= 4
        selected.append(company['company_id'])
    receipt = {}
    before = audit('ACWI', 'annual')
    ctx.emit('diagnostic', 'Weekly ACWI request plan', details={
        'members': len(members), 'selected': selected, 'skipped': skipped,
        'quota_deferred': deferred, 'quota_reserve_per_region': reserve, 'coverage_before': before})
    if selected:
        ctx.progress(0, len(selected), 'Loading available ACWI company figures…')
        fill_company_ids(ctx, 'ACWI', selected, feeds='statements_estimates', force=True,
                         key_ratios=True, best_effort=True, receipt=receipt)
    ctx.check()
    after = audit('ACWI', 'annual')
    snapshots = {}
    for cadence in ('annual', 'quarterly'):
        ctx.check()
        ctx.progress(0, 0, f'Saving ACWI {cadence} charts and statistics…')
        began = datetime.now(timezone.utc).isoformat()
        asyncio.run(warm._warm_one('ACWI', cadence, warm._generation))
        saved = read('ACWI', cadence)
        if not saved or saved['built_at'] < began:
            raise RuntimeError(f'ACWI {cadence} rebuild did not publish; the last complete charts remain available')
        snapshots[cadence] = {'built_at': saved['built_at'], 'coverage': saved['growth'].get('member_counts', {})}
    result = {'members': len(members), 'attempted': receipt.get('attempted', 0), 'loaded': receipt.get('loaded', 0),
              'unavailable': receipt.get('failed', 0) + sum(c['status'] == 'unavailable' for c in skipped),
              'recently_checked': sum(c['status'] == 'recent' for c in skipped), 'quota_deferred': len(deferred),
              'receipt': receipt, 'coverage_before': before, 'coverage_after': after, 'snapshots': snapshots,
              'note': 'Stored figures, usable chart contributors, and latest-year reporters are different counts. '
                      'Non-positive histories and missing weights can exclude a company even when data exists. '
                      'A year that has not yet been reported cannot be fetched or filled with projections.'}
    ctx.emit('diagnostic', 'Weekly ACWI coverage and publication results', details=result)
    return 'ACWI data checked; annual and quarterly charts saved. Available company figures are included.', result
