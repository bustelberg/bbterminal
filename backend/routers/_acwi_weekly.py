"""Weekly best-effort ACWI acquisition and atomic annual/quarterly chart publication."""
from __future__ import annotations

import asyncio
from collections import Counter
from datetime import datetime, timedelta, timezone


def annual_priorities(payloads, snapshot, companies, growth_rows=()):
    """Rank companies by the thin chart periods their missing inputs could fill."""
    from routers._benchmark_snapshots import _capital, _value, derived_specs
    from routers._fundamental_blend import _weight_at

    caps = payloads['universe-period-caps'].get('caps', {})
    by_isin = {c['isin']: cid for cid, c in companies.items() if c.get('isin')}
    specs = {path: dict(variants) for path, (_, variants) in derived_specs().items()}
    specs['cash-return-inputs'].update(invested_capital=_capital, roic=lambda r, p: _value(r, 'roic', p))
    years = [str(year) for year in range(2017, datetime.now(timezone.utc).year)]
    priority = Counter()
    coverage = {}
    for path, variants in specs.items():
        rows = payloads[path]['rows']
        for metric, value_of in variants.items():
            # Reported and SBC-adjusted variants have the same input requirements.
            if metric.endswith('corrected') or metric == 'corrected':
                continue
            chart = snapshot['series'][path][metric]
            actual = {str(point['period']): point['companies'] for point in chart['coverage']}
            counts = coverage[f'{path}/{metric}'] = {}
            for year in years:
                weighted = [row for row in rows if _weight_at({
                    'weight': row['weight_pct'],
                    **({'weights': caps.get(row.get('isin'), {})} if caps else {}),
                }, year)]
                count = actual.get(year, 0)
                counts[year] = count
                # Continue filling beyond the display floor, with priority
                # given to periods that have enough weighted names to reach it.
                score = 1 + (500 - count if count < 500 <= len(weighted) else 0)
                for row in weighted:
                    cid = by_isin.get(row.get('isin'))
                    if cid is not None and value_of(row, year) is None:
                        priority[cid] += score
    observed = {(row['company_id'], row['metric_code'], str(row['target_date'])[:4])
                for row in growth_rows if row.get('numeric_value') is not None}
    growth_codes = {row['metric_code'] for row in growth_rows}
    for metric, counts in snapshot['growth'].get('member_counts', {}).items():
        actual = counts.get('reporting_by_period', {})
        coverage[metric] = {year: actual.get(year, 0) for year in years}
        if metric not in growth_codes:
            continue
        for year, count in coverage[metric].items():
            weighted = [cid for isin, cid in by_isin.items()
                        if _weight_at({'weights': caps.get(isin, {})}, year)]
            score = 1 + (500 - count if count < 500 <= len(weighted) else 0)
            for cid in weighted:
                if (cid, metric, year) not in observed:
                    priority[cid] += score
    return priority, coverage


def run(ctx, *, reserve, statements_only=False):
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
    ctx.progress(0, 0, 'Measuring usable ACWI companies by metric and year…')
    ctx.progress(0, 0, 'Checking ACWI company coverage…')
    members = _members('ACWI', require_market_cap=False)
    companies = company_rows([m['company_id'] for m in members])
    from routers.earnings import _bulk_blend_rows
    growth_rows = _bulk_blend_rows(list(companies), ['revenue', 'eps_nri', 'fcf_ps', 'shares'], 'annual')
    priority, annual_before = {}, {}

    def measure(payloads, snapshot):
        nonlocal priority, annual_before
        priority, annual_before = annual_priorities(payloads, snapshot, companies, growth_rows)

    asyncio.run(warm._warm_one('ACWI', 'annual', warm._generation, on_publish=measure))
    if not annual_before:
        raise RuntimeError('Could not measure annual ACWI chart coverage before backfill')
    asyncio.run(warm.ensure_saved('ACWI', 'quarterly'))
    budget = remaining_budget(supabase)
    # Four possible calls: statements, estimates, estimate history, and key ratios.
    room = {region: max(0, remaining - reserve) for region, remaining in budget.items()}
    # Leave room for a monthly history pass between weekly refreshes.
    cutoff = datetime.now(timezone.utc) - timedelta(days=1 if statements_only else 6)
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

    work = order_work(list(companies.values()))
    work.sort(key=lambda company: -priority.get(company['company_id'], 0))
    cost = 1 if statements_only else 4
    for company in work:
        reason = eligible(company)
        status = 'unavailable'
        if not reason and recent(company.get('financials_fetched_at')) and (
            statements_only or recent(company.get('estimates_fetched_at'))
        ):
            reason = 'Requested feeds checked within the cooldown'
            status = 'recent'
        if reason:
            skipped.append({'company_id': company['company_id'], 'company': company.get('company_name'), 'reason': reason, 'status': status})
            continue
        region = _region_for_exchange(company['gurufocus_exchange']['exchange_code'])
        if room.get(region, 0) < cost:
            deferred.append(company['company_id'])
            continue
        room[region] -= cost
        selected.append(company['company_id'])
    receipt = {}
    before = audit('ACWI', 'annual')
    ctx.emit('diagnostic', 'Weekly ACWI request plan', details={
        'members': len(members), 'selected': selected, 'skipped': skipped,
        'quota_deferred': deferred, 'quota_reserve_per_region': reserve, 'coverage_before': before})
    if selected:
        ctx.progress(0, len(selected), 'Loading available ACWI company figures…')
        fill_company_ids(ctx, 'ACWI', selected, feeds='statements' if statements_only else 'statements_estimates',
                         force=True, key_ratios=not statements_only, best_effort=True, receipt=receipt,
                         priority=priority)
    ctx.check()
    after = audit('ACWI', 'annual')
    snapshots = {}
    annual_after = {}

    def measure_after(payloads, snapshot):
        nonlocal annual_after
        _, annual_after = annual_priorities(payloads, snapshot, companies)

    for cadence in ('annual', 'quarterly'):
        ctx.check()
        ctx.progress(0, 0, f'Saving ACWI {cadence} charts and statistics…')
        began = datetime.now(timezone.utc).isoformat()
        asyncio.run(warm._warm_one('ACWI', cadence, warm._generation,
                                  on_publish=measure_after if cadence == 'annual' else None))
        saved = read('ACWI', cadence)
        if not saved or saved['built_at'] < began:
            raise RuntimeError(f'ACWI {cadence} rebuild did not publish; the last complete charts remain available')
        snapshots[cadence] = {'built_at': saved['built_at'], 'coverage': saved['growth'].get('member_counts', {})}
    result = {'members': len(members), 'attempted': receipt.get('attempted', 0), 'loaded': receipt.get('loaded', 0),
              'unavailable': receipt.get('failed', 0) + sum(c['status'] == 'unavailable' for c in skipped),
              'recently_checked': sum(c['status'] == 'recent' for c in skipped), 'quota_deferred': len(deferred),
              'receipt': receipt, 'coverage_before': before, 'coverage_after': after, 'snapshots': snapshots,
              'annual_chart_coverage_before': annual_before, 'annual_chart_coverage_after': annual_after,
              'note': 'Stored figures, usable chart contributors, and latest-year reporters are different counts. '
                      'Non-positive histories and missing weights can exclude a company even when data exists. '
                      'A year that has not yet been reported cannot be fetched or filled with projections.'}
    ctx.emit('diagnostic', 'Weekly ACWI coverage and publication results', details=result)
    return 'ACWI data checked; annual and quarterly charts saved. Available company figures are included.', result
