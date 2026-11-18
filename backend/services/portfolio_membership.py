"""Exact-ISIN membership from saved holdings, never workbook membership flags."""
from math import isfinite
import re

from management_portfolios import _CORE_LABELS, key

DEFINITIONS = [
    {'code': 'BB', 'name': 'Bustelberg', 'scope': 'All standard profiles; membership in at least one profile',
     'accounts': ['BUS_Defensief_Dyn', 'BUS_Neutraal_Dyn', 'BUS_BepOffensief_Dyn', 'BUS_Offensief_Dyn']},
    {'code': 'BB2', 'name': 'Bustelberg 2.0 Offensief', 'scope': 'Offensief only', 'accounts': ['BUS_2.0_ OFF_DYN']},
    {'code': 'TB', 'name': 'Toppenberg', 'scope': 'All profiles; membership in at least one profile',
     'accounts': ['TOPS_DEF_BEH_DYN', 'TOPS_NEU_BEH_DYN', 'TOPS_BEOFF_BEH_DYN', 'TOPS_OFF_BEH_DYN']},
    {'code': 'IF', 'name': 'Imagine Fund', 'scope': 'Imagine Fund', 'accounts': ['BUS_IF_DYN']},
    {'code': 'MTS', 'name': 'MerkenTopSelectie'},
    {'code': 'FTS', 'name': 'FamilieTopSelectie'},
    {'code': 'ETS', 'name': 'EuropaTopSelectie'},
    {'code': 'DTS', 'name': 'DividendTopSelectie'},
    {'code': 'AI', 'name': 'AITopSelectie'},
    {'code': 'STAR', 'name': 'StarTopSelectie'},
    {'code': 'ATS', 'name': 'AzieTopSelectie'},
    {'code': 'MOM', 'name': 'MomentumTopSelectie'},
    {'code': 'OTS', 'name': 'ObligatieTopSelectie'},
    {'code': 'VTS', 'name': 'VastgoedTopSelectie'},
]


def positive(value):
    try:
        number = float(str(value).replace('%', '').replace('’', '').strip())
        return isfinite(number) and number > 0
    except (TypeError, ValueError):
        return False


def build(roster, load_airs, entries, catalogue, load_leonteq):
    """Independent source failures remain visible; absence is not a confirmed non-holding."""
    from leonteq.management import resolve
    by_account = {}
    for portfolio in roster:
        by_account.setdefault(key(portfolio.get('account')), []).append(portfolio)
    by_name = {entry['display_name']: entry for entry in entries}
    memberships, legend, cached_models = {}, [], {}
    for definition in DEFINITIONS:
        item = {k: v for k, v in definition.items() if k != 'accounts'}
        item.update(source='AIRS' if 'accounts' in definition else 'Leonteq', portfolios=[])
        if 'accounts' in definition:
            # The dashboard roster owns the active scope. Retired/hidden profiles
            # must not keep an otherwise fully loaded family permanently partial.
            # If the whole family is absent, retain unavailable evidence rather
            # than treating an empty roster (including a read failure) as complete.
            active_accounts = [account for account in definition['accounts'] if key(account) in by_account]
            for account in active_accounts or definition['accounts']:
                matches = by_account.get(key(account), [])
                profile = {'name': matches[0]['name'] if len(matches) == 1 else _CORE_LABELS.get(key(account), account),
                           'as_of': None, 'retrieved_at': None, 'available': False, 'loaded': False}
                try:
                    if len(matches) != 1 or matches[0].get('id') is None:
                        raise ValueError('Missing or ambiguous portfolio link')
                    model_id = matches[0]['id']
                    if model_id not in cached_models:
                        cached_models[model_id] = load_airs(model_id)
                    holdings, evidence = cached_models[model_id]
                    # An opaque linked certificate means its companies cannot be ruled out.
                    lookthrough = evidence.get('lookthrough') or {}
                    complete = not positive(lookthrough.get('opaque_pct')) and all(
                        holding.get('isin') or not positive(holding.get('weight_pct')) for holding in holdings)
                    profile.update(as_of=evidence.get('composition_date'), retrieved_at=evidence.get('retrieved_at'), available=bool(evidence.get('composition_date')) and complete, loaded=True)
                    if not complete:
                        profile['reason'] = 'Some company holdings lack an ISIN or linked certificate constituents.'
                    for holding in holdings:
                        if holding.get('isin') and positive(holding.get('weight_pct')):
                            memberships.setdefault(holding['isin'], {}).setdefault(item['code'], []).append(profile['name'])
                except Exception:
                    profile['reason'] = 'Saved portfolio holdings or its linked model are unavailable.'
                item['portfolios'].append(profile)
        else:
            profile = {'name': definition['name'], 'as_of': None, 'retrieved_at': None, 'available': False, 'loaded': False}
            try:
                entry = by_name.get(definition['name'])
                matched = resolve(entry, catalogue.get('portfolios') or []) if entry else None
                snapshot = load_leonteq(matched['name']) if matched else {}
                if not snapshot.get('success') or not snapshot.get('groups'):
                    raise ValueError('Missing Leonteq composition')
                profile.update(as_of=snapshot.get('as_of'), retrieved_at=snapshot.get('retrieved_at'), available=True, loaded=True)
                for group in snapshot['groups']:
                    for holding in group.get('positions') or []:
                        if (not holding.get('isin') and positive(holding.get('weight'))
                                and str(group.get('name') or '').casefold() not in ('cash', 'bonds')):
                            profile.update(available=False, reason='A Leonteq security has no verified ISIN.')
                        if holding.get('isin') and positive(holding.get('weight')):
                            memberships.setdefault(holding['isin'], {}).setdefault(item['code'], []).append(profile['name'])
            except Exception:
                profile['reason'] = 'Saved Leonteq holdings are unavailable; no AIRS fallback.'
            item['portfolios'].append(profile)
        available = sum(profile['available'] for profile in item['portfolios'])
        item['status'] = 'complete' if available == len(item['portfolios']) else 'partial' if any(p['loaded'] for p in item['portfolios']) else 'unavailable'
        item.setdefault('scope', 'Leonteq portfolio holdings')
        legend.append(item)
    return {'memberships': memberships, 'legend': legend,
            'complete': all(item['status'] == 'complete' for item in legend)}


def read():
    from services import portfolio_valuation
    from leonteq import management, portfolio_cache, composition_cache
    try:
        roster = portfolio_valuation.portfolios()
    except Exception:
        roster = []
    return build(roster, portfolio_valuation.company_holdings, management.entries(), portfolio_cache.saved_portfolios(),
                 lambda name: composition_cache.composition(name, saved_only=True))



def all_companies():
    """Union of saved company holdings across the management dashboard."""
    from leonteq import composition_cache, management, portfolio_cache
    from routers._airs_holding_isin import classify_bucket
    from routers._airs_portfolio_analysis import _grid, _is_fund
    from services import portfolio_valuation

    companies, unavailable, models = {}, set(), {}

    def add(holding, portfolio, weight):
        isin = str(holding.get('isin') or '').strip().upper()
        if not isin or not positive(weight):
            return
        name = holding.get('name') or isin
        if (portfolio_valuation.excluded_valuation_instrument(name, isin)
                or re.search(r'\b(?:ETF|ETN|ETC|UCITS|overnight return|money market)\b', name, re.I)
                or re.search(r'\s\d+(?:[.,]\d+|\s+\d+/\d+)?%?\s+\d{1,2}/\d{1,2}/\d{2,4}\s*$', name)):
            return
        company = companies.setdefault(isin, {'name': name, 'portfolios': set()})
        company['portfolios'].add(portfolio)

    for row in portfolio_valuation.portfolios():
        group = row.get('management_group')
        if group == 'topselecties':
            continue
        name, model_id = row['name'], row.get('id')
        if model_id is None:
            unavailable.add(name)
            continue
        scope = model_id, group == 'toppenberg'
        if scope not in models:
            try:
                models[scope] = portfolio_valuation.company_holdings(model_id, look_through=scope[1])
            except ValueError:
                models[scope] = None
        if models[scope] is None:
            unavailable.add(name)
            continue
        holdings, evidence = models[scope]
        if positive((evidence.get('lookthrough') or {}).get('opaque_pct')):
            unavailable.add(name)
        for holding in holdings:
            if not holding.get('isin') and positive(holding.get('weight_pct')):
                unavailable.add(name)
            add(holding, name, holding.get('weight_pct'))

    catalogue = portfolio_cache.saved_portfolios().get('portfolios') or []
    leonteq_positions = []
    non_company_isins = set()
    for entry in management.entries():
        name = entry['display_name']
        matched = management.resolve(entry, catalogue)
        snapshot = composition_cache.composition(matched['name'], saved_only=True) if matched else {}
        if not snapshot.get('success') or not snapshot.get('groups'):
            unavailable.add(name)
            continue
        for group in snapshot['groups']:
            category = str(group.get('name') or '').strip().casefold()
            positions = group.get('positions') or []
            if category in ('bond', 'bonds', 'fund', 'funds', 'cash', 'certificate', 'certificates', 'etf', 'etn', 'note', 'notes'):
                non_company_isins.update(str(holding['isin']).strip().upper() for holding in positions if holding.get('isin'))
            elif category in ('share', 'shares', 'equity', 'equities', 'stock', 'stocks'):
                leonteq_positions.extend((name, holding) for holding in positions)
    for isin in non_company_isins:
        companies.pop(isin, None)
    grid = _grid(sorted(set(companies) | {holding['isin'] for _, holding in leonteq_positions if holding.get('isin')}))
    for name, holding in leonteq_positions:
        if not _is_fund(grid.get(holding.get('isin')), holding.get('name') or ''):
            if not holding.get('isin') and positive(holding.get('weight')):
                unavailable.add(name)
            add(holding, name, holding.get('weight'))
    for isin, company in list(companies.items()):
        asset = grid.get(isin) or {}
        fund = _is_fund(asset, company['name'])
        if isin in non_company_isins or fund or classify_bucket(None, fund, isin, company['name'], asset) in ('Bonds', 'Cash'):
            del companies[isin]
    rows = _valuation_issuers(companies, grid)
    return {'companies': sorted(rows, key=lambda row: (row['name'].casefold(), row['isins'][0])),
            'unavailable_portfolios': sorted(unavailable, key=str.casefold)}


def _valuation_issuers(holdings, grid):
    from deps import fetch_in_chunks, supabase
    from services import company_notes_identity, portfolio_valuation

    unresolved = [{'isin': isin} for isin in sorted(holdings) if not (grid.get(isin) or {}).get('company_id')]
    links, _ = company_notes_identity.load(unresolved, supabase) if unresolved else ({}, {})
    columns = 'company_id,isin,company_name,gurufocus_ticker'

    def lookup(column, values):
        return fetch_in_chunks(sorted(values), lambda group: supabase.table('company').select(columns)
                               .in_(column, group).is_('out_of_scope_at', 'null').execute())

    ids = {asset['company_id'] for isin, asset in grid.items() if isin in holdings and asset.get('company_id')}
    ids.update(link['company_id'] for link in links.values() if link.get('company_id'))
    references = {portfolio_valuation.reference_isin(isin) for isin in holdings}
    records = {row['company_id']: row for row in lookup('company_id', ids)
               + lookup('isin', references | set(holdings))
               + lookup('company_name', {holding['name'] for holding in holdings.values()})}
    return _merge_valuation_issuers(holdings, grid, links, records)


def _merge_valuation_issuers(holdings, grid, links, records):
    from index_universe.acwi.identity import issuer_key
    from services.portfolio_valuation import reference_isin

    by_isin, by_name = {}, {}
    for row in records.values():
        if row.get('isin'):
            by_isin.setdefault(row['isin'], []).append(row)
        if row.get('company_name'):
            by_name.setdefault(issuer_key(row['company_name']), []).append(row)

    issuers = {}
    for isin, company in sorted(holdings.items()):
        asset = grid.get(isin) or {}
        reference = reference_isin(isin)
        known_id = asset.get('company_id') or (links.get(isin) or {}).get('company_id')
        exact = by_isin.get(reference, [])
        exact_record = exact[0] if len(exact) == 1 else None
        # An international issue ISIN can resolve to a company's metadata even
        # when the held instrument is its debt. Require equity evidence for it.
        if isin.startswith('XS') and (not exact_record or str(asset.get('asset_class') or '').casefold() != 'equity'):
            continue
        linked_record = records.get(known_id)
        record = exact_record or linked_record
        if linked_record and linked_record.get('gurufocus_ticker') and not (record or {}).get('gurufocus_ticker'):
            record = linked_record
        name = (record or {}).get('company_name') or asset.get('name') or company['name']
        identity = issuer_key(name) or isin
        candidates = by_name.get(identity, [])
        if not (record or {}).get('gurufocus_ticker'):
            ticker_candidates = [candidate for candidate in candidates if candidate.get('gurufocus_ticker')]
            if len(ticker_candidates) == 1:
                record = ticker_candidates[0]
            elif not record and len(candidates) == 1:
                record = candidates[0]
        issuer = issuers.setdefault(identity, {'name': name, 'isins': [], 'portfolios': set(), 'choice': None})
        score = (bool((record or {}).get('gurufocus_ticker')), reference == isin, bool(record))
        if issuer['choice'] is None or score > issuer['choice'][0]:
            issuer['choice'] = (score, (record or {}).get('isin') or isin, (record or {}).get('company_id'))
            issuer['name'] = name
        issuer['isins'].append(isin)
        issuer['portfolios'].update(company['portfolios'])
    return [{'name': row['name'], 'isins': row['isins'], 'valuation_isin': row['choice'][1],
             'company_id': row['choice'][2], 'portfolios': sorted(row['portfolios'], key=str.casefold)}
            for row in issuers.values()]


def refresh_holdings(ctx, *, source='all', all_portfolios=False):
    """Refresh membership holdings only, using each family's authoritative source."""
    from services import portfolio_valuation, airs_refresh
    from jobs import JobCancelled
    from leonteq import management, portfolio_cache, composition_cache

    if source not in ('all', 'airs', 'leonteq'):
        raise ValueError('Unknown holdings source')
    failures, done = [], 0
    roster = portfolio_valuation.portfolios() if source in ('all', 'airs') else []
    allowed = {key(account) for definition in DEFINITIONS for account in definition.get('accounts', [])}
    selected = [row for row in roster if (all_portfolios and row.get('management_group') != 'topselecties')
                or key(row.get('account')) in allowed]
    ids = {row['id'] for row in selected if row.get('id') is not None}
    leonteq_entries = management.entries() if source in ('all', 'leonteq') else []
    total = len(ids) + len(leonteq_entries)
    ctx.check()
    if source in ('all', 'airs'):
        if not selected:
            failures.append('No AIRS portfolio links were available')
        failures.extend(f"{row['name']}: no linked AIRS model" for row in selected if row.get('id') is None)
        names = {row['id']: row['name'] for row in selected if row.get('id') is not None}
        def on_event(kind, **event):
            if kind == 'count':
                message = 'holdings read from AIRS' if event.get('holdings') is not None else 'no dated holdings received from AIRS'
                ctx.progress(int(event.get('i') or 0), total, f"{names.get(event.get('id'), 'Portfolio')} — {message}")
        if ids:
            ctx.progress(0, total, 'Connecting to AIRS to refresh portfolio holdings…')
            try:
                result = airs_refresh.collect_model_compositions(on_event=on_event, force=True,
                    portfolio_ids=ids, should_stop=lambda: ctx.cancelled)
            except JobCancelled:
                raise
            except Exception:
                failures.append('AIRS could not finish reading portfolio holdings')
                result = {'selected': [], 'errors': []}
            ctx.check()
            if result is None:
                failures.append('Another AIRS scan is running; AIRS holdings were not refreshed')
            else:
                failed_ids = {row['id'] for row in result['errors']}
                returned = {row['id'] for row in result['selected']}
                failures.extend(f"{names[pid]}: AIRS holdings could not be refreshed" for pid in failed_ids)
                failures.extend(f"{names[pid]}: linked AIRS model was not found" for pid in ids - returned)
                failures.extend(f"{names[row['id']]}: AIRS supplied no dated composition" for row in result['selected']
                    if (row.get('no_snapshot') or row.get('holdings') is None) and row['id'] not in failed_ids)
            done = len(ids)
    if leonteq_entries:
        ctx.check()
        ctx.progress(done, total, 'Connecting to Leonteq to refresh Topselecties holdings…')
        catalogue = portfolio_cache.portfolios(refresh=True)
        if not catalogue.get('success') or catalogue.get('refresh_error') or not catalogue.get('snapshot_saved'):
            failures.append('Leonteq portfolio catalogue could not be refreshed and saved')
        else:
            for entry in leonteq_entries:
                ctx.check()
                label = entry['display_name']
                ctx.progress(done, total, f'{label} — reading holdings from Leonteq')
                matched = management.resolve(entry, catalogue.get('portfolios') or [])
                successful = False
                try:
                    snapshot = composition_cache.composition(matched['name'], refresh=True) if matched else {}
                    if not snapshot.get('success') or snapshot.get('refresh_error') or not snapshot.get('snapshot_saved'):
                        failures.append(f'{label}: Leonteq holdings could not be refreshed and saved')
                    else:
                        successful = True
                except Exception:
                    failures.append(f'{label}: Leonteq holdings could not be refreshed')
                done += 1
                ctx.progress(done, total, f'{label} — ' + ('holdings read from Leonteq' if successful else 'Leonteq holdings refresh failed'))
    ctx.check()
    if failures:
        raise RuntimeError('Holdings refresh incomplete. ' + '; '.join(failures) + '. Completed snapshots were saved.')
    return f'{done} portfolio holdings refreshed from ' + ('AIRS and Leonteq' if source == 'all' else source.upper() if source == 'airs' else 'Leonteq')
