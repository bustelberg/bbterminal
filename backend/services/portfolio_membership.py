"""Exact-ISIN membership from saved holdings, never workbook membership flags."""
from math import isfinite

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
