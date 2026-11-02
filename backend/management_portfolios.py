"""Management dashboard account scope, shared by display and acquisition."""
from __future__ import annotations

import re

MANAGEMENT_GROUPS = {
    'busdefensiefdyn': 'bustelberg', 'busneutraaldyn': 'bustelberg',
    'busbepoffensiefdyn': 'bustelberg', 'busoffensiefdyn': 'bustelberg',
    'bus20offdyn': 'bustelberg', 'busifdyn': 'imagine_fund',
    'topsdefbehdyn': 'toppenberg', 'topsneubehdyn': 'toppenberg',
    'topsbeoffbehdyn': 'toppenberg', 'topsoffbehdyn': 'toppenberg',
}

_GROUP_ORDER = ('bustelberg', 'toppenberg', 'topselecties', 'imagine_fund', 'test_topselecties')
_GROUP_LABELS = dict(zip(_GROUP_ORDER, ('Bustelberg portfolios', 'Toppenberg portfolios',
                                      'Building blocks', 'Imagine Fund', 'Test TopSelecties')))
_PROFILE_ORDER = (
    'bus20offdyn', 'busoffensiefdyn', 'busbepoffensiefdyn', 'busneutraaldyn', 'busdefensiefdyn',
    'topsoffbehdyn', 'topsbeoffbehdyn', 'topsneubehdyn', 'topsdefbehdyn',
)
_CORE_LABELS = dict(zip(_PROFILE_ORDER, (
    'Bustelberg 2.0 Offensief', 'Bustelberg Offensief', 'Bustelberg Beperkt Offensief',
    'Bustelberg Neutraal', 'Bustelberg Defensief', 'Toppenberg Offensief',
    'Toppenberg Beperkt Offensief', 'Toppenberg Neutraal', 'Toppenberg Defensief',
)))
_CORE_LABELS['busifdyn'] = 'Imagine Fund'


def key(name: str | None) -> str:
    return re.sub(r'[^a-z0-9]+', '', (name or '').casefold())


def is_dashboard_account(name: str | None) -> bool:
    from airs_portfolio_exclusions import is_excluded_portfolio, topselectie_test_entry_for_account
    from routers._management_topselecties import topselectie_for_account
    return bool(not is_excluded_portfolio(name) and (
        key(name) in MANAGEMENT_GROUPS or topselectie_for_account(name)
        or topselectie_test_entry_for_account(name)))


def _account_details(name: str) -> tuple[str, str]:
    from airs_portfolio_exclusions import topselectie_test_entry_for_account
    from routers._management_topselecties import topselectie_for_account
    normalized = key(name)
    if normalized in MANAGEMENT_GROUPS:
        return MANAGEMENT_GROUPS[normalized], _CORE_LABELS.get(normalized, name)
    if entry := topselectie_for_account(name):
        return 'topselecties', entry['display_name']
    if entry := topselectie_test_entry_for_account(name):
        return 'test_topselecties', entry['display_name']
    return 'other', name


def dashboard_refresh_key(name: str) -> tuple[int, int, str, str]:
    """Dashboard group order, descending risk profiles, then alphabetical building blocks."""
    group, label = _account_details(name)
    normalized = key(name)
    return (_GROUP_ORDER.index(group) if group in _GROUP_ORDER else len(_GROUP_ORDER),
            _PROFILE_ORDER.index(normalized) if normalized in _PROFILE_ORDER else len(_PROFILE_ORDER),
            label.casefold(), normalized)


def dashboard_progress_label(name: str) -> str:
    group, label = _account_details(name)
    return f'{_GROUP_LABELS[group]} · {label}' if group in _GROUP_LABELS and _GROUP_LABELS[group] != label else label


def dashboard_model_rows(rows: list[dict]) -> list[dict]:
    """Paired models and reviewed counterparts, including manual child models."""
    from deps import supabase
    from routers._airs_account_links import _stem, list_account_links
    from routers._airs_ref import positions
    from routers._airs_strategy_map import strategies
    from airs_scanner import has_fixed_model

    names = set(MANAGEMENT_GROUPS)
    names.update(key(row.get('name')) for row in rows if is_dashboard_account(row.get('name')))
    for strategy in strategies():
        if any(is_dashboard_account(name) for name in strategy['airs_names']):
            names.update(key(name) for name in strategy['airs_names'])
            names.add(key(strategy.get('look_through_target')))
    stems = {_stem(name) for name in names if name}
    selected = {row['id'] for row in rows if key(row.get('name')) in names
                or is_dashboard_account(row.get('name')) or _stem(row.get('name') or '') in stems}
    selected.update(link['model_portfolio_id'] for link in list_account_links()['accounts']
                    if is_dashboard_account(link.get('portefeuille')) and link.get('model_portfolio_id') is not None)
    links = (supabase.table('airs_model_portfolio_link').select('isin,fonds,linked_portfolio_id').execute().data or [])
    targets = {(link.get('isin') or link.get('fonds')): link.get('linked_portfolio_id') for link in links}
    composition = positions()
    while True:
        children = {targets.get(row.get('isin') or row.get('fonds')) for row in composition
                    if row['portfolio_id'] in selected} - {None}
        if children <= selected:
            break
        selected.update(children)
    return [row for row in rows if row['id'] in selected
            and has_fixed_model(row.get('fixed') or row.get('portfolio_type'))]
