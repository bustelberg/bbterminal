from types import SimpleNamespace

import pytest

from routers import _airs_portfolio_analysis as analysis, _airs_portfolio_links as links


def setup_links(monkeypatch, target=2):
    monkeypatch.setattr(links, 'resolve_links', lambda _db, _owner, rows: {
        links.link_key(r.get('isin'), r.get('fonds')): SimpleNamespace(linked_portfolio_id=target)
        for r in rows})


def test_sold_wrapper_uses_composition_before_exit_and_keeps_ledger_totals(monkeypatch):
    setup_links(monkeypatch)
    monkeypatch.setattr(analysis, 'ref_positions_for', lambda _: [
        {'datum': '2026-09-30', 'fonds': 'Microsoft', 'isin': 'MS', 'percentage': 60},
        {'datum': '2026-09-30', 'fonds': 'Visa', 'isin': 'V', 'percentage': 40},
        {'datum': '2026-10-20', 'fonds': 'New purchase', 'isin': 'NEW', 'percentage': 100}])
    calls = []
    def prices(isins, years):
        calls.append((isins, years))
        return {'MS': [('2026-10-10', 80), ('2026-10-12', 100), ('2026-10-23', 110)],
                'V': [('2026-10-12', 200), ('2026-10-23', 190)]}
    monkeypatch.setattr(analysis, '_daily_eur', prices)
    p = {'name': 'Star Selection Index', 'isin': 'WRAPPER', 'closed_out': True,
         'last_sale': '2026-10-12', 'result_eur': 290, 'contribution_pct': .29}
    analysis._with_sold_lookthrough({'positions': [p]}, 1)
    assert calls == [(['MS', 'V'], 2)]
    children = p['lookthrough_positions']
    assert [c['name'] for c in children] == ['Microsoft', 'Visa']
    assert [c['weight_pct'] for c in children] == [60, 40]
    assert children[0]['since_close_pct'] == pytest.approx(10)
    assert children[1]['since_close_pct'] == pytest.approx(-5)
    assert children[0]['composition_as_of'] == '2026-09-30'
    assert p['result_eur'] == 290
    assert p['contribution_pct'] == .29
    assert all('contribution_pct' not in c for c in children)


def test_no_historical_snapshot_never_substitutes_todays_holdings(monkeypatch):
    setup_links(monkeypatch)
    monkeypatch.setattr(analysis, 'ref_positions_for', lambda _: [
        {'datum': '2026-10-20', 'fonds': 'New', 'isin': 'NEW', 'percentage': 100}])
    monkeypatch.setattr(analysis, '_daily_eur', lambda *_args, **_kwargs: pytest.fail('no price read needed'))
    p = {'name': 'Certificate', 'closed_out': True, 'last_sale': '2026-10-12'}
    analysis._with_sold_lookthrough({'positions': [p]}, 1)
    assert 'lookthrough_positions' not in p
    assert 'on or before' in p['lookthrough_note']


@pytest.mark.parametrize('target', [None, 1])
def test_dismissed_links_and_self_references_are_not_expanded(monkeypatch, target):
    setup_links(monkeypatch, target)
    monkeypatch.setattr(analysis, 'ref_positions_for', lambda _: pytest.fail('not a child'))
    p = {'name': 'Direct stock', 'closed_out': True, 'last_sale': '2026-10-12'}
    analysis._with_sold_lookthrough({'positions': [p]}, 1)
    assert 'lookthrough_positions' not in p
