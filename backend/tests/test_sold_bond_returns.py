from types import SimpleNamespace

import pytest

from routers import _airs_portfolio_analysis as analysis, _airs_portfolio_perf as perf


@pytest.fixture
def setup(monkeypatch):
    watched = []
    class Watch:
        def upsert(self, rows, **_):
            watched.extend(rows)
            return self
        def execute(self):
            return SimpleNamespace(data=[])
    monkeypatch.setattr(analysis, 'supabase', SimpleNamespace(table=lambda _: Watch()))
    monkeypatch.setattr(analysis, '_holding_risk', lambda *_: {})
    monkeypatch.setattr(analysis, '_sold_position_isins', lambda names: {})
    monkeypatch.setattr(perf, '_executions', lambda _: {
        'BOND_ISIN': {'yahoo_symbol': 'VERIFIED.L', 'currency': 'USD', 'analysis_id': 1}})
    monkeypatch.setattr(analysis, '_daily_eur', lambda *_args, **_kwargs: {'BOND_ISIN': [
        ('2026-10-09', 45), ('2026-10-13', 50), ('2026-10-26', 55)]})
    return watched


def test_verified_bond_fund_gets_same_post_sale_eur_return_and_yahoo_source_as_stock(setup):
    p = {'name': 'Verified bond fund', 'isin': 'BOND_ISIN', 'bucket': 'Bonds',
         'closed_out': True, 'last_sale': '2026-10-12', 'contribution_pct': .08}
    analysis._with_sold_risk({'positions': [p]}, 'ACWI')
    assert p['since_close_pct'] == pytest.approx(10)
    assert p['since_close_date'] == '2026-10-13'
    assert p['since_close_as_of'] == '2026-10-26'
    assert p['since_close_from'] == 50
    assert p['post_close_symbol'] == 'VERIFIED.L'
    assert p['post_close_currency'] == 'USD'
    assert p['post_close_status'] == 'ready'
    assert p['contribution_pct'] == .08  # never add the hypothetical result to actual performance
    assert setup == [{'isin': 'BOND_ISIN', 'last_sale_date': '2026-10-12'}]


def test_unidentified_funds_are_skipped_without_borrowing_a_similar_funds_prices(setup):
    p = {'name': 'Invesco BulletShares 2028 USD Corporate Bond ETF',
         'closed_out': True, 'last_sale': '2026-10-12'}
    analysis._with_sold_risk({'positions': [p]}, 'ACWI')
    assert p['isin'] is None
    assert p['post_close_symbol'] is None
    assert p['post_close_status'] == 'unknown_instrument'
    assert 'since_close_pct' not in p
    assert setup == []


@pytest.mark.parametrize('series,status', [
    ([('2026-10-26', 55)], 'missing_history'),
    ([('2026-10-22', 50), ('2026-10-26', 55)], 'missing_sale_close'),
])
def test_missing_sale_history_does_not_masquerade_as_a_full_post_sale_return(setup, monkeypatch, series, status):
    monkeypatch.setattr(analysis, '_daily_eur', lambda *_args, **_kwargs: {'BOND_ISIN': series})
    p = {'name': 'Bond fund', 'isin': 'BOND_ISIN', 'closed_out': True,
         'last_sale': '2026-10-12', 'since_close_pct': 99}
    analysis._with_sold_risk({'positions': [p]}, 'ACWI')
    assert 'since_close_pct' not in p
    assert p['post_close_status'] == status


def test_bond_maturity_numbers_must_match_exactly():
    assert not analysis._instrument_names_match('Bond Fund 2028', 'Bond Fund 202')
    assert not analysis._instrument_names_match('Bond Fund 2028', 'Bond Fund 2030')
    assert analysis._instrument_names_match('Bond Fund 2028', 'Bond Fund 2028')
