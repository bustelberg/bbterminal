from unittest.mock import Mock

import pytest

from leonteq import price_symbols as symbols
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def setup(monkeypatch):
    db = FakeSupabase({})
    monkeypatch.setattr(symbols, 'supabase', db)
    monkeypatch.setattr(symbols.openfigi, 'lookup_isin', lambda _: [{'name': 'Example Corp', 'ticker': 'RIGHT'}])
    monkeypatch.setattr(symbols, 'build_candidates', lambda *args, **kwargs: ['RIGHT'])
    monkeypatch.setattr(symbols.yahoo, 'search', lambda _: [{'symbol': 'WRONG'}])
    chart = Mock(return_value={'meta': {'symbol': 'RIGHT', 'longName': 'Example Corp', 'currency': 'USD'},
                               'indicators': {'quote': [{'close': [100]}]}})
    monkeypatch.setattr(symbols.yahoo, 'chart', chart)
    return db, chart


def test_resolution_anchors_identity_and_saves_currency_specific_mapping(setup):
    db, chart = setup
    assert symbols.resolve('ISIN', 'USD') == 'RIGHT'
    assert db.tables['leonteq_price_symbol'][0]['currency'] == 'USD'
    assert symbols.resolve('ISIN', 'USD') == 'RIGHT'
    assert chart.call_count == 1
    assert symbols.resolve('ISIN', 'EUR') is None
    assert chart.call_count == 2
    assert len(db.tables['leonteq_price_symbol']) == 1


@pytest.mark.parametrize('meta,closes', [
    ({'symbol': 'OTHER', 'longName': 'Example Corp', 'currency': 'USD'}, [100]),
    ({'symbol': 'RIGHT', 'longName': 'Unrelated Company', 'currency': 'USD'}, [100]),
    ({'symbol': 'RIGHT', 'currency': 'USD'}, [100]),
    ({'symbol': 'RIGHT', 'longName': 'Example Corp', 'currency': 'USD'}, [None, 0, float('nan')]),
])
def test_ambiguous_wrong_or_unpriceable_candidates_are_not_saved(setup, meta, closes):
    db, chart = setup
    chart.return_value = {'meta': meta, 'indicators': {'quote': [{'close': closes}]}}
    assert symbols.resolve('ISIN', 'USD') is None
    assert not db.writes


def test_force_rechecks_saved_listing(setup):
    db, chart = setup
    db.tables['leonteq_price_symbol'] = [{'isin': 'ISIN', 'currency': 'USD', 'symbol': 'OLD'}]
    assert symbols.resolve('ISIN', 'USD', force=True) == 'RIGHT'
    assert db.tables['leonteq_price_symbol'][0]['symbol'] == 'RIGHT'
    chart.assert_called_once()


def test_cache_failure_does_not_discard_a_verified_listing(setup, monkeypatch):
    _, chart = setup
    monkeypatch.setattr(symbols, 'supabase', Mock(table=Mock(side_effect=RuntimeError('unavailable'))))
    warnings = set()
    assert symbols.resolve('ISIN', 'USD', warn=warnings.add) == 'RIGHT'
    assert len(warnings) == 1
    assert 'database migration' in next(iter(warnings))
    chart.assert_called_once()


def test_unresolved_listing_explains_missing_security_identity(setup, monkeypatch):
    monkeypatch.setattr(symbols.openfigi, 'lookup_isin', lambda _: [])
    evidence = []
    assert symbols.resolve('ISIN', 'USD', diagnostics=evidence) is None
    assert 'OpenFIGI returned no security identity' in evidence[0]


def test_currency_rejection_retains_the_candidate_and_required_currency(setup):
    evidence = []
    assert symbols.resolve('ISIN', 'EUR', diagnostics=evidence) is None
    assert evidence == ['RIGHT: Yahoo currency USD, source requires EUR.']
