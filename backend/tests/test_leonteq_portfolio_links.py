import pytest
from fastapi import HTTPException

from leonteq import portfolio_links as links
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def store(monkeypatch):
    from routers import _airs_portfolio_store

    db = FakeSupabase({})
    monkeypatch.setattr(links, 'supabase', db)
    monkeypatch.setattr(links, '_account_key', lambda: 'account-a')
    monkeypatch.setattr(links, 'saved_portfolios', lambda: {'success': True, 'portfolios': [{'name': 'AI-TopSelectie', 'sponsor': 'Bustelberg B.V.'}]})
    monkeypatch.setattr(_airs_portfolio_store, 'load_portfolios', lambda: [
        {'id': 1, 'name': 'Fixed model', 'portfolio_type': 'fixed (13.7)'},
        {'id': 2, 'name': 'Dynamic model', 'portfolio_type': 'normaal'},
        {'id': 3, 'name': 'Multi model', 'portfolio_type': 'meervoudig'},
        {'id': 4, 'name': 'Another fixed model', 'portfolio_type': 'fixed (0)'},
    ])
    return db


def test_options_only_offer_fixed_models(store):
    assert [row['id'] for row in links.options()['airs_portfolios']] == [1, 4]


def test_single_dynamic_comparison_does_not_reload_the_portfolio_catalogues(store, monkeypatch):
    store.tables[links.TABLE] = [{'account_key': 'account-a', 'leonteq_name': 'AI', 'airs_portfolio_id': 2}]
    store.tables['airs_model_portfolio'] = [{'id': 2, 'name': 'Dynamic', 'portfolio_type': 'normaal'}]
    monkeypatch.setattr(links, 'saved_portfolios', lambda: pytest.fail('Comparison loaded the entire catalogue'))
    assert links.linked_dynamic('AI', 2)['airs_name'] == 'Dynamic'
    with pytest.raises(HTTPException):
        links.linked_dynamic('Other', 2)
    monkeypatch.setattr(links, '_account_key', lambda: 'other-account')
    with pytest.raises(HTTPException):
        links.linked_dynamic('AI', 2)


def test_dynamic_connections_are_saved_and_separate_from_fixed(store):
    assert [row['id'] for row in links.options('dynamic')['airs_portfolios']] == [2]
    links.connect('AI-TopSelectie', 1)
    links.connect('AI-TopSelectie', 2, 'dynamic')
    assert [row['airs_portfolio_id'] for row in links.options()['links']] == [1]
    assert [row['airs_portfolio_id'] for row in links.options('dynamic')['links']] == [2]
    with pytest.raises(HTTPException):
        links.connect('AI-TopSelectie', 1, 'dynamic')
    with pytest.raises(HTTPException):
        links.connect('AI-TopSelectie', 3, 'dynamic')


def test_link_is_durable_and_saving_twice_does_not_duplicate_it(store):
    links.connect('AI-TopSelectie', 1)
    links.connect('AI-TopSelectie', 1)
    saved = links.options()['links']
    assert len(saved) == 1
    assert saved[0]['airs_portfolio_id'] == 1
    assert saved[0]['airs_name'] == 'Fixed model'
    assert saved[0]['updated_at']
    assert len(store.tables[links.TABLE]) == 1


@pytest.mark.parametrize('source,target', [('AI-TopSelectie', 2), ('AI-TopSelectie', 3), ('AI-TopSelectie', 999), ('Unknown portfolio', 1)])
def test_unknown_or_non_fixed_selections_are_refused(store, source, target):
    with pytest.raises(HTTPException) as error:
        links.connect(source, target)
    assert error.value.status_code == 400
    assert not store.tables.get(links.TABLE)


def test_multiple_variants_and_account_scoped_disconnect(store, monkeypatch):
    links.connect('AI-TopSelectie', 1)
    links.connect('AI-TopSelectie', 4)
    monkeypatch.setattr(links, '_account_key', lambda: 'account-b')
    assert links.options()['links'] == []
    links.disconnect('AI-TopSelectie', 1)
    monkeypatch.setattr(links, '_account_key', lambda: 'account-a')
    assert len(links.options()['links']) == 2
    links.disconnect('AI-TopSelectie', 1)
    assert [row['airs_portfolio_id'] for row in links.options()['links']] == [4]
