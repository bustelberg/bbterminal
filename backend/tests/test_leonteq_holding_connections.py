import pytest
from fastapi import HTTPException

from leonteq import holding_connections as rows
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def store(monkeypatch):
    db = FakeSupabase({'leonteq_airs_portfolio_link': [
        {'account_key': 'a', 'leonteq_name': 'AI', 'airs_portfolio_id': 1},
        {'account_key': 'a', 'leonteq_name': 'AI', 'airs_portfolio_id': 2},
    ]})
    monkeypatch.setattr(rows, 'supabase', db)
    monkeypatch.setattr(rows, '_account', lambda: 'a')
    return db


def test_disconnect_and_reconnect_are_durable_choices(store):
    assert rows.read('AI', 1) == {'rules': []}
    rows.save('AI', 1, 'isin:US0378331005', None)
    assert rows.read('AI', 1)['rules'][0]['airs_key'] is None
    rows.save('AI', 1, 'isin:US0378331005', 'isin:US0378331005')
    assert rows.read('AI', 1)['rules'][0]['airs_key'] == 'isin:US0378331005'
    assert len(store.tables[rows.TABLE]) == 1


def test_two_left_rows_cannot_claim_the_same_right_row(store):
    rows.save('AI', 1, 'name:euro', 'name:liquiditeiten')
    with pytest.raises(HTTPException) as error:
        rows.save('AI', 1, 'name:usd', 'name:liquiditeiten')
    assert error.value.status_code == 409


def test_choices_are_isolated_by_connection_and_account(store, monkeypatch):
    rows.save('AI', 1, 'name:euro', 'name:liquiditeiten')
    assert rows.read('AI', 2) == {'rules': []}
    monkeypatch.setattr(rows, '_account', lambda: 'b')
    with pytest.raises(HTTPException) as error:
        rows.read('AI', 1)
    assert error.value.status_code == 404


def test_no_rules_can_be_created_for_an_unsaved_portfolio_connection(store):
    with pytest.raises(HTTPException):
        rows.save('Unknown', 1, 'name:euro', None)
    assert not store.tables.get(rows.TABLE)
