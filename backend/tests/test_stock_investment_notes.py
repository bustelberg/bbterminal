import pytest
from fastapi import HTTPException

from services import investment_notes as N
from tests._fake_supabase import FakeSupabase

ALICE = {'id': 'alice', 'email': 'alice@example.com'}
BOB = {'id': 'bob', 'email': 'bob@example.com'}
CLPT = 'US18507C1036'


def test_stock_without_fundamentals_can_have_private_notes():
    db = FakeSupabase({'asset_grid': [{'isin': CLPT, 'company_id': None}], 'company': []})
    N.save(db, CLPT, ALICE, ' My thesis ', [' First pillar ', ''])
    assert N.read(db, CLPT, ALICE)['thesis'] == 'My thesis'
    assert N.read(db, CLPT, ALICE)['pillars'] == ['First pillar']
    assert N.read(db, CLPT, BOB)['thesis'] == ''
    assert len(db.tables['instrument_investment_note']) == 1
    assert not db.tables['company']


def test_adr_and_ordinary_read_the_existing_company_note():
    db = FakeSupabase({'company': [{'isin': 'TW0002330008', 'company_id': 42}],
        'company_investment_note': [{'company_id': 42, 'user_id': 'alice', 'thesis': 'Existing TSMC research',
                                     'pillars': [], 'updated_at': '2026-11-01'}]})
    assert N.read(db, 'US8740391003', ALICE)['thesis'] == 'Existing TSMC research'
    assert N.read(db, 'TW0002330008', ALICE)['thesis'] == 'Existing TSMC research'
    N.save(db, 'US8740391003', ALICE, 'Updated research', [])
    assert N.read(db, 'TW0002330008', ALICE)['thesis'] == 'Updated research'
    assert not db.tables.get('instrument_investment_note')


def test_isin_notes_survive_a_later_company_link():
    db = FakeSupabase({})
    N.save(db, CLPT, ALICE, 'Research before company resolution', [])
    db.tables['company'] = [{'isin': CLPT, 'company_id': 7}]
    assert N.read(db, CLPT, ALICE)['thesis'] == 'Research before company resolution'
    N.save(db, CLPT, ALICE, 'Research after resolution', [])
    assert N.read(db, CLPT, ALICE)['thesis'] == 'Research after resolution'
    assert db.tables['company_investment_note'][0]['company_id'] == 7


def test_invalid_isin_and_oversized_notes_are_rejected_without_writes():
    db = FakeSupabase({})
    with pytest.raises(HTTPException) as invalid:
        N.save(db, 'NOTANISIN', ALICE, 'hello', [])
    assert invalid.value.status_code == 422
    with pytest.raises(HTTPException) as oversized:
        N.save(db, CLPT, ALICE, 'x' * 12001, [])
    assert oversized.value.status_code == 422
    assert not db.writes


def test_existing_company_editor_survives_a_pending_isin_storage_migration():
    db = FakeSupabase({'company': [{'isin': 'TW0002330008', 'company_id': 42}]})
    class RollingDeployment:
        def table(self, name):
            if name == 'instrument_investment_note':
                raise RuntimeError('table not yet migrated')
            return db.table(name)
    assert N.read(RollingDeployment(), 'US8740391003', ALICE)['thesis'] == ''
    with pytest.raises(HTTPException) as missing:
        N.read(RollingDeployment(), CLPT, ALICE)
    assert missing.value.status_code == 503
