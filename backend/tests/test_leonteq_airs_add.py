from decimal import Decimal
from unittest.mock import Mock, MagicMock
import json

import pytest
from fastapi import HTTPException

from leonteq import airs_edit
from tests._fake_supabase import FakeSupabase

ISIN = 'GB00B1YW4409'
INSTRUMENT = {'value': '3i Group', 'field_value': '3i Group', 'isin': ISIN}
LOOKUP = [{'value': '3i Group', 'field_value': '3i Group', 'label': f'3i Group - {ISIN}', 'data': {'ISINCode': ISIN, 'Fonds': '3i Group'}}]


@pytest.fixture
def setup(monkeypatch):
    import airs_scanner
    import airs_vermogen
    from leonteq import portfolio_links, composition_cache, holding_connections
    from routers import _airs_portfolio_store as store
    db = FakeSupabase({'airs_model_portfolio': [{'id': 1, 'name': 'Fixed', 'portfolio_type': 'fixed', 'positions_schema_version': 3}]})
    monkeypatch.setattr(airs_edit, 'supabase', db)
    monkeypatch.setattr(portfolio_links, 'options', lambda kind: {'links': [{'leonteq_name': 'Source', 'airs_portfolio_id': 1}]})
    monkeypatch.setattr(holding_connections, 'read', lambda *args: {'rules': []})
    source = Mock(return_value={'success': True, 'groups': [{'positions': [{'isin': ISIN, 'weight': '1.97%'}]}]})
    monkeypatch.setattr(composition_cache, 'composition', source)
    monkeypatch.setattr(airs_vermogen, '_acquire_session', lambda **kwargs: True)
    lock = Mock()
    monkeypatch.setattr(airs_vermogen, '_LOCK', lock)
    before = {'portfolio': 'Fixed', 'datum': '2026-06-09', 'dates': ['2026-06-09'],
              'rows': [{'id': 1, 'Fonds': 'Liquiditeiten', 'ISINCode': None, 'Percentage': 100}]}
    after = {**before, 'rows': [*before['rows'], {'id': 2, 'Fonds': '3i Group', 'ISINCode': ISIN, 'Percentage': 1.97}]}
    fetch = Mock(side_effect=[before, after])
    lookup = Mock(return_value=INSTRUMENT)
    create = Mock()
    save = Mock()
    monkeypatch.setattr(airs_scanner, 'fetch_portfolio_positions_sync', fetch)
    monkeypatch.setattr(airs_edit, '_lookup_registered', lookup)
    monkeypatch.setattr(airs_edit, '_remote_create', create)
    monkeypatch.setattr(store, 'save_positions', save)
    return db, source, before, after, fetch, lookup, create, save, lock


def test_add_verifies_the_source_and_new_export_then_updates_canonical_cache(setup):
    db, source, before, after, fetch, lookup, create, save, lock = setup
    result = airs_edit.add_position(1, '2026-06-09', ISIN, Decimal('1.97'), 'Source')
    source.assert_called_once_with('Source', refresh=True)
    lookup.assert_called_once_with(ISIN, 'Fixed', '2026-06-09')
    create.assert_called_once_with('Fixed', '2026-06-09', ISIN, Decimal('1.97'), INSTRUMENT)
    save.assert_called_once_with(1, after['datum'], after['rows'], after['dates'])
    assert result['snapshot_saved'] is True
    lock.release.assert_called_once()


def test_existing_position_is_never_added_twice(setup):
    db, source, before, after, fetch, lookup, create, save, lock = setup
    before['rows'].append(after['rows'][1])
    with pytest.raises(HTTPException, match='already in AIRS'):
        airs_edit.add_position(1, '2026-06-09', ISIN, Decimal('1.97'), 'Source')
    lookup.assert_not_called()
    create.assert_not_called()
    assert db.tables['airs_model_portfolio'][0]['positions_schema_version'] == 3
    lock.release.assert_called_once()


def test_missing_registration_alerts_user_without_submitting_or_invalidating_cache(setup):
    db, source, before, after, fetch, lookup, create, save, lock = setup
    lookup.side_effect = HTTPException(422, 'Instrument is not registered in AIRS. Register it manually.')
    with pytest.raises(HTTPException, match='Register it manually'):
        airs_edit.add_position(1, '2026-06-09', ISIN, Decimal('1.97'), 'Source')
    create.assert_not_called()
    save.assert_not_called()
    assert db.tables['airs_model_portfolio'][0]['positions_schema_version'] == 3
    lock.release.assert_called_once()


def test_changed_source_weight_prevents_submission(setup):
    db, source, before, after, fetch, lookup, create, save, lock = setup
    with pytest.raises(HTTPException, match='weight changed'):
        airs_edit.add_position(1, '2026-06-09', ISIN, Decimal('2'), 'Source')
    fetch.assert_not_called()
    create.assert_not_called()


def test_unverified_new_row_is_not_reported_as_saved_or_retried(setup):
    db, source, before, after, fetch, lookup, create, save, lock = setup
    after['rows'][1]['Percentage'] = 2
    with pytest.raises(HTTPException, match='unverified'):
        airs_edit.add_position(1, '2026-06-09', ISIN, Decimal('1.97'), 'Source')
    create.assert_called_once()
    save.assert_not_called()
    assert db.tables['airs_model_portfolio'][0]['positions_schema_version'] == 0


def test_lookup_requires_one_exact_registered_isin():
    assert airs_edit._registered_instrument(LOOKUP, ISIN) == INSTRUMENT
    with pytest.raises(HTTPException, match='not registered'):
        airs_edit._registered_instrument([], ISIN)
    with pytest.raises(HTTPException, match='Multiple'):
        airs_edit._registered_instrument([*LOOKUP, {**LOOKUP[0], 'value': 'Other', 'data': {'ISINCode': ISIN, 'Fonds': 'Other'}}], ISIN)


def test_lookup_opens_fixed_form_context_before_querying_virtual_fonds(monkeypatch):
    import airs_scanner
    page = MagicMock()
    page.locator.return_value.count.return_value = 1
    response = Mock(status=200, url='https://example/lookup')
    response.body.return_value = json.dumps(LOOKUP).encode()
    def get(url):
        assert page.goto.called
        assert 'field=Fonds' in url and f'term={ISIN}' in url
        return response
    page.request.get.side_effect = get
    monkeypatch.setattr(airs_scanner._session, '_submit', lambda job: job(page))
    assert airs_edit._lookup_registered(ISIN, 'Fixed', '2026-06-09') == INSTRUMENT
    assert 'Portefeuille=Fixed' in page.goto.call_args.args[0]
    page.locator.return_value.count.return_value = 0
    with pytest.raises(HTTPException, match='context is unavailable'):
        airs_edit._lookup_registered(ISIN, 'Fixed', '2026-06-09')
    page.request.get.assert_called_once()


def test_native_creation_selects_exact_isin_and_disables_write_retries(monkeypatch):
    import airs_scanner
    page = MagicMock()
    page.locator.return_value.filter.return_value.count.side_effect = [0, 1]
    page.locator.return_value.locator.return_value.input_value.return_value = '3i Group'
    lookup = Mock(status=200)
    lookup.json.return_value = LOOKUP
    saved = Mock(status=200, url=airs_scanner.BASE_URL + '/modelportefeuillefixedEdit.php?action=update&saveType=new&Portefeuille=Fixed&Datum=2026-06-09&type=1')
    saved.request.method = 'POST'
    saved.json.return_value = {'success': True, 'saved': True}
    receipts = [MagicMock(), MagicMock()]
    receipts[0].__enter__.return_value.value = lookup
    receipts[1].__enter__.return_value.value = saved
    page.expect_response.side_effect = receipts
    def submit(job):
        assert job.retry_safe is False
        return job(page)
    monkeypatch.setattr(airs_scanner._session, '_submit', submit)
    airs_edit._remote_create('Fixed', '2026-06-09', ISIN, Decimal('1.97'), INSTRUMENT)
    page.locator.return_value.locator.return_value.fill.assert_any_call(ISIN)
    page.locator.return_value.locator.return_value.fill.assert_any_call('1.97')
    save_filter = page.expect_response.call_args_list[1].args[0]
    assert save_filter(saved)
    saved.request.method = 'GET'
    assert not save_filter(saved)


def test_add_request_rejects_invalid_identity_and_weights():
    from pydantic import ValidationError
    from routers.leonteq import PositionAddRequest
    assert PositionAddRequest(datum='2026-06-09', isin=ISIN, percentage='1.97', leonteq_name='Source').percentage == Decimal('1.97')
    with pytest.raises(ValidationError):
        PositionAddRequest(datum='2026-06-09', isin='not an ISIN', percentage='1.97', leonteq_name='Source')
    for value in ('NaN', 'Infinity', '0', '-1', '101', '1.1234567'):
        with pytest.raises(ValidationError):
            PositionAddRequest(datum='2026-06-09', isin=ISIN, percentage=value, leonteq_name='Source')
