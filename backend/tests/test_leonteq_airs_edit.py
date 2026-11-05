from decimal import Decimal
from unittest.mock import MagicMock, Mock

import pytest
from fastapi import HTTPException

from leonteq import airs_edit
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def setup(monkeypatch):
    import airs_scanner
    import airs_vermogen
    from routers import _airs_portfolio_store as store

    db = FakeSupabase({'airs_model_portfolio': [{'id': 1, 'name': 'Fixed model', 'portfolio_type': 'fixed (13.7)', 'positions_schema_version': 3}]})
    monkeypatch.setattr(airs_edit, 'supabase', db)
    before = {'portfolio_id': 1, 'portfolio': 'Fixed model', 'datum': '2025-02-14', 'dates': ['2025-02-14'],
              'rows': [{'id': 18137, 'Fonds': 'Apple', 'ISINCode': 'US0378331005', 'Percentage': 4.95}]}
    after = {**before, 'rows': [{**before['rows'][0], 'Percentage': 5.5}]}
    fetch = Mock(side_effect=[before, after, after])
    update = Mock()
    save = Mock()
    monkeypatch.setattr(airs_scanner, 'fetch_portfolio_positions_sync', fetch)
    monkeypatch.setattr(airs_edit, '_remote_update', update)
    monkeypatch.setattr(store, 'save_positions', save)
    monkeypatch.setattr(airs_vermogen, '_acquire_session', Mock(return_value=True))
    monkeypatch.setattr(airs_vermogen, '_LOCK', Mock())
    return db, before, after, fetch, update, save


def test_percentage_edit_verifies_remote_result_and_refreshes_latest_cache(setup):
    db, before, after, fetch, update, save = setup
    result = airs_edit.update_percentage(1, 18137, '2025-02-14', Decimal('4.95'), Decimal('5.5'))
    update.assert_called_once_with('Fixed model', '2025-02-14', 18137, Decimal('4.95'), Decimal('5.5'))
    assert result['rows'][0]['Percentage'] == 5.5
    save.assert_called_once_with(1, after['datum'], after['rows'], after['dates'])
    assert result['snapshot_saved'] is True
    assert fetch.call_count == 2
    assert fetch.call_args.kwargs == {'datum': '2025-02-14', 'metadata': before}


@pytest.mark.parametrize('row_id,expected', [(18138, '4.95'), (18137, '5')])
def test_stale_or_wrong_row_is_refused_before_saving(setup, row_id, expected):
    db, before, after, fetch, update, save = setup
    with pytest.raises(HTTPException) as error:
        airs_edit.update_percentage(1, row_id, '2025-02-14', Decimal(expected), Decimal('5.5'))
    assert error.value.status_code == 409
    update.assert_not_called()
    save.assert_not_called()


def test_unverified_save_is_not_reported_as_success_and_cache_stays_invalid(setup):
    db, before, after, fetch, update, save = setup
    fetch.side_effect = [before, before]
    with pytest.raises(HTTPException) as error:
        airs_edit.update_percentage(1, 18137, '2025-02-14', Decimal('4.95'), Decimal('5.5'))
    assert error.value.detail == 'save_unverified'
    assert db.tables['airs_model_portfolio'][0]['positions_schema_version'] == 0
    update.assert_called_once()
    save.assert_not_called()


def test_historical_edit_keeps_canonical_cache_at_latest_date(setup):
    db, before, after, fetch, update, save = setup
    latest = {**after, 'datum': '2026-10-01', 'rows': [{**after['rows'][0], 'Percentage': 7}]}
    before['dates'].append('2026-10-01')
    fetch.side_effect = [before, after, latest]
    result = airs_edit.update_percentage(1, 18137, '2025-02-14', Decimal('4.95'), Decimal('5.5'))
    assert result['datum'] == '2025-02-14'
    save.assert_called_once_with(1, latest['datum'], latest['rows'], latest['dates'])


def test_percentage_request_refuses_invalid_numbers():
    from pydantic import ValidationError
    from routers.leonteq import PercentageEditRequest

    for value in ('NaN', 'Infinity', '-1', '101', '1.1234567'):
        with pytest.raises(ValidationError):
            PercentageEditRequest(datum='2025-02-14', expected_percentage=4.95, percentage=value)


def test_verification_reuses_metadata_and_downloads_only_positions(monkeypatch):
    import airs_scanner
    get = Mock(return_value=Mock(body=b'export'))
    parse = Mock(return_value=[{'id': 18137, 'Percentage': 5.5}])
    monkeypatch.setattr(airs_scanner._session, 'get_response', get)
    monkeypatch.setattr(airs_scanner, '_parse_positions_xls', parse)
    result = airs_scanner.fetch_portfolio_positions_sync(1, datum='2025-02-14', metadata={
        'portfolio': 'Fixed model', 'dates': ['2025-02-14'],
    })
    get.assert_called_once()
    assert 'action=edit' not in get.call_args.args[0]
    assert result['rows'][0]['Percentage'] == 5.5


def test_native_editor_changes_only_percentage_and_disables_retries(monkeypatch):
    import airs_scanner

    page = MagicMock()
    row = page.locator.return_value
    row.count.return_value = 1
    row.locator.return_value.inner_text.return_value = '4.95'
    response = page.expect_response.return_value.__enter__.return_value.value
    response.status = 200
    response.json.return_value = {'success': True, 'saved': True}

    def submit(job):
        assert job.retry_safe is False
        return job(page)

    monkeypatch.setattr(airs_scanner._session, '_submit', submit)
    airs_edit._remote_update('Fixed model', '2025-02-14', 18137, Decimal('4.95'), Decimal('5.5'))
    page.locator.assert_called_once_with('tr[data-lineid="18137"]')
    row.locator.return_value.fill.assert_called_once_with('5.5')
    row.locator.assert_any_call('input[name="Percentage"]')
    row.locator.assert_any_call('.saveInline')
