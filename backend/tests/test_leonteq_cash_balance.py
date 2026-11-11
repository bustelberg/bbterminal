from decimal import Decimal
from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from leonteq import airs_edit
from tests._fake_supabase import FakeSupabase


def snapshot(cash=10, stock=91.96):
    return {'portfolio': 'Fixed', 'portfolio_id': 1, 'datum': '2026-11-10', 'dates': ['2026-11-10'], 'rows': [
        {'id': 1, 'Fonds': 'Liquiditeiten', 'ISINCode': None, 'Percentage': cash},
        {'id': 2, 'Fonds': 'Stock', 'ISINCode': 'US0378331005', 'Percentage': stock}]}


@pytest.fixture
def setup(monkeypatch):
    import airs_scanner
    import airs_vermogen
    db = FakeSupabase({'airs_model_portfolio': [{'id': 1, 'portfolio_type': 'fixed', 'positions_schema_version': 3}]})
    monkeypatch.setattr(airs_edit, 'supabase', db)
    monkeypatch.setattr(airs_vermogen, '_acquire_session', Mock(return_value=True))
    monkeypatch.setattr(airs_vermogen, '_LOCK', Mock())
    fetch = Mock(side_effect=[snapshot(), snapshot(8.04)])
    total = Mock(side_effect=[Decimal('101.96'), Decimal('100')])
    update = Mock()
    monkeypatch.setattr(airs_scanner, 'fetch_portfolio_positions_sync', fetch)
    monkeypatch.setattr(airs_edit, '_native_total', total)
    monkeypatch.setattr(airs_edit, '_remote_update', update)
    monkeypatch.setattr(airs_edit, '_persist_verified', lambda _id, _before, after: after)
    return db, fetch, total, update


def test_preview_reads_latest_total_without_writing(setup):
    _, fetch, _, update = setup
    plan = airs_edit.balance_cash(1)
    assert plan['target'] == Decimal('8.04')
    assert plan['total'] == Decimal('101.96')
    fetch.assert_called_once_with(1)
    update.assert_not_called()


def test_exec_rechecks_and_verifies_total(setup):
    _, _, _, update = setup
    expected = airs_edit._cash_plan(snapshot(), Decimal('101.96'))
    result = airs_edit.balance_cash(1, expected)
    update.assert_called_once_with('Fixed', '2026-11-10', 1, Decimal('10'), Decimal('8.04'))
    assert result['balanced'] is True


def test_changed_total_refuses_write(setup):
    _, _, _, update = setup
    expected = airs_edit._cash_plan(snapshot(stock=90), Decimal('100'))
    with pytest.raises(HTTPException) as error:
        airs_edit.balance_cash(1, expected)
    assert error.value.status_code == 409
    update.assert_not_called()


def test_unverified_total_is_not_retried(setup):
    db, fetch, total, update = setup
    fetch.side_effect = [snapshot(), snapshot()]
    total.side_effect = [Decimal('101.96'), Decimal('101.96')]
    with pytest.raises(HTTPException):
        airs_edit.balance_cash(1, airs_edit._cash_plan(snapshot(), Decimal('101.96')))
    update.assert_called_once()
    assert db.tables['airs_model_portfolio'][0]['positions_schema_version'] == 0


@pytest.mark.parametrize('data,total', [
    (snapshot(stock=110), '120'),
    (snapshot(stock=None), '10'),
    (snapshot(), '100'),
    ({**snapshot(), 'rows': [snapshot()['rows'][1]]}, '91.96'),
    ({**snapshot(), 'rows': [*snapshot()['rows'], {**snapshot()['rows'][0], 'id': 3}]}, '111.96'),
])
def test_unsafe_cash_plans_are_rejected(data, total):
    with pytest.raises(HTTPException):
        airs_edit._cash_plan(data, Decimal(total))


def test_uses_precise_weights_instead_of_rounded_footer():
    plan = airs_edit._cash_plan(snapshot(stock=91.964), Decimal('101.96'))
    assert plan['target'] == Decimal('8.036')


def test_reads_only_native_total_not_afm_stdev(monkeypatch):
    import airs_scanner
    monkeypatch.setattr(airs_scanner._session, 'get_response', Mock(return_value=Mock()))
    monkeypatch.setattr(airs_scanner, '_decode_html', lambda _: '<tr><td>Totaal</td><td><b class="tableTotal">101.96 </b></td></tr><tr><td>Afm stdev</td><td>13.12</td></tr>')
    assert airs_edit._native_total('Fixed', '2026-11-10') == Decimal('101.96')
