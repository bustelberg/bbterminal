from unittest.mock import Mock

import pytest

from leonteq import airs_models
from routers import _airs_portfolio_store as store
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def setup(monkeypatch):
    import airs_scanner
    import airs_vermogen

    db = FakeSupabase({'airs_model_portfolio': [{'id': 1, 'name': 'BUS_WTS_Duurzaam_Fx', 'portfolio_type': 'fixed (13.7)'}]})
    monkeypatch.setattr(airs_models, 'supabase', db)
    monkeypatch.setattr(store, 'supabase', db)
    raw = {'portfolio_id': 1, 'portfolio': 'BUS_WTS_Duurzaam_Fx', 'datum': '2025-02-14',
           'dates': ['2025-02-14'], 'rows': [{'id': 18137, 'Fonds': 'Apple', 'ISINCode': 'US0378331005',
           'Percentage': 4.95, 'valuta': 'USD', 'Beleggingscategorie': 'AAND',
           'Beleggingssector': 'BU-Inf.Technol', 'regio': 'BU-USA', 'afmCategorie': '09AandOntw'}]}
    fetch = Mock(return_value=raw)
    monkeypatch.setattr(airs_scanner, 'fetch_portfolio_positions_sync', fetch)
    monkeypatch.setattr(airs_vermogen, '_acquire_session', Mock(return_value=True))
    monkeypatch.setattr(airs_vermogen, '_LOCK', Mock())
    return db, fetch


def test_complete_afm_composition_is_persisted_and_reused(setup):
    db, fetch = setup
    assert airs_models.composition(1)['rows'][0]['afmCategorie'] == '09AandOntw'
    cached = airs_models.composition(1)
    assert cached['rows'][0]['afmCategorie'] == '09AandOntw'
    assert cached['schema_version'] == 3
    assert cached['rows'][0]['id'] == 18137
    assert cached['cached_at']
    fetch.assert_called_once_with(1, datum=None)
    assert db.tables['airs_model_portfolio_position'][0]['afm_categorie'] == '09AandOntw'


def test_saved_only_cache_miss_never_fetches_airs(setup):
    from fastapi import HTTPException
    db, fetch = setup
    with pytest.raises(HTTPException) as error:
        airs_models.composition(1, saved_only=True)
    assert error.value.status_code == 409
    fetch.assert_not_called()


def test_fixed_refresh_cancellation_is_not_reported_as_a_source_failure(setup):
    import airs_vermogen
    from jobs import JobCancelled

    _, fetch = setup
    fetch.side_effect = JobCancelled('Stopped before downloading')
    with pytest.raises(JobCancelled):
        airs_models.composition(1, refresh=True)
    airs_vermogen._LOCK.release.assert_called_once()


def test_historical_date_does_not_overwrite_latest_composition(setup):
    db, fetch = setup
    airs_models.composition(1)
    original = db.tables['airs_model_portfolio'][0]['positions_datum']
    historical = {**fetch.return_value, 'datum': '2024-01-01', 'rows': []}
    fetch.return_value = historical
    assert airs_models.composition(1, datum='2024-01-01')['rows'] == []
    assert db.tables['airs_model_portfolio'][0]['positions_datum'] == original
    assert len(db.tables['airs_model_portfolio_position']) == 1


def test_non_fixed_models_do_not_start_an_airs_read(setup):
    db, fetch = setup
    db.tables['airs_model_portfolio'][0]['portfolio_type'] = 'meervoudig'
    assert airs_models.composition(1)['no_fixed_model'] is True
    fetch.assert_not_called()


def test_selecting_the_already_saved_date_does_not_fetch_airs(setup):
    db, fetch = setup
    airs_models.composition(1)
    result = airs_models.composition(1, datum='2025-02-14')
    assert result['cached_at']
    fetch.assert_called_once()


def test_dynamic_inspection_uses_latest_account_holdings_without_a_browser(setup):
    db, fetch = setup
    model = db.tables['airs_model_portfolio'][0]
    model['portfolio_type'] = 'normaal'
    db.tables['airs_holding'] = [
        {'id': 1, 'portefeuille': model['name'], 'as_of_date': '2026-11-05',
         'holding_name': 'Old holding', 'weight': 1},
        {'id': 2, 'portefeuille': model['name'], 'as_of_date': '2026-11-06',
         'holding_name': 'Apple', 'isin': 'US0378331005', 'weight': 0.0495, 'currency': 'USD'},
        {'id': 3, 'portefeuille': 'Other account', 'as_of_date': '2026-11-09',
         'holding_name': 'Other holding', 'weight': 1},
    ]
    result = airs_models.composition(1)
    assert result['datum'] == '2026-11-06'
    assert result['rows'] == [{'Fonds': 'Apple', 'ISINCode': 'US0378331005',
                               'Percentage': 4.95, 'valuta': 'USD', 'price': None,
                               'price_date': '2026-11-06'}]
    fetch.assert_not_called()


def test_dynamic_refresh_requires_successful_holdings_and_keeps_fixed_store_untouched(setup, monkeypatch):
    import airs_vermogen
    from fastapi import HTTPException

    db, fetch = setup
    db.tables['airs_model_portfolio'][0]['portfolio_type'] = 'normaal'
    monkeypatch.setattr(airs_vermogen, '_acquire_session', Mock(return_value=False))
    full = Mock(side_effect=AssertionError('The inspector must not download all five reports'))
    monkeypatch.setattr(airs_vermogen, 'refresh_one_portfolio', full)
    with pytest.raises(HTTPException) as error:
        airs_models.composition(1, refresh=True)
    assert error.value.status_code == 409
    fetch.assert_not_called()
    full.assert_not_called()
    assert not db.tables.get('airs_model_portfolio_position')


def test_dynamic_refresh_returns_new_account_snapshot(setup, monkeypatch):
    db, fetch = setup
    model = db.tables['airs_model_portfolio'][0]
    model['portfolio_type'] = 'normaal'

    def refresh(name):
        db.tables['airs_holding'] = [{'id': 1, 'portefeuille': name, 'as_of_date': '2026-11-09',
                                     'holding_name': 'Cash', 'weight': 0, 'isin': None}]
        return {'vermogen_stored': True}

    monkeypatch.setattr(airs_models, '_refresh_dynamic_holdings', refresh)
    result = airs_models.composition(1, refresh=True)
    assert result['datum'] == '2026-11-09'
    assert result['rows'][0]['Percentage'] == 0
    fetch.assert_not_called()


def test_dynamic_connections_exclude_accrued_interest_but_preserve_accounting_rows(setup):
    db, fetch = setup
    model = db.tables['airs_model_portfolio'][0]
    model['portfolio_type'] = 'normaal'
    principal = {'id': 1, 'portefeuille': model['name'], 'as_of_date': '2026-11-06',
                 'holding_name': '3,75% Booking Holdings 24-36', 'isin': 'XS2776512035',
                 'weight': 0.0367, 'valuation_component': 'security'}
    interest = {**principal, 'id': 2, 'weight': 0.001, 'valuation_component': 'accrued_interest'}
    db.tables['airs_holding'] = [principal, interest]
    result = airs_models.composition(1)
    assert len(result['rows']) == 1
    assert result['rows'][0]['Percentage'] == pytest.approx(3.67)
    assert db.tables['airs_holding'] == [principal, interest]
    fetch.assert_not_called()


def test_legacy_interest_requires_verified_principal_and_never_drops_genuine_lots():
    principal = {'isin': 'XS2776512035', 'currency': 'EUR', 'quantity': 39200,
                 'current_price_local': 93.416, 'current_value_eur': 36619.072,
                 'start_value_eur': 37824, 'valuation_component': None}
    interest = {**principal, 'current_value_eur': 1007, 'start_value_eur': 0}
    assert airs_models._security_holdings([principal, interest]) == [principal]
    assert airs_models._security_holdings([interest]) == [interest]
    assert airs_models._security_holdings([principal, dict(principal)]) == [principal, principal]
    uncertain = {**interest, 'quantity': 1000}
    assert airs_models._security_holdings([principal, uncertain]) == [principal, uncertain]
    # The user's Booking export reports whole EUR: 39,200 × 93.416 / 100
    # is 36,619.072, displayed as 36,619. It must still verify as principal.
    rounded = {**principal, 'current_value_eur': 36619}
    assert airs_models._security_holdings([rounded, interest]) == [rounded]


def test_dynamic_inspector_downloads_only_holdings_and_releases_the_session(monkeypatch):
    import airs_vermogen
    import portfolio
    from types import SimpleNamespace

    monkeypatch.setattr(airs_vermogen, '_acquire_session', lambda **_: True)
    lock = Mock()
    monkeypatch.setattr(airs_vermogen, '_LOCK', lock)
    monkeypatch.setattr(airs_vermogen, '_reset_valuation_memo', lambda: None)
    monkeypatch.setattr(airs_vermogen, 'latest_completed_market_date', lambda: '2026-11-06')
    download = Mock(return_value=('2026-11-06', b'holdings-export'))
    monkeypatch.setattr(airs_vermogen, '_vermogen_most_recent', download)
    holding = SimpleNamespace(holding_name='Apple')
    monkeypatch.setattr(portfolio, 'parse_airs_excel', lambda raw: [holding])
    save = Mock(return_value=1)
    monkeypatch.setattr(airs_vermogen, '_save_holdings', save)
    monkeypatch.setattr(airs_vermogen, 'refresh_one_portfolio', lambda *a, **kw: pytest.fail('Five-report scan invoked'))
    airs_models._refresh_dynamic_holdings('Dynamic')
    download.assert_called_once_with('Dynamic', '2026-01-01', through='2026-11-06')
    save.assert_called_once_with('Dynamic', '2026-11-06', [holding])
    lock.release.assert_called_once()


def test_bulk_preload_reads_all_position_pages_and_keeps_empty_snapshots(setup):
    db, fetch = setup
    model = db.tables['airs_model_portfolio'][0]
    model.update(positions_schema_version=3, positions_scanned_at='2026-11-05T10:00:00Z', positions_datum='2025-02-14')
    db.tables['airs_model_portfolio'].extend([
        {**model, 'id': 2, 'name': 'Empty fixed model'},
        {**model, 'id': 3, 'name': 'Invalidated snapshot', 'positions_schema_version': 0},
        {**model, 'id': 4, 'name': 'Never scanned', 'positions_scanned_at': None},
        {**model, 'id': 5, 'name': 'Older snapshot', 'positions_schema_version': 1},
    ])
    db.tables['airs_model_portfolio_position'] = [
        {'id': i, 'portfolio_id': 1, 'fonds': f'Holding {i}', 'percentage': 0,
         'airs_row_id': i, 'afm_categorie': '09AandOntw'} for i in range(1, 502)]
    result = airs_models.saved_compositions()['compositions']
    assert [row['portfolio_id'] for row in result] == [1, 2, 5]
    assert len(result[0]['rows']) == 501
    assert result[0]['rows'][-1]['id'] == 501
    assert result[0]['rows'][0]['Percentage'] == 0
    assert result[0]['rows'][0]['afmCategorie'] == '09AandOntw'
    assert result[1]['rows'] == []
    assert result[2]['cache_upgrade_available'] is True
    fetch.assert_not_called()


def test_older_saved_rows_do_not_force_a_browser_fetch(setup):
    db, fetch = setup
    airs_models.composition(1)
    db.tables['airs_model_portfolio'][0]['positions_schema_version'] = 1
    result = airs_models.composition(1)
    assert result['cache_upgrade_available'] is True
    assert result['rows'][0]['Fonds'] == 'Apple'
    fetch.assert_called_once()
