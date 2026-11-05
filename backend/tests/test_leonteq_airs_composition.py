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
    db.tables['airs_model_portfolio'][0]['portfolio_type'] = 'normaal'
    assert airs_models.composition(1)['no_fixed_model'] is True
    fetch.assert_not_called()


def test_selecting_the_already_saved_date_does_not_fetch_airs(setup):
    db, fetch = setup
    airs_models.composition(1)
    result = airs_models.composition(1, datum='2025-02-14')
    assert result['cached_at']
    fetch.assert_called_once()


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
