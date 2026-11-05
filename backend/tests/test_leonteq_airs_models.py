from unittest.mock import Mock

import pytest

from leonteq.airs_models import refresh_models


def test_refresh_reads_and_saves_only_the_model_list(monkeypatch):
    import airs_scanner
    import airs_vermogen
    from routers import _airs_portfolio_store as store

    lock = Mock()
    lock.acquire.return_value = True
    monkeypatch.setattr(airs_vermogen, '_LOCK', lock)
    rows = [{'id': 1, 'name': 'BUS_WTS_Duurzaam', 'omschrijving': 'WTS Duurzaam FX', 'fixed': 'fixed (13.7)', 'fixed_datum': '2024-02-28'}]
    monkeypatch.setattr(airs_scanner, 'fetch_model_portfolios_sync', Mock(return_value=rows))
    holdings = Mock(side_effect=AssertionError('List refresh must not read holdings'))
    monkeypatch.setattr(airs_scanner, 'count_model_portfolio_holdings_sync', holdings)
    save = Mock()
    monkeypatch.setattr(store, 'save_portfolios', save)
    assert refresh_models(Mock()) == '1 model portfolios loaded from AIRS.'
    save.assert_called_once_with(rows)
    lock.release.assert_called_once_with()
    holdings.assert_not_called()


def test_busy_session_does_not_start_a_scan(monkeypatch):
    import airs_scanner
    import airs_vermogen

    lock = Mock()
    lock.acquire.return_value = False
    monkeypatch.setattr(airs_vermogen, '_LOCK', lock)
    scan = Mock()
    monkeypatch.setattr(airs_scanner, 'fetch_model_portfolios_sync', scan)
    with pytest.raises(RuntimeError, match='busy'):
        refresh_models(Mock())
    scan.assert_not_called()
    lock.release.assert_not_called()


def test_empty_scan_preserves_saved_list_and_releases_session(monkeypatch):
    import airs_scanner
    import airs_vermogen
    from routers import _airs_portfolio_store as store

    lock = Mock()
    lock.acquire.return_value = True
    monkeypatch.setattr(airs_vermogen, '_LOCK', lock)
    monkeypatch.setattr(airs_scanner, 'fetch_model_portfolios_sync', Mock(return_value=[]))
    save = Mock()
    monkeypatch.setattr(store, 'save_portfolios', save)
    with pytest.raises(RuntimeError, match='no model portfolios'):
        refresh_models(Mock())
    save.assert_not_called()
    lock.release.assert_called_once_with()
