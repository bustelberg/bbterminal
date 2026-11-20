from types import SimpleNamespace
from unittest.mock import Mock
import threading

import pytest

from jobs import UserFacingJobError
from services.dashboard_returns import refresh


def test_dashboard_users_can_start_returns_refresh():
    from routers._auth_middleware import _is_user_refresh
    assert _is_user_refresh('/api/airs/dashboard/returns/refresh/job')


@pytest.fixture
def source(monkeypatch):
    import airs_vermogen
    import airs_scanner
    from routers import airs
    lock = threading.Lock()
    monkeypatch.setattr(airs_vermogen, '_LOCK', lock)
    monkeypatch.setattr(airs_vermogen, '_roster_names', lambda: [
        'BUS_Offensief_Dyn', 'TOPS_OFF_BEH_DYN', 'BUS_IF_DYN', 'UNRELATED'])
    monkeypatch.setattr(airs_vermogen, 'latest_completed_market_date', lambda: '2026-11-19')
    download = Mock(return_value=b'ATT')
    save = Mock()
    monkeypatch.setattr(airs_scanner, 'download_portfolio_sync', download)
    monkeypatch.setattr(airs, '_parse_att_excel', lambda data: [{'periode': '2026-11-19'}])
    monkeypatch.setattr(airs, '_save_performance_to_db', save)
    return SimpleNamespace(check=Mock(), progress=Mock()), download, save, lock


@pytest.mark.parametrize('tab,name', [('bustelberg', 'BUS_Offensief_Dyn'),
                                    ('imagine_fund', 'BUS_IF_DYN')])
def test_only_requested_tabs_returns_are_downloaded_and_saved(source, tab, name):
    ctx, download, save, lock = source
    refresh(ctx, tab)
    download.assert_called_once_with(name, '2026-01-01', '2026-11-19')
    assert save.call_args.args[0] == name
    assert not lock.locked()


def test_failed_return_download_is_not_reported_as_success(source):
    ctx, download, save, lock = source
    download.side_effect = ValueError('source unavailable')
    with pytest.raises(UserFacingJobError, match='source unavailable'):
        refresh(ctx, 'bustelberg')
    save.assert_not_called()
    assert not lock.locked()


def test_topselecties_uses_returns_and_publishes_table_only(monkeypatch):
    from leonteq import management
    returns = Mock(return_value={'saved': ['Example'], 'errors': {}})
    publish = Mock(return_value=True)
    monkeypatch.setattr(management, 'refresh_returns', returns)
    monkeypatch.setattr(management, 'prepare_dashboard_overview', publish)
    ctx = SimpleNamespace(check=Mock(), progress=Mock())
    assert '1 portfolios' in refresh(ctx, 'topselecties')
    assert returns.call_args.args == (ctx,)
    publish.assert_called_once()
    returns.call_args.kwargs['on_saved'](1, 1, 'Example')
    ctx.progress.assert_called_once_with(1, 1, 'Example — MTD and YTD updated',
                                        account='Example', account_done=True)
