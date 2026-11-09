from unittest.mock import Mock
import pytest
from fastapi import HTTPException
from leonteq import monthly_performance as monthly
from tests._fake_supabase import FakeSupabase


def test_monthly_airs_cancellation_keeps_its_cancelled_status(monkeypatch):
    import airs_vermogen
    import airs_scanner
    from jobs import JobCancelled

    lock = Mock()
    monkeypatch.setattr(airs_vermogen, '_LOCK', lock)
    monkeypatch.setattr(airs_vermogen, '_acquire_session', lambda **_: True)
    monkeypatch.setattr(airs_vermogen, 'latest_completed_market_date', lambda: '2026-11-06')
    monkeypatch.setattr(airs_scanner, 'download_portfolio_sync', Mock(side_effect=JobCancelled('Stopped')))
    with pytest.raises(JobCancelled):
        monthly._refresh_airs('Dynamic')
    lock.release.assert_called_once()

def test_details_table_preserves_missing_zero_and_negative_returns():
    table = [['Year', *[m.title() for m in monthly.MONTHS], 'Total'],
             ['2026', '-', '0%', '-1.2%', '0.69%', '1.94%', '1.57%', '-0.22%', '2.29%', '-5.13%', '3.59%', '2%', '-', '6.68%']]
    rows = monthly.parse_tables([table])
    assert len(rows) == 12
    assert rows[0]['return_pct'] is None
    assert rows[1]['return_pct'] == 0
    assert rows[2]['return_pct'] == -1.2
    assert rows[10]['return_pct'] == 2
    table[1][3] = 'unknown'
    with pytest.raises(ValueError):
        monthly.parse_tables([table])


def test_details_waits_for_populated_rows_before_parsing_asia_returns():
    from unittest.mock import MagicMock
    page = MagicMock()
    page.locator.return_value.filter.return_value.count.return_value = 1
    snapshot = page.wait_for_function.return_value
    snapshot.json_value.return_value = [[
        ['YEAR', *[month.upper() for month in monthly.MONTHS], 'TOTAL'],
        ['2026', '-', '-', '-', '-', '-', '-', '-1.13%', '0.41%', '1.39%', '-4.91%', '-0.92%', '-', '-5.16%'],
    ]]
    result = monthly.read_details(page, 'AziëTopSelectie')
    page.wait_for_function.assert_called_once_with(monthly._READY_PERFORMANCE_TABLES, timeout=25000)
    snapshot.dispose.assert_called_once()
    assert result['success'] is True
    assert result['months'][6]['return_pct'] == -1.13
    assert result['months'][10]['return_pct'] == -.92
    assert result['yearly_totals'] == {'2026': -5.16}

def test_comparison_caches_details_and_reads_latest_monthly_airs_return(monkeypatch):
    from leonteq import portfolio_links, portal_login
    db = FakeSupabase({'airs_performance': [
        {'portefeuille': 'Dynamic', 'periode': '2026-11-03', 'rendement': 1, 'fetched_at': '2026-11-04'},
        {'portefeuille': 'Dynamic', 'periode': '2026-11-04', 'rendement': 1.5, 'cumulatief_rendement': 5.68, 'fetched_at': '2026-11-05'},
        {'portefeuille': 'Dynamic', 'periode': '2026-10-31', 'rendement': 0, 'fetched_at': '2026-11-05'},
        {'portefeuille': 'Other', 'periode': '2026-11-04', 'rendement': 99, 'fetched_at': '2026-11-05'},
    ]})
    monkeypatch.setattr(monthly, 'supabase', db)
    airs_refresh = Mock()
    monkeypatch.setattr(monthly, '_refresh_airs', airs_refresh)
    monkeypatch.setattr(monthly, '_account_key', lambda: 'account')
    db.tables['leonteq_airs_portfolio_link'] = [{'account_key': 'account', 'leonteq_name': 'Source', 'airs_portfolio_id': 2}]
    db.tables['airs_model_portfolio'] = [{'id': 2, 'name': 'Dynamic', 'portfolio_type': 'normaal'}]
    monkeypatch.setattr(portfolio_links, 'supabase', db)
    monkeypatch.setattr(portfolio_links, '_account_key', lambda: 'account')
    login = Mock(return_value={'success': True, 'months': [{'month': '2026-11', 'return_pct': 2}, {'month': '2026-10', 'return_pct': 0}], 'yearly_totals': {'2026': 6.68}, 'retrieved_at': '2026-11-05T13:00:00Z'})
    monkeypatch.setattr(portal_login, 'check_performance', login)
    assert monthly.comparison('Source', 2)['months'][0]['difference_pp'] == .5
    assert monthly.comparison('Source', 2)['months'][1]['difference_pp'] == 0
    ytd = monthly.comparison('Source', 2)['ytd']['2026']
    assert ytd['airs_pct'] == 5.68
    assert ytd['leonteq_pct'] == 6.68
    assert ytd['difference_pp'] == pytest.approx(1)
    assert login.call_count == 1
    login.return_value = {'success': False, 'status': 'timeout'}
    result = monthly.comparison('Source', 2, refresh=True)
    airs_refresh.assert_called_once_with('Dynamic')
    assert result['refresh_error'] == 'timeout'
    assert result['refresh_errors'][0]['source'] == 'Leonteq'
    assert result['refresh_errors'][0]['code'] == 'timeout'
    assert result['saved_data'] is True
    assert result['months'][0]['leonteq_pct'] == 2
    with pytest.raises(HTTPException):
        monthly.comparison('Unknown', 2)


def test_explicit_refresh_reads_both_independent_sources_concurrently(monkeypatch):
    from threading import Barrier
    from leonteq import portfolio_links, portal_login
    from leonteq.progress import observe, report

    monkeypatch.setattr(monthly, 'supabase', FakeSupabase({}))
    monkeypatch.setattr(monthly, '_account_key', lambda: 'account')
    monkeypatch.setattr(portfolio_links, 'linked_dynamic', lambda *args: {'airs_name': 'Dynamic'})
    rendezvous = Barrier(2, timeout=3)
    messages = []

    def airs(_name):
        rendezvous.wait()
        report('AIRS complete')

    def leonteq(_name):
        rendezvous.wait()
        report('Leonteq complete')
        return {'success': True, 'months': [], 'yearly_totals': {}, 'retrieved_at': '2026-11-09'}

    monkeypatch.setattr(monthly, '_refresh_airs', airs)
    monkeypatch.setattr(portal_login, 'check_performance', leonteq)
    with observe(lambda en, nl: messages.append(en)):
        result = monthly.comparison('Source', 2, refresh=True)
    assert result['success'] is True
    assert not result['refresh_errors']
    assert 'AIRS complete' in messages and 'Leonteq complete' in messages


def test_refresh_downloads_only_performance_and_releases_session(monkeypatch):
    import airs_scanner
    import airs_vermogen
    from routers import airs
    download, save, lock = Mock(return_value=b'report'), Mock(), Mock()
    monkeypatch.setattr(airs_scanner, 'download_portfolio_sync', download)
    monkeypatch.setattr(airs_vermogen, '_acquire_session', Mock(return_value=True))
    monkeypatch.setattr(airs_vermogen, '_LOCK', lock)
    monkeypatch.setattr(airs_vermogen, 'latest_completed_market_date', lambda: '2026-11-04')
    monkeypatch.setattr(airs, '_parse_att_excel', Mock(return_value=[{'periode': '2026-11-04', 'rendement': 2}]))
    monkeypatch.setattr(airs, '_save_performance_to_db', save)
    monthly._refresh_airs('Dynamic')
    download.assert_called_once_with('Dynamic', '2026-01-01', '2026-11-04')
    assert save.call_args.kwargs == {'prune_after': '2026-11-04'}
    lock.release.assert_called_once()
