from datetime import date
from types import SimpleNamespace
import threading

import pytest
from fastapi import HTTPException

import airs_scanner
import airs_vermogen
import portfolio
from airs_mutaties import Mutatie
from airs_transacties import ParsedSheet
from jobs import JobCancelled, UserFacingJobError
from routers import airs, _airs_monthly_report as report
from tests._fake_supabase import FakeSupabase


def test_acwi_months_use_own_month_boundaries_and_eur_fx(monkeypatch):
    from routers import _benchmark_index
    db = FakeSupabase({'benchmark': [{'ticker': 'ACWI', 'benchmark_id': 1}], 'benchmark_price': [
        {'benchmark_id': 1, 'target_date': '2026-08-31', 'price': 100},
        {'benchmark_id': 1, 'target_date': '2026-09-30', 'price': 110},
        {'benchmark_id': 1, 'target_date': '2026-10-23', 'price': 121},
    ]})
    monkeypatch.setattr(report, 'supabase', db)
    monkeypatch.setattr(_benchmark_index, '_fx_to_eur', lambda *_: {})
    monkeypatch.setattr(_benchmark_index, '_rate', lambda _fx, _ccy, day: 1.1 if day == '2026-10-23' else 1)
    result = report._acwi_months([
        {'month': '2026-09', 'periode': '2026-09-30'},
        {'month': '2026-10', 'periode': '2026-10-23'},
    ])
    assert result['2026-09']['return_pct'] == pytest.approx(10)
    assert result['2026-10']['return_pct'] == pytest.approx(0)
    assert result['2026-10']['end_date'] == '2026-10-23'
    assert result['2026-10']['start_price_eur'] == pytest.approx(110)
    assert result['2026-10']['end_price_eur'] == pytest.approx(110)
    assert db.writes == []


def test_neutraal_monthly_benchmark_uses_reviewed_60_40_policy(monkeypatch):
    def returns(_months, ticker='ACWI', currency='USD'):
        return {'2026-09': {'return_pct': 10 if ticker == 'ACWI' else 2,
                            'start_date': '2026-08-31', 'end_date': '2026-09-30'}}
    monkeypatch.setattr(report, '_acwi_months', returns)
    months = [{'month': '2026-09', 'periode': '2026-09-30'}]
    result = report._weighted_months('BUS_Neutraal_FX', months)
    assert result['2026-09']['return_pct'] == pytest.approx(6.8)
    assert result['2026-09']['source'] == '60% ACWI / 40% AGGH.AS · EUR'
    assert [(c['ticker'], c['weight_pct'], c['return_pct']) for c in result['2026-09']['components']] == [('ACWI', 60, 10), ('AGGH.AS', 40, 2)]
    monkeypatch.setattr(report, '_acwi_months', lambda *args: {})
    assert report._weighted_months('BUS_Neutraal_FX', months) == {}
    assert report._weighted_months('UNKNOWN', months) == {}


def test_calendar_window_never_requests_unvalued_today_or_future_month():
    assert report.month_window('2026-10', date(2026, 10, 26)) == ('2026-10-01', '2026-10-23', '2026-09-30')
    assert report.month_window('2024-02', date(2026, 10, 26)) == ('2024-02-01', '2024-02-29', '2024-01-31')
    for month in ('2026-13', 'bad', '2027-01'):
        with pytest.raises(HTTPException) as exc:
            report.month_window(month, date(2026, 10, 26))
        assert exc.value.status_code == 422


def test_read_uses_latest_monthly_observation_and_does_not_download(monkeypatch):
    db = FakeSupabase({'airs_performance': [
        {'portefeuille': 'BOOK', 'periode': '2026-04-15', 'rendement': 1},
        {'portefeuille': 'BOOK', 'periode': '2026-04-30', 'rendement': 2},
        {'portefeuille': 'OTHER', 'periode': '2026-04-30', 'rendement': 99},
    ]})
    monkeypatch.setattr(report, 'supabase', db)
    result = report.read_report('BOOK', '2026-04')
    assert result['months'][0]['rendement'] == 2
    assert result['snapshot'] is None
    assert result['needs_refresh'] is True
    assert db.writes == []


def test_month_strip_does_not_load_or_reconcile_position_details(monkeypatch):
    db = FakeSupabase({'airs_performance': [{'portefeuille': 'BOOK', 'periode': '2026-10-23', 'rendement': 1}],
        'airs_monthly_report': [{'portefeuille': 'BOOK', 'month': '2026-10-01', 'datum_tot': '2026-10-23',
                                'report': {'positions': [{'name': 'Not needed yet'}]}}]})
    monkeypatch.setattr(report, 'supabase', db)
    from routers import _airs_monthly_positions as positions
    monkeypatch.setattr(positions, 'position_performance', lambda *_: pytest.fail('positions on month strip'))
    result = report.read_report('BOOK', '2026-10', include_details=False)
    assert result['snapshot'] is None
    assert result['months'][0]['rendement'] == 1
    assert db.writes == []


def test_custom_month_report_cannot_replace_the_standard_airs_month_return(monkeypatch):
    canonical = {'portefeuille': 'BOOK', 'periode': '2026-10-23', 'rendement': 1.014177,
                 'cumulatief_rendement': 3.752587}
    custom = {'periode': '2026-10-23', 'rendement': .868548}
    db = FakeSupabase({'airs_performance': [canonical], 'airs_monthly_report': [{
        'portefeuille': 'BOOK', 'month': '2026-10-01', 'datum_tot': '2026-10-23',
        'performance': custom, 'report': {'positions': [], 'sections': {
            'performance': {'status': 'ok', 'data': custom},
        }},
    }]})
    monkeypatch.setattr(report, 'supabase', db)
    result = report.read_report('BOOK', '2026-10')
    assert result['months'][0]['rendement'] == 1.014177
    assert result['snapshot']['sections']['performance']['data']['rendement'] == 1.014177
    assert result['snapshot']['reconciliation']['status'] == 'unavailable'
    assert db.writes == []


def test_cached_historical_months_keep_their_returns_in_the_year_strip(monkeypatch):
    db = FakeSupabase({'airs_performance': [{'portefeuille': 'BOOK', 'periode': '2026-10-23'}],
        'airs_monthly_report': [{
            'portefeuille': 'BOOK', 'month': '2025-04-01', 'datum_tot': '2025-04-30',
            'fetched_at': '2026-10-26T12:00:00Z',
            'performance': {'periode': '2025-04-30', 'rendement': -2},
            'report': {'sections': {'performance': {'status': 'ok', 'data': {'rendement': -2}}}},
        }]})
    monkeypatch.setattr(report, 'supabase', db)
    result = report.read_report('BOOK', '2025-05')
    assert result['months'][0]['month'] == '2025-04'
    assert result['months'][0]['rendement'] == -2
    assert result['snapshot'] is None
    assert result['needs_refresh'] is True


@pytest.fixture
def collection(monkeypatch):
    db = FakeSupabase({'airs_performance': [{'portefeuille': 'BOOK', 'periode': '2026-04-30'}]})
    monkeypatch.setattr(report, 'supabase', db)
    monkeypatch.setattr(report, 'month_window', lambda _month: ('2026-04-01', '2026-04-30', '2026-03-31'))
    lock = threading.Lock()
    monkeypatch.setattr(airs_vermogen, '_LOCK', lock)
    monkeypatch.setattr(airs_vermogen, '_acquire_session', lambda _wait: lock.acquire(False))
    calls = []
    for name in ('download_portfolio_sync', 'download_vermogensoverzicht_sync', 'download_transacties_sync', 'download_mutaties_sync'):
        def download(account, start, end, name=name):
            calls.append((name, account, start, end))
            return b'spreadsheet'
        monkeypatch.setattr(airs_scanner, name, download)
    monkeypatch.setattr(airs, '_parse_att_excel', lambda _raw: [
        {'periode': '2026-03-31', 'rendement': 99},
        {'periode': '2026-04-30', 'rendement': -2, 'beleggingsresultaat': -200},
    ])
    monkeypatch.setattr(portfolio, 'parse_airs_excel', lambda _raw: [SimpleNamespace(
        holding_name='Stock', isin='TEST', currency='EUR', quantity=1,
        current_value_eur=100, airs_weight=10,
        ytd_return_pct=999, fund_result_eur=999,
    )])
    monkeypatch.setattr('airs_transacties.parse_transacties', lambda _raw: ParsedSheet())
    monkeypatch.setattr('airs_mutaties.parse_mutaties', lambda _raw: [
        Mutatie('Dividend', date(2026, 4, 15), 'Stock', 'Dividend', 5),
        Mutatie('Dividend', date(2026, 3, 15), 'Stock', 'Prior month', 50),
    ])
    ctx = SimpleNamespace(check=lambda: None, progress=lambda *_: None)
    return db, lock, calls, ctx


def test_collection_uses_exact_windows_and_never_overwrites_current_data(collection):
    db, lock, calls, ctx = collection
    report.collect_report('BOOK', '2026-04', ctx)
    assert not lock.locked()
    assert len(calls) == 4
    assert ('download_vermogensoverzicht_sync', 'BOOK', '2026-03-31', '2026-03-31') in calls
    assert ('download_vermogensoverzicht_sync', 'BOOK', '2026-04-30', '2026-04-30') in calls
    assert ('download_portfolio_sync', 'BOOK', '2026-01-01', '2026-04-30') in calls
    saved = db.tables['airs_monthly_report'][0]['report']['sections']
    assert saved['performance']['data']['rendement'] == -2
    assert 'journal' not in saved
    assert 'ytd_return_pct' not in saved['closing']['data']['rows'][0]
    assert db.tables['airs_performance'] == [{'portefeuille': 'BOOK', 'periode': '2026-04-30'}]
    assert all(w[1] == 'airs_monthly_report' for w in db.writes)


def test_partial_failure_is_saved_and_reported_instead_of_silent_success(collection, monkeypatch):
    db, lock, calls, ctx = collection
    def fail(*_args):
        raise RuntimeError('login unavailable')
    monkeypatch.setattr(airs_scanner, 'download_transacties_sync', fail)
    with pytest.raises(UserFacingJobError, match='partial report'):
        report.collect_report('BOOK', '2026-04', ctx)
    assert not lock.locked()
    sections = db.tables['airs_monthly_report'][0]['report']['sections']
    assert sections['transactions']['status'] == 'error'
    assert sections['performance']['status'] == 'ok'


def test_cancel_releases_session_and_does_not_persist_partial_work(collection):
    db, lock, calls, _ctx = collection
    def check():
        if calls:
            raise JobCancelled('stop')
    ctx = SimpleNamespace(check=check, progress=lambda *_: None)
    with pytest.raises(JobCancelled):
        report.collect_report('BOOK', '2026-04', ctx)
    assert not lock.locked()
    assert db.writes == []


def test_busy_session_does_not_download_or_write(collection):
    db, lock, calls, ctx = collection
    lock.acquire()
    try:
        with pytest.raises(RuntimeError, match='busy'):
            report.collect_report('BOOK', '2026-04', ctx)
        assert not calls
        assert db.writes == []
    finally:
        lock.release()


def test_historical_valuation_walks_back_without_claiming_the_requested_date(collection, monkeypatch):
    db, lock, calls, ctx = collection
    def valuation(_account, start, end):
        if end == '2026-04-30':
            raise airs_scanner.AirsNoData('no valuation')
        return b'sheet'
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync', valuation)
    report.collect_report('BOOK', '2026-04', ctx)
    closing = db.tables['airs_monthly_report'][0]['report']['sections']['closing']['data']
    assert closing['requested_date'] == '2026-04-30'
    assert closing['as_of'] == '2026-04-29'
    assert not lock.locked()


def test_known_no_data_is_distinct_from_an_empty_successful_report(collection, monkeypatch):
    db, lock, calls, ctx = collection
    def no_data(*_args):
        raise airs_scanner.AirsNoData('no trades')
    monkeypatch.setattr(airs_scanner, 'download_transacties_sync', no_data)
    report.collect_report('BOOK', '2026-04', ctx)
    sections = db.tables['airs_monthly_report'][0]['report']['sections']
    assert sections['transactions']['status'] == 'no_data'
    assert sections['transactions']['data'] is None


def test_report_job_is_an_exact_dashboard_refresh_permission():
    from routers._auth_middleware import _is_user_refresh
    assert _is_user_refresh('/api/airs/accounts/BOOK/monthly-report/job')
    assert not _is_user_refresh('/api/airs/accounts/BOOK/monthly-report/delete')
