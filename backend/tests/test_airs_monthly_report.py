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


def test_report_flags_missing_benchmark_separately_from_airs_refresh(monkeypatch):
    account = 'BUS_Neutraal_Dyn'
    db = FakeSupabase({'airs_performance': [{'portefeuille': account, 'periode': '2026-11-02',
        'rendement': .5, 'cumulatief_rendement': 4}], 'airs_monthly_report': []})
    monkeypatch.setattr(report, 'supabase', db)
    monkeypatch.setattr(report, '_weighted_months', lambda *_: {})
    assert report.read_report(account, '2026-11', include_details=False)['benchmark_needs_refresh'] is True
    monkeypatch.setattr(report, '_weighted_months', lambda *_: {'2026-11': {'return_pct': 0}})
    assert report.read_report(account, '2026-11', include_details=False)['benchmark_needs_refresh'] is False


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


def test_performance_bundle_reads_all_years_and_keeps_canonical_tile_values(monkeypatch):
    class Clock:
        @staticmethod
        def now(tz):
            return SimpleNamespace(year=2026, strftime=lambda _: "2026-11")

    monkeypatch.setattr(report, "datetime", Clock)
    monkeypatch.setattr(report, "month_window", lambda key: (key + "-01", key + "-28", key + "-01"))
    db = FakeSupabase({
        "airs_performance": [
            {"portefeuille": "BOOK", "periode": "2026-11-28", "rendement": .41},
            {"portefeuille": "BOOK", "periode": "2025-12-28", "rendement": 2},
        ],
        "airs_monthly_report": [{
            "portefeuille": "BOOK", "month": "2026-11-01", "datum_tot": "2026-11-28",
            "performance": {"periode": "2026-11-28", "rendement": 99},
            "report": {"positions": [], "sections": {}, "raw_transactions": ["unused"]},
        }],
    })
    monkeypatch.setattr(report, "supabase", db)
    from routers import _airs_monthly_positions
    monkeypatch.setattr(_airs_monthly_positions, "position_performance", lambda *_: [])

    bundle = report.read_performance("BOOK")
    assert bundle["available_years"] == [2026, 2025]
    current = bundle["years"]["2026"]
    assert current["rows"]["2026-11"]["rendement"] == .41
    details = current["details"]["2026-11"]
    assert details["snapshot"]["positions"] == []
    assert "raw_transactions" not in details["snapshot"]
    assert not details["needs_refresh"]
    assert bundle["years"]["2025"]["rows"]["2025-12"]["rendement"] == 2
    assert db.writes == []


def test_performance_prepares_missing_observed_months_before_returning(monkeypatch):
    import dashboard_daily

    bundle = {"portefeuille": "BOOK", "years": {"2026": {
        "warning": None, "months": ["2026-07", "2026-08", "2026-09", "2026-10", "2026-11"],
        "rows": {"2026-07": {"rendement": 1}, "2026-08": {"rendement": 1}, "2026-09": {"rendement": 1}, "2026-11": {"rendement": 0}},
        "details": {
            "2026-07": {"refresh_reason": "ambiguous_position"},
            "2026-08": {"refresh_reason": "incomplete_report"},
            "2026-09": {"refresh_reason": None},
            "2026-10": {"refresh_reason": "missing_report"},
            "2026-11": {"refresh_reason": "missing_report"},
        },
    }}}
    details = bundle["years"]["2026"]["details"]
    attempted = []
    downloaded = []
    def prepare(account, *, month, complete_only):
        assert account == "BOOK" and complete_only
        attempted.append(month)
        if month == "2026-11":
            return {"errors": {"opening": "Missing valuation"}}
        details[month] = {"refresh_reason": None, "snapshot": {"positions": [{"name": "Stock"}]}}
        return {"errors": {}}
    def collect(account, month, ctx):
        assert account == "BOOK"
        downloaded.append(month)
        details[month] = {"refresh_reason": None, "snapshot": {"positions": [{"name": "Stock"}]}}

    monkeypatch.setattr(report, "read_performance", lambda _: bundle)
    monkeypatch.setattr(dashboard_daily, "prepare_month", prepare)
    monkeypatch.setattr(report, "collect_report", collect)

    result = report.prepare_performance("BOOK")
    assert attempted == ["2026-11", "2026-08"]
    assert downloaded == ["2026-11", "2026-07"]
    for month in ["2026-11", "2026-08", "2026-07"]:
        assert result["years"]["2026"]["details"][month]["snapshot"]["positions"] == [{"name": "Stock"}]
    report.prepare_performance("BOOK")
    assert attempted == ["2026-11", "2026-08"]
    assert downloaded == ["2026-11", "2026-07"]


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


def test_filtered_rabobank_exports_keep_principal_and_interest_identifiable(collection, monkeypatch):
    db, lock, calls, ctx = collection
    monkeypatch.setattr('airs_transacties.realised_results', lambda _: SimpleNamespace(unreadable=None))
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync', lambda account, start, end: start.encode())
    def parse(raw):
        closing = raw == b'2026-04-30'
        common = dict(holding_name='6,5% Rabobank Certificaten 14-perp.', isin='XS1002121454',
                      currency='EUR', airs_weight=10, current_price_local=110 if closing else 100)
        return [SimpleNamespace(**common, quantity=1000, current_value_eur=1100 if closing else 1000,
                                valuation_component='security'),
                SimpleNamespace(**common, quantity=1, current_value_eur=60 if closing else 50,
                                valuation_component='accrued_interest')]
    monkeypatch.setattr(portfolio, 'parse_airs_excel', parse)
    report.collect_report('BOOK', '2026-04', ctx)
    saved = db.tables['airs_monthly_report'][0]['report']
    assert saved['positions'][0]['return_pct'] == pytest.approx(10)
    assert saved['sections']['opening']['data']['as_of'] == '2026-03-31'
    assert saved['sections']['closing']['data']['as_of'] == '2026-04-30'
    assert len(saved['sections']['closing']['data']['rows']) == 2
    assert saved['sections']['opening']['data']['rows'][0]['price_local'] == 100
    assert saved['sections']['closing']['data']['rows'][0]['price_local'] == 110


def test_legacy_ambiguous_components_offer_a_dated_airs_refresh(monkeypatch):
    principal = dict(name='Rabo', isin='XS1002121454', currency='EUR', quantity=1000, value_eur=1000)
    interest = {**principal, 'quantity': 1, 'value_eur': 50}
    snapshot = {'positions': [], 'sections': {
        'opening': {'status': 'ok', 'data': {'as_of': '2026-03-31', 'rows': [principal, interest]}},
        'closing': {'status': 'ok', 'data': {'as_of': '2026-04-30', 'rows': [principal, interest]}},
        'transactions': {'status': 'ok', 'data': {'readable': True, 'activity': []}},
    }}
    db = FakeSupabase({'airs_account_roster': [{'portefeuille': 'BOOK'}],
        'airs_monthly_report': [{'portefeuille': 'BOOK', 'month': '2026-04-01',
        'datum_tot': '2026-04-30', 'fetched_at': '2026-11-01T00:00:00Z', 'report': snapshot}]})
    monkeypatch.setattr(report, 'supabase', db)
    result = report.read_report('BOOK', '2026-04')
    assert result['snapshot']['positions'][0]['reason'] == 'ambiguous_position'
    assert result['needs_refresh'] is True
    details = report.read_performance('BOOK')['years']['2026']['details']['2026-04']
    assert details['refresh_reason'] == 'ambiguous_position'
    assert db.writes == []


def test_performance_bundle_resolves_rabobank_principal_with_unquoted_interest_row(monkeypatch):
    principal = dict(name='6,5% Rabobank Certificaten 14-perp.', isin='XS1002121454',
                     currency='EUR', quantity=1000, price_local=100, value_eur=1000)
    interest = {**principal, 'quantity': None, 'price_local': None, 'value_eur': 50}
    closing = {**principal, 'price_local': 101, 'value_eur': 1010}
    sections = {
        'opening': {'status': 'ok', 'data': {'as_of': '2026-03-31', 'rows': [interest, principal]}},
        'closing': {'status': 'ok', 'data': {'as_of': '2026-04-30', 'rows': [closing, interest]}},
        'transactions': {'status': 'ok', 'data': {'readable': True, 'activity': []}},
    }
    db = FakeSupabase({
        'airs_performance': [dict(portefeuille='BOOK', periode='2026-04-30', rendement=100 * 10 / 1050,
                                  beginvermogen=1050, eindvermogen=1060, stortingen=0, onttrekkingen=0,
                                  opbrengsten=0, mutatie_opgelopen_rente=0, kosten=0)],
        'airs_monthly_report': [dict(portefeuille='BOOK', month='2026-04-01', datum_tot='2026-04-30',
                                     report={'positions': [], 'sections': sections})],
    })
    monkeypatch.setattr(report, 'supabase', db)
    details = report.read_performance('BOOK')['years']['2026']['details']['2026-04']
    position, = details['snapshot']['positions']
    assert position['isin'] == 'XS1002121454'
    assert position['return_pct'] == pytest.approx(1)
    assert position['opening_weight_pct'] == pytest.approx(100 * 1000 / 1050)
    assert position['contribution_pp'] == pytest.approx(100 * 10 / 1050)
    assert details['refresh_reason'] is None
    assert db.writes == []
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
