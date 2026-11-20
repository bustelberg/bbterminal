from unittest.mock import Mock

import pytest

from leonteq import management as m
from leonteq import accounting
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def sources(monkeypatch):
    monkeypatch.setattr(accounting, 'saved', Mock(return_value=None))
    store = FakeSupabase({})
    monkeypatch.setattr(m, 'supabase', store)
    monkeypatch.setattr(m.portfolio_cache, '_account_key', lambda: 'account')
    catalogue = {'success': True, 'status': 'success', 'retrieved_at': '2026-11-16T10:00:00Z',
                 'portfolios': [{'name': 'AI-TopSelectie', 'ytd': '12.34%'}]}
    monkeypatch.setattr(m.portfolio_cache, 'saved_portfolios', Mock(return_value=catalogue))
    monkeypatch.setattr(m.portfolio_cache, 'portfolios', Mock(return_value=catalogue))
    composition = Mock(return_value={'success': True, 'groups': [{'name': 'Equity', 'positions': [
        {'name': 'Correct stock', 'isin': 'TEST', 'weight': '25.00%'}]}]})
    monkeypatch.setattr(m.composition_cache, 'composition', composition)
    return store, composition


def test_explicit_refresh_acquires_source_accounting_and_keeps_missing_boundary_visible(sources, monkeypatch):
    sources[1].return_value['as_of'] = '2026-11-12'
    monkeypatch.setattr(m, 'performance', lambda *_: {'success': True})
    ensure_window = Mock(return_value=(None, 'History unavailable'))
    monkeypatch.setattr(m.return_windows, 'ensure', ensure_window)
    ledger = {'success': False, 'reason': 'Leonteq opening valuation is unavailable for 2026-06-08', 'result': {'available': False}}
    refresh = Mock(return_value=ledger)
    monkeypatch.setattr(accounting, 'refresh', refresh)
    result = m.detail('AITopSelectie', refresh=True)
    assert result['success'] is True
    assert result['accounting'] == ledger
    ensure_window.assert_called_once()
    refresh.assert_called_once_with('AI-TopSelectie', '2026-11-12', None)


def test_monthly_report_reuses_verified_history_when_catalogue_observation_has_not_changed():
    window = {'portfolio_name': 'AI-TopSelectie', 'currency': 'EUR',
        'from': '2026-06-08', 'to': '2026-11-17', 'ytd_pct': -3.66,
        'retrieved_at': '2026-11-18T08:00:00Z'}
    source = {'leonteq_name': 'AI-TopSelectie', 'return_window': window,
        'catalogue_retrieved_at': '2026-11-18T09:00:00Z',
        'summary': {'as_of': '2026-11-17', 'ytd': '-3.66%'}}
    assert m.return_windows.cached(source) is None
    assert m.return_windows.for_monthly_report(source) == window
    source['summary']['as_of'] = '2026-11-18'
    assert m.return_windows.for_monthly_report(source) is None


def test_previous_accounting_cannot_leak_into_a_newer_source_snapshot(sources):
    sources[1].return_value['retrieved_at'] = '2026-11-16T10:00:00Z'
    accounting.saved.return_value = {'success': True, 'retrieved_at': '2026-11-15T10:00:00Z',
        'result': {'available': True, 'reported_ytd_pct': 12.34}}
    result = m.detail('AITopSelectie')
    assert result['accounting']['success'] is False
    assert result['accounting']['result']['available'] is False
    assert 'predates' in result['accounting']['reason']


def test_analysis_uses_dated_accounting_without_replacing_newer_headline(monkeypatch):
    from leonteq import portal_api

    monkeypatch.setattr(portal_api, 'composition', lambda closing, name: {
        'as_of': '2026-11-17', 'groups': closing['groups']})
    monkeypatch.setattr(m.return_windows, 'cached', lambda source: {
        'to': '2026-11-17', 'ytd_pct': 3.0})
    ledger = {'success': True, 'retrieved_at': '2026-11-18',
              'result': {'available': True, 'to': '2026-11-17', 'reported_ytd_pct': 3.0},
              'valuations': {'2026-11-17': {'groups': [{'name': 'dated holdings'}]}}}
    result = m._snapshot_detail('EuropaTopSelectie', {}, {'name': 'Source', 'as_of': '2026-11-19', 'ytd': '3.46%'},
                                {'retrieved_at': '2026-11-20', 'as_of': '2026-11-19'},
                                {}, ledger, prefer_accounting_snapshot=True)
    assert result['accounting']['result']['available']
    assert result['composition']['as_of'] == '2026-11-17'
    assert result['summary']['as_of'] == '2026-11-19'


def test_reconciled_ledger_selects_its_actual_dated_closing_composition(sources):
    m.portfolio_cache.saved_portfolios.return_value['portfolios'][0]['ytd'] = '0.00%'
    sources[1].return_value['retrieved_at'] = '2026-11-16T08:00:00Z'
    raw = {'closingPriceDate': '2026-11-12', 'rows': [{
        'name': 'EUR', 'instrumentCategory': 'Cash', 'amcCurrency': 'EUR', 'currency': 'EUR',
        'totalValueAmcCcy': 100, 'weightPercentAmcCcy': 100, 'totalUnits': 100,
        'currentPrice': {'value': 1}, 'instrumentIsin': None,
    }], 'totals': {'totalValueAmcCcy': 100, 'weightPercentAmcCcy': 100}}
    accounting.saved.return_value = {'success': True, 'retrieved_at': '2026-11-16T11:00:00Z',
        'result': {'available': True, 'reported_ytd_pct': 0, 'to': '2026-11-12'},
        'valuations': {'2026-11-12': raw}}
    result = m.detail('AITopSelectie')
    assert result['accounting']['success'] is True
    assert result['summary']['as_of'] == '2026-11-12'
    assert result['composition']['as_of'] == '2026-11-12'
    assert result['composition']['total_value'] == '100.00 EUR'


def test_overview_uses_complete_allowlist_even_without_airs_accounts(sources):
    rows = m.overview()
    assert len(rows) == 10
    assert rows[0]['summary']['ytd'] == '12.34%'
    assert rows[0]['source'] == 'Leonteq'
    assert rows[1]['summary'] is None
    assert rows[1]['status'] == 'portfolio_not_found'


def test_missing_leonteq_is_unavailable_and_never_requests_composition(sources):
    _, composition = sources
    assert m.detail('EuropaTopSelectie')['success'] is False
    composition.assert_not_called()
    assert m.positions_for_identity('EuropaTopSelect OFF FX') == []
    assert m.positions_for_identity('BUS_Offensief_Dyn') is None


def test_ambiguous_catalogue_fails_closed(sources):
    assert m.resolve(m.entries()[0], [{'name': 'AI-TopSelectie Index'}, {'name': 'AI TopSelectie Index'}]) is None


def test_reviewed_canonical_name_takes_precedence_over_other_product_aliases():
    star = next(row for row in m.entries() if row['display_name'] == 'StarTopSelectie')
    selected = m.resolve(star, [{'name': 'Star Selection Index'}, {'name': 'StarTopSelectie Index'}])
    assert selected['name'] == 'Star Selection Index'


def test_accented_leonteq_name_matches_reviewed_ascii_identity():
    asia = next(row for row in m.entries() if row['display_name'] == 'AzieTopSelectie')
    assert m.resolve(asia, [{'name': 'AziëTopSelectie'}])['name'] == 'AziëTopSelectie'


def test_reading_detail_never_logs_in_or_refreshes_airs(sources, monkeypatch):
    login = Mock(side_effect=AssertionError('Opening the page must not log in'))
    monkeypatch.setattr('leonteq.portal_login.check_performance', login)
    result = m.detail('AITopSelectie')
    assert result['composition']['success']
    assert result['performance'] == {'success': False, 'status': 'not_cached'}
    sources[1].assert_called_once_with('AI-TopSelectie', refresh=False, saved_only=True)
    login.assert_not_called()


def test_failed_performance_refresh_retains_only_leonteq_snapshot(sources, monkeypatch):
    store, _ = sources
    saved = {'success': True, 'retrieved_at': '2026-11-15', 'months': [{'month': '2026-10', 'return_pct': 4}]}
    store.tables['leonteq_performance_snapshot'] = [{'account_key': 'account', 'portfolio_name': 'AI-TopSelectie', 'payload': saved}]
    monkeypatch.setattr('leonteq.portal_login.check_performance', lambda _: {'success': False, 'status': 'timeout'})
    assert m.performance('AI-TopSelectie', True) == {**saved, 'refresh_error': 'timeout'}
    assert m.performance('Other', True) == {'success': False, 'status': 'timeout'}


def test_lookthrough_never_uses_airs_constituents(sources, monkeypatch):
    from routers import _airs_lookthrough as look
    monkeypatch.setattr(look, 'ref_model', lambda _: {'name': 'AITopSelectie OFF FX'})
    monkeypatch.setattr(look, 'ref_positions_for', lambda _: pytest.fail('AIRS fallback'))
    assert look._positions_of(42, '2026-01-01')[0]['fonds'] == 'Correct stock'
    sources[1].return_value = {'success': False, 'status': 'not_cached'}
    assert look._positions_of(42, '2026-01-01') == []


def test_regular_portfolio_lookthrough_keeps_airs(sources, monkeypatch):
    from routers import _airs_lookthrough as look
    monkeypatch.setattr(look, 'ref_model', lambda _: {'name': 'BUS_Offensief_Dyn'})
    positions = [{'fonds': 'AIRS stock', 'datum': '2026-11-16'}]
    monkeypatch.setattr(look, 'ref_positions_for', lambda _: positions)
    assert look._positions_of(42, '2026-11-16') == positions
    sources[1].assert_not_called()


def test_dynamic_certificate_fallback_is_also_leonteq_only(sources):
    from routers._airs_portfolio_analysis import _dynamic_account_positions
    assert _dynamic_account_positions('EuropaTopSelectie Index') == []
    assert _dynamic_account_positions('AI-TopSelectie Index')[0]['source'] == 'Leonteq'


def test_monthly_child_reports_do_not_read_airs_for_topselecties(sources, monkeypatch):
    from routers import _airs_monthly_lookthrough as monthly
    monkeypatch.setattr(monthly, 'supabase', Mock(side_effect=AssertionError('AIRS read')))
    assert monthly._child_reports('StarTopSelectie OFF DYN', {'2026-10': '2026-10-31'}) == {}


def test_prepared_dashboard_reads_the_bundle_without_loading_source_snapshots(sources, monkeypatch):
    rows = [{'name': 'AITopSelectie', 'fetched_at': '2026-11-18T08:00:00Z'}]
    build = Mock(return_value=rows)
    monkeypatch.setattr(m, '_build_dashboard_overview', build)
    assert m.prepare_dashboard_overview()
    build.reset_mock()
    assert m.dashboard_overview() == rows
    build.assert_not_called()


def test_shared_dashboard_contract_uses_source_returns_and_explicit_dates(monkeypatch):
    month = m.datetime.now(m.ZoneInfo('Europe/Amsterdam')).strftime('%Y-%m')
    monkeypatch.setattr(m, '_dashboard_snapshots', lambda entries, **_: {entries[0]['display_name']: {
        'success': True, 'catalogue_retrieved_at': '2026-11-16T10:00:00Z',
        'summary': {'ytd': '8.78%', 'inception_date': '2026-06-01', 'as_of': '2026-11-12'},
        'composition': {'groups': [{'positions': [{'isin': 'TEST'}]}]},
        'performance': {'months': [{'month': month, 'return_pct': 1.82}]},
    }})
    row = m.dashboard_overview()[0]
    assert row['ytd_pct'] == 8.78
    assert row['latest_month_pct'] == 1.82
    assert row['ytd_from'] == '2026-06-01'
    assert row['as_of'] == '2026-11-12'
    assert row['fixed_portfolio_id'] is None
    assert row['link_source'] == 'leonteq'


def test_shared_dashboard_does_not_invent_values_or_valuation_dates(monkeypatch):
    monkeypatch.setattr(m, '_dashboard_snapshots', lambda entries, **_: {entries[0]['display_name']: {
        'success': False, 'catalogue_retrieved_at': '2026-11-16T10:00:00Z',
        'summary': {'ytd': '0.00%', 'start_date': '19 May 2025'},
        'performance': {'months': [{'month': '2025-01', 'return_pct': 99}]},
        'composition': {'groups': [{'positions': [{'price_date': '2026-11-12'}]}]},
    }})
    row = m.dashboard_overview()[0]
    assert row['ytd_pct'] == 0
    assert row['latest_month_pct'] is None
    assert row['as_of'] is None
    assert row['ytd_from'] == '2026-01-01'
    monkeypatch.setattr(m, '_dashboard_snapshots', lambda entries, **_: {entries[0]['display_name']: {'success': False}})
    missing = m.dashboard_overview()[0]
    assert missing['ytd_pct'] is None
    assert missing['latest_month_pct'] is None
    assert missing['ytd_from'] is None


def test_missing_monthly_cache_uses_verified_index_history(monkeypatch):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    month = datetime.now(ZoneInfo('Europe/Amsterdam')).strftime('%Y-%m')
    window = {'from': '2025-12-31', 'to': month + '-16', 'ytd_pct': -1.819,
        'returns': [{'date': '2025-12-31', 'cum_pct': 0},
                    {'date': month + '-02', 'cum_pct': -2}, {'date': month + '-16', 'cum_pct': -1.819}]}
    monkeypatch.setattr(m, '_dashboard_snapshots', lambda entries, **_: {entries[0]['display_name']: {'performance': {'months': []}}})
    monkeypatch.setattr(m.return_windows, 'cached', lambda _: window)
    row = m.dashboard_overview()[0]
    assert row['latest_month_pct'] == pytest.approx(-1.819)
    assert row['periode'] == month + '-01'


def test_dashboard_dates_follow_the_return_observation_not_catalogue(monkeypatch):
    window = {'from': '2025-12-31', 'to': '2026-11-16', 'ytd_pct': 2,
        'retrieved_at': '2026-11-17T12:00:00Z', 'returns': [
            {'date': '2026-10-30', 'cum_pct': 0}, {'date': '2026-11-16', 'cum_pct': 2}]}
    monkeypatch.setattr(m, '_dashboard_snapshots', lambda entries, **_: {entries[0]['display_name']: {'catalogue_retrieved_at': '2026-11-13T12:00:00Z',
        'performance': {'months': []}}})
    monkeypatch.setattr(m.return_windows, 'cached', lambda _: window)
    row = m.dashboard_overview()[0]
    assert row['as_of'] == '2026-11-16'
    assert row['fetched_at'] == '2026-11-17T12:00:00Z'


def test_mtd_uses_compounded_returns_and_requires_a_month_opening_mark():
    window = {'from': '2025-12-31', 'returns': [
        {'date': '2026-10-30', 'cum_pct': 20}, {'date': '2026-11-16', 'cum_pct': 26}]}
    assert m._month_return(window, '2026-11') == pytest.approx(5)
    assert m._month_return(window, '2026-12') is None
    assert m._month_return({'from': '2025-12-31', 'returns': [
        {'date': '2026-11-16', 'cum_pct': 26}]}, '2026-11') is None


def test_monthly_benchmark_uses_same_partial_month_windows_and_saved_eur_prices(monkeypatch):
    from routers import _benchmark_etf
    window = {'from': '2026-06-08', 'to': '2026-07-15', 'returns': [
        {'date': '2026-06-08', 'cum_pct': 0}, {'date': '2026-06-30', 'cum_pct': 20},
        {'date': '2026-07-15', 'cum_pct': 26}]}
    monkeypatch.setattr(m.return_windows, 'cached', lambda _: window)
    def series(label, anchor, dates, **kwargs):
        assert label == 'ACWI' and anchor == '2026-06-08'
        assert kwargs == {'read_only': True}
        return {'ticker': 'ACWI', 'start_price': 120, 'fx_start': 1.2, 'points': [
            {'date': stamp, 'price_date': stamp, 'cum_pct': value}
            for stamp, value in zip(dates, [0, 10, 21])]}
    monkeypatch.setattr(_benchmark_etf, 'etf_return_series', series)
    result = m._monthly_benchmark({'performance': {'months': [
        {'month': '2026-06', 'return_pct': 20}, {'month': '2026-07', 'return_pct': 5}]}}, 'ACWI')
    assert result['2026-06']['return_pct'] == pytest.approx(10)
    assert result['2026-06']['start_date'] == '2026-06-08'
    assert result['2026-07']['return_pct'] == pytest.approx(10)
    assert result['2026-07']['end_date'] == '2026-07-15'
    assert result['2026-07']['components'][0]['start_price_eur'] == pytest.approx(110)
    assert result['2026-07']['components'][0]['end_price_eur'] == pytest.approx(121)
