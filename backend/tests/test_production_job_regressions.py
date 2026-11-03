from datetime import datetime

import pytest

from asset_pipeline import yahoo, store, price_refresh
from tests._fake_supabase import FakeSupabase


def stamp(text):
    return int(datetime.fromisoformat(text).timestamp())


def test_model_refresh_does_not_crash_on_unavailable_contribution(monkeypatch):
    from routers import _airs_portfolio_refresh as refresh, _airs_portfolio_perf as perf
    trace = {'portfolio': {'ytd_anchor': '2026-11-03', 'covered_pct': 0, 'resolved_holdings': 1,
                          'unresolved_holdings': 1, 'ytd_pct': None},
             'legs': [{'status': 'priced', 'weight': 10, 'isin': 'X', 'fonds': 'Holding',
                       'start_date': '2026-11-03', 'end_date': '2026-11-02', 'start_price_eur': 100,
                       'end_price_eur': 110, 'return_pct': 10, 'contribution_pp': None}]}
    monkeypatch.setattr(perf, 'explain_portfolio_ytd', lambda _: trace)
    messages = []
    assert refresh._recompute(2125, lambda *a, **kw: messages.append(kw.get('message', ''))) == trace
    assert any('unavailable' in message for message in messages)
    assert any('no YTD' in message for message in messages)


def test_resolved_instrument_without_yahoo_ticker_does_not_crash_refresh(monkeypatch):
    from routers import _airs_portfolio_refresh as refresh
    fake = FakeSupabase({'asset_execution': [
        {'isin': 'TEST', 'analysis_id': 123, 'yahoo_symbol': None,
         'currency': None, 'name': 'AIRS fund', 'status': 'resolved'}]})
    monkeypatch.setattr(refresh, 'supabase', fake)
    messages = []
    result = refresh._instruments(['TEST'], lambda *a, **kw: messages.append(kw.get('message', '')))
    assert result['TEST']['analysis_id'] == 123
    assert result['TEST']['yahoo_symbol'] is None
    assert any('AIRS fund' in message for message in messages)


def test_fx_complete_for_last_market_close_does_not_request_unpublished_fixing(monkeypatch):
    from routers import _airs_portfolio_refresh as refresh
    import airs_vermogen
    import fx_rates
    fake = FakeSupabase({'fx_rate': [
        {'currency_code': 'USD', 'rate_date': '2025-11-01', 'rate': 1.1},
        {'currency_code': 'USD', 'rate_date': '2026-11-02', 'rate': 1.2}]})
    monkeypatch.setattr(refresh, 'supabase', fake)
    monkeypatch.setattr(airs_vermogen, 'latest_completed_market_date', lambda: '2026-11-02')
    def unexpected_fetch(*args, **kwargs):
        pytest.fail('Completed-day FX is already stored')
    monkeypatch.setattr(fx_rates, 'fetch_history', unexpected_fetch)
    assert refresh._fx({'USD'}, '2026-01-01', lambda *a, **kw: None)['failed'] == 0


def test_prepared_snapshot_identity_is_portable_between_deployments(monkeypatch, tmp_path):
    from routers import _prepared_analyse as prepared
    digests = []
    for deployment in ('operator-checkout', 'railway-container'):
        root = tmp_path / deployment
        source = root / 'routers' / 'analysis.py'
        source.parent.mkdir(parents=True)
        source.write_text('same calculation code', encoding='utf-8')
        monkeypatch.setattr(prepared, '_ROOT', root)
        digests.append(prepared._digest(((str(source), 0, source.stat().st_size),)))
    assert digests[0] == digests[1]


def test_queue_pauses_for_daily_refresh_in_another_process(monkeypatch):
    import dashboard_daily
    import scheduler
    from asset_pipeline import queue
    monkeypatch.setattr(dashboard_daily, 'daily_refresh_active', lambda: True)
    monkeypatch.setattr(scheduler, '_scheduled_futures', {})
    def unexpected_slice(**kwargs):
        pytest.fail('A daily refresh in another process has priority')
    monkeypatch.setattr(queue, 'process_slice', unexpected_slice)
    scheduler._fire_asset_ingest_queue()


@pytest.mark.parametrize('acquired,active', [(True, False), (False, True)])
def test_daily_queue_pause_observes_shared_lease(monkeypatch, acquired, active):
    import dashboard_daily
    import common.pg
    import psycopg
    from unittest.mock import MagicMock
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (acquired,)
    conn.__enter__.return_value = conn
    monkeypatch.setattr(common.pg, '_db_url', lambda: 'test-database')
    monkeypatch.setattr(psycopg, 'connect', lambda *a, **kw: conn)
    assert dashboard_daily.daily_refresh_active() is active
    assert conn.execute.call_count == (2 if acquired else 1)


def test_scheduled_fx_sync_stops_at_completed_market_day(monkeypatch):
    import scheduler
    import airs_vermogen
    from momentum import data
    from datetime import date
    monkeypatch.setattr(airs_vermogen, 'latest_completed_market_date', lambda: '2026-11-02')
    ends = []
    def sync(db, currencies, start, end, **kwargs):
        ends.append(end)
        return {}
    monkeypatch.setattr(data, 'sync_fx_rates_to_db', sync)
    scheduler._body_fx_sync()
    assert ends == [date(2026, 11, 2)]


@pytest.mark.parametrize('stored,newer_available', [('2026-10-30', False), ('2026-10-30', True), ('2026-11-02', False)])
def test_strict_proxy_refresh_distinguishes_unpublished_and_unsaved_prices(monkeypatch, stored, newer_available):
    from routers import _benchmark_etf as proxy
    import airs_vermogen
    from unittest.mock import Mock
    monkeypatch.setattr(proxy, '_benchmark_id', lambda _: 1)
    monkeypatch.setattr(proxy, '_latest', lambda _: (stored, 100))
    monkeypatch.setattr(proxy, '_behind_the_market', lambda _: True)
    monkeypatch.setattr(proxy, '_proxy_fetched_at', lambda _: proxy.datetime.now(proxy.timezone.utc).isoformat())
    monkeypatch.setattr(proxy, '_stamp_fetched', lambda _: None)
    refresh = Mock(return_value=0)
    monkeypatch.setattr(proxy, '_refresh', refresh)
    monkeypatch.setattr(airs_vermogen, 'latest_completed_market_date', lambda: '2026-11-02')
    check = Mock(side_effect=RuntimeError('newer close not stored') if newer_available else None,
                 return_value={'reason': 'Yahoo has no newer completed close'})
    monkeypatch.setattr(price_refresh, '_unavailable_close', check)
    if newer_available:
        with pytest.raises(RuntimeError, match='newer close not stored'):
            proxy.ensure_fresh('BONDS', force=True, strict=True)
    else:
        assert proxy.ensure_fresh('BONDS', force=True, strict=True) == (1, (stored, 100))
    refresh.assert_called_once()
    assert check.call_count == (1 if stored < '2026-11-02' else 0)


def test_completed_airs_boundary_does_not_request_today(monkeypatch):
    import airs_scanner
    import airs_vermogen as vermogen
    requested = []
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync',
                        lambda name, start, end: requested.append(end) or b'valid report')
    vermogen._reset_valuation_memo()
    result = vermogen._vermogen_most_recent('BOOK', '2026-01-01', through='2026-11-02')
    assert requested == ['2026-11-02']
    assert result[0] == '2026-11-02'


def test_daily_bar_uses_exchange_date_instead_of_utc():
    assert yahoo.session_date(stamp('2026-11-01T23:00:00+00:00'), {'exchangeTimezoneName': 'Australia/Sydney'}) == '2026-11-02'
    assert yahoo.session_date(stamp('2026-11-02T14:30:00+00:00'), {'exchangeTimezoneName': 'America/New_York'}) == '2026-11-02'


def test_date_repair_removes_only_proven_artifacts_in_this_instrument(monkeypatch):
    fake = FakeSupabase({'asset_price': [
        {'analysis_id': 1, 'target_date': '2026-11-01', 'close': 100},
        {'analysis_id': 1, 'target_date': '2026-11-02', 'close': 101},
        {'analysis_id': 2, 'target_date': '2026-11-01', 'close': 200}]})
    monkeypatch.setattr(store, 'supabase', fake)
    store._remove_utc_date_artifacts(1, {'meta': {'exchangeTimezoneName': 'Australia/Sydney'},
        'timestamp': [stamp('2026-11-01T23:00:00+00:00'), stamp('2026-11-02T23:00:00+00:00')]})
    assert [(r['analysis_id'], r['target_date']) for r in fake.tables['asset_price']] == [(1, '2026-11-02'), (2, '2026-11-01')]


def test_old_vendor_price_is_unavailable_not_current(monkeypatch):
    monkeypatch.setattr(yahoo, 'chart_window', lambda *a: {
        'timestamp': [stamp('2026-10-30T14:30:00+00:00'), stamp('2026-11-02T14:30:00+00:00')],
        'indicators': {'quote': [{'close': [100, None]}]}, 'meta': {'exchangeTimezoneName': 'America/New_York'}})
    result = price_refresh._unavailable_close('FUND', '2026-10-30', '2026-11-02')
    assert result['status'] == 'unavailable'
    assert result['vendor_latest_close'] == '2026-10-30'


def test_newer_vendor_price_not_saved_remains_a_real_failure(monkeypatch):
    monkeypatch.setattr(yahoo, 'chart_window', lambda *a: {
        'timestamp': [stamp('2026-11-02T14:30:00+00:00')], 'indicators': {'quote': [{'close': [100]}]}})
    with pytest.raises(RuntimeError, match='storage still ends'):
        price_refresh._unavailable_close('STOCK', '2026-10-30', '2026-11-02')
