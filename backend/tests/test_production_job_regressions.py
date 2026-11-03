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
