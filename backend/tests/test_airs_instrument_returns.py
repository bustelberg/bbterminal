from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from routers import _airs_instrument_returns as returns
from tests._fake_supabase import FakeSupabase


def quote(isin='A', price=100, **extra):
    return {'isin': isin, 'holding_name': isin, 'currency': 'EUR', 'current_price_local': price, **extra}


def snapshot(day, *holdings):
    return {'portefeuille': 'BOOK', 'as_of_date': day, 'holdings': list(holdings)}


def individual(day, price=90, isin='A', **extra):
    return {'portefeuille': 'BOOK', 'instrument_id': isin + '|EUR', 'target_date': day,
            'as_of_date': day, 'holdings': [quote(isin, price)], 'status': 'observed',
            'checked_at': datetime.now(timezone.utc).isoformat(), **extra}


@pytest.fixture
def environment(monkeypatch):
    from routers import _airs_monthly_report
    monkeypatch.setattr(_airs_monthly_report, '_account', lambda _: None)
    monkeypatch.setattr(returns, 'latest_date', lambda: date(2026, 1, 16))
    fake = FakeSupabase({'airs_weekly_holding_snapshot': [snapshot('2026-01-16', quote(price=121), quote('B', 300))], returns.TABLE: []})
    monkeypatch.setattr(returns, 'supabase', fake)
    return fake


def test_hardcoded_start_has_a_separate_year_opening_observation():
    assert returns.targets(date(2026, 1, 9)) == [date(2025, 12, 31), date(2026, 1, 4), date(2026, 1, 9)]
    assert returns.targets(date(2025, 12, 31)) == []
    assert returns.targets(date(2026, 11, 2))[-1] == date(2026, 11, 2)


def test_catalogue_includes_old_and_closed_instruments_but_response_has_only_selected_series(environment):
    environment.tables['airs_weekly_holding_snapshot'] = [
        snapshot('2025-12-19', quote('OLD')),
        snapshot('2026-01-02', quote(), quote('B', 0, quantity=0, current_value_eur=0)),
        snapshot('2026-01-09', quote(price=110)), snapshot('2026-01-16', quote('B', 200))]
    environment.tables[returns.TABLE] = [individual('2025-12-31')]
    result = returns.read('BOOK', 'A|EUR')
    assert {row['isin'] for row in result['options']} == {'A', 'B', 'OLD'}
    assert result['start_date'] == '2026-01-01'
    assert result['selected']['isin'] == 'A'
    assert result['selected']['weeks'][0]['return_pct'] == pytest.approx((100 / 90 - 1) * 100)
    assert result['selected']['weeks'][1]['return_pct'] == pytest.approx(10)
    assert result['selected']['weeks'][2]['status'] == 'sold'
    assert 'instruments' not in result  # No unrelated instrument return matrix.


def test_risk_bundle_includes_the_same_series_for_every_instrument(environment):
    environment.tables['airs_weekly_holding_snapshot'].append(snapshot('2026-01-09', quote(), quote('B', 290)))
    environment.tables[returns.TABLE] = [individual('2025-12-31'), individual('2025-12-31', price=280, isin='B')]
    bundled = returns.bundle('BOOK')
    assert set(bundled['instruments']) == {'A|EUR', 'B|EUR'}
    for instrument, series in bundled['instruments'].items():
        single = returns.read('BOOK', instrument)
        assert series == {key: single[key] for key in ('selected', 'periods', 'missing_observations', 'start_date', 'history_cutoff', 'ytd_return')}


def test_first_bar_never_uses_a_whole_week_before_january_first(environment, monkeypatch):
    monkeypatch.setattr(returns, 'latest_date', lambda: date(2026, 1, 2))
    environment.tables['airs_weekly_holding_snapshot'] = [snapshot('2025-12-26', quote(price=85)), snapshot('2026-01-02', quote())]
    result = returns.read('BOOK', 'A|EUR')
    assert result['selected']['weeks'][0]['return_pct'] is None
    assert result['missing_observations'] == 1


def test_opening_quote_is_not_reused_as_the_first_week_closing_quote(environment, monkeypatch):
    monkeypatch.setattr(returns, 'latest_date', lambda: date(2026, 1, 2))
    environment.tables['airs_weekly_holding_snapshot'] = [snapshot('2025-12-19', quote())]
    environment.tables[returns.TABLE] = [individual('2025-12-31')]
    result = returns.read('BOOK', 'A|EUR')
    assert result['selected']['weeks'][0]['status'] == 'missing_snapshot'
    assert result['selected']['weeks'][0]['return_pct'] is None


def test_missing_week_does_not_become_a_two_week_return(environment):
    environment.tables['airs_weekly_holding_snapshot'] = [snapshot('2026-01-02', quote()), snapshot('2026-01-16', quote(price=121))]
    result = returns.read('BOOK', 'A|EUR')
    assert result['selected']['weeks'][-1]['status'] == 'missing_previous'
    assert result['selected']['weeks'][-1]['return_pct'] is None


def test_current_partial_week_and_actual_weekend_source_dates_remain_explicit(environment, monkeypatch):
    monkeypatch.setattr(returns, 'latest_date', lambda: date(2026, 1, 5))
    environment.tables['airs_weekly_holding_snapshot'] = [snapshot('2026-01-03', quote()), snapshot('2026-01-05', quote(price=110))]
    result = returns.read('BOOK', 'A|EUR')
    assert result['periods'][-1]['partial']
    assert result['periods'][-1]['from_date'] == '2026-01-03'
    assert result['selected']['weeks'][-1]['return_pct'] == pytest.approx(10)


def test_unknown_instrument_cannot_start_a_history_load(environment):
    with pytest.raises(Exception) as error:
        returns.state('BOOK', 'UNKNOWN|USD')
    assert error.value.status_code == 404


def test_ambiguous_name_only_report_is_not_misread_as_a_sale(environment):
    environment.tables['airs_weekly_holding_snapshot'] += [snapshot('2026-01-09', quote(None, 110, holding_name='A'))]
    environment.tables['airs_weekly_holding_snapshot'][0]['holdings'].append(quote('OTHER', 120, holding_name='A'))
    result = returns.read('BOOK', 'A|EUR')
    assert result['selected']['weeks'][1]['price'] is None
    assert result['missing_observations'] == 3


def test_unique_isin_replaces_the_name_only_option_and_keeps_its_saved_history(environment):
    name = 'High Income Quality fund'
    environment.tables['airs_weekly_holding_snapshot'] = [
        snapshot('2026-01-16', quote('CH1369849273', 121, holding_name=name)),
        snapshot('2026-01-02', quote(None, 100, holding_name=name)),
        snapshot('2026-01-09', quote(None, 110, holding_name=name))]
    environment.tables[returns.TABLE] = [{**individual('2025-12-31'),
        'instrument_id': 'name:high income quality fund|EUR',
        'holdings': [quote(None, 90, holding_name=name)]}]
    result = returns.read('BOOK', 'CH1369849273|EUR')
    assert len(result['options']) == 1
    assert result['options'][0]['isin'] == 'CH1369849273'
    assert [cell['return_pct'] for cell in result['selected']['weeks']] == pytest.approx([100 / 90 * 100 - 100, 10, 10])
    assert result['missing_observations'] == 0
    assert returns.read('BOOK', 'name:high income quality fund|EUR')['selected']['id'] == 'CH1369849273|EUR'


def test_same_name_does_not_merge_different_isins_or_currencies(environment):
    name = 'High Income Quality fund'
    environment.tables['airs_weekly_holding_snapshot'] = [snapshot('2026-01-16',
        quote('CH1369849273', holding_name=name), quote('OTHER', holding_name=name),
        quote(None, holding_name=name), quote(None, holding_name=name, currency='USD'))]
    result = returns.read('BOOK', 'CH1369849273|EUR')
    assert len(result['options']) == 4


def test_valid_complete_quote_replaces_an_older_failed_instrument_check_on_same_date(environment):
    environment.tables['airs_weekly_holding_snapshot'] += [snapshot('2026-01-02', quote()), snapshot('2026-01-09', quote(price=110))]
    environment.tables[returns.TABLE] = [individual('2026-01-09', price=None)]
    result = returns.read('BOOK', 'A|EUR')
    assert result['selected']['weeks'][-1]['return_pct'] == pytest.approx(10)


@pytest.fixture
def loader(environment, monkeypatch):
    import airs_vermogen
    import airs_scanner
    import dashboard_daily
    import portfolio
    from routers import _airs_weekly_returns
    @dataclass
    class Holding:
        holding_name: str
        isin: str
        currency: str
        current_price_local: float
    requests, released = [], []
    def download(account, start, end):
        requests.append(end)
        return end.encode()
    def parse(blob):
        prices = {'2025-12-31': 90, '2026-01-04': 100, '2026-01-11': 110}
        return [Holding('A', 'A', 'EUR', prices.get(blob.decode(), 100)), Holding('B', 'B', 'EUR', 999)]
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync', download)
    monkeypatch.setattr(portfolio, 'parse_airs_excel', parse)
    monkeypatch.setattr(dashboard_daily, 'daily_lease', lambda *a: nullcontext(True))
    monkeypatch.setattr(airs_vermogen, '_acquire_session', lambda *a: True)
    monkeypatch.setattr(airs_vermogen, '_LOCK', SimpleNamespace(release=lambda: released.append(True)))
    monkeypatch.setattr(airs_vermogen, '_save_holdings', lambda *a: pytest.fail('must not change live holdings'))
    monkeypatch.setattr(_airs_weekly_returns, 'save_snapshot', lambda *a: pytest.fail('must not publish a partial portfolio snapshot'))
    ctx = SimpleNamespace(check=lambda: None, progress=lambda *a: None, emit=lambda *a, **kw: None)
    return ctx, requests, released


def test_bulk_downloads_each_week_once_for_all_instruments(environment, loader):
    ctx, requests, released = loader
    result = returns.collect_all('BOOK', ctx, date(2026, 1, 2))
    assert requests == ['2026-01-09', '2026-01-02']
    assert released == [True]
    assert '2 instruments' in result
    rows = environment.tables[returns.TABLE]
    assert len(rows) == 6
    for target in ('2026-01-16', '2026-01-09', '2026-01-02'):
        assert {row['instrument_id'] for row in rows if row['target_date'] == target} == {'A|EUR', 'B|EUR'}
    assert returns.read('BOOK', 'A|EUR')['selected']['weeks'][-1]['return_pct'] == pytest.approx(21)
    returns.collect_all('BOOK', ctx, date(2026, 1, 2))
    assert requests == ['2026-01-09', '2026-01-02']  # All completed instruments resume without downloading again.


def test_bulk_fallback_shares_missing_reports_and_respects_hard_date(environment, loader, monkeypatch):
    import airs_scanner
    ctx, requests, released = loader
    def unavailable(account, start, end):
        requests.append(end)
        raise airs_scanner.AirsNoData('No report')
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync', unavailable)
    returns.collect_all('BOOK', ctx, date(2026, 1, 7))
    assert requests == ['2026-01-09', '2026-01-08', '2026-01-07']
    assert released == [True]
    assert sum(row['status'] == 'unavailable' for row in environment.tables[returns.TABLE]) == 2


def test_bulk_failure_keeps_completed_week_and_releases_session(environment, loader, monkeypatch):
    import airs_scanner
    ctx, _, released = loader
    def fail(*args):
        raise RuntimeError('login failed')
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync', fail)
    with pytest.raises(RuntimeError, match='login failed'):
        returns.collect_all('BOOK', ctx, date(2026, 1, 2))
    assert released == [True]
    assert len(environment.tables[returns.TABLE]) == 2


def test_loader_saves_only_selected_instrument_and_never_refetches_known_history(environment, loader):
    ctx, requests, released = loader
    returns.collect('BOOK', 'A|EUR', ctx)
    assert requests == ['2025-12-31', '2026-01-04', '2026-01-11']
    assert released == [True]
    assert len(environment.tables[returns.TABLE]) == 3
    assert all(row['instrument_id'] == 'A|EUR' and len(row['holdings']) == 1
               and row['holdings'][0]['isin'] == 'A' for row in environment.tables[returns.TABLE])
    assert returns.read('BOOK', 'A|EUR')['selected']['weeks'][-1]['return_pct'] == pytest.approx(10)
    returns.collect('BOOK', 'A|EUR', ctx)
    assert len(requests) == 3


def test_progress_publishes_committed_observations_before_history_finishes(environment, loader):
    ctx, _, _ = loader
    updates = []

    def progress(done, total, message):
        if not done:
            return
        # A client rereading at each event must already see the saved quote.
        result = returns.read('BOOK', 'A|EUR')
        updates.append((done, len(environment.tables[returns.TABLE]),
                        sum(cell['return_pct'] is not None for cell in result['selected']['weeks'])))

    ctx.progress = progress
    returns.collect('BOOK', 'A|EUR', ctx)
    assert updates[:3] == [(1, 1, 0), (2, 2, 1), (3, 3, 3)]


def test_instrument_repair_replaces_missing_quote_on_same_source_date(environment, loader):
    ctx, requests, _ = loader
    environment.tables['airs_weekly_holding_snapshot'] += [snapshot('2026-01-02', quote()), snapshot('2026-01-09', quote(price=None))]
    environment.tables[returns.TABLE] = [individual('2025-12-31')]
    returns.collect('BOOK', 'A|EUR', ctx)
    assert requests == ['2026-01-11']
    assert returns.read('BOOK', 'A|EUR')['selected']['weeks'][-1]['return_pct'] == pytest.approx(10)


def test_known_absence_is_not_filled_with_another_instruments_quote(environment, loader):
    ctx, requests, _ = loader
    environment.tables['airs_weekly_holding_snapshot'] += [snapshot('2026-01-02', quote()), snapshot('2026-01-09', quote('B', 200))]
    environment.tables[returns.TABLE] = [individual('2025-12-31')]
    returns.collect('BOOK', 'A|EUR', ctx)
    assert not requests
    assert returns.read('BOOK', 'A|EUR')['selected']['weeks'][-1]['status'] == 'reentered'


def test_missing_portfolio_dates_stay_unknown_and_repeated_download_dates_are_shared(environment, loader, monkeypatch):
    import airs_scanner
    ctx, requests, _ = loader
    def no_data(account, start, end):
        requests.append(end)
        raise RuntimeError('Response too small (49 bytes)')
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync', no_data)
    returns.collect('BOOK', 'A|EUR', ctx)
    assert len(requests) == len(set(requests))
    assert all(row['status'] == 'unavailable' and row['as_of_date'] is None for row in environment.tables[returns.TABLE])
    before = len(requests)
    returns.collect('BOOK', 'A|EUR', ctx)
    assert len(requests) > before  # Explicit retries may repair previously unavailable dates.
    assert returns.read('BOOK', 'A|EUR')['selected']['weeks'][-1]['return_pct'] is None


def test_login_failures_fail_the_load_and_release_the_session(environment, loader, monkeypatch):
    import airs_scanner
    ctx, _, released = loader
    def fail(*a):
        raise RuntimeError('AIRS login failed')
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync', fail)
    with pytest.raises(RuntimeError, match='login failed'):
        returns.collect('BOOK', 'A|EUR', ctx)
    assert released == [True]
    assert not environment.tables[returns.TABLE]


def test_full_history_continues_when_instrument_absent_and_stops_at_empty_report_year(environment, loader, monkeypatch):
    import airs_scanner
    import portfolio
    ctx, requests, released = loader
    original_parse = portfolio.parse_airs_excel
    def download(account, start, end):
        requests.append(end)
        if end < '2025-01-01':
            raise airs_scanner.AirsNoData('no historical report')
        return end.encode()
    def parse(blob):
        rows = original_parse(blob)
        return rows if blob.decode() >= '2026-01-01' else [rows[1]]  # report exists, selected A was absent
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync', download)
    monkeypatch.setattr(portfolio, 'parse_airs_excel', parse)
    returns.collect('BOOK', 'A|EUR', ctx, full_history=True)
    stored = environment.tables[returns.TABLE]
    assert any(row['target_date'].startswith('2025') and row['status'] == 'observed' and not row['holdings'] for row in stored)
    assert any(row['target_date'].startswith('2024') and row['status'] == 'unavailable' for row in stored)
    assert not any(day.startswith('2022') for day in requests)
    assert returns.read('BOOK', 'A|EUR')['start_date'] == '2025-01-01'
    count = len(requests)
    returns.collect('BOOK', 'A|EUR', ctx, full_history=True)
    assert len(requests) == count  # historical observations and completed empty-year checks resume without downloads
    assert released == [True, True]


def test_full_history_ytd_uses_its_own_year_opening_instead_of_oldest_price(environment):
    environment.tables[returns.TABLE] = [individual('2024-12-31', 50), individual('2025-01-05', 55),
                                         individual('2025-12-31', 100), individual('2026-01-04', 110)]
    result = returns.read('BOOK', 'A|EUR')
    assert result['start_date'] == '2025-01-01'
    assert result['ytd_return']['from'] == '2025-12-31'
    assert result['ytd_return']['value'] == pytest.approx(21)
    assert returns.bundle('BOOK')['instruments']['A|EUR']['ytd_return'] == result['ytd_return']


def test_full_history_parse_failure_does_not_count_as_the_end_of_airs_history(environment, loader, monkeypatch):
    import portfolio
    ctx, _, released = loader
    monkeypatch.setattr(portfolio, 'parse_airs_excel', lambda _: [])
    with pytest.raises(ValueError, match='cannot confirm'):
        returns.collect('BOOK', 'A|EUR', ctx, full_history=True)
    assert released == [True]


def test_cancelled_historical_load_keeps_committed_weeks_and_releases_session(environment, loader):
    ctx, _, released = loader
    class Cancelled(Exception):
        pass
    def check():
        if len(environment.tables[returns.TABLE]) >= 4:
            raise Cancelled
    ctx.check = check
    with pytest.raises(Cancelled):
        returns.collect('BOOK', 'A|EUR', ctx, full_history=True)
    assert len(environment.tables[returns.TABLE]) == 4
    assert released == [True]
    assert any(row['target_date'].startswith('2025') for row in environment.tables[returns.TABLE])
    result = returns.read('BOOK', 'A|EUR')
    assert result['start_date'] == result['history_cutoff']


def test_new_year_without_a_completed_day_does_not_label_previous_year_as_ytd():
    chosen = {'id': 'A|EUR', 'name': 'A', 'isin': 'A', 'currency': 'EUR'}
    result = returns._full_series('BOOK', date(2025, 12, 31), [], [individual('2025-12-31')], chosen)
    assert result['ytd_return'] is None


def test_full_history_stops_on_two_zero_return_weeks_without_chasing_flat_older_reports(environment, loader):
    ctx, requests, released = loader
    message = returns.collect('BOOK', 'A|EUR', ctx, full_history=True)
    assert 'two consecutive 0% weeks' in message
    assert len(requests) < 10
    assert not any(day.startswith('2024') for day in requests)
    result = returns.read('BOOK', 'A|EUR')
    assert result['history_cutoff'] is not None
    assert all(period['target_date'] > result['history_cutoff'] for period in result['periods'])
    assert result['ytd_return']['value'] == pytest.approx(121 / 90 * 100 - 100)
    count = len(requests)
    returns.collect('BOOK', 'A|EUR', ctx, full_history=True)
    assert len(requests) == count
    assert released == [True, True]


def test_discovery_follows_exact_seven_day_steps_before_the_separate_ytd_anchor(environment, loader):
    ctx, requests, _ = loader
    returns.collect('BOOK', 'A|EUR', ctx, full_history=True)
    targets = [row['target_date'] for row in environment.tables[returns.TABLE]]
    assert targets[:4] == ['2026-01-16', '2026-01-09', '2026-01-02', '2025-12-26']
    assert requests == ['2026-01-09', '2026-01-02', '2025-12-26', '2025-12-31']


def test_saved_flat_tail_is_trimmed_and_a_missing_week_breaks_the_zero_run(environment):
    chosen = {'id': 'A|EUR', 'name': 'A', 'isin': 'A', 'currency': 'EUR'}
    cells = [{'status': 'priced', 'return_pct': value} for value in [0, 0, 5, 0, 10]]
    data = {'selected': {**chosen, 'weeks': cells}, 'periods': [{'target_date': f'2025-01-{day:02}'} for day in [5, 12, 19, 26, 31]]}
    assert returns.flat_history_cutoff(data) == '2025-01-12'
    cells[1] = {'status': 'missing_snapshot', 'return_pct': None}
    assert returns.flat_history_cutoff(data) is None


def test_known_zero_cutoff_still_repairs_the_independent_ytd_opening(environment, loader):
    ctx, requests, _ = loader
    environment.tables['airs_weekly_holding_snapshot'] = [snapshot(day, quote()) for day in ['2026-01-02', '2026-01-09', '2026-01-16']]
    assert returns.read('BOOK', 'A|EUR')['history_cutoff'] == '2026-01-16'
    returns.collect('BOOK', 'A|EUR', ctx, full_history=True)
    assert requests == ['2025-12-31']
    assert returns.read('BOOK', 'A|EUR')['ytd_return']['value'] == pytest.approx(100 / 90 * 100 - 100)


def test_loaded_chart_uses_the_same_rolling_weekly_dates_and_never_a_future_quote(environment):
    environment.tables['airs_weekly_holding_snapshot'].append(snapshot('2026-01-04', quote(price=999)))
    environment.tables[returns.TABLE] = [individual(day, price) for day, price in [
        ('2025-12-12', 90), ('2025-12-19', 90), ('2025-12-26', 90),
        ('2025-12-31', 95), ('2026-01-02', 100), ('2026-01-09', 110), ('2026-01-16', 121)]]
    result = returns.read('BOOK', 'A|EUR')
    assert result['history_cutoff'] == '2025-12-26'
    assert [row['target_date'] for row in result['periods']] == ['2026-01-02', '2026-01-09', '2026-01-16']
    assert [cell['price'] for cell in result['selected']['weeks']] == [100, 110, 121]


def test_todays_unpublished_report_falls_back_across_the_calendar_week(environment, loader, monkeypatch):
    import airs_scanner
    ctx, requests, _ = loader
    monkeypatch.setattr(returns, 'latest_date', lambda: date(2026, 1, 12))  # Monday
    environment.tables['airs_weekly_holding_snapshot'] = [snapshot('2026-01-11', quote(price=110))]
    def download(account, start, end):
        requests.append(end)
        if end == '2026-01-12':
            raise airs_scanner.AirsNoData('not published yet')
        return end.encode()
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync', download)
    returns.collect('BOOK', 'A|EUR', ctx, full_history=True)
    assert requests[:2] == ['2026-01-12', '2026-01-11']
    latest = next(row for row in environment.tables[returns.TABLE] if row['target_date'] == '2026-01-12')
    assert latest['as_of_date'] == '2026-01-11'


def test_configured_hard_date_bounds_weekly_requests_and_separate_ytd_anchor(environment, loader):
    ctx, requests, _ = loader
    result = returns.collect('BOOK', 'A|EUR', ctx, full_history=True, earliest_date=date(2026, 1, 3))
    assert requests == ['2026-01-09']
    assert 'requested earliest date 2026-01-03' in result
    assert all(row['target_date'] >= '2026-01-03' for row in environment.tables[returns.TABLE])


def test_fallback_never_crosses_the_configured_hard_date(environment, loader, monkeypatch):
    import airs_scanner
    ctx, requests, _ = loader
    def unavailable(account, start, end):
        requests.append(end)
        raise airs_scanner.AirsNoData('no report')
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync', unavailable)
    returns.collect('BOOK', 'A|EUR', ctx, full_history=True, earliest_date=date(2026, 1, 7))
    assert requests == ['2026-01-09', '2026-01-08', '2026-01-07']


@pytest.mark.parametrize('lower', [date(1899, 1, 1), date(2027, 1, 1)])
def test_invalid_hard_date_is_rejected_before_acquiring_airs_session(environment, loader, lower):
    ctx, requests, released = loader
    with pytest.raises(ValueError, match='Earliest date'):
        returns.collect('BOOK', 'A|EUR', ctx, full_history=True, earliest_date=lower)
    assert not requests
    assert not released
