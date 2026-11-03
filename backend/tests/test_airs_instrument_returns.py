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
