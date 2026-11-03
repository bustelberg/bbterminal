from datetime import date
from types import SimpleNamespace
from contextlib import nullcontext
from dataclasses import dataclass

import pytest

from routers import _airs_weekly_returns as weekly


def holding(isin='A', price=100, **extra):
    return {'isin': isin, 'holding_name': isin or 'Private fund', 'currency': 'EUR',
            'current_price_local': price, **extra}


def snapshot(day, *holdings):
    return {'as_of_date': day, 'holdings': list(holdings)}


def result(snapshots, through=date(2026, 10, 30), weeks=4):
    return weekly.matrix('BOOK', snapshots, weeks, through)


def test_price_return_ignores_quantity_and_portfolio_value_changes():
    data = result([snapshot('2026-10-23', holding(price=100, quantity=1, current_value_eur=100)),
                   snapshot('2026-10-30', holding(price=110, quantity=20, current_value_eur=2200))])
    cell = data['instruments'][0]['weeks'][-1]
    assert cell['status'] == 'priced'
    assert cell['return_pct'] == pytest.approx(10)
    assert cell['from_price'] == 100 and cell['price'] == 110


def test_buys_sales_and_reentry_are_not_invented_returns():
    data = result([snapshot('2026-10-02', holding('A')),
                   snapshot('2026-10-09', holding('A'), holding('B')),
                   snapshot('2026-10-16', holding('B', 120)),
                   snapshot('2026-10-23', holding('A', 130), holding('B', 130)),
                   snapshot('2026-10-30', holding('A', 140))])
    rows = {row['isin']: row for row in data['instruments']}
    assert [cell['status'] for cell in rows['A']['weeks']] == ['priced', 'sold', 'reentered', 'priced']
    assert rows['A']['weeks'][1]['return_pct'] is None
    assert rows['A']['weeks'][2]['return_pct'] is None
    assert rows['B']['weeks'][0]['status'] == 'added'
    assert rows['B']['held'] is False
    assert rows['B']['weeks'][-1]['status'] == 'sold'


def test_missing_week_never_turns_a_two_week_move_into_one_week_return():
    data = result([snapshot('2026-10-16', holding(price=100)), snapshot('2026-10-30', holding(price=120))])
    cells = data['instruments'][0]['weeks']
    assert cells[-2]['status'] == 'missing_snapshot'
    assert cells[-1]['status'] == 'missing_previous'
    assert all(cell['return_pct'] is None for cell in cells)


def test_name_changes_use_isin_but_currencies_and_isins_do_not_merge():
    data = result([snapshot('2026-10-23', holding('A', 100, holding_name='Old name')),
                   snapshot('2026-10-30', holding('A', 110, holding_name='New name'),
                            holding('B', 50, holding_name='New name'), holding('A', 120, currency='USD'))])
    rows = data['instruments']
    assert len(rows) == 3
    eur = next(row for row in rows if row['isin'] == 'A' and row['currency'] == 'EUR')
    assert eur['name'] == 'New name'
    assert eur['weeks'][-1]['return_pct'] == pytest.approx(10)
    assert next(row for row in rows if row['currency'] == 'USD')['weeks'][-1]['return_pct'] is None


@pytest.mark.parametrize('price', [None, 0, -1, float('inf')])
def test_missing_or_unusable_quote_does_not_become_zero_or_minus_100(price):
    data = result([snapshot('2026-10-23', holding()), snapshot('2026-10-30', holding(price=price))])
    cell = data['instruments'][0]['weeks'][-1]
    assert cell['return_pct'] is None and cell['status'] == 'missing_price'


def test_interest_lines_do_not_replace_the_security_quote():
    data = result([snapshot('2026-10-23', holding()), snapshot('2026-10-30', holding(price=110),
                   holding(price=2, valuation_component='accrued_interest'))])
    assert data['instruments'][0]['weeks'][-1]['return_pct'] == pytest.approx(10)


def test_closed_reporting_rows_are_not_held_but_fractional_units_still_are():
    data = result([snapshot('2026-10-23', holding('A'), holding('B')),
                   snapshot('2026-10-30', holding('A', 110, quantity=0, current_value_eur=0),
                            holding('B', 110, quantity=0, current_value_eur=55))])
    rows = {row['isin']: row for row in data['instruments']}
    assert rows['A']['weeks'][-1]['status'] == 'sold'
    assert rows['B']['weeks'][-1]['status'] == 'priced'


def test_conflicting_duplicate_quotes_are_not_averaged():
    data = result([snapshot('2026-10-23', holding()), snapshot('2026-10-30', holding(price=110), holding(price=120))])
    assert data['instruments'][0]['weeks'][-1]['status'] == 'missing_price'


@pytest.mark.parametrize('currency', [None, '', 'nan', '?'])
def test_unknown_currency_does_not_create_a_comparable_quote(currency):
    data = result([snapshot('2026-10-23', holding(currency=currency)),
                   snapshot('2026-10-30', holding(price=110, currency=currency))])
    assert data['instruments'][0]['weeks'][-1]['return_pct'] is None


def test_latest_snapshot_in_week_is_used_and_current_week_is_explicitly_partial():
    data = result([snapshot('2026-10-29', holding(price=100)), snapshot('2026-10-30', holding(price=110)),
                   snapshot('2026-11-02', holding(price=121)), snapshot('2026-11-04', holding(price=999))],
                  through=date(2026, 11, 2))
    assert data['periods'][-1]['partial'] is True
    assert data['periods'][-1]['as_of_date'] == '2026-11-02'
    assert data['periods'][-1]['from_date'] == '2026-10-30'
    assert data['instruments'][0]['weeks'][-1]['return_pct'] == pytest.approx(10)


def test_save_is_one_atomic_publication_with_source_dates(monkeypatch):
    calls = []
    fake = SimpleNamespace(rpc=lambda name, args: calls.append((name, args)) or SimpleNamespace(execute=lambda: None))
    weekly.save_snapshot('BOOK', '2026-10-30', [holding()], client=fake)
    assert calls[0][0] == 'save_airs_weekly_holding_snapshot'
    assert calls[0][1]['p_as_of'] == '2026-10-30'
    assert calls[0][1]['p_holdings'][0]['current_price_local'] == 100


def test_opening_returns_is_read_only_and_never_acquires_airs(monkeypatch):
    import airs_vermogen
    from routers import _airs_monthly_report
    from tests._fake_supabase import FakeSupabase
    monkeypatch.setattr(_airs_monthly_report, '_account', lambda _: None)
    monkeypatch.setattr(airs_vermogen, 'latest_completed_market_date', lambda *a: '2026-10-30')
    monkeypatch.setattr(weekly, 'supabase', FakeSupabase({'airs_weekly_holding_snapshot': []}))
    monkeypatch.setattr(airs_vermogen, '_vermogen_most_recent', lambda *a, **kw: pytest.fail('must not acquire AIRS'))
    assert weekly.read_returns('BOOK')['missing_weeks'] == 12


@pytest.fixture
def history(monkeypatch):
    import airs_vermogen
    import dashboard_daily
    import portfolio
    from tests._fake_supabase import FakeSupabase
    @dataclass
    class Quote:
        holding_name: str = 'A'
        isin: str = 'A'
        currency: str = 'EUR'
        current_price_local: float = 110
    monkeypatch.setattr(weekly, 'read_returns', lambda *a: {'through': '2026-10-30'})
    monkeypatch.setattr(weekly, 'supabase', FakeSupabase({'airs_weekly_holding_snapshot': [
        {'portefeuille': 'BOOK', 'week_end': d, 'as_of_date': d}
        for d in ('2026-10-02', '2026-10-09', '2026-10-16', '2026-10-30')]}))
    monkeypatch.setattr(dashboard_daily, 'daily_lease', lambda *a: nullcontext(True))
    monkeypatch.setattr(airs_vermogen, '_acquire_session', lambda *a: True)
    released, saved = [], []
    monkeypatch.setattr(airs_vermogen, '_LOCK', SimpleNamespace(release=lambda: released.append(True)))
    monkeypatch.setattr(airs_vermogen, '_save_holdings', lambda *a: pytest.fail('history must not replace live holdings'))
    monkeypatch.setattr(portfolio, 'parse_airs_excel', lambda _: [Quote()])
    monkeypatch.setattr(weekly, 'save_snapshot', lambda *args: saved.append(args))
    ctx = SimpleNamespace(check=lambda: None, progress=lambda *a: None, emit=lambda *a, **kw: None)
    return ctx, saved, released


def test_history_uses_actual_holiday_date_and_never_relabels_an_older_week(monkeypatch, history):
    import airs_scanner
    ctx, saved, released = history
    requested = []
    def download(account, start, end):
        requested.append(end)
        if end == '2026-10-23':
            raise RuntimeError('Response too small (49 bytes)')
        return b'valid report'
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync', download)
    weekly.collect_history('BOOK', 4, ctx)
    assert requested == ['2026-10-23', '2026-10-22']
    assert saved[0][1] == '2026-10-22'
    assert released == [True]


def test_unavailable_week_does_not_borrow_quotes_from_a_previous_week(monkeypatch, history):
    import airs_scanner
    ctx, saved, released = history
    requested = []
    def download(account, start, end):
        requested.append(end)
        raise RuntimeError('Response too small (49 bytes)')
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync', download)
    assert '1 weeks unavailable' in weekly.collect_history('BOOK', 4, ctx)
    assert requested == ['2026-10-23', '2026-10-22', '2026-10-21', '2026-10-20', '2026-10-19']
    assert not saved and released == [True]


def test_authentication_errors_are_not_treated_as_missing_weeks(monkeypatch, history):
    import airs_scanner
    ctx, saved, released = history
    def download(*args):
        raise RuntimeError('AIRS login failed')
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync', download)
    with pytest.raises(RuntimeError, match='login failed'):
        weekly.collect_history('BOOK', 4, ctx)
    assert not saved and released == [True]


def test_year_boundary_fallback_uses_the_actual_reports_year(monkeypatch, history):
    import airs_scanner
    from tests._fake_supabase import FakeSupabase
    ctx, saved, _ = history
    monkeypatch.setattr(weekly, 'read_returns', lambda *a: {'through': '2027-01-01'})
    monkeypatch.setattr(weekly, 'supabase', FakeSupabase({'airs_weekly_holding_snapshot': [
        {'portefeuille': 'BOOK', 'week_end': d, 'as_of_date': d}
        for d in ('2026-12-04', '2026-12-11', '2026-12-18', '2026-12-25')]}))
    requested = []
    def download(account, start, end):
        requested.append((start, end))
        if end == '2027-01-01':
            raise RuntimeError('Response too small (49 bytes)')
        return b'valid report'
    monkeypatch.setattr(airs_scanner, 'download_vermogensoverzicht_sync', download)
    weekly.collect_history('BOOK', 4, ctx)
    assert requested == [('2027-01-01', '2027-01-01'), ('2026-01-01', '2026-12-31')]
    assert saved[0][1] == '2026-12-31'


def test_cancellation_releases_airs_session_without_writing_a_partial_snapshot(monkeypatch, history):
    ctx, saved, released = history
    calls = []
    def check():
        calls.append(True)
        if len(calls) == 2:
            raise RuntimeError('cancelled')
    ctx.check = check
    with pytest.raises(RuntimeError, match='cancelled'):
        weekly.collect_history('BOOK', 4, ctx)
    assert not saved and released == [True]
