"""Frozen database-free checks of valuation maths, publication and job boundaries."""
from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from services import portfolio_valuation as V
from scheduler_jobs import valuation as J
from scheduler_runtime.execution import Cancelled

TODAY = date(2026, 11, 13)


def inputs():
    prices = []
    day = date(2020, 1, 1)
    while day <= TODAY:
        if day.weekday() < 5:
            prices.append({'target_date': day.isoformat(), 'close': 80 if day < TODAY else 60, 'currency': 'EUR'})
        day += timedelta(days=1)
    earnings = [{'target_date': f'{year}-{month:02d}-{last}', 'numeric_value': 1, 'is_prediction': False}
                for year in range(2019, 2027) for month, last in ((3, 31), (6, 30), (9, 30), (12, 31))]
    return prices, earnings


def test_daily_prices_reprice_discount_against_positive_historical_median():
    prices, earnings = inputs()
    result = V.estimate(prices, earnings, 'EUR', TODAY)
    assert result['status'] == 'ok'
    assert result['eps_ttm'] == 4
    assert result['median_pe'] == 20
    assert result['estimated_value'] == 80
    assert result['discount_pct'] == 25  # Discount is not the 33.33% upside.
    assert result['current_pe'] == 15
    assert result['observations'] > 1000
    prices[-1]['close'] = 100
    assert V.estimate(prices, earnings, 'EUR', TODAY)['discount_pct'] == -25


@pytest.mark.parametrize('scenario,reason', [
    ('missing_prices', 'No saved Yahoo'), ('short', 'Insufficient positive-earnings history'),
    ('quarters', 'four consecutive'), ('negative', 'zero or negative'),
    ('currency', 'differs from earnings'), ('unknown_currency', 'missing or inconsistent'),
    ('stale_price', 'seven days'), ('stale_eps', 'refresh reported fundamentals'),
])
def test_missing_inputs_are_explicit(scenario, reason):
    prices, earnings = inputs()
    currency = 'EUR'
    if scenario == 'missing_prices':
        prices = []
    elif scenario == 'short':
        prices = prices[-100:]
    elif scenario == 'quarters':
        earnings = earnings[-2:]
    elif scenario == 'negative':
        earnings = [{**row, 'numeric_value': -1} for row in earnings]
    elif scenario == 'currency':
        currency = 'USD'
    elif scenario == 'unknown_currency':
        currency = None
    elif scenario == 'stale_price':
        prices = prices[:-10]
    elif scenario == 'stale_eps':
        earnings = [row for row in earnings if row['target_date'] < '2026-01-01']
    result = V.estimate(prices, earnings, currency, TODAY)
    assert result['status'] == 'unavailable'
    assert reason in result['reason']
    assert result['estimated_value'] is None


def test_forecasts_future_periods_and_duplicate_metric_spellings_are_not_extra_quarters():
    _, earnings = inputs()
    actuals = V.ttm_earnings(earnings, TODAY.isoformat())
    modified = earnings + [{**row, 'is_prediction': True, 'numeric_value': 500} for row in earnings] + earnings
    assert V.ttm_earnings(modified, TODAY.isoformat()) == actuals
    assert actuals[-1][0] == '2026-09-30'
    with pytest.raises(ValueError, match='Conflicting EPS'):
        V.ttm_earnings(earnings + [{'target_date': '2026-09-30', 'numeric_value': 10}], TODAY.isoformat())


def test_missing_quarter_never_sums_across_the_hole():
    rows = [{'target_date': day, 'numeric_value': 1} for day in
            ['2025-03-31', '2025-06-30', '2025-12-31', '2026-03-31']]
    assert V.ttm_earnings(rows, TODAY.isoformat()) == []


def test_history_waits_120_days_for_earnings_and_does_not_use_future_prices():
    prices, earnings = inputs()
    result = V.estimate(prices, earnings, 'EUR', TODAY)
    future = prices + [{'target_date': '2027-01-01', 'close': 90000, 'currency': 'EUR'}]
    assert V.estimate(future, earnings, 'EUR', TODAY) == result
    recent_earnings = [row for row in earnings if row['target_date'] >= '2025-12-31']
    limited = V.estimate(prices, recent_earnings, 'EUR', TODAY)
    assert limited['observations'] == 0  # First TTM ends September; not available until January.


def test_minor_quote_units_are_normalized_before_valuation():
    prices, earnings = inputs()
    prices = [{**row, 'currency': 'GBp', 'close': row['close'] * 100} for row in prices]
    result = V.estimate(prices, earnings, 'GBP', TODAY)
    assert result['currency'] == 'GBP'
    assert result['price'] == 60
    assert result['estimated_value'] == 80


def fake_store(monkeypatch):
    saved = []
    class Query:
        def upsert(self, body, **_kwargs):
            saved.append(body)
            return self

        def execute(self):
            return SimpleNamespace(data=[])
    monkeypatch.setattr(V, 'supabase', SimpleNamespace(table=lambda _name: Query()))
    return saved


def test_unavailable_company_is_saved_but_database_failure_retains_previous_bundle(monkeypatch):
    saved = fake_store(monkeypatch)
    monkeypatch.setattr(V, 'company_holdings', lambda _pid: ([{'isin': 'EXACT', 'name': 'Company', 'weight_pct': 5}], {}))
    monkeypatch.setattr(V, 'load_inputs', lambda *_: (None, [], [], 'Missing exact company mapping'))
    result = V.publish(7, today=TODAY)
    assert result['rows'][0]['status'] == 'unavailable'
    assert len(saved) == 1
    def failure(*_):
        raise RuntimeError('database timeout')
    monkeypatch.setattr(V, 'load_inputs', failure)
    with pytest.raises(RuntimeError, match='previous results retained.*EXACT.*database timeout'):
        V.publish(7, today=TODAY)
    assert len(saved) == 1


def test_cancellation_does_not_publish_incomplete_portfolio(monkeypatch):
    saved = fake_store(monkeypatch)
    monkeypatch.setattr(V, 'company_holdings', lambda _pid: ([{'isin': None, 'name': 'No ISIN', 'weight_pct': 1}], {}))
    def stop():
        raise Cancelled('stopped')
    with pytest.raises(Cancelled):
        V.publish(7, today=TODAY, check_cancel=stop)
    assert not saved


def test_scheduled_job_keeps_completed_portfolios_and_error_evidence(monkeypatch):
    monkeypatch.setattr(V, 'portfolios', lambda: [{'id': 1, 'name': 'One'}, {'id': 2, 'name': 'Two'}])
    saved = []
    def publish(pid, **_kwargs):
        if pid == 2:
            raise RuntimeError('source unavailable')
        saved.append(pid)
        return {'rows': [{'status': 'ok'}]}
    monkeypatch.setattr(V, 'publish', publish)
    from dashboard_daily import IncompleteRefresh
    with pytest.raises(IncompleteRefresh, match='Two.*source unavailable'):
        J.run()
    assert saved == [1]


def test_read_is_saved_only_and_missing_migration_is_actionable(monkeypatch):
    monkeypatch.setattr(V, 'portfolios', lambda: [{'id': 7}])
    monkeypatch.setattr(V, 'company_holdings', lambda _pid: ([{'name': 'Apple', 'isin': 'US0378331005', 'weight_pct': 5}], {}))
    monkeypatch.setattr(V, 'publish', lambda *_: pytest.fail('Read must never calculate'))
    class Query:
        def __getattr__(self, _name):
            return lambda *_args, **_kwargs: self

        def execute(self):
            return SimpleNamespace(data=[])
    monkeypatch.setattr(V, 'supabase', SimpleNamespace(table=lambda _name: Query()))
    result = V.read(7)
    assert result['ready'] is False
    assert result['rows'] == [{'name': 'Apple', 'isin': 'US0378331005', 'weight_pct': 5,
                               'status': 'pending', 'reason': 'Valuation has not been prepared yet.'}]
    def no_composition(_pid):
        raise ValueError('No saved composition available.')
    monkeypatch.setattr(V, 'company_holdings', no_composition)
    assert V.read(7)['detail'] == 'No saved composition available.'
    def failure(_name):
        raise RuntimeError('missing table')
    monkeypatch.setattr(V, 'supabase', SimpleNamespace(table=failure))
    from fastapi import HTTPException
    with pytest.raises(HTTPException, match='20261113190000_portfolio_valuation.sql'):
        V.read(7)


def test_shared_company_is_calculated_once_without_caching_raw_price_history(monkeypatch):
    fake_store(monkeypatch)
    monkeypatch.setattr(V, 'company_holdings', lambda _pid: ([{'isin': 'EXACT', 'name': 'Company', 'weight_pct': 5}], {}))
    calls = []
    def load(*_args):
        calls.append('EXACT')
        prices, earnings = inputs()
        return {'gurufocus_exchange': {'currency_code': 'EUR'}}, prices, earnings, None
    monkeypatch.setattr(V, 'load_inputs', load)
    cache = {}
    V.publish(1, today=TODAY, input_cache=cache)
    V.publish(2, today=TODAY, input_cache=cache)
    assert calls == ['EXACT']
    assert cache['EXACT']['estimated_value'] == 80
    assert 'prices' not in cache['EXACT']


def test_holdings_expand_certificates_and_exclude_funds_cash_and_bonds(monkeypatch):
    from routers import _airs_ref as ref, _airs_lookthrough as look, _airs_portfolio_analysis as analysis
    monkeypatch.setattr(ref, 'model', lambda _pid: {'positions_datum': '2026-11-12'})
    monkeypatch.setattr(ref, 'positions_for', lambda _pid: [{'datum': '2026-11-12'}])
    rows = [
        {'isin': 'STOCK', 'fonds': 'Operating Company', 'percentage': 3, 'categorie': 'Aandelen'},
        {'isin': 'STOCK', 'fonds': 'Operating Company', 'percentage': 2, 'categorie': 'Aandelen'},
        {'isin': 'FUND', 'fonds': 'World ETF', 'percentage': 10, 'categorie': 'Aandelen'},
        {'isin': None, 'fonds': 'Liquiditeiten', 'percentage': 5, 'categorie': 'Liquiditeiten'},
        {'isin': 'BOND', 'fonds': 'Government bond', 'percentage': 8, 'categorie': 'Obligaties'},
    ]
    monkeypatch.setattr(look, 'expand_positions', lambda *_: (rows, {'expanded': ['Certificate']}))
    monkeypatch.setattr(analysis, '_grid', lambda _isins: {})
    holdings, evidence = V.company_holdings(7)
    assert holdings == [{'isin': 'STOCK', 'name': 'Operating Company', 'weight_pct': 5}]
    assert evidence['excluded_funds_cash_bonds'] == 3
    assert evidence['lookthrough']['expanded'] == ['Certificate']


def test_portfolio_roster_reuses_dashboard_account_names_and_keeps_unlinked_accounts(monkeypatch):
    from routers import _airs_overview as overview, _airs_ref as ref
    monkeypatch.setattr(ref, 'models', lambda: [
        {'id': 7, 'name': 'BUS_OFF_FX', 'display_name': 'Wrong model nickname', 'positions_datum': '2026-11-12'},
        {'id': 8, 'name': 'CHILD_MODEL_NOT_ON_DASHBOARD'},
    ])
    monkeypatch.setattr(overview, 'list_overview', lambda: [
        {'name': 'Bustelberg Offensief', 'dynamic_portefeuille': 'BUS_Offensief_Dyn',
         'management_group': 'bustelberg', 'fixed_portfolio_id': 7},
        {'name': 'My account nickname', 'dynamic_portefeuille': 'TEST_DYN',
         'management_group': 'test_topselecties', 'fixed_portfolio_id': None},
        {'name': 'Outside dashboard collections', 'dynamic_portefeuille': 'OTHER',
         'management_group': 'other', 'fixed_portfolio_id': 8},
    ])
    rows = V.portfolios()
    assert [row['name'] for row in rows] == ['Bustelberg Offensief', 'My account nickname']
    assert rows[0]['id'] == 7
    assert rows[0]['composition_date'] == '2026-11-12'
    assert rows[0]['account'] == 'BUS_Offensief_Dyn'
    assert rows[1]['id'] is None
    assert 'No linked AIRS Fixed model' in rows[1]['unavailable_reason']


def test_daily_job_publishes_shared_model_once_and_reports_unlinked_portfolio(monkeypatch):
    monkeypatch.setattr(V, 'portfolios', lambda: [
        {'id': 7, 'name': 'Account One'}, {'id': 7, 'name': 'Account Two'},
        {'id': None, 'name': 'Unlinked', 'account': 'UNLINKED', 'unavailable_reason': 'Connect model'},
    ])
    calls = []
    def publish(pid, **_kwargs):
        calls.append(pid)
        return {'rows': []}
    monkeypatch.setattr(V, 'publish', publish)
    _, summary = J.run()
    assert calls == [7]
    assert summary['unavailable_portfolios'] == [{'name': 'Unlinked', 'account': 'UNLINKED', 'reason': 'Connect model'}]


def test_selected_refresh_prepares_missing_or_stale_results_and_force_recalculates(monkeypatch):
    state = {'ready': False}
    monkeypatch.setattr(V, 'read', lambda _pid: dict(state))
    calls = []
    def publish(pid):
        calls.append(pid)
        state.update(ready=True, stale=False, rows=[{'name': 'Calculated'}])
        return dict(state)
    monkeypatch.setattr(V, 'publish', publish)
    assert V.refresh(7)['ready'] is True
    V.refresh(7)  # Attaching after a completed first preparation does not repeat work.
    assert calls == [7]
    V.refresh(7, force=True)
    assert calls == [7, 7]
    state['stale'] = True
    V.refresh(7)
    assert calls == [7, 7, 7]


def test_selected_refresh_failure_preserves_evidence(monkeypatch, caplog):
    from fastapi import HTTPException
    monkeypatch.setattr(V, 'read', lambda _pid: {'ready': False})
    def failure(_pid):
        raise RuntimeError('EXACT: source timeout')
    monkeypatch.setattr(V, 'publish', failure)
    with pytest.raises(HTTPException, match='previous results remain saved.*RuntimeError'):
        V.refresh(7)
    assert 'EXACT: source timeout' in caplog.text


def test_legacy_eps_aliases_select_one_consistent_current_series_regardless_of_row_order():
    prices, earnings = inputs()
    legacy = [{**row, 'metric_code': V.EPS_CODES[0], 'numeric_value': 10} for row in earnings]
    current = [{**row, 'metric_code': V.EPS_CODES[-1]} for row in earnings]
    for rows in (legacy + current, list(reversed(current + legacy))):
        result = V.estimate(prices, rows, 'EUR', TODAY)
        assert result['status'] == 'ok'
        assert result['eps_ttm'] == 4
        assert result['median_pe'] == 20
        assert result['eps_metric_code'] == V.EPS_CODES[-1]


def test_newer_actual_period_wins_over_schema_preference_without_splicing_history():
    _, earnings = inputs()
    recent = [{**row, 'metric_code': V.EPS_CODES[0]} for row in earnings]
    older = [{**row, 'metric_code': V.EPS_CODES[-1], 'numeric_value': 100}
             for row in earnings if row['target_date'] < '2026-01-01']
    selected, code = V.select_eps_series(older + recent, TODAY.isoformat())
    assert code == V.EPS_CODES[0]
    assert all(row['numeric_value'] == 1 for row in selected)
    # A short current series cannot borrow older aliases on potentially different split bases.
    short = [row for row in recent if row['target_date'] >= '2026-06-30']
    assert V.ttm_earnings(older + short, TODAY.isoformat()) == []


def test_genuine_eps_conflict_retains_price_and_does_not_fail_other_portfolio_rows(monkeypatch):
    saved = fake_store(monkeypatch)
    prices, earnings = inputs()
    conflict = earnings + [{'target_date': '2026-09-30', 'numeric_value': 5}]
    monkeypatch.setattr(V, 'company_holdings', lambda _pid: ([
        {'isin': 'HEALTHY', 'name': 'Healthy', 'weight_pct': 5},
        {'isin': 'CONFLICT', 'name': 'Conflict', 'weight_pct': 3},
    ], {}))
    monkeypatch.setattr(V, 'load_inputs', lambda isin, _today: (
        {'gurufocus_exchange': {'currency_code': 'EUR'}}, prices,
        conflict if isin == 'CONFLICT' else earnings, None))
    result = V.publish(7, today=TODAY)
    by_isin = {row['isin']: row for row in result['rows']}
    assert result['ready'] is True
    assert by_isin['HEALTHY']['estimated_value'] == 80
    assert by_isin['CONFLICT']['status'] == 'unavailable'
    assert by_isin['CONFLICT']['price'] == 60
    assert 'Conflicting EPS' in by_isin['CONFLICT']['reason']
    assert len(saved) == 1
