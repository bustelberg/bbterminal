"""Hand-worked annual rebalances, monthly TE and strict common-history admission."""
from calendar import monthrange
from datetime import date
import math

import pytest

from routers import _annual_portfolio_simulation as S


def points(start_year=2021, months=60, growth=None):
    result, value = [], 100.0
    for index in range(months + 1):
        year, month = divmod(start_year * 12 + 8 + index, 12)
        if index and growth:
            value *= growth(index)
        result.append((date(year, month + 1, monthrange(year, month + 1)[1]).isoformat(), value))
    return result


def inputs(a=None, b=None, benchmark=None):
    return {'weight': {'A': .5, 'B': .5}, 'names': {'A': 'Company A', 'B': 'Company B'},
            'series': {'A': a if a is not None else points(), 'B': b if b is not None else points(),
                       'INDEX': benchmark if benchmark is not None else points()},
            'benchmark_component_weights': {'ACWI': 100}, 'benchmark_component_isins': {'ACWI': 'INDEX'},
            'benchmark_label': '100% ACWI'}


def run(data, years=5):
    return S.simulate(data, years, date(2026, 10, 27))


def test_weights_drift_then_reset_annually_not_monthly():
    a = points(2024, 24, lambda month: 2 if month in (1, 13) else 1)
    b = points(2024, 24)
    result = run(inputs(a, b, b), 2)
    assert result['available']
    # Each year earns 50%: 0.5 x 2 + 0.5 x 1. Rebalance to 50/50 after year one.
    assert result['portfolio_return_total_pct'] == pytest.approx(125)
    assert result['monthly_returns'][0]['portfolio_return_pct'] == pytest.approx(50)
    assert result['monthly_returns'][12]['portfolio_return_pct'] == pytest.approx(50)
    assert result['rebalance_dates'] == ['2025-09-30']


def test_no_monthly_rebalance_and_all_metrics_share_the_same_series():
    a = points(2024, 24, lambda month: 2 if month == 1 else 1)
    b = points(2024, 24, lambda month: 2 if month == 2 else 1)
    result = run(inputs(a, b, points(2024, 24)), 2)
    returns = result['monthly_returns']
    assert returns[0]['portfolio_return_pct'] == pytest.approx(50)
    assert returns[1]['portfolio_return_pct'] == pytest.approx(100 / 3)
    portfolio_growth = math.prod(1 + row['portfolio_return_pct'] / 100 for row in returns)
    assert result['portfolio_return_total_pct'] == pytest.approx((portfolio_growth - 1) * 100)
    active = [row['active_return_pp'] for row in returns]
    mean = sum(active) / len(active)
    expected_te = math.sqrt(sum((value - mean) ** 2 for value in active) / (len(active) - 1)) * math.sqrt(12)
    assert result['tracking_error_pct'] == pytest.approx(expected_te)
    assert result['active_return_ann_pct'] == pytest.approx(result['portfolio_return_ann_pct'] - result['benchmark_return_ann_pct'])
    assert result['window_from'] == result['benchmark_history']['start_date'] == result['historical_comparison']['start_date']
    assert result['window_to'] == result['benchmark_history']['end_date'] == result['historical_comparison']['end_date']


def test_identical_policy_and_portfolio_have_zero_active_return_and_te():
    data = inputs(points(growth=lambda _: 1.01), points(growth=lambda _: 1.002))
    data['benchmark_component_weights'] = {'ACWI': 50, 'BONDS': 50}
    data['benchmark_component_isins'] = {'ACWI': 'A', 'BONDS': 'B'}
    result = run(data)
    assert result['observations'] == 60
    assert result['active_return_ann_pct'] == pytest.approx(0)
    assert result['tracking_error_pct'] == pytest.approx(0)


def test_missing_history_shortens_both_metrics_and_names_the_position():
    result = run(inputs(b=points()[36:]))
    assert result['available']
    assert result['history_limited']
    assert result['window_from'] == '2024-09-30'
    assert result['observations'] == 24
    assert result['history_issues'][0]['name'] == 'Company B'
    assert result['history_issues'][0]['missing_months'] == 36
    assert result['historical_comparison']['min_weight_covered_pct'] == 100


def test_very_young_holding_does_not_silently_fall_back_to_daily_or_disappear():
    result = run(inputs(b=[('2026-09-11', 100), ('2026-09-30', 101), ('2026-10-26', 200)]))
    assert not result['available']
    assert result['frequency'] == 'monthly'
    assert result['observations'] == 0
    assert result['history_issues'][0]['first_price_date'] == '2026-09-11'
    assert 'no daily fallback' in result['reason']


def test_current_partial_month_is_excluded_from_both_metrics():
    data = inputs()
    for rows in data['series'].values():
        rows.append(('2026-10-26', 10000))
    result = run(data)
    assert result['window_to'] == '2026-09-30'
    assert result['portfolio_return_total_pct'] == 0


def test_internal_gap_restarts_the_shared_period():
    data = inputs(b=[row for row in points() if row[0] != '2024-12-31'])
    result = run(data)
    assert result['window_from'] == '2025-01-31'
    assert result['observations'] == 20


def test_missing_position_entirely_is_reported_not_excluded():
    result = run(inputs(b=[]))
    assert not result['available']
    issue = result['history_issues'][0]
    assert issue['first_price_date'] is None
    assert issue['missing_months'] == 61


@pytest.mark.parametrize('currency', ['EUR', None, ''])
def test_yahoo_equity_simulation_excludes_cash(monkeypatch, currency):
    monkeypatch.setattr(S, '_risk_prices', lambda *_args: {'STOCK': points(growth=lambda _: 1.01),
                                                        'IE00B6R52259': points()})
    monkeypatch.setattr(S, '_yahoo_mappings', lambda _isins: {
        'STOCK': {'yahoo_symbol': 'STOCK', 'analysis_id': 1, 'asset_class': 'equity', 'name': 'STOCK'},
        'IE00B6R52259': {'yahoo_symbol': 'ISAC.L', 'analysis_id': 2}})
    class Clock(date):
        @classmethod
        def today(cls):
            return cls(2026, 10, 27)
    monkeypatch.setattr(S, 'date', Clock)
    result = S.compute_simulation([{'isin': 'STOCK', 'weight_pct': 50},
                                   {'name': 'EUR cash', 'weight_pct': 50, 'bucket': 'Cash', 'currency': currency}], 'ACWI')
    assert result['available']
    assert result['total_holdings'] == 1
    assert result['covered_portfolio_pct'] == pytest.approx(50)
    assert result['excluded_holdings'][0]['name'] == 'EUR cash'
    assert result['monthly_returns'][0]['portfolio_return_pct'] == pytest.approx(1)


def test_monthly_nav_is_not_allowed_to_relax_yahoo_equity_history():
    data = inputs()
    data['series']['B'] = [(day[:8] + '15', value) for day, value in points()]
    data['monthly_nav_isins'] = {'B'}
    result = run(data)
    assert not result['available']
    assert result['history_issues'][0]['missing_months'] == 61


def test_fixed_yahoo_cohort_excludes_funds_young_stocks_and_bad_mappings(monkeypatch):
    class Clock(date):
        @classmethod
        def today(cls):
            return cls(2026, 10, 27)
    monkeypatch.setattr(S, 'date', Clock)
    monkeypatch.setattr(S, '_yahoo_mappings', lambda _isins: {
        'OLD': {'name': 'Old Company Inc.', 'asset_class': 'equity', 'yahoo_symbol': 'OLD', 'analysis_id': 1},
        'YOUNG': {'name': 'Young Company', 'asset_class': 'equity', 'yahoo_symbol': 'YOUNG', 'analysis_id': 2},
        'WRONG': {'name': 'ETP on Leonteq EUR Overnight Return Index', 'asset_class': 'equity', 'yahoo_symbol': 'EONR.DE', 'analysis_id': 42},
        'IE00B6R52259': {'yahoo_symbol': 'ISAC.L', 'analysis_id': 3}})
    asked = []
    def load(isins, *_args):
        asked.extend(isins)
        return {'OLD': points(growth=lambda _: 1.01), 'YOUNG': points()[55:], 'IE00B6R52259': points()}
    monkeypatch.setattr(S, '_risk_prices', load)
    result = S.compute_simulation([
        {'isin': 'OLD', 'name': 'Old Company', 'weight_pct': 60, 'bucket': 'Equity'},
        {'isin': 'YOUNG', 'name': 'Young Company', 'weight_pct': 10, 'bucket': 'Equity'},
        {'isin': 'ETF', 'name': 'Global ETF', 'weight_pct': 10, 'bucket': 'Equity', 'is_fund': True},
        {'isin': 'WRONG', 'name': 'High Income Quality', 'weight_pct': 10, 'bucket': 'Equity'},
        {'isin': 'NAV', 'name': 'Alternative fund', 'weight_pct': 10, 'bucket': 'Alternatives'},
    ], 'ACWI')
    assert result['available']
    assert result['observations'] == 60
    assert result['covered_portfolio_pct'] == pytest.approx(60)
    assert result['total_holdings'] == 1
    assert {row['isin'] for row in result['excluded_holdings']} == {'YOUNG', 'ETF', 'WRONG', 'NAV'}
    assert 'ETF' not in asked and 'NAV' not in asked and 'WRONG' not in asked
    assert result['monthly_returns'][0]['portfolio_return_pct'] == pytest.approx(1)


@pytest.mark.parametrize('held,quoted', [
    ('Adobe Systems', 'Adobe Inc.'), ('Alphabet - C', 'Alphabet Inc.'),
    ('Berkshire Hathaway - B', 'Berkshire Hathaway Inc.'), ('MSCI Inc. A', 'MSCI Inc.'),
    ('Nestle', 'Nestlé S.A.'), ('L` Oreal', "L'Oréal S.A."),
    ('Hermes Internationale', 'Hermès International Société en commandite par actions'),
])
def test_equity_name_aliases_do_not_exclude_real_yahoo_shares(held, quoted):
    assert S._equity_name_key(held) == S._equity_name_key(quoted)


def test_fund_names_cannot_pass_as_the_underlying_equity():
    assert S._equity_name_key('High Income Quality fund') != S._equity_name_key('ETP on Leonteq EUR Overnight Return Index')
