from datetime import date, timedelta
from math import sqrt

import pytest

from routers import _airs_weight_search as W


def test_statistical_check_rejects_short_or_unsupported_validation():
    baseline = [.02, -.02, .01, -.01] * 26
    assert not W.statistical_check(baseline[:13], baseline[:13], 2)['supported']
    assert not W.statistical_check(baseline, baseline, 2)['supported']
    assert W.statistical_check([value + .005 for value in baseline], baseline, 2)['supported']
    assert not W.statistical_check([.02] * 104, baseline, 2)['supported']


def test_sortino_includes_zero_weeks_and_uses_all_weeks_in_downside_denominator():
    result = W.metrics([.02, -.01, 0, .03], annual_target=0)
    assert result['sortino'] == pytest.approx(sqrt(52) * .01 / .005)
    assert result['max_drawdown_pct'] == pytest.approx(-1)
    assert result['weeks'] == 4
    assert W.metrics([0] * 52, annual_target=0)['sortino'] is None


def test_grid_is_long_only_sums_to_100_and_holdout_cannot_select_weights():
    rows = [[.02 if i % 3 else -.01, .005 if i % 4 else -.005, -.005 if i % 2 else .008, 0] for i in range(80)]
    first = W.search(rows, [.4, .3, .2, .1])
    second = W.search(rows[:60] + [[-.2, .2, .3, 0]] * 20, [.4, .3, .2, .1])
    assert first['candidates_tested'] == 1771
    assert first['training_weeks'] == 54
    assert first['validation_weeks'] == 26
    assert [row['weights'] for row in first['candidates']] == [row['weights'] for row in second['candidates']]
    for row in first['candidates']:
        assert sum(row['weights'].values()) == pytest.approx(100)
        assert all(0 <= weight <= 100 for weight in row['weights'].values())
    assert first['candidates'][0]['training']['sortino'] >= first['current']['training']['sortino']


def test_74_weeks_reserve_26_for_validation_without_leaking_them_into_selection():
    rows = [[.02 if i % 3 else -.01, .005 if i % 4 else -.005, -.005 if i % 2 else .008, 0] for i in range(74)]
    first = W.search(rows, [.4, .3, .2, .1])
    second = W.search(rows[:48] + [[-.2, .2, .3, 0]] * 26, [.4, .3, .2, .1])
    assert first['training_weeks'] == 48
    assert first['validation_weeks'] == 26
    assert [row['weights'] for row in first['candidates']] == [row['weights'] for row in second['candidates']]
    assert 'at least 26 needed' not in first['statistical_check']['reason']


def test_short_history_and_unmeasurable_downside_are_explicit():
    assert not W.search([[0, 0, 0, 0]] * 51, [.25] * 4)['available']
    assert not W.search([[0, 0, 0, 0]] * 52, [.25] * 4, annual_target=0)['available']


def test_absent_non_cash_sleeves_cannot_receive_optimised_weight():
    result = W.search([[.01 if i % 2 else -.005, 0, 0, 0] for i in range(60)], [.9, 0, 0, .1])
    assert all(row['weights']['Bonds'] == row['weights']['Alternatives'] == 0 for row in result['candidates'])


def test_every_candidate_respects_all_profile_bounds():
    bounds = {'Equity': (40, 60), 'Bonds': (20, 35), 'Alternatives': (10, 20), 'Cash': (0, 5)}
    rows = [[.02 if i % 3 else -.01, .005 if i % 4 else -.005, -.005 if i % 2 else .008, 0] for i in range(80)]
    result = W.search(rows, [.4, .3, .2, .1], bounds=bounds)
    assert result['available']
    assert result['candidates_tested'] < 1771
    for candidate in result['candidates']:
        for bucket, (lo, hi) in bounds.items():
            assert lo <= candidate['weights'][bucket] <= hi
    assert not W.search(rows, [.4, .3, .2, .1], bounds=dict.fromkeys(W.BUCKETS, (30, 40)))['available']


def test_profile_policy_uses_the_shared_saved_bands_and_passes_them_to_search(monkeypatch):
    from routers import _airs_allocation_bands as bands
    cells = [{'variant': 'Offensief', 'bucket': bucket, 'min_pct': 70 if bucket == 'Equity' else 0,
              'max_pct': 100 if bucket == 'Equity' else 30, 'default_pct': 25} for bucket in W.BUCKETS]
    monkeypatch.setattr(bands, 'load_bands', lambda: cells)
    observed = []
    monkeypatch.setattr(W, '_run', lambda *args: observed.append(args[-2]) or {'available': False})
    response = W.run('BUS_Offensief_DYN', date(2025, 1, 1), profile='Offensief')
    assert response['profile'] == 'Offensief'
    assert response['bounds'] == cells
    assert observed[0]['Equity'] == (70, 100)
    cells[0]['min_pct'] = 101
    assert 'invalid' in W.run('BOOK', date(2025, 1, 1), profile='Offensief')['reason']
    assert len(observed) == 1  # Invalid policy never runs an unconstrained fallback.


@pytest.fixture
def data(monkeypatch):
    from routers import _airs_instrument_returns as H, _airs_holding_isin as I, _benchmark_index as FX
    from routers import _airs_portfolio_analysis as PA
    from tests._fake_supabase import FakeSupabase
    monkeypatch.setattr(W, 'policy', lambda *a: {'profile': 'Offensief', 'bounds': [
        {'bucket': bucket, 'min_pct': 0, 'max_pct': 100} for bucket in W.BUCKETS]})
    options = [{'id': f'{name}|EUR', 'isin': name, 'name': name, 'currency': 'EUR'} for name in ('A', 'B', 'C')]
    rows = [{'holding_name': name, 'isin': name, 'currency': 'EUR', 'bucket': bucket, 'current_value_eur': 100}
            for name, bucket in zip(('A', 'B', 'C', 'cash'), W.BUCKETS)]
    monkeypatch.setattr(H, '_snapshots', lambda account: (date(2026, 3, 6), []))
    monkeypatch.setattr(H, '_catalogue', lambda _: (options, {}, {}, {}))
    monkeypatch.setattr(H, 'supabase', FakeSupabase({H.TABLE: []}))
    monkeypatch.setattr(I, 'resolve_account_isins', lambda *a, **kw: {'rows': rows})
    monkeypatch.setattr(PA, 'classify_account_holdings', lambda account, rows: rows)
    monkeypatch.setattr(FX, '_fx_to_eur', lambda *a: {})
    series = {}
    for index, option in enumerate(options):
        periods, cells = [], []
        for week in range(60):
            day = date(2025, 1, 10) + timedelta(weeks=week)
            periods.append({'target_date': day.isoformat(), 'as_of_date': day.isoformat(), 'from_date': (day - timedelta(days=7)).isoformat()})
            change = 0 if week % 4 == 0 else .01 if week % 2 else -.005 * (index + 1)
            cells.append({'status': 'priced', 'from_price': 100, 'price': 100 * (1 + change)})
        series[option['id']] = {'periods': periods, 'selected': {'weeks': cells}}
    monkeypatch.setattr(H, '_series', lambda account, through, snapshots, individual, instrument, **kw: series[instrument['id']])
    return rows, options, series


def test_saved_data_retains_genuine_zero_weeks_and_reports_assumptions(data):
    result = W.run('BOOK', date(2025, 1, 1))
    assert result['available']
    assert result['training_weeks'] + result['validation_weeks'] == 60
    assert result['current']['weights'] == dict.fromkeys(W.BUCKETS, 25)
    assert result['annual_cash_pct'] == 0
    assert 'dividends' in result['limitations'][0]


def test_missing_week_is_not_compressed_into_a_continuous_backtest(data):
    _, _, series = data
    series['A|EUR']['selected']['weeks'][40]['status'] = 'missing_price'
    result = W.run('BOOK', date(2025, 1, 1))
    assert not result['available']
    assert '19 consecutive' in result['reason']


def test_unclassified_or_negative_holdings_do_not_get_silently_dropped(data):
    rows, _, _ = data
    rows[0]['bucket'] = 'Unclassified'
    assert W.run('BOOK', date(2025, 1, 1))['issues']
    rows[0]['current_value_eur'] = -1
    assert 'Short positions' in W.run('BOOK', date(2025, 1, 1))['reason']


def test_mismatched_actual_valuation_dates_are_not_paired(data):
    _, _, series = data
    series['B|EUR']['periods'][40]['as_of_date'] = '2025-01-01'
    assert not W.run('BOOK', date(2025, 1, 1))['available']


def test_foreign_currency_uses_both_historical_fx_marks(data, monkeypatch):
    from routers import _benchmark_index as FX
    rows, options, series = data
    rows[0]['currency'] = options[0]['currency'] = 'USD'
    periods = series['A|EUR']['periods']
    rates = {period['from_date']: 2 for period in periods}
    rates.update({period['as_of_date']: 2 for period in periods})
    rates[periods[0]['as_of_date']] = 4
    monkeypatch.setattr(FX, '_fx_to_eur', lambda *a: {'USD': rates})
    captured = []
    monkeypatch.setattr(W, 'search', lambda matrix, *a, **kw: captured.extend(matrix) or {'available': False})
    W.run('BOOK', date(2025, 1, 1))
    assert captured[0][0] == pytest.approx(-.5)  # Flat USD price, USD/EUR rate doubles.
    assert captured[0][1] == 0  # Genuine zero EUR return remains zero.


def test_stale_fx_is_not_silently_used(data, monkeypatch):
    from routers import _benchmark_index as FX
    rows, options, _ = data
    rows[0]['currency'] = options[0]['currency'] = 'USD'
    monkeypatch.setattr(FX, '_fx_to_eur', lambda *a: {'USD': {'2024-01-01': 1.1}})
    assert not W.run('BOOK', date(2025, 1, 1))['available']


def test_proxy_mode_excludes_new_holdings_and_preserves_true_current_class_weights(data):
    rows, _, _ = data
    rows.append({'holding_name': 'New share', 'isin': 'NEW', 'currency': 'EUR', 'bucket': 'Equity', 'current_value_eur': 100})
    assert not W.run('BOOK', date(2025, 1, 1))['available']
    result = W.run('BOOK', date(2025, 1, 1), available_history=True)
    assert result['available'] and result['proxy_history']
    assert result['training_weeks'] + result['validation_weeks'] == 60
    assert result['current']['weights']['Equity'] == 40  # Full actual allocation, not proxy-only value.
    stocks = next(row for row in result['coverage'] if row['bucket'] == 'Equity')
    assert stocks['coverage_pct'] == stocks['minimum_coverage_pct'] == 50
    assert stocks['included'] == ['A'] and stocks['excluded'] == ['New share']


def test_proxy_mode_allows_26_weeks_when_a_whole_class_has_less_than_a_year(data):
    rows, _, series = data
    rows.append({'holding_name': 'New alternative', 'isin': 'NEW', 'currency': 'EUR', 'bucket': 'Alternatives', 'current_value_eur': 50})
    series['C|EUR']['periods'] = series['C|EUR']['periods'][-30:]
    series['C|EUR']['selected']['weeks'] = series['C|EUR']['selected']['weeks'][-30:]
    result = W.run('BOOK', date(2025, 1, 1), available_history=True)
    assert result['available']
    assert result['training_weeks'] == 22
    assert result['validation_weeks'] == 8
    alternatives = next(row for row in result['coverage'] if row['bucket'] == 'Alternatives')
    assert alternatives['coverage_pct'] == pytest.approx(100 / 150 * 100)
    assert alternatives['excluded'] == ['New alternative']


def test_proxy_mode_cannot_fabricate_an_entire_missing_asset_class(data):
    _, _, series = data
    series['C|EUR']['selected']['weeks'] = []
    result = W.run('BOOK', date(2025, 1, 1), available_history=True)
    assert not result['available']
    assert 'every held asset class' in result['reason']


def test_weekly_baskets_use_alternating_instruments_without_diluting_missing_returns():
    first, second, full = {}, {}, {}
    for week in range(52):
        day = date(2025, 1, 10) + timedelta(weeks=week)
        key, opening = day.isoformat(), (day - timedelta(days=7)).isoformat()
        full[key] = (.005, opening, key)
        (first if week % 2 == 0 else second)[key] = (0 if week == 0 else .02 if week % 2 == 0 else -.01, opening, key)
    sleeves = {'Equity': [(100, first), (300, second)], 'Bonds': [(100, full)], 'Alternatives': [(100, full)]}
    dates, matrix, membership, coverage, skipped = W.weekly_proxy(sleeves)
    assert len(dates) == 52 and skipped == []
    assert matrix[0][0] == 0  # Actual zero, not missing data.
    assert matrix[1][0] == -.01  # Entire class represented by the sole available instrument.
    assert matrix[2][0] == .02
    assert coverage['Equity'][:2] == [25, 75]
    assert membership[id(first)] == membership[id(second)] == 26


def test_whole_class_missing_week_is_skipped_and_not_compressed_into_a_validated_path(data):
    _, _, series = data
    series['C|EUR']['selected']['weeks'][30]['status'] = 'missing_price'
    result = W.run('BOOK', date(2025, 1, 1), available_history=True)
    assert result['available']
    assert result['training_weeks'] + result['validation_weeks'] == 59
    assert len(result['skipped_weeks']) == 1
    assert not result['statistical_check']['supported']
    assert result['candidates'][0]['full_history']['max_drawdown_pct'] is None
    assert result['candidates'][0]['full_history']['annual_return_pct'] is None


def test_weekly_bins_allow_nearby_report_dates_but_reject_future_or_wrong_week_marks():
    day = date(2025, 1, 10)
    key = day.isoformat()
    good = {key: (.01, '2025-01-03', '2025-01-10')}
    near = {key: (.02, '2025-01-02', '2025-01-09')}
    future = {key: (.99, '2025-01-03', '2025-01-11')}
    sleeves = {'Equity': [(100, good), (100, future)], 'Bonds': [(100, good)], 'Alternatives': [(100, near)]}
    dates, matrix, _, _, skipped = W.weekly_proxy(sleeves)
    assert dates == [key] and skipped == []
    assert matrix == [[.01, .01, .02]]


def test_nine_week_history_can_be_calculated_without_claiming_statistical_support(data):
    _, _, series = data
    for value in series.values():
        value['periods'] = value['periods'][-9:]
        value['selected']['weeks'] = value['selected']['weeks'][-9:]
    result = W.run('BOOK', date(2025, 1, 1), available_history=True)
    assert result['available']
    assert result['candidates_tested'] > 0
    assert result['training_weeks'] == 6 and result['validation_weeks'] == 3
    assert not result['statistical_check']['supported']
    assert 'Calculated from 9 usable weeks' in result['statistical_check']['reason']
    assert sum(result['candidates'][0]['weights'].values()) == pytest.approx(100)
    for value in series.values():
        value['periods'] = value['periods'][-3:]
        value['selected']['weeks'] = value['selected']['weeks'][-3:]
    assert 'at least 4 needed' in W.run('BOOK', date(2025, 1, 1), available_history=True)['reason']


def test_sharpe_uses_sample_deviation_and_cash_rate_not_the_sortino_target():
    values = [.02, -.01, 0, .03]
    risk_free = 1.03 ** (1 / 52) - 1
    result = W.metrics(values, annual_target=15, annual_risk_free=3)
    assert result['sharpe'] == pytest.approx(sqrt(52) * (.01 - risk_free) / sqrt(.001 / 3))
    assert W.metrics([.01] * 8)['sharpe'] is None


def test_sharpe_ranking_is_independent_of_test_returns_and_does_not_require_downside():
    rows = [[.02 if i % 3 else .01, .005 if i % 4 else .001, .002 if i % 2 else .008, 0] for i in range(74)]
    first = W.search(rows, [.4, .3, .2, .1], objective='sharpe', annual_target=0)
    second = W.search(rows[:48] + [[-.2, .2, .3, 0]] * 26, [.4, .3, .2, .1], objective='sharpe', annual_target=0)
    assert first['available'] and first['objective'] == 'sharpe'
    assert [row['weights'] for row in first['candidates']] == [row['weights'] for row in second['candidates']]
    scores = [row['training']['sharpe'] for row in first['candidates']]
    assert scores == sorted(scores, reverse=True)
    assert first['candidates'][0]['training']['sortino'] is None  # Sharpe remains defined without downside.


def test_instrument_details_use_global_split_and_actual_weighted_class_returns():
    first, second = {}, {}
    for index, (a, b) in enumerate(zip([.02, -.01, .03, -.02, .01], [0, .04, -.01, .02, -.03])):
        day = date(2025, 1, 10) + timedelta(weeks=index)
        key, opening = day.isoformat(), (day - timedelta(days=7)).isoformat()
        first[key] = (a, opening, key)
        if index:
            second[key] = (b, opening, key)
    sleeves = {'Equity': [(100, first), (300, second)], 'Bonds': [], 'Alternatives': [], 'Cash': []}
    dates, matrix, _, _, _ = W.weekly_proxy(sleeves)
    matrix = [row + [0] for row in matrix]
    block = W.instrument_breakdown(sleeves, {id(first): 'First', id(second): 'Second'}, dates, matrix, 3, 2, 0, True)[0]
    a, b = block['instruments']
    assert a['training']['weeks'] == 3 and b['training']['weeks'] == 2
    assert a['validation']['weeks'] == b['validation']['weeks'] == 2
    assert a['training_weight_pct'] == b['training_weight_pct'] == 50
    assert a['test_weight_pct'] == 25 and b['test_weight_pct'] == 75
    assert block['training']['sharpe'] == pytest.approx(W.metrics([row[0] for row in matrix[:3]])['sharpe'])
    assert block['training']['sharpe'] != pytest.approx(.5 * a['training']['sharpe'] + .5 * b['training']['sharpe'])
    assert a['overall']['sharpe'] == pytest.approx(W.metrics([.02, -.01, .03, -.02, .01])['sharpe'])
    assert a['overall']['start'] == '2025-01-03'
    assert b['overall']['start'] == '2025-01-10'
    assert a['overall']['end'] == b['overall']['end'] == '2025-02-07'
    assert block['average_sharpe'] == pytest.approx((a['overall']['sharpe'] + b['overall']['sharpe']) / 2)
    assert block['period'] == {'start': '2025-01-03', 'end': '2025-02-07'}


def test_saved_data_response_contains_all_instruments_and_block_scores(data):
    result = W.run('BOOK', date(2025, 1, 1), available_history=True)
    blocks = result['instrument_breakdown']
    assert [block['bucket'] for block in blocks] == list(W.BUCKETS)
    assert blocks[0]['instruments'][0]['name'] == 'A'
    assert blocks[0]['training']['sharpe'] == blocks[0]['instruments'][0]['training']['sharpe']
    assert blocks[3]['instruments'][0]['name'] == 'cash'
    assert blocks[3]['training']['sharpe'] is None
    assert result['training_period']['start'] == '2025-01-03'
    assert result['training_period']['end'] == result['test_period']['start']
    assert result['test_period']['end'] == result['through']


def test_optimizer_consumes_shared_holdings_classification(data, monkeypatch):
    from routers import _airs_portfolio_analysis as PA
    def classify(account, rows):
        assert account == 'BOOK'
        return [{**row, 'bucket': {'A': 'Alternatives', 'C': 'Equity'}.get(row.get('isin'), row['bucket'])} for row in rows]
    monkeypatch.setattr(PA, 'classify_account_holdings', classify)
    result = W.run('BOOK', date(2025, 1, 1), available_history=True)
    assert result['instrument_breakdown'][0]['instruments'][0]['name'] == 'C'
    assert result['instrument_breakdown'][2]['instruments'][0]['name'] == 'A'


def test_shared_classification_uses_exact_account_model_and_reviewed_overrides(monkeypatch):
    from routers import _airs_portfolio_analysis as PA, _airs_account_links as links
    monkeypatch.setattr(links, 'list_account_links', lambda: {'accounts': [
        {'portefeuille': 'OTHER', 'model_portfolio_id': 99}, {'portefeuille': 'BOOK', 'model_portfolio_id': 7}]})
    def positions(model):
        assert model == 7
        return [{'isin': 'MINT', 'categorie': 'ALTBEL'}, {'isin': 'RABO', 'categorie': 'ALTBEL'},
                {'isin': 'BOND', 'categorie': 'OBL'}]
    monkeypatch.setattr(PA, 'ref_positions_for', positions)
    monkeypatch.setattr(PA, '_grid', lambda isins: {isin: {'asset_class': 'equity'} for isin in isins})
    rows = [
        {'isin': 'MINT', 'holding_name': 'Mint Tower Arbitrage Fund I - EUR', 'bucket': 'Equity'},
        {'isin': 'AQR', 'holding_name': 'AQR Apex UCITS Fund IAEFT EUR Acc.', 'bucket': 'Equity'},
        {'isin': 'RABO', 'holding_name': '6,5% Rabobank Certificaten 14-perp.', 'bucket': 'Equity'},
        {'isin': 'BOND', 'holding_name': 'Bond ETF', 'bucket': 'Equity'},
        {'isin': 'SHARE', 'holding_name': 'Share', 'bucket': 'Equity'},
        {'isin': None, 'holding_name': 'Effectenrekening', 'bucket': 'Cash'},
    ]
    classified = PA.classify_account_holdings('BOOK', rows)
    assert [row['bucket'] for row in classified] == ['Alternatives', 'Alternatives', 'Alternatives', 'Bonds', 'Equity', 'Cash']
    assert rows[0]['bucket'] == 'Equity'  # Does not mutate the shared resolver result.


