from types import SimpleNamespace

import pytest

from services import toppenberg


def test_position_performance_uses_saved_attribution_and_preserves_its_dates():
    row = SimpleNamespace(isin='TEST', currency='EUR', category='Share', source_return=9)
    source = {'accounting': {'success': True, 'result': {
        'available': True, 'from': '2026-06-08', 'to': '2026-11-17', 'estimated': True,
        'basis_eur': 1000, 'positions': {'TEST': {'return_pct': -3, 'contribution_pct': -.4, 'opening': 200}},
    }}}
    actual = toppenberg._position_performance(row, source, .25, report=source['accounting']['result'])
    assert actual['own_return_pct'] == -3
    assert actual['contribution_pct'] == pytest.approx(-.1)
    assert actual['own_return_from'] == '2026-06-08'
    assert actual['own_return_as_of'] == '2026-11-17'
    assert actual['own_return_estimated']
    assert actual['weight_start_pct'] == 5
    dated_return = toppenberg._position_performance(row, source, .25)
    assert dated_return['own_return_pct'] == -3
    assert dated_return['contribution_pct'] is None
    for fallback in (toppenberg._position_performance(row, {}, .25),
                     toppenberg._position_performance(row, source, .25, unique=False)):
        assert fallback['own_return_pct'] == 9
        assert fallback['contribution_pct'] is None


def test_attribution_reconciles_held_sold_fees_and_unallocated_sleeves():
    window = {'to': '2026-11-19', 'ytd_pct': 1.2, 'sleeve_returns': {'A': 4, 'B': -2, 'Cash': 0}}
    weights = {'A': .5, 'B': .4, 'Cash': .1}
    report = {'positions': {
        'HELD': {'contribution_pct': 2.5},
        'SOLD': {'isin': 'SOLD', 'name': 'Sold Stock', 'closing': 0, 'contribution_pct': 1},
        'fee:AMC': {'contribution_pct': -.5},
    }}
    rows = [{'via_names': ['A'], 'accounting_identity': 'HELD', 'contribution_pct': 1.25}]
    summary = toppenberg._attribution_summary(weights, window, {'A': report}, rows)
    assert summary['positions'][0]['contribution_pct'] == .5
    adjustments = {row['label']: row['contribution_pct'] for row in summary['certificate_adjustments']}
    assert adjustments == pytest.approx({'Total unallocated': -.55})
    assert 1.25 + .5 + sum(adjustments.values()) == pytest.approx(window['ytd_pct'])


def test_aligned_attribution_requires_all_native_valuations_and_reads_saved_cash_flows(monkeypatch):
    from leonteq import accounting

    days = ['2026-09-22', '2026-09-23', '2026-09-24']
    window = {'from': days[0], 'to': days[-1], 'sleeve_returns': {'A': 2},
              'sleeve_marks': {'A': dict(zip(days, [1, 1.01, 1.02]))}}
    raw = {'transactions': [], 'valuations': {days[0]: {}, days[-1]: {}},
           'cash_history': {'complete': True, 'certificates': [{'cash_movements': [
               {'dateTime': days[1] + 'T00:00:00', 'netFlows': {'value': 25, 'precision': 2}},
           ]}]}}
    assert toppenberg._calculate_aligned_report(raw, window, 'A') is None
    raw['valuations'][days[1]] = {}
    monkeypatch.setattr(accounting, 'valuation', lambda _, day: day)

    def calculate(boundaries, transactions, marks, target, *, cash_flows):
        assert boundaries == [day for day, _ in marks] == days
        assert transactions == []
        assert target == 2
        assert cash_flows == [{'date': days[1], 'cash_eur': '25'}]
        return {'available': True}

    monkeypatch.setattr(accounting, 'calculate', calculate)
    assert toppenberg._calculate_aligned_report(raw, window, 'A') == {'available': True}


@pytest.mark.parametrize(('sleeve', 'category', 'expected'), [
    ('MerkenTopSelectie', 'Share', 'Equity'),
    ('ObligatieTopSelectie', 'Bond', 'Bonds'),
    ('ObligatieTopSelectie', 'Fund', 'Bonds'),
    ('VastgoedTopSelectie', 'Share', 'Alternatives'),
    ('AlternatievenTopSelectie', 'Fund', 'Alternatives'),
    ('AlternatievenTopSelectie', 'Cash', 'Cash'),
    ('MerkenTopSelectie', 'Option', 'Alternatives'),
    ('MerkenTopSelectie', 'Fund', 'Unclassified'),
])
def test_toppenberg_uses_leonteq_categories(sleeve, category, expected):
    assert toppenberg._position_bucket(sleeve, category, lambda: 'Unclassified') == expected


def test_toppenberg_rebases_each_leonteq_sleeve_to_shared_open(monkeypatch):
    monkeypatch.setattr(toppenberg.leonteq.return_windows, 'for_monthly_report', lambda source: source or None)
    sources = {
        'A': {'returns': [{'date': '2026-01-01', 'cum_pct': 0}, {'date': '2026-02-01', 'cum_pct': 10},
                          {'date': '2026-03-01', 'cum_pct': 21}]},
        'B': {'returns': [{'date': '2026-02-01', 'cum_pct': 0}, {'date': '2026-03-01', 'cum_pct': -5}]},
    }
    weights = {'A': .5, 'B': .4, 'Cash': .09}
    window, missing, coverage = toppenberg._return_window(weights, sources)
    assert window['from'] == '2026-02-01'
    assert window['ytd_pct'] == pytest.approx(3 / .99)
    assert missing == []
    assert coverage == pytest.approx(90 / .99)
    rows = toppenberg._sleeve_holdings(weights, window)
    assert [row['return_pct'] for row in rows] == pytest.approx([10, -5, 0])
    assert [row['weight_pct'] for row in rows] == pytest.approx([50, 40, 9])
    assert [row['contribution_pct'] for row in rows] == pytest.approx([5 / .99, -2 / .99, 0])
    assert sum(row['contribution_pct'] for row in rows) == pytest.approx(window['ytd_pct'])
    monthly = toppenberg._monthly_performance(weights, window, None)
    assert monthly['performance']['months'] == [
        {'month': '2026-03', 'return_pct': pytest.approx(window['ytd_pct']), 'as_of': '2026-03-01'},
    ]
    positions = monthly['monthly_attribution']['2026-03']['positions']
    assert sum(row['opening_weight_pct'] for row in positions) == pytest.approx(100)
    assert sum(row['contribution_pp'] for row in positions) == pytest.approx(window['ytd_pct'])


def test_toppenberg_never_substitutes_a_missing_leonteq_sleeve(monkeypatch):
    monkeypatch.setattr(toppenberg.leonteq.return_windows, 'for_monthly_report', lambda source: source or None)
    window, missing, coverage = toppenberg._return_window({'A': .5, 'B': .5}, {'A': {'returns': [
        {'date': '2026-02-01', 'cum_pct': 0}, {'date': '2026-03-01', 'cum_pct': 3},
    ]}})
    assert window is None
    assert missing == ['B']
    assert coverage == 50
    assert all(row['contribution_pct'] is None and row['return_pct'] is None
               for row in toppenberg._sleeve_holdings({'A': .5, 'B': .5}, window))


def test_toppenberg_bond_and_alternatives_returns_use_leonteq(monkeypatch):
    monkeypatch.setattr(toppenberg.leonteq.return_windows, 'for_monthly_report', lambda source: source or None)
    names = ('Stocks', 'ObligatieTopSelectie', 'AlternatievenTopSelectie')
    weights = dict(zip(names, [.5, .3, .2]))
    sources = {name: {'returns': [{'date': '2026-09-22', 'cum_pct': 0},
                                  {'date': '2026-11-19', 'cum_pct': value}]}
               for name, value in zip(names, [4, 1, -2])}
    window, missing, _ = toppenberg._return_window(weights, sources)
    assert not missing
    assert window['ytd_pct'] == pytest.approx(1.9)
    assert all(row['source'] == 'leonteq' for row in toppenberg._sleeve_holdings(weights, window))


def test_toppenberg_start_details_distinguishes_late_history_and_date_gaps():
    window = {'from': '2026-10-13', 'sleeve_marks': {
        'Stocks': {'2026-10-08': 1, '2026-10-12': 1, '2026-10-13': 1},
        'ObligatieTopSelectie': {'2026-10-09': 1, '2026-10-13': 1},
    }}
    assert toppenberg._return_start_details(window) == {
        'first_possible_date': '2026-10-09',
        'limiting_sleeves': [{'name': 'ObligatieTopSelectie', 'source': 'Leonteq'}],
        'previous_observation_date': '2026-10-12', 'missing_sleeves': ['ObligatieTopSelectie'],
    }
