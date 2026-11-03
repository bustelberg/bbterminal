from routers._fundamental_blend import blend_series

CODE = 'annuals__Income Statement__Revenue'


def test_acwi_uses_500_reporters_per_period_instead_of_weight_or_percentage():
    members = [{'weight': 0.01 if i < 500 else 1, 'min_reporting_companies': 500,
                'points': {'2024-12-31': 10, **({'2025-12-31': 11} if i < 499 else {})}
                          if i < 500 else {}} for i in range(2000)]
    result = blend_series(members, CODE)
    assert [p['period'] for p in result['points']] == ['2024']
    assert result['points'][0]['reporting_companies'] == 500
    assert result['points'][0]['covered_pct'] < 50


def test_500_index_values_do_not_draw_an_aggregate_with_only_499_euro_inputs():
    members = [{'weight': 1, 'min_reporting_companies': 500,
                'points': {'2024-12-31': 10},
                'fund_points': {'2024-12-31': 100} if i < 499 else {}} for i in range(500)]
    assert blend_series(members, CODE)['points'] == []
