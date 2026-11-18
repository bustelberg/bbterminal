from services import portfolio_membership as M


def entry(name):
    return {'display_name': name, 'leonteq_name': name + ' Index'}


def test_all_companies_merges_verified_issuer_across_sources_and_keeps_missing_coverage(monkeypatch):
    from leonteq import composition_cache, management, portfolio_cache
    from routers import _airs_portfolio_analysis as analysis
    from services import portfolio_valuation as valuation

    monkeypatch.setattr(valuation, 'portfolios', lambda: [
        {'name': 'Bustelberg', 'id': 1, 'management_group': 'bustelberg'},
        {'name': 'Toppenberg', 'id': 2, 'management_group': 'toppenberg'},
        {'name': 'Imagine Fund', 'id': None, 'management_group': 'imagine_fund'},
    ])
    calls = []

    def holdings(model, *, look_through):
        calls.append((model, look_through))
        rows = [{'isin': 'A' if model == 1 else 'B', 'weight_pct': 10}]
        if look_through:
            rows += [{'isin': 'C', 'name': 'Booking Holdings Inc.', 'weight_pct': 5},
                     {'isin': 'XS1002121454', 'name': 'Rabobank Certificaat', 'weight_pct': 5}]
        return rows, {}

    monkeypatch.setattr(valuation, 'company_holdings', holdings)
    monkeypatch.setattr(management, 'entries', lambda: [entry('AITopSelectie')])
    monkeypatch.setattr(portfolio_cache, 'saved_portfolios', lambda: {'portfolios': [{'name': 'AITopSelectie Index'}]})
    monkeypatch.setattr(composition_cache, 'composition', lambda _, *, saved_only: {'success': saved_only,
        'groups': [{'name': 'Share', 'positions': [{'isin': 'A', 'weight': '10%'},
                     {'isin': 'D', 'name': 'Leonteq USD Overnight Return EUR ETN', 'weight': '10%'},
                     {'isin': 'E', 'name': 'IBM 1 1/4 02/09/34', 'weight': '10%'}]},
                   {'name': 'Bond', 'positions': [{'isin': 'C', 'weight': '10%'}]}]})
    monkeypatch.setattr(analysis, '_grid', lambda _: {'A': {'company_id': 101, 'name': 'Example Inc.'},
                                                     'B': {'company_id': 102, 'name': 'Example Inc. Class A'}})
    monkeypatch.setattr(analysis, '_is_fund', lambda *_: False)
    monkeypatch.setattr(M, '_valuation_issuers', lambda holdings, grid: M._merge_valuation_issuers(
        holdings, grid, {}, {101: {'company_id': 101, 'isin': 'A', 'company_name': 'Example Inc.'},
                            102: {'company_id': 102, 'isin': 'B', 'company_name': 'Example Inc.', 'gurufocus_ticker': 'EX'}}))
    assert M.all_companies() == {'companies': [{'name': 'Example Inc.', 'isins': ['A', 'B'],
        'valuation_isin': 'B', 'company_id': 102,
        'portfolios': ['AITopSelectie', 'Bustelberg', 'Toppenberg']}], 'unavailable_portfolios': ['Imagine Fund']}
    assert calls == [(1, False), (2, True)]


def test_bustelberg_unions_profiles_but_keeps_bb2_separate_and_ignores_zero_positions():
    roster = [{'id': 1, 'account': 'BUS_Offensief_Dyn', 'name': 'Bustelberg Offensief'},
        {'id': 2, 'account': 'BUS_Defensief_Dyn', 'name': 'Bustelberg Defensief'},
        {'id': None, 'account': 'BUS_Neutraal_Dyn', 'name': 'Bustelberg Neutraal'},
        {'id': 3, 'account': 'BUS_2.0_ OFF_DYN', 'name': 'Bustelberg 2.0 Offensief'}]
    def load(model):
        return ([{'isin': 'COMMON' if model != 3 else 'BB2ONLY', 'weight_pct': 5},
                 {'isin': 'CLOSED', 'weight_pct': 0}], {'composition_date': '2026-11-13'})
    result = M.build(roster, load, [], {}, lambda _: None)
    assert result['memberships']['COMMON']['BB'] == ['Bustelberg Defensief', 'Bustelberg Offensief']
    assert set(result['memberships']['BB2ONLY']) == {'BB2'}
    assert 'CLOSED' not in result['memberships']
    bb = next(row for row in result['legend'] if row['code'] == 'BB')
    assert bb['status'] == 'partial'  # A listed but unlinked profile stays visible.


def test_unlisted_risk_profiles_do_not_create_permanent_coverage_gaps():
    roster = [{'id': 1, 'account': 'TOPS_OFF_BEH_DYN', 'name': 'Toppenberg Offensief'},
        {'id': 2, 'account': 'TOPS_DEF_BEH_DYN', 'name': 'Toppenberg Defensief'}]
    result = M.build(roster, lambda _: ([{'isin': 'KNOWN', 'weight_pct': 5}],
        {'composition_date': '2026-11-13'}), [], {}, lambda _: None)
    tb = next(row for row in result['legend'] if row['code'] == 'TB')
    assert tb['status'] == 'complete'
    assert len(tb['portfolios']) == 2


def test_topselecties_use_only_leonteq_and_never_airs_or_workbook_membership():
    calls = []
    airs_calls = []
    def airs(model_id):
        airs_calls.append(model_id)
        raise AssertionError('Topselecties must not request AIRS holdings')
    def leonteq(name):
        calls.append(name)
        return {'success': True, 'as_of': '2026-11-13', 'groups': [{'positions': [
            {'isin': 'EXACT', 'weight': '5.00%'}, {'isin': 'ZERO', 'weight': '0.00%'}]}]}
    catalogue = {'portfolios': [{'name': 'FamilieTopSelectie'}]}
    result = M.build([{'id': 99, 'account': 'BUS_FTS_OFF_DYN', 'name': 'FamilieTopSelectie'}],
                     airs, [entry('FamilieTopSelectie')], catalogue, leonteq)
    assert result['memberships']['EXACT'] == {'FTS': ['FamilieTopSelectie']}
    assert 'ZERO' not in result['memberships']
    assert calls == ['FamilieTopSelectie']
    assert airs_calls == []
    missing = M.build([], airs, [entry('FamilieTopSelectie')], catalogue, lambda _: {'success': False})
    assert missing['memberships'] == {}
    assert airs_calls == []
    assert next(row for row in missing['legend'] if row['code'] == 'FTS')['status'] == 'unavailable'


def test_opaque_constituents_mark_membership_partial_without_dropping_known_positions():
    roster = [{'id': 1, 'account': 'BUS_2.0_ OFF_DYN', 'name': 'BB2'}]
    result = M.build(roster, lambda _: ([{'isin': 'KNOWN', 'weight_pct': 5}],
        {'composition_date': '2026-11-13', 'lookthrough': {'opaque_pct': 10}}), [], {}, lambda _: None)
    assert result['memberships']['KNOWN'] == {'BB2': ['BB2']}
    assert next(row for row in result['legend'] if row['code'] == 'BB2')['status'] == 'partial'


def test_unidentified_security_is_partial_but_unidentified_cash_is_not_a_membership_gap():
    catalogue = {'portfolios': [{'name': 'FamilieTopSelectie'}]}
    def snapshot(category):
        return {'success': True, 'groups': [{'name': category,
            'positions': [{'isin': None, 'weight': '10%'}]}]}
    unknown = M.build([], lambda _: None, [entry('FamilieTopSelectie')], catalogue, lambda _: snapshot('Shares'))
    cash = M.build([], lambda _: None, [entry('FamilieTopSelectie')], catalogue, lambda _: snapshot('Cash'))
    assert next(row for row in unknown['legend'] if row['code'] == 'FTS')['status'] == 'partial'
    assert next(row for row in cash['legend'] if row['code'] == 'FTS')['status'] == 'complete'
    assert unknown['memberships'] == cash['memberships'] == {}


def test_leonteq_source_date_and_retrieval_time_are_separate_and_never_inferred():
    catalogue = {'portfolios': [{'name': 'FamilieTopSelectie'}]}
    retrieved = '2026-11-16T15:56:28+00:00'
    for source_date in ('2026-11-13', None):
        result = M.build([], lambda _: (_ for _ in ()).throw(AssertionError('No AIRS fallback')),
            [entry('FamilieTopSelectie')], catalogue, lambda _: {'success': True,
                'as_of': source_date, 'retrieved_at': retrieved,
                'groups': [{'positions': [{'isin': 'KNOWN', 'weight': '5%'}]}]})
        definition = next(item for item in result['legend'] if item['code'] == 'FTS')
        profile = definition['portfolios'][0]
        assert profile['as_of'] == source_date
        assert profile['retrieved_at'] == retrieved
        assert definition['status'] == 'complete'  # Coverage is not a freshness assertion.



def test_airs_retrieval_time_is_independent_of_its_older_effective_date():
    result = M.build([{'id': 1, 'account': 'BUS_Offensief_Dyn', 'name': 'Bustelberg Offensief'}],
        lambda _: ([{'isin': 'KNOWN', 'weight_pct': 5}], {'composition_date': '2025-04-28',
            'retrieved_at': '2026-11-16T16:03:06+00:00'}), [], {}, lambda _: None)
    profile = next(item for item in result['legend'] if item['code'] == 'BB')['portfolios'][0]
    assert profile['as_of'] == '2025-04-28'
    assert profile['retrieved_at'] == '2026-11-16T16:03:06+00:00'
