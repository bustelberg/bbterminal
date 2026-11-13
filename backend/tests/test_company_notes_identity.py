from services.company_notes_identity import resolve


def test_all_stocks_resolve_through_exact_isin_or_verified_alias_chains():
    companies = [{'company_id': 1, 'isin': 'HOME-A'}, {'company_id': 2, 'isin': 'HOME-B'},
                 {'company_id': 3, 'isin': 'DIRECT'}, {'company_id': 4, 'isin': 'TW0002330008'}]
    rows = [{'isin': isin} for isin in ['ADR-A', 'ALT-A', 'ADR-B', 'DIRECT', 'US8740391003']]
    before = [dict(row) for row in rows]
    links = resolve(rows, companies, {'ADR-A': 'ALT-A', 'ALT-A': 'HOME-A', 'ADR-B': 'HOME-B'})
    assert {isin: link['company_id'] for isin, link in links.items()} == {
        'ADR-A': 1, 'ALT-A': 1, 'ADR-B': 2, 'DIRECT': 3, 'US8740391003': 4}
    assert links['ADR-A']['matched_isin'] == 'HOME-A'
    assert rows == before  # No symbol, currency, price or persistence mutations.


def test_cycles_ambiguous_and_unknown_companies_are_reported_instead_of_guessed():
    links = resolve([{'isin': key, 'name': 'Same company name'} for key in ['LOOP', 'DUP', 'UNKNOWN', 'INACTIVE']],
                    [{'isin': 'DUP', 'company_id': 1}, {'isin': 'DUP', 'company_id': 2},
                     {'isin': 'OTHER', 'company_id': 3, 'company_name': 'Same company name'},
                     {'isin': 'INACTIVE', 'company_id': 4, 'out_of_scope_at': '2020-01-01'}],
                    {'LOOP': 'LOOP-2', 'LOOP-2': 'LOOP'})
    assert all(link['company_id'] is None for link in links.values())
    assert 'cycle' in links['LOOP']['reason']
    assert 'Multiple active' in links['DUP']['reason']
    assert 'No active' in links['UNKNOWN']['reason']
    assert 'No active' in links['INACTIVE']['reason']


def test_existing_links_are_preserved_and_exact_isin_can_back_up_an_absent_alias_target():
    links = resolve([{'isin': 'A', 'company_id': 99}, {'isin': 'B'}],
                    [{'isin': 'A', 'company_id': 1}, {'isin': 'B', 'company_id': 2}], {'B': 'MISSING'})
    assert links['A']['company_id'] == 99
    assert links['B'] == {'company_id': 2, 'method': 'exact_isin', 'matched_isin': 'B'}


def test_reviewed_exchange_ticker_links_work_when_company_isin_is_missing():
    rows = [{'isin': isin} for isin in ['GB0004052071', 'GB0032398678', 'GB00B5ZN1N88']]
    companies = [
        {'company_id': 11, 'isin': None, 'company_name': 'Halma PLC', 'gurufocus_ticker': 'HLMA', 'exchange_code': 'LSE'},
        {'company_id': 12, 'isin': None, 'company_name': 'Judges Scientific', 'gurufocus_ticker': 'JDG', 'exchange_code': 'LSE'},
        {'company_id': 13, 'isin': None, 'company_name': 'SEGRO REIT PLC', 'gurufocus_ticker': 'SGRO', 'exchange_code': 'LSE'},
        {'company_id': 99, 'isin': None, 'company_name': 'Halma PLC', 'gurufocus_ticker': 'HLMA', 'exchange_code': 'OTHER'},
    ]
    links = resolve(rows, companies, {})
    assert [links[row['isin']]['company_id'] for row in rows] == [11, 12, 13]
    assert all(link['method'] == 'reviewed_exchange_ticker' for link in links.values())
