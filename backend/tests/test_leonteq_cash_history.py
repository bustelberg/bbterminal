import pytest

from leonteq import cash_history as c
from leonteq.portal_api import ApiUnavailable


def test_collects_cash_details_and_fees_with_daily_total_verification():
    calls = []
    def get(path):
        calls.append(path)
        if path.endswith('/certificates'):
            return [{'isin': 'CERT', 'currency': 'EUR'}]
        if '/secondary-market-transactions?' in path:
            return {'totalRows': 1, 'rows': [{'date': '2026-06-10', 'currency': 'EUR',
                'totalNetFlows': {'value': 1002.9}}]}
        if '/secondary-market-transaction-details?' in path:
            return [{'dateTime': '2026-06-10T15:05:33Z', 'netFlows': {'value': 1002.9}}]
        return {'totalRows': 0, 'rows': []}
    result = c.collect(get, 'amc/INDEX')
    assert result['complete'] is True
    evidence = result['certificates'][0]
    assert evidence['cash_movements'][0]['dateTime'] == '2026-06-10T15:05:33Z'
    assert len(calls) == 4


def test_pagination_advances_by_actual_returned_rows():
    calls = []
    def get(path):
        calls.append(path)
        return {'totalRows': 3, 'rows': [{'date': f'2026-06-{10 + len(calls)}'}]}
    assert len(c._pages(get, 'history', '-date')) == 3
    assert 'offset=1&' in calls[1]
    assert 'offset=2&' in calls[2]


@pytest.mark.parametrize('second', [
    {'totalRows': 2, 'rows': []},
    {'totalRows': 3, 'rows': [{'date': '2026-06-11'}]},
    {'totalRows': 2, 'rows': [{'date': '2026-06-10'}]},
])
def test_truncated_changing_or_overlapping_history_is_rejected(second):
    pages = iter([{'totalRows': 2, 'rows': [{'date': '2026-06-10'}]}, second])
    with pytest.raises(ApiUnavailable):
        c._pages(lambda _: next(pages), 'history', '-date')


def test_cash_details_must_match_summary():
    def get(path):
        if path.endswith('/certificates'):
            return [{'isin': 'CERT', 'currency': 'EUR'}]
        if '/secondary-market-transactions?' in path:
            return {'totalRows': 1, 'rows': [{'date': '2026-06-10', 'currency': 'EUR', 'totalNetFlows': 100}]}
        return [{'dateTime': '2026-06-10T15:05:33Z', 'netFlows': 90}]
    with pytest.raises(ApiUnavailable, match='daily total'):
        c.collect(get, 'amc/INDEX')


def test_unchanged_old_cash_details_are_reused_but_latest_date_is_refetched():
    calls = []
    summaries = [{'date': day, 'currency': 'EUR', 'totalNetFlows': 100}
                 for day in ['2026-06-10', '2026-06-11']]
    previous = {'certificates': [{'isin': 'CERT', 'cash_summaries': summaries,
        'cash_movements': [{'dateTime': day + 'T15:00:00Z', 'netFlows': 100}
                           for day in ['2026-06-10', '2026-06-11']]}]}
    def get(path):
        calls.append(path)
        if path.endswith('/certificates'):
            return [{'isin': 'CERT', 'currency': 'EUR'}]
        if '/secondary-market-transactions?' in path:
            return {'totalRows': 2, 'rows': summaries}
        if '/secondary-market-transaction-details?' in path:
            assert path.endswith('date=2026-06-11')
            return [{'dateTime': '2026-06-11T15:00:00Z', 'netFlows': 100}]
        return {'totalRows': 0, 'rows': []}
    result = c.collect(get, 'amc/INDEX', previous)
    assert len(result['certificates'][0]['cash_movements']) == 2
    assert len(calls) == 4
