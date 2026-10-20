from common.quote_range import recent_close_range


def test_last_five_distinct_valid_sessions_exclude_report_day():
    rows = [{'date': f'2026-10-{day:02}', 'close': day} for day in [9, 12, 13, 14, 15, 16, 19, 20]]
    rows += [{'date': '2026-10-19', 'close': 19}, {'date': '2026-10-18', 'close': float('nan')}]
    assert recent_close_range(rows, '2026-10-20') == {
        'count': 5, 'start': '2026-10-13', 'end': '2026-10-19', 'low': 13, 'high': 19}


def test_partial_or_missing_history_is_not_fabricated():
    assert recent_close_range([], '2026-10-20')['count'] == 0
    assert recent_close_range([{'date': '2026-10-19', 'close': 10}], '2026-10-20')['count'] == 1
