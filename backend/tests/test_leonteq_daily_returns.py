from unittest.mock import Mock

from leonteq import management as m
from scheduler_runtime.context import LogContext


def test_return_refresh_updates_monthly_and_daily_history_and_keeps_partial_success(monkeypatch):
    entries = [{'display_name': name, 'leonteq_name': name} for name in ['One', 'Two']]
    monkeypatch.setattr(m, 'entries', lambda: entries)
    monkeypatch.setattr(m.portfolio_cache, 'portfolios', lambda **_: {'success': True,
        'retrieved_at': 'now', 'portfolios': [{'name': 'One'}, {'name': 'Two'}]})
    monthly = Mock(return_value={'success': True, 'months': [{'month': '2026-11', 'return_pct': 1}]})
    monkeypatch.setattr(m, 'performance', monthly)
    daily = Mock(side_effect=[({'to': '2026-11-16'}, None), (None, 'Source history unavailable')])
    monkeypatch.setattr(m.return_windows, 'ensure', daily)
    result = m.refresh_returns(LogContext())
    assert result == {'saved': ['One'], 'errors': {'Two': 'Source history unavailable'}}
    assert monthly.call_count == daily.call_count == 2
    assert daily.call_args_list[0].args[0]['performance']['return_window'] is None


def test_daily_job_refreshes_returns_before_portfolio_preparation(monkeypatch):
    from scheduler_jobs import valuation
    from routers import _benchmark_etf
    from services import valuation_dashboard
    calls = []
    def refresh_benchmark(label, *, force, strict):
        assert (label, force, strict) == ('ACWI', True, True)
        calls.append('benchmark')
        return (1, ('2026-11-16', 100))

    monkeypatch.setattr(_benchmark_etf, 'ensure_fresh', refresh_benchmark)
    monkeypatch.setattr(m, 'refresh_returns', lambda _, **kwargs: calls.append('returns') or {'saved': ['One'], 'errors': {}})
    monkeypatch.setattr(valuation_dashboard, 'prepare_all', lambda _: calls.append('valuations') or {'saved': [1], 'errors': {}})
    monkeypatch.setattr(m, 'prepare_dashboard_overview', lambda: calls.append('dashboard') or True)
    message, result = valuation.run()
    assert calls == ['benchmark', 'returns', 'valuations', 'dashboard']
    assert result['leonteq_returns']['saved'] == ['One']
    assert '1 portfolios' in message


def test_all_topselecties_get_contribution_preparation_and_one_failure_does_not_stop_others(monkeypatch):
    from leonteq import accounting
    monkeypatch.setattr(m, 'entries', lambda: [{'display_name': n, 'leonteq_name': n} for n in ['One', 'Two']])
    monkeypatch.setattr(m.portfolio_cache, 'saved_portfolios', lambda: {'portfolios': [
        {'name': 'One', 'as_of': '2026-11-16'}, {'name': 'Two', 'as_of': '2026-11-16'}]})
    refresh = Mock(side_effect=[{'success': False, 'reason': 'Missing opening'},
        {'success': True, 'result': {'available': True, 'estimated': True}}])
    monkeypatch.setattr(accounting, 'refresh', refresh)
    result = m.prepare_contributions(LogContext())
    assert result == {'saved': ['Two'], 'estimated': ['Two'], 'errors': {'One': 'Missing opening'}}
    assert refresh.call_count == 2
