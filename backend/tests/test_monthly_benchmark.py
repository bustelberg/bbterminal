from unittest.mock import Mock

import pytest

from routers import _monthly_benchmark as B, _airs_monthly_report as R, _benchmark_etf as E
import momentum.data as data


@pytest.fixture
def sources(monkeypatch):
    monkeypatch.setattr(R, '_account', lambda _: None)
    monkeypatch.setattr(R, 'month_window', lambda _: ('2026-11-01', '2026-11-02', '2026-10-31'))
    monkeypatch.setattr(R, '_monthly_rows', lambda *a: [{'month': '2026-11', 'periode': '2026-11-02'}])
    etf, fx = Mock(), Mock()
    monkeypatch.setattr(E, 'ensure_fresh', etf)
    monkeypatch.setattr(data, 'sync_fx_rates_to_db', fx)
    return etf, fx


def test_already_available_benchmark_never_fetches_vendors(sources, monkeypatch):
    monkeypatch.setattr(R, '_weighted_months', lambda *a: {'2026-11': {'return_pct': -.35}})
    assert B.ensure('BUS_BepOffensief_Dyn', '2026-11')['repaired'] is False
    for source in sources:
        source.assert_not_called()


def test_missing_benchmark_requests_etfs_and_fx_then_verifies_result(sources, monkeypatch):
    weighted = Mock(side_effect=[{}, {'2026-11': {'return_pct': -.35}}])
    monkeypatch.setattr(R, '_weighted_months', weighted)
    assert B.ensure('BUS_BepOffensief_Dyn', '2026-11')['benchmark']['return_pct'] == -.35
    assert [call.args[0] for call in sources[0].call_args_list] == ['ACWI', 'BONDS']
    assert sources[1].call_args.args[1] == ['USD']


def test_provider_failure_cannot_report_success(sources, monkeypatch):
    monkeypatch.setattr(R, '_weighted_months', lambda *a: {})
    with pytest.raises(RuntimeError, match='still incomplete'):
        B.ensure('BUS_Neutraal_Dyn', '2026-11')


def test_products_without_a_reviewed_policy_do_not_trigger_vendor_requests(sources):
    assert B.ensure('UNKNOWN', '2026-11') == {'applicable': False}
    for source in sources:
        source.assert_not_called()


def test_daily_recovery_repairs_shared_sources_once_and_checks_all_accounts(monkeypatch):
    calls, ready = [], [False]
    def ensure(account, month, ctx=None, *, repair=True):
        calls.append((account, repair))
        if repair:
            ready[0] = True
        if not ready[0]:
            raise B.BenchmarkMissing('missing')
        return {'benchmark': {'return_pct': 0}}
    monkeypatch.setattr(B, 'ensure', ensure)
    result = B.ensure_daily(['one', 'two', 'three'])
    assert not result['errors'] and len(result['accounts']) == 3
    assert sum(repair for _, repair in calls) == 1


def test_daily_provider_failure_is_reported_for_all_affected_accounts(monkeypatch):
    def ensure(account, month, ctx=None, *, repair=True):
        if repair:
            raise RuntimeError('provider unavailable')
        raise B.BenchmarkMissing('missing')
    monkeypatch.setattr(B, 'ensure', ensure)
    assert set(B.ensure_daily(['one', 'two'])['errors']) == {'one', 'two'}
