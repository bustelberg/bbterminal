import pytest

from routers import _airs_accounts as accounts, _airs_overview as overview
from tests._fake_supabase import FakeSupabase


@pytest.mark.parametrize('periods,expected', [
    ([('2026-01-31', 100), ('2026-10-30', 110)], '2026-01-01'),
    ([('2026-01-31', 0), ('2026-05-31', 0), ('2026-06-30', 100)], '2026-06-01'),
    ([('2025-01-31', 100), ('2026-01-31', 0), ('2026-07-31', 100)], '2026-07-01'),
    ([('2026-01-15', 100), ('2026-01-31', 0), ('2026-06-30', 100)], '2026-01-01'),
    ([('2026-01-31', 0), ('2026-06-30', 0)], None),
])
def test_return_start_uses_funded_observations_in_report_year(monkeypatch, periods, expected):
    monkeypatch.setattr(accounts, 'supabase', FakeSupabase({'airs_performance': [
        dict(portefeuille='BOOK', periode=day, beginvermogen=0, eindvermogen=value,
             cumulatief_rendement=2) for day, value in periods]}))
    assert accounts._year_perf()['BOOK']['ytd_from'] == expected


def test_rpc_return_start_reaches_overview_without_extra_performance_read(monkeypatch):
    payload = {'accounts': [{'portefeuille': 'BUS_Defensief_Dyn', 'ytd_from': '2026-06-01'}], 'models': []}
    monkeypatch.setattr(overview, '_overview_payload', lambda: payload)
    monkeypatch.setattr(accounts, '_year_perf', lambda: pytest.fail('RPC already supplied the window'))
    assert overview.list_overview()[0]['ytd_from'] == '2026-06-01'


def test_old_rpc_uses_one_batched_read_during_schema_rollout(monkeypatch):
    payload = {'accounts': [{'portefeuille': 'BUS_Defensief_Dyn', 'ytd_pct': 2}], 'models': []}
    monkeypatch.setattr(overview, '_overview_payload', lambda: payload)
    calls = []
    def performance():
        calls.append(True)
        return {'BUS_Defensief_Dyn': {'ytd_from': '2026-01-01'}}
    monkeypatch.setattr(accounts, '_year_perf', performance)
    assert overview.list_overview()[0]['ytd_from'] == '2026-01-01'
    assert len(calls) == 1
    assert 'ytd_from' not in payload['accounts'][0]
