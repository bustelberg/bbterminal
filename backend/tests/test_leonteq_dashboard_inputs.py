from types import SimpleNamespace
from unittest.mock import Mock


def test_dashboard_fallback_reads_compact_accounting_inputs(monkeypatch):
    from leonteq import management as m, accounting
    from tests._fake_supabase import FakeSupabase

    db = FakeSupabase({})
    rpc = Mock(return_value=SimpleNamespace(execute=lambda: SimpleNamespace(data=[{
        'portfolio_name': 'Source', 'payload': {'version': accounting.VERSION}}])))
    monkeypatch.setattr(db, 'rpc', rpc, raising=False)
    monkeypatch.setattr(m, 'supabase', db)
    monkeypatch.setattr(m.portfolio_cache, '_account_key', lambda: 'account')
    monkeypatch.setattr(m.portfolio_cache, 'saved_portfolios', lambda: {'portfolios': []})
    monkeypatch.setattr(m, 'resolve', lambda *_: {'name': 'Source'})
    monkeypatch.setattr(m, '_snapshot_detail', lambda *args: {'ledger': args[-1]})
    table = db.table
    def read(name):
        assert name != accounting.TABLE, 'must not read full accounting payloads'
        return table(name)
    monkeypatch.setattr(db, 'table', read)
    result = m._dashboard_snapshots([{'display_name': 'Example'}])
    assert result['Example']['ledger']['version'] == accounting.VERSION
    rpc.assert_called_once_with('leonteq_dashboard_accounting_inputs',
                                {'p_account_key': 'account', 'p_names': ['Source']})
