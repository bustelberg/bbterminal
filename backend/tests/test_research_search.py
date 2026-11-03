from types import SimpleNamespace

from routers import asset_pipeline as api
import deps


def test_search_uses_small_company_rpc_and_reports_company_limit(monkeypatch):
    calls = []
    class DB:
        def rpc(self, name, params):
            calls.append((name, params))
            return SimpleNamespace(execute=lambda: SimpleNamespace(data=[
                {'isin': 'US92826C8394', 'analysis_id': 315, 'name': 'Visa Inc.'},
                {'isin': 'US40049J2069', 'analysis_id': 5567, 'name': 'Grupo Televisa'},
            ]))
    monkeypatch.setattr(deps, 'supabase', DB())
    async def inline(fn):
        return fn()
    monkeypatch.setattr(api.asyncio, 'to_thread', inline)
    request = api.search_assets(q=' Visa ', limit=1)
    try:
        request.send(None)
    except StopIteration as done:
        result = done.value
    assert calls == [('search_research_companies', {'search_term': 'Visa', 'result_limit': 1})]
    assert result == {'rows': [{'isin': 'US92826C8394', 'analysis_id': 315, 'name': 'Visa Inc.'}], 'truncated': True}
