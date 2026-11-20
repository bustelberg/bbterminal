"""Bundle diagnostics and partial-source results without a server or event loop."""
from fastapi import Response
import pytest


@pytest.mark.parametrize('failed', [None, 'airs', 'leonteq'])
def test_bundle_times_both_sources_and_retains_independent_results(monkeypatch, failed):
    from routers import airs
    from leonteq import management

    async def inline_thread(fn):
        return fn()

    async def gather(*calls, **kwargs):
        result = []
        for call in calls:
            try:
                result.append(await call)
            except Exception as exc:
                result.append(exc)
        return result

    async def load_airs():
        if failed == 'airs':
            raise ValueError('unavailable')
        return [{'name': 'AIRS'}]

    def load_leonteq():
        if failed == 'leonteq':
            raise ValueError('unavailable')
        return [{'name': 'Leonteq'}]

    monkeypatch.setattr(airs.asyncio, 'gather', gather)
    monkeypatch.setattr(airs.asyncio, 'to_thread', inline_thread)
    monkeypatch.setattr(airs, 'airs_portfolios_overview', load_airs)
    monkeypatch.setattr(management, 'dashboard_overview', load_leonteq)
    response = Response()
    call = airs.management_portfolios_bundle(response)
    with pytest.raises(StopIteration) as done:
        call.send(None)
    result = done.value.value
    assert result['airs'] == (None if failed == 'airs' else [{'name': 'AIRS'}])
    assert result['leonteq'] == (None if failed == 'leonteq' else [{'name': 'Leonteq'}])
    assert 'airs;dur=' in response.headers['server-timing']
    assert 'leonteq;dur=' in response.headers['server-timing']
