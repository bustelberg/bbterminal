"""Holdings refresh routes each portfolio only to its authoritative source."""
import pytest
from jobs import JobCancelled
from services import portfolio_membership as M, portfolio_valuation as V, airs_refresh
from leonteq import management, portfolio_cache, composition_cache


class Context:
    cancelled = False
    def __init__(self): self.events = []
    def check(self):
        if self.cancelled:
            raise JobCancelled()
    def progress(self, done, total, message): self.events.append((done, total, message))


@pytest.fixture
def feeds(monkeypatch):
    calls = []
    def roster():
        calls.append('roster')
        return [{'id': 1, 'account': 'BUS_Offensief_Dyn', 'name': 'Bustelberg Offensief'},
                {'id': 99, 'account': 'BUS_FTS_OFF_DYN', 'name': 'FamilieTopSelectie'}]
    def collect(**kwargs):
        calls.append(('airs', kwargs['portfolio_ids']))
        assert kwargs['force'] is True
        kwargs['on_event']('count', id=1, i=1, holdings=5)
        return {'selected': [{'id': 1, 'holdings': 5}], 'errors': []}
    def catalogue(*, refresh):
        assert refresh is True
        calls.append('catalogue')
        return {'success': True, 'snapshot_saved': True, 'portfolios': [{'name': 'Family Index'}]}
    def composition(name, *, refresh):
        calls.append(('leonteq', name))
        assert refresh is True
        return {'success': True, 'snapshot_saved': True, 'as_of': '2026-11-13'}
    monkeypatch.setattr(V, 'portfolios', roster)
    monkeypatch.setattr(airs_refresh, 'collect_model_compositions', collect)
    monkeypatch.setattr(management, 'entries', lambda: [{'display_name': 'FamilieTopSelectie', 'leonteq_name': 'Family Index'}])
    monkeypatch.setattr(portfolio_cache, 'portfolios', catalogue)
    monkeypatch.setattr(composition_cache, 'composition', composition)
    return calls


@pytest.mark.parametrize('source', ['all', 'airs', 'leonteq'])
def test_refresh_routes_only_to_requested_authoritative_sources(feeds, source):
    ctx = Context()
    result = M.refresh_holdings(ctx, source=source)
    assert (('airs', {1}) in feeds) == (source != 'leonteq')
    assert (('leonteq', 'Family Index') in feeds) == (source != 'airs')
    assert '99' not in repr(feeds)  # Never refresh the AIRS Topselectie model.
    assert ('roster' in feeds) == (source != 'leonteq')
    assert result.startswith('2' if source == 'all' else '1')


def test_an_airs_outage_does_not_turn_into_a_topselecties_fallback(feeds, monkeypatch):
    monkeypatch.setattr(airs_refresh, 'collect_model_compositions', lambda **_: (_ for _ in ()).throw(RuntimeError('private diagnostic')))
    with pytest.raises(RuntimeError, match='Holdings refresh incomplete') as exc:
        M.refresh_holdings(Context(), source='all')
    assert 'private diagnostic' not in str(exc.value)
    assert ('leonteq', 'Family Index') in feeds


def test_a_failed_leonteq_refresh_cannot_claim_a_cached_snapshot_as_fresh(feeds, monkeypatch):
    monkeypatch.setattr(composition_cache, 'composition', lambda *_args, **_kwargs: {
        'success': True, 'cached': True, 'refresh_error': 'source_unavailable', 'as_of': '2020-01-01'})
    with pytest.raises(RuntimeError, match='Leonteq holdings could not be refreshed'):
        M.refresh_holdings(Context(), source='leonteq')
    assert not any(isinstance(call, tuple) and call[0] == 'airs' for call in feeds)


def test_cancel_stops_before_source_work(feeds):
    ctx = Context()
    ctx.cancelled = True
    with pytest.raises(JobCancelled):
        M.refresh_holdings(ctx, source='leonteq')
    assert feeds == []
