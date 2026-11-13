from types import SimpleNamespace

import pytest

from services import weekly_returns as W
from scheduler_jobs import weekly_returns as J
from scheduler_runtime.execution import Cancelled
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def db(monkeypatch):
    db = FakeSupabase({})
    monkeypatch.setattr(W, 'supabase', db)
    monkeypatch.setattr('routers._airs_monthly_report._account', lambda account: None)
    return db


def test_publish_is_atomic_and_reads_do_not_rebuild_series(db, monkeypatch):
    payload = {'portefeuille': 'BOOK', 'through': '2026-11-13', 'options': [{'id': 'A'}, {'id': 'B'}],
               'instruments': {'A': {'periods': [1]}, 'B': {'periods': [2]}}}
    monkeypatch.setattr('routers._airs_instrument_returns.bundle', lambda _: payload)
    assert W.read('BOOK')['ready'] is False
    W.publish('BOOK')
    monkeypatch.setattr('routers._airs_instrument_returns.bundle', lambda _: pytest.fail('Read must not calculate'))
    assert W.read('BOOK')['instruments'] == payload['instruments']
    assert len(db.tables[W.TABLE]) == 1


def test_failed_calculation_retains_the_previous_complete_publication(db, monkeypatch):
    db.tables[W.TABLE] = [{'portefeuille': 'BOOK', 'prepared_at': 'earlier', 'payload': {'instruments': {'OLD': {}}}}]
    def fail(_):
        raise RuntimeError('cannot read all observations')
    monkeypatch.setattr('routers._airs_instrument_returns.bundle', fail)
    with pytest.raises(RuntimeError):
        W.publish('BOOK')
    assert W.read('BOOK')['instruments'] == {'OLD': {}}


def test_cancelled_collection_publishes_committed_observations_without_masking_cancel(db, monkeypatch):
    monkeypatch.setattr('routers._airs_instrument_returns.bundle', lambda _: {'instruments': {'PARTIAL': {}}})
    def cancel():
        raise Cancelled('stopped between weeks')
    with pytest.raises(Cancelled):
        W.collect_and_publish('BOOK', cancel)
    assert 'PARTIAL' in W.read('BOOK')['instruments']


@pytest.mark.parametrize('collect', [False, True])
def test_scheduled_publication_covers_all_instruments_and_daily_path_does_not_scrape(monkeypatch, collect):
    monkeypatch.setattr('routers._airs_accounts.list_accounts', lambda: [
        {'portefeuille': 'BUS_Offensief_Dyn', 'holdings': 2}])
    calls = []
    monkeypatch.setattr('routers._airs_instrument_returns.collect_all', lambda *a, **kw: calls.append((a, kw)))
    monkeypatch.setattr(W, 'publish', lambda _: {'instruments': {'A': {}, 'B': {}}, 'through': '2026-11-13', 'prepared_at': 'now'})
    ctx = SimpleNamespace(check=lambda: None)
    _, result = J.run(ctx, collect_history=collect)
    assert result['accounts'][0]['instruments'] == 2
    assert len(calls) == int(collect)
    if collect:
        assert calls[0][1] == {'stop_on_flat': False}
