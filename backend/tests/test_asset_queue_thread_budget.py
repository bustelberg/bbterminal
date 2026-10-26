"""The durable asset queue must remain usable on a thread-constrained deployment."""
from __future__ import annotations


def test_railway_queue_uses_one_resolver_even_when_env_requests_more(monkeypatch):
    from asset_pipeline import queue

    monkeypatch.setenv("RAILWAY_PUBLIC_DOMAIN", "api.example.test")
    monkeypatch.setenv("YAHOO_CONCURRENCY", "8")

    assert queue._resolver_workers() == 1


def test_local_queue_keeps_configured_parallelism(monkeypatch):
    from asset_pipeline import queue

    monkeypatch.delenv("RAILWAY_PUBLIC_DOMAIN", raising=False)
    monkeypatch.setenv("YAHOO_CONCURRENCY", "3")

    assert queue._resolver_workers() == 3


def test_serial_queue_drains_without_constructing_an_executor(monkeypatch):
    from asset_pipeline import queue
    from tests._fake_supabase import FakeSupabase
    from tests._fake_supabase import _Result

    monkeypatch.setattr(_Result, "count", 0, raising=False)

    monkeypatch.setenv("RAILWAY_PUBLIC_DOMAIN", "api.example.test")
    monkeypatch.setattr(queue, "supabase", FakeSupabase(tables={
        "asset_ingest_queue": [{"isin": "TEST", "status": "pending", "added_at": "2026-10-26"}],
    }))
    monkeypatch.setattr(queue.openfigi, "lookup_isins", lambda _ids: {})
    monkeypatch.setattr(queue.openfigi, "extract_columns", lambda _rows: {})
    monkeypatch.setattr(queue, "resolve", lambda *_args, **_kw: {"asset_class": "bond"})
    monkeypatch.setattr(queue.store, "upsert_unmapped", lambda *_args, **_kw: None)
    monkeypatch.setattr(queue.store, "set_default_executions", lambda: None)
    marked = []
    monkeypatch.setattr(queue, "_mark", lambda *args: marked.append(args))
    monkeypatch.setattr(queue, "_reapply_symbol_overrides", lambda: None)
    monkeypatch.setattr(queue, "_reapply_aliases", lambda: None)

    def no_executor(**_kwargs):
        raise AssertionError("serial queue must not allocate another thread")

    monkeypatch.setattr(queue, "ThreadPoolExecutor", no_executor)
    result = queue.process_slice()
    assert result["processed"] == result["unmapped"] == 1
    assert marked == [("TEST", "done", None)]
