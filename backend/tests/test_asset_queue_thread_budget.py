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
