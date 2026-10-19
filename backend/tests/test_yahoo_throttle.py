"""Provider outages release workers; only one recovery caller spends HTTP requests."""
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from asset_pipeline import yahoo as Y


@pytest.fixture
def throttle(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(Y.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(Y.time, "sleep", lambda _: pytest.fail("cooldown must not sleep"))
    t = Y._Throttle()
    t.delay = 0
    t.min_delay = 0
    return t, clock


def test_confirmed_ban_opens_shared_circuit_without_sleeping(throttle, monkeypatch):
    t, _ = throttle
    calls = []
    monkeypatch.setattr(Y, "_raw_get", lambda url: calls.append(url) or (429, ""))
    with pytest.raises(Y.YahooThrottled):
        t.get("ticker")
    assert len(calls) == 3  # request, quick retry, canary
    for _ in range(20):
        with pytest.raises(Y.YahooThrottled):
            t.get("another ticker")
    assert len(calls) == 3


def test_failed_recovery_backs_off_and_success_resets(throttle, monkeypatch):
    t, clock = throttle
    monkeypatch.setattr(t, "_canary_throttled", lambda: True)
    with pytest.raises(Y.YahooThrottled):
        t.run(lambda: (429, ""))
    assert t._open_until == clock[0] + t.cooldown0
    clock[0] = t._open_until
    with pytest.raises(Y.YahooThrottled):
        t.run(lambda: pytest.fail("must probe before original request"))
    assert t._open_until == clock[0] + min(t.max_cooldown, 2 * t.cooldown0)
    clock[0] = t._open_until
    monkeypatch.setattr(t, "_canary_throttled", lambda: False)
    assert t.run(lambda: (200, "recovered")) == (200, "recovered")
    assert t._open_until == 0
    assert t._cooldown == t.cooldown0


def test_peer_fails_fast_while_single_recovery_probe_runs(throttle, monkeypatch):
    t, clock = throttle
    t._open_until = clock[0]
    probing, release = threading.Event(), threading.Event()

    def canary():
        probing.set()
        assert release.wait(2)
        return False

    monkeypatch.setattr(t, "_canary_throttled", canary)
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(t.run, lambda: (200, "ok"))
        try:
            assert probing.wait(2)
            with pytest.raises(Y.YahooThrottled):
                t.run(lambda: pytest.fail("peer must not send a request"))
        finally:
            release.set()
        assert first.result(timeout=2) == (200, "ok")


@pytest.mark.parametrize("status", [200, 404, 401])
def test_definitive_answers_do_not_trip_the_circuit(throttle, status):
    t, _ = throttle
    assert t.run(lambda: (status, "")) == (status, "")
    assert t._open_until == 0


def test_ticker_failure_with_healthy_canary_is_not_a_global_ban(throttle, monkeypatch):
    t, _ = throttle
    monkeypatch.setattr(t, "_canary_throttled", lambda: False)
    assert t.run(lambda: (None, "")) == (None, "")
    assert t.run(lambda: (200, "next ticker")) == (200, "next ticker")


def test_unexpected_probe_exception_does_not_poison_recovery(throttle, monkeypatch):
    t, clock = throttle
    t._open_until = clock[0]

    def crash():
        raise RuntimeError("transport crashed")

    monkeypatch.setattr(t, "_canary_throttled", crash)
    with pytest.raises(RuntimeError, match="transport crashed"):
        t.run(lambda: (200, ""))
    assert not t._recovering
    assert t._open_until > clock[0]


def test_quote_retries_expired_auth_only_once(throttle, monkeypatch):
    t, _ = throttle
    calls = []
    session = SimpleNamespace(get=lambda *_a, **_kw:
        calls.append("quote") or SimpleNamespace(status_code=401, text="unauthorized"))
    monkeypatch.setattr(Y, "_quote_session", lambda: (session, "crumb"))
    monkeypatch.setattr(Y, "_track_request", lambda *_a: None)
    monkeypatch.setattr(Y, "_throttle", t)
    assert Y.quote(["AAPL"]) == {}
    assert calls == ["quote", "quote"]


def test_quote_does_not_prime_a_session_during_provider_cooldown(throttle, monkeypatch):
    t, clock = throttle
    t._open_until = clock[0] + 90
    monkeypatch.setattr(Y, "_throttle", t)
    monkeypatch.setattr(Y, "_quote_session", lambda: pytest.fail("open circuit must prevent all HTTP"))
    with pytest.raises(Y.YahooThrottled):
        Y.quote(["AAPL"])


def test_backfill_stops_on_provider_ban_without_sleeping_per_instrument(monkeypatch):
    from asset_pipeline import price_refresh as P, store

    rows = [{"analysis_id": i, "yahoo_symbol": str(i), "isin": str(i)} for i in range(20)]
    monkeypatch.setattr(P, "_executions", lambda _isins: rows)
    monkeypatch.setattr(P, "latest_close_by_analysis", lambda _ids: {})
    calls = []

    def banned(*_args):
        calls.append(1)
        raise Y.YahooThrottled("circuit open")

    monkeypatch.setattr(store, "store_series", banned)
    monkeypatch.setattr(P.time, "sleep", lambda _: pytest.fail("must release worker immediately"))
    assert P.backfill_missing(set()) == {
        "missing": 20, "backfilled": 0, "failed": 1, "skipped": 19,
    }
    assert calls == [1]


def test_stale_refresh_accounts_for_deferred_rows_after_ban(monkeypatch):
    from asset_pipeline import price_refresh as P, store

    rows = [{"analysis_id": i, "yahoo_symbol": str(i), "last_close": "2026-10-01"} for i in range(20)]
    monkeypatch.setattr(P, "find_stale", lambda *_a, **_kw: (rows, "2026-10-16", 20))

    def banned(*_args):
        raise Y.YahooThrottled("circuit open")

    monkeypatch.setattr(store, "extend_series", banned)
    result = P.refresh_stale(limit=10)
    assert result["failed"] == 1
    assert result["skipped"] == 19  # 10 capped + 9 deferred
    assert result["moved"] == result["unchanged"] == 0
