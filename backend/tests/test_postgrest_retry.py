"""A transient transport fault must not 500 a page — and must never replay a write.

Production, 2026-08-11: `httpcore.ReadTimeout` on `GET /api/airs/portfolios/overview`, thrown out
of `_year_perf`. That read is `airs_performance` — **1,815 rows, 608 kB, two pages** — so it was
never a slow query, and optimising it would have fixed nothing. One stall against the 30s PostgREST
timeout, on a client with no retry, took the whole page down.

⚠⚠ THE ASYMMETRY IS THE ONLY THING HERE THAT CAN CAUSE DAMAGE. A read that times out may safely be
repeated. A POST or PATCH that times out MAY ALREADY HAVE BEEN APPLIED — the timeout describes the
missing RESPONSE, not the write — so replaying it risks a duplicate with nothing afterwards able to
tell. That is the same rule the clone script's retry follows, and it is why these tests exist.
"""
from __future__ import annotations

import httpx
import pytest


class _Boom:
    """A transport that fails `fail_times` times, then succeeds."""

    def __init__(self, exc: Exception, fail_times: int = 1) -> None:
        self.exc = exc
        self.left = fail_times
        self.calls: list[str] = []

    def request(self, method, url, **kw):
        self.calls.append(method)
        if self.left > 0:
            self.left -= 1
            raise self.exc
        return type("R", (), {"is_success": True, "content": b"[]"})()


def _session(stub):
    """A `_CachingSession` over a stub transport — built without httpx, since the wrapper only
    ever touches `self._c`."""
    from deps import _CachingSession

    s = _CachingSession.__new__(_CachingSession)
    object.__setattr__(s, "_c", stub)
    return s


TRANSIENT = [
    httpx.ReadTimeout("read timed out"),
    httpx.ConnectTimeout("connect timed out"),
    httpx.ConnectError("connection refused"),
    httpx.RemoteProtocolError("server disconnected"),
]


class TestAReadSurvivesOneStall:

    @pytest.mark.parametrize("exc", TRANSIENT, ids=lambda e: type(e).__name__)
    def test_it_retries_and_succeeds(self, exc):
        stub = _Boom(exc)
        r = _session(stub).request("GET", "/rest/v1/airs_performance")
        assert r.is_success
        assert stub.calls == ["GET", "GET"], "one stall should cost one retry, not zero and not two"

    def test_two_stalls_still_raise(self):
        """⚠ ONE EXTRA ATTEMPT, NOT FIVE. The timeout is 30s, so retries are expensive: two
        attempts cover stall-and-recover while capping the worst case near 60s. A dependency that
        is genuinely down must surface as an error, not as a page that hangs for minutes first."""
        stub = _Boom(httpx.ReadTimeout("x"), fail_times=2)
        with pytest.raises(httpx.ReadTimeout):
            _session(stub).request("GET", "/rest/v1/airs_performance")
        assert stub.calls == ["GET", "GET"]


class TestAWriteIsNeverReplayed:
    """⚠⚠ THE ONE THAT MATTERS. A timed-out write may have landed."""

    @pytest.mark.parametrize("method", ["POST", "PATCH", "DELETE", "PUT"])
    def test_a_timed_out_write_raises_on_the_first_attempt(self, method):
        stub = _Boom(httpx.ReadTimeout("the write may already have been applied"))
        with pytest.raises(httpx.ReadTimeout):
            _session(stub).request(method, "/rest/v1/metric_data")
        assert stub.calls == [method], f"{method} must not be replayed"


class TestARealErrorIsNotRetried:
    """Only TRANSPORT faults are transient. An HTTP error is an answer — retrying it wastes the
    timeout and delays the report."""

    def test_a_non_transport_exception_propagates_immediately(self):
        stub = _Boom(ValueError("malformed filter"))
        with pytest.raises(ValueError):
            _session(stub).request("GET", "/rest/v1/company")
        assert stub.calls == ["GET"]

    def test_an_http_error_response_is_returned_not_retried(self):
        """A 400/500 comes back as a RESPONSE, never as an exception, so it reaches postgrest's own
        error handling untouched — the retry must not sit in front of that."""
        class _Http500:
            def __init__(self):
                self.calls = 0

            def request(self, method, url, **kw):
                self.calls += 1
                return type("R", (), {"is_success": False, "content": b'{"message":"boom"}'})()

        stub = _Http500()
        r = _session(stub).request("GET", "/rest/v1/company")
        assert r.is_success is False
        assert stub.calls == 1
