"""Shared retry-with-backoff primitive (`common/retry.py`).

Pins the contract the four call-site bindings depend on: when it retries, how
long it sleeps for each backoff shape, which exceptions it lets through, and
the default transient-error sniff. `time.sleep` is monkeypatched to a recorder
so the delays are asserted without the test actually waiting.
"""
from __future__ import annotations

import pytest

import common.retry as retry_mod
from common.retry import is_transient_error, retry


@pytest.fixture
def slept(monkeypatch):
    """Capture the delays passed to time.sleep instead of sleeping."""
    delays: list[float] = []
    monkeypatch.setattr(retry_mod.time, "sleep", delays.append)
    return delays


class _Flaky:
    """A callable that raises `exc` for the first `fail_times` calls, then
    returns `value`. Records how many times it was invoked."""

    def __init__(self, fail_times: int, exc: Exception, value: str = "ok"):
        self.fail_times = fail_times
        self.exc = exc
        self.value = value
        self.calls = 0

    def __call__(self) -> str:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise self.exc
        return self.value


class TestRetry:
    def test_returns_immediately_on_success(self, slept):
        fn = _Flaky(0, TimeoutError())
        assert retry(fn) == "ok"
        assert fn.calls == 1
        assert slept == []

    def test_retries_transient_then_succeeds(self, slept):
        fn = _Flaky(2, TimeoutError("timed out"))
        assert retry(fn, attempts=3) == "ok"
        assert fn.calls == 3
        assert len(slept) == 2  # slept before tries 2 and 3, not after 3

    def test_raises_last_exception_after_exhausting_attempts(self, slept):
        boom = TimeoutError("still timing out")
        fn = _Flaky(99, boom)
        with pytest.raises(TimeoutError) as ei:
            retry(fn, attempts=3)
        assert ei.value is boom
        assert fn.calls == 3
        assert len(slept) == 2  # no sleep after the final failed attempt

    def test_non_retryable_propagates_without_retry(self, slept):
        fn = _Flaky(99, ValueError("nope"))  # not transient by default
        with pytest.raises(ValueError):
            retry(fn, attempts=3)
        assert fn.calls == 1
        assert slept == []

    def test_linear_backoff_delays(self, slept):
        fn = _Flaky(99, TimeoutError())
        with pytest.raises(TimeoutError):
            retry(fn, attempts=4, base_delay=5, backoff="linear")
        assert slept == [5, 10, 15]  # base*1, base*2, base*3

    def test_exponential_backoff_delays(self, slept):
        fn = _Flaky(99, TimeoutError())
        with pytest.raises(TimeoutError):
            retry(fn, attempts=4, base_delay=2, backoff="exponential")
        assert slept == [2, 4, 8]  # base*2^0, base*2^1, base*2^2

    def test_custom_should_retry_predicate(self, slept):
        fn = _Flaky(1, ValueError("retry me"), value="done")
        out = retry(fn, attempts=3, should_retry=lambda e: isinstance(e, ValueError))
        assert out == "done"
        assert fn.calls == 2
        assert len(slept) == 1

    def test_invalid_attempts_rejected(self, slept):
        with pytest.raises(ValueError):
            retry(lambda: "x", attempts=0)

    def test_invalid_backoff_rejected(self, slept):
        with pytest.raises(ValueError):
            retry(lambda: "x", backoff="quadratic")


class TestIsTransientError:
    @pytest.mark.parametrize("exc", [
        TimeoutError("connection timed out"),
        RuntimeError("502 Bad Gateway"),
        RuntimeError("got a 503 from upstream"),
        RuntimeError("HTTP 504 gateway timeout"),
        RuntimeError("connection reset by peer"),
        RuntimeError("connection aborted"),
    ])
    def test_transient_cases(self, exc):
        assert is_transient_error(exc) is True

    @pytest.mark.parametrize("exc", [
        ValueError("bad input"),
        RuntimeError("404 not found"),
        RuntimeError("401 unauthorized"),
        KeyError("missing"),
    ])
    def test_non_transient_cases(self, exc):
        assert is_transient_error(exc) is False
