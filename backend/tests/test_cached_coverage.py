"""THE COVERAGE VERDICTS ARE COMPUTED ONCE PER MEMBER LIST, AND THE COST IS WHY.

`coverage_for` is ~137 round trips on ACWI — a chunked `asset_grid` + `company` read per 100
ISINs, then the sentinel `metric_data` probe in chunks of 20 — and `portfolio-revenue-matrix` now
needs its answer to stamp `in_line` on every row. `TablesTab` fires that endpoint once per rate row
(five for the book, five for the benchmark), so uncached this is the classifier run TEN TIMES per
page load for an answer that is identical every time.

⚠ THE KEY IS THE MEMBER LIST ITSELF, not a universe label: an ad-hoc `holdings` basket has no
label, and two callers passing the same ISINs must collide.

⚠ AND `invalidate()` HAS TO CLEAR IT — a company classed `no_metrics` becomes `covered` the moment
its financials land, which is exactly what the ingest that calls `invalidate()` just did. Stale
here means the drill-down keeps a row out of the line the chart has already put back in.
"""
from __future__ import annotations

import threading

import pytest

from routers import _blend_cache as bc


@pytest.fixture(autouse=True)
def _clean():
    """Each test starts with empty caches — they are module-level and shared."""
    bc.invalidate()
    bc._metrics_inflight.clear()
    yield
    bc.invalidate()
    bc._metrics_inflight.clear()


ISINS = ["US0378331005", "NL0010273215", "DE0007164600"]


def _counter(result=None):
    calls: list[int] = []

    def compute():
        calls.append(1)
        return result if result is not None else {"US0378331005"}
    return calls, compute


class TestItComputesOncePerMemberList:
    def test_a_second_call_with_the_same_isins_does_not_recompute(self):
        calls, compute = _counter()
        first = bc.cached_coverage(ISINS, compute)
        second = bc.cached_coverage(ISINS, compute)
        assert first == second == {"US0378331005"}
        assert len(calls) == 1

    def test_order_does_not_matter_because_a_member_list_is_a_SET(self):
        """⚠ THE FIVE MATRIX REQUESTS ARE BUILT FROM THE SAME MEMBERS AND NEED NOT ORDER THEM THE
        SAME WAY. Keyed on the sequence, four of the five would miss and the cache would look like
        it was working while paying for itself four times over."""
        calls, compute = _counter()
        bc.cached_coverage(ISINS, compute)
        bc.cached_coverage(list(reversed(ISINS)), compute)
        bc.cached_coverage([*ISINS, ISINS[0]], compute)      # a duplicate is not a new list
        assert len(calls) == 1

    def test_a_different_member_list_is_a_different_answer(self):
        calls, compute = _counter()
        bc.cached_coverage(ISINS, compute)
        bc.cached_coverage(ISINS[:2], compute)
        assert len(calls) == 2


class TestInvalidateClearsIt:
    def test_an_ingest_drops_the_verdicts_with_everything_else(self):
        calls, compute = _counter()
        bc.cached_coverage(ISINS, compute)
        bc.invalidate()
        bc.cached_coverage(ISINS, compute)
        assert len(calls) == 2

    def test_invalidate_counts_it_so_the_log_line_is_true(self):
        _calls, compute = _counter()
        bc.cached_coverage(ISINS, compute)
        assert bc.invalidate() >= 1


class TestSingleFlight:
    def test_concurrent_callers_share_one_computation(self):
        """The five matrix requests for one target leave the browser together, so against a cold
        cache they would otherwise all miss and all pay."""
        started = threading.Event()
        release = threading.Event()
        calls: list[int] = []

        def compute():
            calls.append(1)
            started.set()
            release.wait(timeout=5)
            return {"US0378331005"}

        out: list[object] = []
        threads = [threading.Thread(target=lambda: out.append(bc.cached_coverage(ISINS, compute)))
                   for _ in range(4)]
        threads[0].start()
        started.wait(timeout=5)                 # the owner is inside `compute`
        for t in threads[1:]:
            t.start()
        release.set()
        for t in threads:
            t.join(timeout=10)

        assert len(calls) == 1
        assert out == [{"US0378331005"}] * 4
