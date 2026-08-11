"""The metric-read cache has to win TWO ways at once, and the obvious fix for one breaks the other.

THE TWO SAVINGS
  * ACROSS the Long Equity tab's thirteen concurrent card requests, 30 metric requests are only 18
    DISTINCT lines — `sbc` is wanted by five cards, `fcf` by four. Those must collapse to one read
    each.
  * WITHIN one request, an endpoint wanting five lines must not open five Postgres connections. On
    the COPY transport every read is its own connect + TLS + auth, which is ~2ms locally and
    150-250ms against Supabase.

⚠⚠ AND BATCHING PER ENDPOINT — the obvious way to get the second — DESTROYS THE FIRST: the five
    cards that want `sbc` would each fetch it, turning 18 shared reads into 30 unshared ones. That
    is SLOWER than what it replaces while looking like an optimisation, and nothing about the
    result would show it. So these tests assert the read COUNT and the BATCH SHAPE, not just the
    values — the values are identical under every one of these designs.

Measured on ACWI (1,949 constituents), driving all eleven card endpoints concurrently:
21 Postgres connections -> 13.
"""
from __future__ import annotations

import threading
import time

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


def _recorder():
    """A `compute_many` that records the BATCHES it was handed, not just the metrics."""
    batches: list[list[str]] = []

    def compute(metrics: list[str]) -> dict[str, object]:
        batches.append(list(metrics))
        return {m: {"data": m} for m in metrics}
    return batches, compute


IDS = [1, 2, 3]


class TestOneReadPerCaller:
    def test_several_metrics_arrive_as_a_single_batch(self):
        batches, compute = _recorder()
        out = bc.cached_metric_reads(IDS, ["revenue", "fcf", "sbc"], "annual", compute)
        assert len(batches) == 1, f"expected ONE read, got {len(batches)}: {batches}"
        assert sorted(batches[0]) == ["fcf", "revenue", "sbc"]
        assert out == {m: {"data": m} for m in ("revenue", "fcf", "sbc")}

    def test_a_repeated_metric_is_asked_for_once(self):
        # A caller listing a line twice must not widen the batch — `_prefetch` unions tuples from
        # several cards and duplicates are normal there.
        batches, compute = _recorder()
        bc.cached_metric_reads(IDS, ["fcf", "fcf", "sbc"], "annual", compute)
        assert batches == [["fcf", "sbc"]]

    def test_nothing_to_do_issues_no_read_at_all(self):
        batches, compute = _recorder()
        bc.cached_metric_reads(IDS, ["fcf"], "annual", compute)
        bc.cached_metric_reads(IDS, ["fcf"], "annual", compute)
        assert len(batches) == 1, "the second call re-read a cached line"


class TestTheBatchIsOnlyWhatIsMissing:
    def test_a_second_caller_reads_only_its_new_lines(self):
        # ⚠ THE REGRESSION THIS FILE EXISTS FOR. Card A loads revenue+fcf+sbc; card B wants
        # sbc+ocf. B must read ONLY ocf — batching "everything B asked for" would re-fetch sbc,
        # which across five cards is how 18 reads become 30.
        batches, compute = _recorder()
        bc.cached_metric_reads(IDS, ["revenue", "fcf", "sbc"], "annual", compute)
        out = bc.cached_metric_reads(IDS, ["sbc", "ocf"], "annual", compute)
        assert batches[1] == ["ocf"], f"re-read a cached line: {batches[1]}"
        # ...and still ANSWERS for both, from cache plus the new read.
        assert set(out) == {"sbc", "ocf"}

    def test_a_different_company_set_is_a_different_line(self):
        # ⚠ THE COMPANY SET IS IN THE KEY. Serving ACWI's revenue to an S&P request would be
        # silent and wrong; two universes are two reads.
        batches, compute = _recorder()
        bc.cached_metric_reads([1, 2], ["revenue"], "annual", compute)
        bc.cached_metric_reads([1, 2, 3], ["revenue"], "annual", compute)
        assert len(batches) == 2

    def test_a_different_cadence_is_a_different_line(self):
        # Annual and trailing-twelve-month are different numbers under the same metric name.
        batches, compute = _recorder()
        bc.cached_metric_reads(IDS, ["revenue"], "annual", compute)
        bc.cached_metric_reads(IDS, ["revenue"], "quarterly", compute)
        assert len(batches) == 2


class TestConcurrentCallersShareOneRead:
    def test_the_shared_line_is_computed_once(self):
        """Two cards want `sbc` at the same instant; exactly one read of it may happen.

        ⚠ THIS IS THE SINGLE-FLIGHT, AND IT IS THE WHOLE REASON THE TAB IS NOT 30 READS. Without
        it both callers miss at the same moment and both compute — a plain cache saves nothing on
        the one load that hurts, because the thirteen requests are concurrent, not sequential.
        """
        started = threading.Event()
        release = threading.Event()
        batches: list[list[str]] = []
        lock = threading.Lock()

        def compute(metrics: list[str]) -> dict[str, object]:
            with lock:
                batches.append(list(metrics))
            if "sbc" in metrics:
                started.set()
                release.wait(timeout=5)      # hold the read open so the second caller must wait
            return {m: {"data": m} for m in metrics}

        results: dict[str, object] = {}

        def first():
            results["a"] = bc.cached_metric_reads(IDS, ["sbc"], "annual", compute)

        def second():
            started.wait(timeout=5)          # arrive while the first read is in flight
            results["b"] = bc.cached_metric_reads(IDS, ["sbc"], "annual", compute)

        ta, tb = threading.Thread(target=first), threading.Thread(target=second)
        ta.start()
        tb.start()
        started.wait(timeout=5)
        time.sleep(0.05)                     # let the waiter reach its wait()
        release.set()
        ta.join(timeout=5)
        tb.join(timeout=5)

        assert batches == [["sbc"]], f"the shared line was read {len(batches)} times: {batches}"
        assert results["a"] == results["b"] == {"sbc": {"data": "sbc"}}


class TestAFailedReadDoesNotStallTheTab:
    def test_the_in_flight_markers_are_released_when_compute_raises(self):
        """⚠ THE `finally` IS LOAD-BEARING, AND THIS CALL OWNS SEVERAL KEYS AT ONCE.

        If a raising `compute_many` left its in-flight events unset, every waiter on ANY of those
        metrics would block for the full `_INFLIGHT_TIMEOUT` (60s) — one failed read freezing the
        whole tab for a minute. The single-metric version it replaced only ever held one key, so
        this is a new way to get it wrong.
        """
        def boom(metrics: list[str]) -> dict[str, object]:
            raise RuntimeError("read failed")

        with pytest.raises(RuntimeError):
            bc.cached_metric_reads(IDS, ["revenue", "fcf", "sbc"], "annual", boom)
        assert not bc._metrics_inflight, (
            f"in-flight markers survived the failure: {list(bc._metrics_inflight)}")

        # And the next caller is free to try again rather than inheriting the failure.
        batches, compute = _recorder()
        out = bc.cached_metric_reads(IDS, ["revenue"], "annual", compute)
        assert out == {"revenue": {"data": "revenue"}}
        assert batches == [["revenue"]]

    def test_a_metric_the_read_omits_is_simply_absent(self):
        # ⚠ `compute_many` MUST key every metric it is given — but if it does not, the answer is
        # "we have nothing for it", never a wrong value and never a crash. `_prefetch` then leaves
        # it out of the request cache and the per-company path resolves it.
        def partial(metrics: list[str]) -> dict[str, object]:
            return {m: {"data": m} for m in metrics if m != "sbc"}

        out = bc.cached_metric_reads(IDS, ["revenue", "sbc"], "annual", partial)
        assert set(out) == {"revenue"}


class TestInvalidateDropsTheseToo:
    def test_an_ingest_clears_the_metric_rows_it_just_rewrote(self):
        # ⚠ THE ONE OUTCOME WORSE THAN NOT CACHING: rebuilding "fresh" responses on top of stale
        # fundamentals, because it looks like it worked.
        batches, compute = _recorder()
        bc.cached_metric_reads(IDS, ["revenue"], "annual", compute)
        bc.invalidate()
        bc.cached_metric_reads(IDS, ["revenue"], "annual", compute)
        assert len(batches) == 2, "the ingest did not drop the cached metric rows"
