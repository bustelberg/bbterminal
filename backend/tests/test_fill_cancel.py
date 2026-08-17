"""Cancel on the BULK fundamentals fill — where it lands, and what it does not do afterwards.

⚠⚠ THE DEFECT WAS AN ARGUMENT THAT WAS NEVER PASSED. `ingest_company` has taken a `should_stop`
hook all along and checks it BETWEEN feeds — `benchmarks.py`'s single-company job passes it, which
is exactly why the per-row Refresh cancelled promptly while the bulk one felt broken. Without it a
company already inside the call ran all THREE of its remaining GuruFocus fetches after the press:
each a wait on the global 1.5s gate, an HTTP round trip, a Storage upload and a write, times three
workers.

The tests below drive `fill_company_ids` with a fake `ingest_company`, so they are about the
CONTRACT — where the stop lands, what is banked, what is refused — not about timing.
"""
from __future__ import annotations

import threading

import pytest

import routers._fundamental_fill as fill
from jobs import JobCancelled


class FakeCtx:
    """The job surface `fill_company_ids` uses, with a cancel that can be armed mid-run."""

    def __init__(self, cancel_after: int | None = None):
        self.events: list[tuple[str, str]] = []
        self.spent_calls = 0
        self._cancel = threading.Event()
        self._cancel_after = cancel_after
        self.started = 0

    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

    def check(self) -> None:
        if self._cancel.is_set():
            raise JobCancelled

    def emit(self, kind: str, message: str, **_data) -> None:
        self.events.append((kind, message))

    def progress(self, done, total, message, **data) -> None:
        self.emit("progress", message)

    def spent(self, calls: int) -> None:
        self.spent_calls += calls

    def cancel_now(self) -> None:
        self._cancel.set()

    def note_company(self) -> None:
        """Called by the fake ingest as each company begins."""
        self.started += 1
        if self._cancel_after is not None and self.started >= self._cancel_after:
            self.cancel_now()


def _company(cid: int) -> dict:
    return {"company_id": cid, "company_name": f"Co {cid}", "gurufocus_ticker": f"T{cid}",
            "gurufocus_exchange": {"exchange_code": "NYSE"}}


@pytest.fixture
def rig(monkeypatch):
    """Wire `fill_company_ids` to fakes: no database, no vendor, no quota."""
    import ingest.api_usage as api_usage
    import routers._blend_cache as blend_cache
    import routers._fundamental_backfill as backfill

    state = {"ingested": [], "stop_hook_seen": False, "blend_invalidated": 0}

    def _company_rows(cids):
        return {c: _company(c) for c in cids}

    def _smart(cids):
        return {c: {"need_fin": True, "need_est": True, "need_ind": True} for c in cids}

    monkeypatch.setattr(backfill, "company_rows", _company_rows)
    monkeypatch.setattr(backfill, "smart_flags_bulk", _smart)
    monkeypatch.setattr(backfill, "eligible", lambda c: None)
    monkeypatch.setattr(api_usage, "remaining_budget",
                        lambda _s: {"usa": 100, "europe": 100, "asia": 100})
    monkeypatch.setattr(blend_cache, "invalidate",
                        lambda: state.__setitem__("blend_invalidated",
                                                  state["blend_invalidated"] + 1))
    # ⚠ ONE WORKER, so "which companies ran" is deterministic. Concurrency is what the real thing
    # does; it is not what these assertions are about.
    monkeypatch.setattr(fill, "FILL_WORKERS", 1)
    return state


def _fake_ingest(state, ctx, *, rows=10, error=None):
    """Stands in for `ingest_company`, honouring `should_stop` between its three feeds exactly as
    the real one does."""
    def _ingest(c, *, refresh_cache=False, on_step=None, should_stop=None):
        ctx.note_company()
        if should_stop is not None:
            state["stop_hook_seen"] = True
        done, calls, loaded = [], 0, 0
        for i, tag in enumerate(("fin", "est", "ind"), 1):
            if should_stop is not None and should_stop():
                return {"done": done, "rows": loaded, "unchanged": 0, "calls": calls,
                        "error": None, "stopped": True}
            if on_step:
                on_step(tag, i, 3)
            calls += 1
            loaded += rows
            done.append(f"{tag} {rows}")
        state["ingested"].append(c["company_id"])
        return {"done": done, "rows": loaded, "unchanged": 0, "calls": calls,
                "error": error, "stopped": False}
    return _ingest


class TestTheStopHookIsPassedAtAll:
    """The regression that started this: the argument existed and was not being used."""

    def test_ingest_company_is_given_should_stop(self, rig, monkeypatch):
        ctx = FakeCtx()
        monkeypatch.setattr("routers._fundamental_backfill.ingest_company",
                            _fake_ingest(rig, ctx))
        fill.fill_company_ids(ctx, "IDX", [1, 2], feeds="smart")
        assert rig["stop_hook_seen"], (
            "ingest_company was called without should_stop — Cancel can then only land BETWEEN "
            "companies, i.e. after up to three more GuruFocus feeds per worker")


class TestWhereTheStopLands:

    def test_the_run_stops_after_the_company_that_was_in_flight(self, rig, monkeypatch):
        # Cancel arms as the 2nd company starts, so exactly one completes and the second halts
        # between feeds.
        ctx = FakeCtx(cancel_after=2)
        monkeypatch.setattr("routers._fundamental_backfill.ingest_company",
                            _fake_ingest(rig, ctx))
        with pytest.raises(JobCancelled):
            fill.fill_company_ids(ctx, "IDX", [1, 2, 3, 4, 5], feeds="smart")
        # ⚠ A COUNT, NOT AN ID — AND THAT IS `order_work`'S DOING, NOT A LOOSENING. The work list is
        # no longer in `company_id` order: it is sorted least-recently-checked-first with a random
        # tie-break, and every company here is equally never-checked, so WHICH one runs first is
        # deliberately unpredictable. Asserting `== [1]` was asserting the bug that ordering fixed.
        assert len(rig["ingested"]) == 1, (
            f"expected exactly one company to finish, got {rig['ingested']}")

    def test_nothing_queued_is_started_after_the_press(self, rig, monkeypatch):
        ctx = FakeCtx(cancel_after=1)
        monkeypatch.setattr("routers._fundamental_backfill.ingest_company",
                            _fake_ingest(rig, ctx))
        with pytest.raises(JobCancelled):
            fill.fill_company_ids(ctx, "IDX", list(range(1, 21)), feeds="smart")
        # ⚠ THE QUEUED FUTURES ARE CANCELLED, NOT RUN-AND-RAISED. They would each have had to be
        # dispatched to a worker just to hit `ctx.check()`; on an index that is ~1,600 hand-offs
        # between the press and the card turning over.
        assert ctx.started == 1, f"{ctx.started} companies were entered after a cancel at 1"


class TestWhatACancelMustNotDo:

    def test_it_does_not_retry_a_stopped_company(self, rig, monkeypatch):
        """⚠ THE RETRY-ONCE FIRES ON AN EMPTY ANSWER, and a company halted between feeds wrote
        nothing — which looks identical. Retrying spends fresh GuruFocus calls on the far side of a
        Cancel: the one moment the reader has explicitly asked us not to."""
        ctx = FakeCtx(cancel_after=1)
        monkeypatch.setattr("routers._fundamental_backfill.ingest_company",
                            _fake_ingest(rig, ctx, rows=0))
        with pytest.raises(JobCancelled):
            fill.fill_company_ids(ctx, "IDX", [1, 2, 3], feeds="smart")
        assert ctx.started == 1, "the stopped company was fetched a second time after the cancel"

    def test_the_quota_already_spent_is_still_reported(self, rig, monkeypatch):
        """⚠ THE SPEND IS BANKED BEFORE THE RAISE. Those calls left the monthly quota whether or not
        the run was cancelled, and a cancelled card reporting zero is the one that gets pressed
        again."""
        ctx = FakeCtx(cancel_after=3)
        monkeypatch.setattr("routers._fundamental_backfill.ingest_company",
                            _fake_ingest(rig, ctx))
        with pytest.raises(JobCancelled):
            fill.fill_company_ids(ctx, "IDX", [1, 2, 3, 4], feeds="smart")
        assert ctx.spent_calls > 0

    def test_the_blend_cache_is_invalidated_on_a_cancel_too(self, rig, monkeypatch):
        """⚠⚠ `JobCancelled` USED TO PROPAGATE STRAIGHT PAST IT. A press that stopped a fill
        part-way left the blend cache holding pre-fill rows for whatever the run HAD written —
        exactly the "I pressed refresh and the row is still empty" failure the invalidation exists
        to prevent, arriving through the one door it was not guarded on."""
        ctx = FakeCtx(cancel_after=2)
        monkeypatch.setattr("routers._fundamental_backfill.ingest_company",
                            _fake_ingest(rig, ctx))
        with pytest.raises(JobCancelled):
            fill.fill_company_ids(ctx, "IDX", [1, 2, 3], feeds="smart")
        assert rig["blend_invalidated"] == 1


class TestTheCancelledCardSaysWhatHappened:
    """⚠ `jobs.py` PROMOTES A `JobCancelled` MESSAGE TO THE JOB'S SUMMARY, precisely so a worker
    that stopped part-way can say what the registry cannot. Raised bare it read "cancelled — stopped
    at a safe point", which is indistinguishable from a Cancel that did nothing at all — and "did it
    even work?" is the whole question a reader has after pressing it."""

    def test_the_summary_names_the_stop_point_and_the_tally(self, rig, monkeypatch):
        ctx = FakeCtx(cancel_after=3)
        monkeypatch.setattr("routers._fundamental_backfill.ingest_company",
                            _fake_ingest(rig, ctx))
        with pytest.raises(JobCancelled) as e:
            fill.fill_company_ids(ctx, "IDX", [1, 2, 3, 4, 5], feeds="smart")
        msg = str(e.value)
        assert msg, "a bare JobCancelled tells the reader nothing"
        assert "CANCELLED after" in msg and "of 5" in msg
        assert "IDX" in msg
        # ⚠ AND IT SAYS THE WORK IS KEPT — a reader who believes a cancel rolled something back
        # presses the expensive button again.
        assert "stored" in msg


class TestAPressDuringTheSetup:

    def test_it_lands_before_any_company_is_fetched(self, rig, monkeypatch):
        """Deciding what to fetch is seconds of database work with nothing on the bar yet. A press
        in that window used to sit unacknowledged until the pool started and the first worker
        reached its own check — on a forced run, three companies' quota later."""
        ctx = FakeCtx()
        ctx.cancel_now()
        monkeypatch.setattr("routers._fundamental_backfill.ingest_company",
                            _fake_ingest(rig, ctx))
        with pytest.raises(JobCancelled):
            fill.fill_company_ids(ctx, "IDX", [1, 2, 3], feeds="smart")
        assert ctx.started == 0
