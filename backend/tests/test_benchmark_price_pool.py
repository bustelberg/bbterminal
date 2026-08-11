"""The benchmark price step runs on a pool now, and three things it used to get for free from a
`for` loop are no longer free.

It was serial, with a hardcoded `time.sleep(0.4)` on top of the shared Yahoo governor that already
paces every request (`asset_pipeline.yahoo`: token bucket on starts, semaphore on in-flight, canary
probe and cooldown on a ban). So it held ONE of the four permitted slots and paced itself twice —
on the S&P, ~3 minutes of pure sleep before counting a single round trip. It now runs
`_PRICE_WORKERS` at a time and leaves the rate to the governor.

⚠ THAT IS ONLY SAFE BECAUSE THIS IS NOT RESOLUTION. The hazard that makes this repo a single Yahoo
consumer is `resolve()` — an overloaded caller gets an EMPTY search rather than a 429, and an empty
candidate set hands the win to a thin foreign listing. `extend_series` asks about a symbol we have
already identified; an empty answer there means "no new bars".

What these tests pin is the bookkeeping, because every one of these failures produces a plausible
run rather than an error:

    1. EVERY CONSTITUENT EXACTLY ONCE, and the tally adds up to the total. A double-counted
       constituent inflates "price series fetched" in the receipt with no visible symptom.
    2. `[n/total]` IS AN ATOMIC COUNTER, so the progress bar only ever moves forward. Threads
       finish out of order; a positional index would send the bar backwards mid-run.
    3. CANCEL IS A COUNT, NOT A PREFIX. With several in flight there is no "everything before k";
       `stopped_at` is how many finished, and everything fetched is stored either way.

Plus the one piece of arithmetic the hoisting changed: `moved` vs `unchanged` is decided against
the newest close we held BEFORE the run, now read for the whole index in one grouped COPY rather
than per constituent. Getting that backwards would report every constituent as freshly moved,
which reads exactly like a healthy run.

No network, no database: the fetch, the marks read and the pre-run close map are all replaced.
"""
from __future__ import annotations

import re
import threading

import pytest

from routers import _benchmark_refresh as br

_TOTAL = 40
# Comfortably past the pool width, so the assertions below are about concurrency and not about a
# pool that happened to run everything in one wave.
assert _TOTAL > br._PRICE_WORKERS


def _isin(i: int) -> str:
    return f"XX{i:010d}"


def _fixture(n: int = _TOTAL):
    """`n` constituents, all resolved and priceable: (companies, isins, grid)."""
    companies = [{"isin": _isin(i), "company_name": f"Company {i}"} for i in range(n)]
    isins = [_isin(i) for i in range(n)]
    grid = {_isin(i): {"analysis_id": 1000 + i, "yahoo_symbol": f"SYM{i}"} for i in range(n)}
    return companies, isins, grid


@pytest.fixture
def rig(monkeypatch):
    """Replace the three things `_prices` reaches outside itself, and record every call.

    ⚠ `extend_series` AND `latest_close_by_analysis` ARE PATCHED ON THEIR OWN MODULES, not on
    `_benchmark_refresh` — it imports both INSIDE the function body (a deliberate lazy import), so
    there is no module-level name here to replace.
    """
    from asset_pipeline import price_refresh, store

    state = {
        "fetched": [],          # analysis_ids handed to extend_series, in completion order
        "lines": [],            # every progress message emitted
        "lock": threading.Lock(),
        "raise_for": set(),     # symbols whose fetch blows up
        "end": "2026-08-10",    # the close every constituent reads back after its fetch
    }

    def _extend(aid, sym, since):
        with state["lock"]:
            state["fetched"].append(aid)
        if sym in state["raise_for"]:
            raise RuntimeError("boom")
        return 1

    def _marks(aid, lookback, anchor):
        return (("2026-01-02", 100.0), (state["end"], 110.0))

    monkeypatch.setattr(store, "extend_series", _extend)
    monkeypatch.setattr(br, "_marks", _marks)
    # Empty by default: nobody had a close before the run, so everything is a NEW close.
    monkeypatch.setattr(price_refresh, "latest_close_by_analysis", lambda ids: state.get("before", {}))

    def emit(_kind, **kw):
        msg = kw.get("message")
        if msg:
            with state["lock"]:
                state["lines"].append(msg)

    state["emit"] = emit
    return state


def _steps(lines: list[str]) -> list[int]:
    """The `n` of every `[n/total]` line, in the order they were emitted."""
    return [int(m.group(1)) for m in (re.match(r"\s*\[(\d+)/(\d+)]", ln) for ln in lines) if m]


class TestEveryConstituentExactlyOnce:

    def test_the_tally_adds_up_and_nothing_is_fetched_twice(self, rig):
        companies, isins, grid = _fixture()
        out = br._prices(companies, isins, grid, "2026-08-10", rig["emit"])
        assert out["total"] == _TOTAL
        assert out["fetched"] == _TOTAL
        assert out["failed"] == 0
        assert out["moved"] + out["unchanged"] + out["no_start"] + out["no_end"] == _TOTAL
        assert sorted(rig["fetched"]) == sorted(1000 + i for i in range(_TOTAL))

    def test_the_step_counter_is_a_permutation_and_never_repeats(self, rig):
        companies, isins, grid = _fixture()
        br._prices(companies, isins, grid, "2026-08-10", rig["emit"])
        steps = _steps(rig["lines"])
        # ⚠ THE BAR ONLY MOVES FORWARD. Each n is handed out once, and the sequence as EMITTED is
        # ascending — which is the property the toast's progress bar depends on, and the one a
        # positional index would break the moment two threads finish out of order.
        assert sorted(steps) == list(range(1, _TOTAL + 1))
        assert steps == sorted(steps)


class TestMovedVersusUnchanged:
    """Decided against the closes we held BEFORE the run — one grouped read for the whole index."""

    def test_a_constituent_already_at_that_close_is_unchanged_not_moved(self, rig):
        companies, isins, grid = _fixture()
        # The first ten already sat at the close they read back; the rest had nothing.
        rig["before"] = {1000 + i: rig["end"] for i in range(10)}
        out = br._prices(companies, isins, grid, "2026-08-10", rig["emit"])
        assert out["unchanged"] == 10
        assert out["moved"] == _TOTAL - 10
        # ⚠ "unchanged" IS AN ANSWER AND SAYS SO — the vendor has no closed bar after the one we
        # hold. Silence here is how a working button gets reported as broken.
        assert sum("unchanged, Yahoo has no closed bar" in ln for ln in rig["lines"]) == 10

    def test_with_no_prior_close_everything_reads_as_new(self, rig):
        companies, isins, grid = _fixture()
        out = br._prices(companies, isins, grid, "2026-08-10", rig["emit"])
        assert out["moved"] == _TOTAL
        assert out["unchanged"] == 0


class TestOneDeadSymbolDoesNotEndTheRun:

    def test_a_failure_is_counted_and_the_rest_still_fetch(self, rig):
        companies, isins, grid = _fixture()
        rig["raise_for"] = {"SYM3", "SYM17"}
        out = br._prices(companies, isins, grid, "2026-08-10", rig["emit"])
        assert out["failed"] == 2
        assert out["fetched"] == _TOTAL - 2
        # Every constituent still got its line, failures included — they carry the exception.
        assert sorted(_steps(rig["lines"])) == list(range(1, _TOTAL + 1))
        assert sum("FAILED RuntimeError" in ln for ln in rig["lines"]) == 2


class TestCancelIsACountNotAPrefix:

    def test_it_stops_early_keeps_what_it_fetched_and_says_so_once(self, rig):
        companies, isins, grid = _fixture()
        gate = threading.Event()

        def _stop() -> bool:
            # Let a handful through, then refuse the rest. Everything already in flight finishes —
            # that is the boundary where the database is consistent.
            with rig["lock"]:
                if len(rig["fetched"]) >= 8:
                    gate.set()
            return gate.is_set()

        out = br._prices(companies, isins, grid, "2026-08-10", rig["emit"], should_stop=_stop)
        assert "stopped_at" in out
        # ⚠ THE COUNT IS WHAT RAN, NOT AN INDEX INTO THE LIST. Both halves are kept: a cancelled
        # run that fetched 140 of 491 has 140 constituents freshly priced, and reporting that as
        # nothing invites pressing the button again from scratch.
        assert out["stopped_at"] == out["fetched"] + out["failed"]
        assert 0 < out["fetched"] < _TOTAL
        assert len(rig["fetched"]) == out["fetched"] + out["failed"]
        # ⚠ SAID ONCE. Every queued constituent passes the check after a Cancel; a line each would
        # be hundreds of them.
        assert sum("cancelling —" in ln for ln in rig["lines"]) == 1
        assert sum("cancelled —" in ln for ln in rig["lines"]) == 1


class TestNothingToDo:

    def test_an_index_with_no_resolved_constituents_is_an_answer(self, rig):
        out = br._prices([], [], {}, "2026-08-10", rig["emit"])
        assert out["total"] == 0
        assert out["fetched"] == 0
        assert "stopped_at" not in out
