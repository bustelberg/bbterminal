"""The run's step transcript — what `current_message` structurally cannot be.

`current_message` is ONE field, throttled to ~1/s and overwritten by every
subsequent write. It can say "refreshing 812/1479"; it can never say WHICH 812,
what each one returned, or which of the 24 names a strategy finally holds — by
the time you poll it, 1,400 lines have been written over the same field.

So a run also keeps an append-only, cursor-readable transcript that the
/schedule Run-now buttons tail into the browser console. Two properties are
load-bearing and pinned here:

  * the CURSOR — a reader that misses a poll resumes exactly where it stopped
    (a log that silently skips lines is worse than no log), and
  * the ADMISSION — the buffer is bounded, so when it drops the head it SAYS so
    instead of handing back a truncated transcript that looks complete.
"""
from __future__ import annotations

import inspect

from ingest.phases import runlog


def _fresh(run_id: int) -> None:
    """Isolate a run id — the buffers are module-level and shared."""
    with runlog._log_lock:
        runlog._log_buffers.pop(run_id, None)
        runlog._log_state.pop(run_id, None)


class TestTheCursor:
    def test_a_reader_resumes_exactly_where_it_stopped(self):
        _fresh(901)
        for i in range(5):
            runlog.log_step(901, f"step {i}")
        first = runlog.read_log(901, after=0)
        assert [e["message"] for e in first["entries"]] == [f"step {i}" for i in range(5)]

        runlog.log_step(901, "step 5")
        second = runlog.read_log(901, after=first["next"])
        assert [e["message"] for e in second["entries"]] == ["step 5"]
        assert second["next"] == 6

    def test_an_idle_poll_returns_nothing_and_keeps_the_cursor(self):
        """The button polls twice a second; most polls have no news. An empty
        page must not rewind the cursor to 0 and re-print the whole run."""
        _fresh(902)
        runlog.log_step(902, "only step")
        page = runlog.read_log(902, after=1)
        assert page["entries"] == []
        assert page["next"] == 1

    def test_an_unknown_run_is_empty_not_an_error(self):
        """An old run whose buffer was recycled, or a run id that never logged.
        `latest` distinguishes them: 0 means nothing was ever recorded."""
        _fresh(903)
        page = runlog.read_log(903)
        assert page["entries"] == [] and page["latest"] == 0

    def test_a_capped_page_says_there_is_MORE(self):
        """Otherwise a 1,400-company burst trickles out one page per poll and the
        console lags minutes behind the run it is describing."""
        _fresh(904)
        for i in range(10):
            runlog.log_step(904, f"s{i}")
        page = runlog.read_log(904, limit=4)
        assert len(page["entries"]) == 4 and page["more"] is True
        assert runlog.read_log(904, after=page["next"], limit=100)["more"] is False


class TestABoundedBufferMustAdmitWhatItDropped:
    def test_it_counts_the_entries_that_fell_off(self, monkeypatch):
        _fresh(905)
        monkeypatch.setattr(runlog, "_LOG_MAX_ENTRIES", 3)
        for i in range(6):
            runlog.log_step(905, f"s{i}")
        page = runlog.read_log(905)
        assert [e["message"] for e in page["entries"]] == ["s3", "s4", "s5"]
        assert page["dropped"] == 3, "a silent truncation reads as a complete transcript"
        assert page["latest"] == 6, "the seq counter still knows how many steps really happened"

    def test_a_reader_whose_cursor_fell_off_the_ring_is_told(self, monkeypatch):
        _fresh(906)
        monkeypatch.setattr(runlog, "_LOG_MAX_ENTRIES", 3)
        runlog.log_step(906, "s1")
        page1 = runlog.read_log(906)                 # cursor now 1
        assert page1["gap"] == 0
        for i in range(2, 8):
            runlog.log_step(906, f"s{i}")            # 1..4 evicted
        page2 = runlog.read_log(906, after=1)
        assert page2["gap"] > 0, "the reader MISSED lines and has to know"

    def test_only_a_few_runs_are_retained(self, monkeypatch):
        """It is a live console tail, not an archive — `ingest_run` + the
        snapshots remain the durable record of what a run did."""
        monkeypatch.setattr(runlog, "_LOG_MAX_RUNS", 2)
        for rid in (910, 911, 912):
            _fresh(rid)
            runlog.log_step(rid, "x")
        assert runlog.read_log(910)["entries"] == []
        assert runlog.read_log(912)["entries"] != []


class TestLoggingCannotBreakTheWork:
    def test_log_step_swallows_everything(self, monkeypatch):
        """A transcript line is a description of the work. If describing it could
        fail it, the observability would be the outage."""
        class _Boom:
            def __enter__(self, *a): raise RuntimeError("lock exploded")
            def __exit__(self, *a): return False

        monkeypatch.setattr(runlog, "_log_lock", _Boom())
        runlog.log_step(913, "should not raise")     # no assertion needed: no raise

    def test_the_itemiser_never_fails_the_strategy_that_succeeded(self):
        from ingest.phases import momentum

        src = inspect.getsource(momentum._log_holdings)
        assert "except Exception" in src


class TestItLogsWhatTheRunActuallyDID:
    def test_every_company_the_price_phase_touches(self):
        """The aggregate counters say "1,479 processed"; they cannot say whether
        the ONE name you care about got Friday's close."""
        from ingest.phases import prices

        src = inspect.getsource(prices._run_prices_phase)
        assert "_step(" in src
        for outcome in ("quota exhausted", "403 unsubscribed", "delisted", "already current"):
            assert outcome in src, f"the {outcome!r} outcome is invisible in the transcript"

    def test_the_holdings_are_read_back_from_the_SNAPSHOT(self):
        """⚠ The ETF overlay and the cash sleeve rewrite the weights AFTER the
        compute returns, so the selection the engine handed back is not the book
        that was stored. Printing that one would itemise a portfolio nobody
        holds."""
        from ingest.phases import momentum

        src = inspect.getsource(momentum._log_holdings)
        assert 'table("current_picks_snapshot")' in src
        called = inspect.getsource(momentum._run_momentum_phase)
        sleeves = called.index("_apply_sleeves_to_snapshot")
        assert called.index("_log_holdings(", sleeves) > sleeves, (
            "itemise AFTER the ETF + cash sleeves, or the weights printed are not the ones held"
        )

    def test_the_rebalance_narrates_each_phase(self):
        from ingest.phases import pipeline

        src = inspect.getsource(pipeline._run_rebalance_pipeline_sync)
        for phase in ("plan", "templates", "prices", "freshness", "momentum", "done"):
            assert f'phase="{phase}"' in src

    def test_every_orchestrator_narrates_itself(self):
        """A transcript that covers one of the four ops is a transcript you can't
        trust to be there when the op you're debugging is a different one."""
        from ingest.phases import pipeline

        for fn in (
            pipeline._run_price_update_pipeline_sync,
            pipeline._run_rebalance_pipeline_sync,
            pipeline._run_full_price_refresh_pipeline_sync,
            pipeline._run_universe_price_refresh_pipeline_sync,
        ):
            src = inspect.getsource(fn)
            assert "log_step(" in src, f"{fn.__name__} is silent"
            assert 'phase="start"' in src, f"{fn.__name__} never says it started"

    def test_the_engine_states_the_date_it_decided_FOR_and_off_WHICH_bar(self):
        """The first thing to check when a rebalance looks wrong, and the one
        thing the holdings table can never show you afterwards."""
        from momentum.backtest import current_portfolio

        src = inspect.getsource(current_portfolio.run_current_portfolio)
        assert "walk:" in src
        assert "deciding_bar(rebalance_date)" in src
        assert "latest loaded close" in src

    def test_the_sector_table_is_aggregated_over_the_pool_the_SELECTION_ranked(self):
        """⚠ `score_and_select` ranks sectors over EVERY scored company. Logging a
        table built from the `min_price_score`-filtered pool instead would explain
        the choice with the survivor bias that was deliberately removed on
        2026-07-31 — and it would look authoritative doing it."""
        from momentum.backtest import current_portfolio

        src = inspect.getsource(current_portfolio.run_current_portfolio)
        block = src.split("Sector ranking", 1)[0][-1400:]
        assert "score_universe(" in block
        # the CALL, not the comment that explains why it isn't used
        assert "= selection_pool(" not in block
        assert "sector_pool_scores(scored_for_log)" in src

    def test_a_diagnostic_can_never_fail_the_rebalance(self):
        from momentum.backtest import current_portfolio

        src = inspect.getsource(current_portfolio.run_current_portfolio)
        after = src.split("sector_pool_scores(", 1)[1][:600]
        assert "except Exception" in after


class TestTheTranscriptSurvivesTheProcessThatWroteIt:
    """⚠ THE RING BUFFER IS PER-PROCESS. A job run from a script, or one whose
    backend has since restarted, leaves NOTHING for the `/log` endpoint to serve —
    and "no entries" is indistinguishable from "the run did nothing". So every
    step is mirrored to a logger as well."""

    def test_steps_mirror_to_a_logger(self, caplog):
        import logging

        from ingest.phases import runlog

        with caplog.at_level(logging.INFO, logger="ingest.steps"):
            runlog.log_step(940, "hello", phase="prices")
        rendered = [r.getMessage() for r in caplog.records]
        assert any("hello" in m and "prices" in m and "940" in m for m in rendered), rendered

    def test_warnings_mirror_at_WARNING_so_production_sees_them(self, caplog):
        """uvicorn leaves the root logger at WARNING, so an `info` mirror is
        invisible in a deploy log — which is fine for the routine steps and NOT
        fine for the ones that say something went wrong."""
        import logging

        from ingest.phases import runlog

        with caplog.at_level(logging.WARNING, logger="ingest.steps"):
            runlog.log_step(941, "bad thing", level="error", phase="prices")
            runlog.log_step(941, "routine", level="info", phase="prices")
        levels = {r.levelno for r in caplog.records}
        assert logging.WARNING in levels
        assert logging.INFO not in levels
