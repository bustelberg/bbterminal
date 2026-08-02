"""The monthly full-history refetch — the only thing that corrects our past.

`ingest/prices.py::_upsert_metric_rows` writes only rows with `d > existing_max`,
so a bar, once stored, is never revisited. `force_refresh=True` does not change
that: it re-downloads the whole series and still writes only the newer rows. Every
vendor correction to the PAST is therefore invisible to the normal pipeline.

    Measured on the 1,479-name Leonteq universe, 2026-08-02:
        173 companies had wrong CLOSE history    46,969 bars
        887 companies had wrong VOLUME history   68,311 bars

⚠ AND THE BIG ONES ARE NOT THE DANGEROUS ONES. Worldline's 1-for-40 is a 40×
overnight jump — a detector finds it (and it had put a stock that fell 69% into
the live book on a +1142% momentum). Air Liquide's 1-for-10 free share attribution
re-scales the whole history by 10/11 and reads as a −9.1% day. No threshold
separates that from an ordinary move, which is why the refetch is unconditional
and scheduled rather than triggered by suspicion.
"""
from __future__ import annotations

import inspect
from datetime import datetime, timezone

from ingest import refetch_history


class TestAnEmptyVendorResponseNeverDeletes:
    def test_it_keeps_what_we_have(self):
        """A 404 (delisted ticker — `SATS`), a 403 (unsubscribed region) or a
        throttle must leave the stored series alone. Trading a stale series for
        NO series is strictly worse, and it is exactly what "replace with
        whatever the API returns" does."""
        src = inspect.getsource(refetch_history.refetch_full_history)
        empty = src.split("if not series:", 1)[1][:400]
        assert "continue" in empty
        assert "kept ours" in empty
        # ...and the skip is counted, not silent.
        assert "empty_vendor" in empty


class TestOnlyWhatMovedIsWritten:
    def test_the_diff_gates_the_write(self):
        """~5,000 bars × 1,479 companies × 2 metrics is ~15M row-writes to say
        almost nothing. The diff turns that into the few thousand that changed —
        and the diff IS the audit trail."""
        src = inspect.getsource(refetch_history.refetch_full_history)
        assert "if changed and apply:" in src
        assert "CHANGE_TOLERANCE" in src

    def test_the_tolerance_is_not_zero(self):
        """A float round-trip through JSON differs in the last digit; at zero
        tolerance every bar would 'move' on every run."""
        assert 0 < refetch_history.CHANGE_TOLERANCE < 1e-3


class TestTheMonthGuard:
    """Once per calendar month: a monthly strategy pays it on its rebalance, a
    weekly one on the first rebalance of the month, and a Force re-rebalance
    doesn't pay it again."""

    def test_it_is_keyed_off_the_DATA_not_a_flag(self):
        """An old bar can only carry a recent `recorded_at` if a full refetch
        wrote it — the normal pipeline cannot reach back that far. A stored flag
        would be a second source of truth someone has to remember to set."""
        src = inspect.getsource(refetch_history.last_full_refetch)
        assert "target_date < %s" in src
        assert "max(recorded_at)" in src

    def test_recorded_at_is_stamped_explicitly(self):
        """⚠ `recorded_at` defaults on INSERT only. An upsert that resolves to an
        UPDATE keeps the original timestamp, so a corrected 2015 bar would still
        read 'first seen 2026-06' and the guard would never advance."""
        src = inspect.getsource(refetch_history.refetch_full_history)
        assert '"recorded_at": _now_iso' in src

    def test_a_clean_run_still_counts_as_asking(self):
        """⚠ THE COMMON CASE. Only changed bars are written, so a universe that is
        already correct leaves no trace — and a weekly strategy would then re-ask
        every single week precisely BECAUSE the data was fine."""
        src = inspect.getsource(refetch_history.refetch_full_history)
        marker = src.split("STAMP THE MARKER", 1)[1]
        assert "refetch marker" in marker
        # ...gated on the fetch having actually happened, so a run that reached
        # nobody can't claim the month.
        assert 'counters.get("close_price_fetched")' in marker

    def test_same_month_is_true_different_month_is_false(self, monkeypatch):
        now = datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(refetch_history, "last_full_refetch",
                            lambda _c: datetime(2026, 8, 1, 9, 0, tzinfo=timezone.utc))
        assert refetch_history.refetched_this_month([1], now=now) is True
        monkeypatch.setattr(refetch_history, "last_full_refetch",
                            lambda _c: datetime(2026, 7, 31, 23, 59, tzinfo=timezone.utc))
        assert refetch_history.refetched_this_month([1], now=now) is False

    def test_never_refetched_is_false_not_an_error(self, monkeypatch):
        monkeypatch.setattr(refetch_history, "last_full_refetch", lambda _c: None)
        assert refetch_history.refetched_this_month([1]) is False

    def test_a_naive_timestamp_is_treated_as_utc(self, monkeypatch):
        """Postgres hands back a naive string on some paths; comparing it against
        an aware `now` raises, which would fail the whole rebalance for a marker."""
        monkeypatch.setattr(refetch_history, "last_full_refetch",
                            lambda _c: datetime(2026, 8, 2, 9, 0))
        assert refetch_history.refetched_this_month(
            [1], now=datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc)) is True


class TestTheRebalanceRunsItFirst:
    def test_before_anything_is_selected(self):
        from ingest.phases import pipeline

        src = inspect.getsource(pipeline._run_rebalance_pipeline_sync)
        assert src.index("_maybe_full_refetch(") < src.index("universe_freshness(")
        assert src.index("_maybe_full_refetch(") < src.index("_run_momentum_phase(")

    def test_a_failed_refetch_does_not_cancel_the_rebalance(self):
        """Degraded and said out loud beats skipped: the strategies still have to
        be rebalanced, on the history we already hold."""
        from ingest.phases import pipeline

        src = inspect.getsource(pipeline._maybe_full_refetch)
        assert "except Exception" in src
        assert "rebalancing on the history we already hold" in src

    def test_it_can_be_disabled_and_forced(self):
        from ingest.phases import pipeline

        src = inspect.getsource(pipeline._maybe_full_refetch)
        assert "REBALANCE_FULL_REFETCH" in src
        assert 'mode != "force"' in src

    def test_the_cli_and_the_pipeline_share_ONE_implementation(self):
        """A script that drifts from the scheduled job is a script that debugs a
        different program."""
        from ingest.phases import pipeline

        assert "refetch_full_history" in inspect.getsource(pipeline._maybe_full_refetch)
        cli = (__import__("pathlib").Path(__file__).parent.parent
               / "scripts" / "refetch_full_history.py").read_text(encoding="utf-8")
        assert "from ingest.refetch_history import" in cli
