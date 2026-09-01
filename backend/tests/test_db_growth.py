"""Database growth, measured in BYTES ON DISK rather than rows written.

⚠⚠ THE MEASUREMENT THIS REPLACES WOULD HAVE INVERTED THE RANKING. The intuitive instrumentation is
"have each job count what it inserts" — and the AIRS model scan delete-then-inserts every
portfolio's positions (thousands of rows written, zero growth) while several others are
delete-then-insert snapshots or upserts. A row count is also blind to indexes and bloat, which on this database's 18 GB
`metric_data` are most of the disk. `pg_total_relation_size` is exact and is what the hosting bills.

Measured 2026-08-13 on the local database: 59 tables, 22.4 GB, of which `metric_data` is 18.7 GB and
`asset_price` 3.5 GB. Every other table is under 16 MB — so "which tables grow" has a two-line
answer, and the useful question is the trend.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import db_growth
from db_growth import _per_day_mb, growth

MB = 1_048_576
NOW = datetime(2026, 8, 13, 22, 0, tzinfo=timezone.utc)


def _row(name: str, latest_mb: float, earlier_mb: float | None, days_back: float = 7.0) -> dict:
    return {
        "table_name": name,
        "latest_bytes": int(latest_mb * MB),
        "latest_at": NOW.isoformat(),
        "earlier_bytes": None if earlier_mb is None else int(earlier_mb * MB),
        "earlier_at": None if earlier_mb is None else (NOW - timedelta(days=days_back)).isoformat(),
        "rows_estimate": 0,
    }


@pytest.fixture
def rpc(monkeypatch):
    """Stand in for `supabase.rpc("table_growth", …)` with a canned row set."""
    holder: dict = {"rows": []}

    class _Resp:
        def __init__(self, data):
            self.data = data

    class _Call:
        def execute(self):
            return _Resp(holder["rows"])

    class _Fake:
        def rpc(self, _name, _params=None):
            return _Call()

    monkeypatch.setattr(db_growth, "supabase", _Fake())
    return holder


class TestNoBaselineIsNotZeroGrowth:
    def test_a_table_with_no_earlier_sample_reports_None(self, rpc):
        """⚠ A FRESH INSTALL HAS SIZES AND NO GROWTH. Rendering that as "0 MB added" presents a
        database nobody has measured yet as one that is not growing — the reader's next move is to
        stop worrying about a number that was never taken."""
        rpc["rows"] = [_row("metric_data", 18_746, None)]
        out = growth(7)
        assert out["rows"][0]["delta_mb"] is None
        assert out["rows"][0]["per_day_mb"] is None
        assert out["total_delta_mb"] is None
        assert out["has_baseline"] is False

    def test_has_baseline_is_true_as_soon_as_one_table_can_be_measured(self, rpc):
        rpc["rows"] = [_row("a", 10, None), _row("b", 10, 5)]
        assert growth(7)["has_baseline"] is True

    def test_the_total_delta_counts_only_the_measurable_tables(self, rpc):
        """A table with no baseline must not contribute 0 to the total — it contributes nothing,
        and the total is over what could be measured."""
        rpc["rows"] = [_row("a", 100, None), _row("b", 30, 20)]
        assert growth(7)["total_delta_mb"] == pytest.approx(10.0)


class TestPerDayIsDividedByTheWindowACTUALLYMEASURED:
    def test_not_by_the_window_requested(self):
        """⚠ THE BASELINE IS THE NEWEST SAMPLE AT-OR-BEFORE THE CUTOFF, which on a sparse history
        can be much older than asked for. Dividing 14 days of growth by the requested 7 would
        report double the real rate — measured: 4.77 MB over 14 days is 0.349/day, not 0.681."""
        a = (NOW - timedelta(days=14)).isoformat()
        b = NOW.isoformat()
        assert _per_day_mb(int(4.77 * MB), a, b) == pytest.approx(0.341, abs=0.02)

    def test_a_zero_or_negative_span_has_no_rate(self):
        """Two samples at the same instant cannot imply a rate, and a reversed pair would report a
        negative one off nothing but clock skew."""
        assert _per_day_mb(1000, NOW.isoformat(), NOW.isoformat()) is None
        assert _per_day_mb(1000, NOW.isoformat(), (NOW - timedelta(days=1)).isoformat()) is None

    def test_no_delta_and_no_dates_give_no_rate(self):
        assert _per_day_mb(None, NOW.isoformat(), NOW.isoformat()) is None
        assert _per_day_mb(1000, None, NOW.isoformat()) is None

    def test_a_naive_timestamp_does_not_crash_the_rate(self):
        assert _per_day_mb(int(7 * MB), "2026-08-06T22:00:00", "2026-08-13T22:00:00") \
            == pytest.approx(1.0, abs=0.01)


class TestOrdering:
    def test_biggest_grower_first_then_unmeasurable_last(self, rpc):
        """The page is read top-down for "what is eating the disk". A table with no baseline sinks
        to the bottom in every direction — absent is not a small number."""
        rpc["rows"] = [
            _row("small_grower", 10, 9),          # +1 MB
            _row("unmeasured", 900, None),        # no baseline
            _row("big_grower", 200, 100),         # +100 MB
        ]
        assert [r["table"] for r in growth(7)["rows"]] \
            == ["big_grower", "small_grower", "unmeasured"]

    def test_a_shrinking_table_sorts_below_a_growing_one(self, rpc):
        """A vacuum or a delete can genuinely shrink a table; it is a real reading, not an error,
        and it belongs below everything that grew."""
        rpc["rows"] = [_row("shrank", 50, 80), _row("grew", 10, 9)]
        out = growth(7)
        assert [r["table"] for r in out["rows"]] == ["grew", "shrank"]
        assert out["rows"][1]["delta_mb"] == pytest.approx(-30.0)


class TestTheWindowIsCarriedOnEveryRow:
    def test_each_row_states_the_span_it_was_measured_over(self, rpc):
        """⚠ WITHOUT IT THE DELTA IS UNREADABLE. Rows can have different baselines — a table
        created last week has a shorter history than one from 2024 — so a single window in the
        header would describe some rows and misdescribe others."""
        rpc["rows"] = [_row("a", 10, 5, days_back=3), _row("b", 10, 5, days_back=20)]
        by = {r["table"]: r for r in growth(7)["rows"]}
        assert by["a"]["measured_from"] != by["b"]["measured_from"]
        assert by["a"]["per_day_mb"] > by["b"]["per_day_mb"]
