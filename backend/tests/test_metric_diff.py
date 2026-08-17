"""Writing only what changed — and, more importantly, never swallowing something that did.

⚠⚠ THE MEASUREMENT THIS EXISTS FOR (local, 2026-08-17, `metric_data` at 69,003,374 rows): a
fundamentals refresh re-parses the whole GuruFocus blob — 263 leaf fields x ~160 periods — and
upserted every row of it, 500 at a time.

    Dassault Systemes  36,494 rows parsed  ->  0 changed   73 upsert round trips   17.48s
    Legrand            25,800 rows parsed  ->  0 changed   52 round trips           3.96s
    Lotus Bakeries     16,512 rows parsed  ->  0 changed   34 round trips
    Sanofi             20,382 rows parsed  ->  0 changed   41 round trips
    Vinci              19,092 rows parsed  ->  0 changed   39 round trips

Zero changed rows in every one. Through the real path afterwards, Dassault's whole refresh is
**1.58s** against 17.48s of upserting alone, and Legrand's 0.70s against 3.96s.

⚠ A FAST NO-OP WOULD SCORE IDENTICALLY ON ALL OF THAT, which is why the tests below are mostly
about the writes it must NOT skip. Verified end to end against the live local database by corrupting
three of Legrand's stored rows three different ways — a wrong value, a NULLed value, a deleted row —
and re-running the real `fetch_financials`: **3 written, 25,797 skipped, all three restored.** The
cases here are those three plus the ones a fixture can reach that a probe cannot.
"""
from __future__ import annotations

import pytest

from ingest.metric_upsert import changed_rows, partition_changed, rows_match


def _row(code="annuals__Income Statement__Revenue", date="2025-12-31", value=9480.6,
         *, cid=62, pred=False, source="gurufocus"):
    return {"company_id": cid, "metric_code": code, "source_code": source,
            "target_date": date, "numeric_value": value, "is_prediction": pred}


def _stored(rows):
    return {(r["metric_code"], r["target_date"]): r for r in rows}


class TestWhatCountsAsUnchanged:

    def test_an_identical_row_is_skipped(self):
        fresh, same = partition_changed([_row()], _stored([_row()]))
        assert fresh == [] and same == 1

    def test_a_different_value_is_written(self):
        fresh, same = partition_changed([_row(value=9480.6)], _stored([_row(value=8648.9)]))
        assert len(fresh) == 1 and same == 0

    def test_a_row_we_do_not_hold_is_written(self):
        """The whole point of a refresh — the newly filed period."""
        fresh, same = partition_changed([_row(date="2026-03-31")], _stored([_row()]))
        assert len(fresh) == 1 and same == 0

    def test_nothing_stored_at_all_writes_everything(self):
        rows = [_row(date=d) for d in ("2023-12-31", "2024-12-31", "2025-12-31")]
        fresh, same = partition_changed(rows, {})
        assert fresh == rows and same == 0


class TestNullIsAValueNotAnAbsence:
    """⚠⚠ `_parse_financials` DELIBERATELY EMITS A ROW WITH `numeric_value = None` where GuruFocus
    reported "N/A", so the dashboard can show the period EXISTS with no figure rather than walking
    back to a numeric from years ago. Both directions have to be caught."""

    def test_null_matching_null_is_unchanged(self):
        fresh, same = partition_changed([_row(value=None)], _stored([_row(value=None)]))
        assert fresh == [] and same == 1

    def test_a_value_that_became_null_is_written(self):
        """GuruFocus restated a period to N/A. Skipping it would leave a stale number on the chart
        for ever — the exact defect the null rows were introduced to fix."""
        fresh, _same = partition_changed([_row(value=None)], _stored([_row(value=9480.6)]))
        assert len(fresh) == 1

    def test_a_null_that_became_a_value_is_written(self):
        fresh, _same = partition_changed([_row(value=9480.6)], _stored([_row(value=None)]))
        assert len(fresh) == 1

    def test_zero_is_not_null(self):
        """⚠ `0.0` IS FALSY. A truthiness test anywhere in the comparison would call a real zero
        missing — Apple nets its interest expense to exactly 0, and that is a filed figure."""
        assert not rows_match(_row(value=0.0), _row(value=None))
        assert not rows_match(_row(value=None), _row(value=0.0))
        assert rows_match(_row(value=0.0), _row(value=0.0))


class TestIsPredictionIsPartOfTheComparison:
    """⚠ NOT JUST OF THE KEY. The estimates feed writes True and the other two False, so a row that
    changed ONLY in that flag still has to be written or the forecast/actual split rots silently —
    and that split is what `/earnings` draws its `2026e` columns from."""

    def test_a_flipped_flag_is_written(self):
        fresh, _same = partition_changed([_row(pred=True)], _stored([_row(pred=False)]))
        assert len(fresh) == 1

    def test_the_same_flag_with_the_same_value_is_skipped(self):
        fresh, same = partition_changed([_row(pred=True)], _stored([_row(pred=True)]))
        assert fresh == [] and same == 1


class TestFloatComparisonIsExact:
    """`metric_data.numeric_value` is `double precision`, so a Python float written and read back is
    the identical value — no tolerance is needed and none is wanted: the question is "would writing
    this change anything", and any difference at all means yes."""

    def test_a_tiny_difference_is_a_difference(self):
        assert not rows_match(_row(value=1.0000000000000002), _row(value=1.0))

    def test_an_int_and_its_float_are_the_same_number(self):
        """Postgres hands `1000` back through `row_to_json` as an int; the parser produced a float."""
        assert rows_match(_row(value=1000.0), _row(value=1000))


class TestTheRead:

    def test_it_asks_only_for_the_codes_it_is_about_to_write(self, monkeypatch):
        """⚠ SCOPED TO THE CODES, NOT TO THE COMPANY. `fetch_financials` takes a `metric_codes`
        filter — `_asset_dividends` persists TWO codes, ~320 rows — and reading the company's whole
        36,000-row history to diff 320 of them would make the narrow path far worse than before."""
        seen = {}

        def _fake(table, columns, key_col, values, *, where=None):
            seen.update(table=table, columns=columns, key_col=key_col,
                        values=list(values), where=dict(where or {}))
            return []

        monkeypatch.setattr("common.pg.load_rows_via_copy", _fake)
        changed_rows(object(), [_row(code="A"), _row(code="B", date="2024-12-31")])
        assert seen["table"] == "metric_data"
        assert seen["key_col"] == "metric_code"
        assert seen["values"] == ["A", "B"]
        assert seen["where"] == {"company_id": 62, "source_code": "gurufocus"}
        # ⚠ `is_prediction` MUST BE READ BACK, or the comparison above cannot see a flipped flag.
        assert "is_prediction" in seen["columns"] and "numeric_value" in seen["columns"]

    def test_it_groups_by_company_and_source(self, monkeypatch):
        """Every earnings feed passes one group, so this is one query in practice — but a silent
        cross-company comparison would skip real writes."""
        calls = []

        def _fake(table, columns, key_col, values, *, where=None):
            calls.append(dict(where or {}))
            return []

        monkeypatch.setattr("common.pg.load_rows_via_copy", _fake)
        changed_rows(object(), [_row(cid=1), _row(cid=2), _row(cid=1, source="derived")])
        assert len(calls) == 3
        assert {(c["company_id"], c["source_code"]) for c in calls} == {
            (1, "gurufocus"), (2, "gurufocus"), (1, "derived")}

    def test_no_copy_path_writes_EVERYTHING(self, monkeypatch):
        """⚠ DEGRADING THE OPTIMISATION IS FINE; DEGRADING THE ANSWER IS NOT. Without a direct
        Postgres connection there is no cheap way to diff, so every row is handed back for writing —
        which is precisely the behaviour this replaced: slower, never wrong."""
        monkeypatch.setattr("common.pg.load_rows_via_copy",
                            lambda *a, **kw: None)
        rows = [_row(), _row(date="2024-12-31")]
        fresh, same = changed_rows(object(), rows)
        assert fresh == rows and same == 0

    def test_an_empty_list_asks_nothing(self, monkeypatch):
        called = []
        monkeypatch.setattr("common.pg.load_rows_via_copy",
                            lambda *a, **kw: called.append(1) or [])
        assert changed_rows(object(), []) == ([], 0)
        assert called == []

    def test_a_stored_timestamp_still_matches_a_date(self, monkeypatch):
        """`target_date` comes back from `row_to_json` as a date string, but a caller that ever hands
        over a timestamp must not read as a different period — the key is truncated to 10 chars on
        both sides."""
        monkeypatch.setattr(
            "common.pg.load_rows_via_copy",
            lambda *a, **kw: [{"metric_code": "annuals__Income Statement__Revenue",
                               "target_date": "2025-12-31T00:00:00+00:00",
                               "numeric_value": 9480.6, "is_prediction": False}])
        fresh, same = changed_rows(object(), [_row()])
        assert fresh == [] and same == 1


class TestTheCountsCallersDependOn:
    """⚠⚠ `rows_loaded == 0` USED TO MEAN "the fetch came back empty" AND NOW MEANS "nothing needed
    writing", which for an up-to-date company is the normal outcome. `_fundamental_fill._one` retries
    once on an empty answer, so reading the first as the second would re-fetch every healthy
    constituent — ~1,700 companies x up to 3 feeds of pure waste on ACWI, and it would look like this
    change had made the run twice as expensive. The two are told apart by the second number."""

    def test_unchanged_counts_the_rows_that_were_not_written(self):
        rows = [_row(date=d) for d in ("2023-12-31", "2024-12-31", "2025-12-31")]
        stored = _stored(rows[:2])
        fresh, same = partition_changed(rows, stored)
        assert same == 2 and len(fresh) == 1
        # The two always account for every row handed in — a caller adding them must get the
        # vendor's own row count back.
        assert same + len(fresh) == len(rows)

    @pytest.mark.parametrize("written,unchanged,is_empty", [
        (0, 0, True),        # the vendor returned nothing — worth one retry
        (0, 25797, False),   # up to date — the common case, and NOT an empty answer
        (3, 25797, False),   # a real filing landed
    ])
    def test_only_both_at_zero_means_an_empty_answer(self, written, unchanged, is_empty):
        assert (written == 0 and not unchanged) is is_empty
