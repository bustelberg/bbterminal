"""The due detector reads the fiscal-period axis off ONE exact `metric_code`, never a prefix.

⚠⚠ THIS IS A PERFORMANCE FIX PINNED AS A CORRECTNESS TEST, BECAUSE THE SYMPTOM WAS NEITHER. The
query used to be `metric_code LIKE 'quarterly\\_\\_%'`, and the database collates `en_US.UTF-8` — so
no btree we own gives a range for a prefix. Measured 2026-08-17 over ACWI's 1,949 constituents
against 69,003,374 rows of `metric_data`:

    Seq Scan on metric_data ... rows=31,424,726 ... 479 MB spilled to disk    26-57 s
    the same answer off one exact code (index scan)                            1.8 s

Every one of those seconds fell BEFORE the job's first progress line, so the "Refresh benchmark"
button on the ACWI fundamentals drill-down reported "starting…" and looked hung. Nothing errored,
nothing was wrong on screen, and no test could fail — which is why the query shape is pinned here
rather than left to be re-derived by whoever next reads the `LIKE` and thinks it reads more
naturally.

⚠ IT IS THE SAME ANSWER, MEASURED, NOT A CHEAPER APPROXIMATION. Over all 1,949 constituents the one
code reproduces the prefix scan exactly: 1,712 companies, 127,001 distinct (company, period) pairs,
an identical `due` set, and the same newest period for every company. The quarterly block is written
as a unit, so every line in it carries the whole period axis.

⚠ AND IT IS NOT THE QUARTERLY TWIN OF THE `fin` SENTINEL, WHICH IS THE OBVIOUS PICK.
`quarterly__Cashflow Statement__Free Cash Flow` is missing for 11 constituents — a bank's template
omits it, exactly as it omits gross profit — giving 126,063 pairs and moving a company in and out
of `due`.
"""
from __future__ import annotations

import io
from datetime import date

import routers._fundamental_fill as fill
from routers._fundamental_fill import DUE_PERIOD_CODE, due_company_ids


class _Copy:
    """Stands in for `common.pg._run_copy`, recording the SQL and params it was handed."""

    def __init__(self, rows: list[tuple[int, str]] | None = None):
        self.rows = rows or []
        self.sql: str | None = None
        self.params: tuple | None = None

    def __call__(self, sql, params):
        self.sql, self.params = sql, params
        body = "".join(f"{cid},{d}\n" for cid, d in self.rows)
        return io.BytesIO(body.encode())


def _run(monkeypatch, rows, ids, today=date(2026, 8, 17)):
    spy = _Copy(rows)
    monkeypatch.setattr("common.pg._run_copy", spy)
    out, note = due_company_ids(ids, today)
    return out, note, spy


class TestTheQueryShape:

    def test_it_matches_ONE_code_by_equality(self, monkeypatch):
        """⚠ NO `LIKE`, NO `%`. An equality predicate is what makes the index usable; it is also
        what makes the wildcard trap below unexpressible."""
        _out, _note, spy = _run(monkeypatch, [], [1, 2, 3])
        assert "LIKE" not in spy.sql.upper()
        assert "%%" not in spy.sql          # the escaped-literal-percent form the prefix needed
        assert "metric_code = %s" in spy.sql

    def test_the_code_is_passed_as_a_parameter_not_interpolated(self, monkeypatch):
        _out, _note, spy = _run(monkeypatch, [], [1, 2, 3])
        assert spy.params == ([1, 2, 3], DUE_PERIOD_CODE)

    def test_the_code_is_a_quarterly_line(self, monkeypatch):
        """⚠ THE CADENCE IS IN THE CODE. An `annuals__` line here would infer a 12-month cadence
        for every company and the detector would go quiet for a year at a time."""
        assert DUE_PERIOD_CODE.startswith("quarterly__")

    def test_it_is_the_widest_covered_line_not_the_cashflow_one(self):
        """⚠ MEASURED, NOT PREFERRED. `quarterly__Cashflow Statement__Free Cash Flow` is the
        intuitive pick — the quarterly twin of the `fin` sentinel — and 11 ACWI constituents have
        no such row because a bank's template omits the line."""
        assert DUE_PERIOD_CODE == "quarterly__Per Share Data__Revenue per Share"
        assert "Cashflow" not in DUE_PERIOD_CODE

    def test_the_analyst_forecast_rows_cannot_be_matched(self, monkeypatch):
        """⚠ THE TRAP THE OLD PREFIX CARRIED. `_` is a single-character wildcard in SQL LIKE, so an
        unescaped `'quarterly__%'` also matched `quarterly_revenue_estimate` — forecast rows whose
        period dates are YEARS in the future (ASML had 2028-03-31). Fed to the detector they make
        every company look comfortably up to date, so the button goes quiet exactly when there is
        work to do. An equality match on a `quarterly__…` code cannot reach them."""
        _out, _note, spy = _run(monkeypatch, [], [1])
        assert not DUE_PERIOD_CODE.startswith("quarterly_revenue")
        assert spy.params[1] == DUE_PERIOD_CODE


class TestWhatItReturns:

    def test_a_company_whose_next_quarter_is_overdue_is_offered(self, monkeypatch):
        rows = [(7, d) for d in
                ("2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31")]
        out, note, _spy = _run(monkeypatch, rows, [7])
        assert out == [7] and note is None

    def test_a_company_that_is_up_to_date_is_left_alone(self, monkeypatch):
        rows = [(7, d) for d in
                ("2025-09-30", "2025-12-31", "2026-03-31", "2026-06-30")]
        out, _note, _spy = _run(monkeypatch, rows, [7], today=date(2026, 8, 1))
        assert out == []

    def test_a_company_missing_this_ONE_line_is_due_not_fresh(self, monkeypatch):
        """⚠ THE SAFETY NET UNDER THE CODE CHOICE, AND THE DIRECTION IS THE WHOLE POINT. A company
        with quarterly data but not this particular row reads as "no periods" and is offered,
        costing one call. The opposite fallback — absent means fresh — would quietly retire it from
        every future press."""
        out, _note, _spy = _run(monkeypatch, [(7, "2026-03-31")], [7, 99])
        assert 99 in out

    def test_no_copy_path_returns_EVERYTHING_and_says_so(self, monkeypatch):
        """⚠ DEGRADING THE OPTIMISATION IS FINE; DEGRADING THE ANSWER IS NOT. Without a direct
        connection the period axis cannot be read cheaply, so the caller gets the full list back
        with a note — never a silently narrowed one."""
        monkeypatch.setattr("common.pg._run_copy", lambda sql, params: None)
        out, note = due_company_ids([1, 2, 3], date(2026, 8, 17))
        assert out == [1, 2, 3]
        assert note and "could not check" in note

    def test_an_empty_id_list_asks_nothing(self, monkeypatch):
        called = []
        monkeypatch.setattr("common.pg._run_copy",
                            lambda sql, params: called.append(sql) or None)
        assert due_company_ids([], date(2026, 8, 17)) == ([], None)
        assert called == []


class TestTheSetupNarratesItself:
    """⚠ THE OTHER HALF OF THE SAME BUG REPORT. Even at 1.8s the deciding is silent, and the first
    per-company line only lands once that company's three GuruFocus feeds have been fetched and
    written — so the card sat on "starting…" long after the query was fast. The setup now emits
    before each stretch of database work."""

    def test_it_reports_before_it_reads(self, monkeypatch):
        """The first line must land BEFORE the deciding, not after it."""
        import ingest.api_usage as api_usage
        import routers._blend_cache as blend_cache
        import routers._fundamental_backfill as backfill

        seen: list[str] = []

        class _Ctx:
            def __init__(self):
                self.lines: list[tuple[str, str, dict]] = []

            def emit(self, kind, message, **data):
                self.lines.append((kind, message, data))

            def progress(self, done, total, message, **data):
                self.emit("progress", message, done=done, total=total, **data)

            def spent(self, calls):
                pass

            def check(self):
                pass

        def _company_rows(cids):
            seen.append("company_rows")
            return {}

        def _smart(cids):
            seen.append("smart_flags_bulk")
            return {}

        monkeypatch.setattr(backfill, "company_rows", _company_rows)
        monkeypatch.setattr(backfill, "smart_flags_bulk", _smart)
        monkeypatch.setattr(api_usage, "remaining_budget",
                            lambda _s: {"usa": 1, "europe": 2, "asia": 3})
        monkeypatch.setattr(blend_cache, "invalidate", lambda: None)

        ctx = _Ctx()
        # An empty id list, so nothing is fetched — the only question here is whether the reader
        # was told anything before the work list existed.
        out = fill.fill_company_ids(ctx, "ACWI", [], feeds="smart")

        assert ctx.lines, "the setup emitted nothing at all — the card would read 'starting…'"
        _kind, first_msg, first_data = ctx.lines[0]
        assert "ACWI" in first_msg
        # ⚠ NO `done`/`total` YET. There is no work list to count against; a percentage here would
        # be of a thing not yet decided, and would jump backwards when the real total arrived.
        assert "total" not in first_data and "done" not in first_data
        # The `start` line — the one that DOES carry the bar's total — comes after the narration.
        kinds = [k for k, _m, _d in ctx.lines]
        assert kinds.index("start") > 0
        assert seen == ["company_rows", "smart_flags_bulk"]
        assert isinstance(out, str)
