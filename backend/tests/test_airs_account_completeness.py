"""Which of an account's four AIRS reports the last refresh retrieved — and what that is FOR.

⚠ IT WAS BRIEFLY A FILTER, AND THAT WAS THE WRONG SHAPE. Accounts short a report were withheld
from the portfolios list entirely, so a scan that reached all 44 portfolios displayed 22: the work
was done and invisible, and nobody could see which report was missing or for whom. It is now a
per-row marker (`_missing_reports`), and the row is shown with a badge naming the gap.

`_complete_accounts` survives as the honest expression of "is this account whole" — nothing lists
on it any more, but an alert or a health check should reuse it rather than re-derive it.


⚠ A MISSING REPORT IS NOT A SLIGHTLY-WORSE ROW, IT IS A MIXTURE OF DATES. Measured 2026-07-29:
Rendement 44/44, Vermogensoverzicht 31/44 — so thirteen accounts rendered this week's return
beside last week's holdings, and nothing on screen said which figure came from when. Every number
was real; only their combination was fiction, which is precisely the failure that survives a
glance.

The two decisions worth pinning are both about ABSENCE, because that is where this can go wrong
quietly: "we have not measured yet" must not read as "incomplete" (it would blank the page), and
"the report returned nothing" must not read as "the report failed" (it would hide the quietest,
healthiest books).
"""
from __future__ import annotations

import pytest

from airs_vermogen import REPORTS
from routers import _airs_accounts as A

ALL = list(REPORTS)


class _Q:
    """Minimal PostgREST stand-in — only the calls `_complete_accounts` makes."""

    def __init__(self, rows: list[dict] | Exception):
        self._rows = rows

    def select(self, *_a, **_k):
        return self

    def order(self, *_a, **_k):
        return self

    def limit(self, *_a, **_k):
        return self

    def execute(self):
        if isinstance(self._rows, Exception):
            raise self._rows
        return type("R", (), {"data": self._rows})()


def _wire(monkeypatch, rows):
    monkeypatch.setattr(A, "supabase", type("S", (), {"table": staticmethod(lambda _n: _Q(rows))})())


class TestOnlyWholeAccountsAreListed:
    NOW, EARLIER = "2026-07-29T11:00:00Z", "2026-07-22T11:00:00Z"

    def test_all_four_reports_is_complete(self, monkeypatch):
        _wire(monkeypatch, [{"portefeuille": "BUS_A", "reports_ok": ALL, "reports_at": self.NOW}])
        assert A._complete_accounts() == {"bus_a"}

    def test_one_missing_report_is_not(self, monkeypatch):
        """The 13 accounts from the measurement: Rendement fine, Vermogensoverzicht absent."""
        _wire(monkeypatch, [
            {"portefeuille": "BUS_A", "reports_ok": ALL, "reports_at": self.NOW},
            {"portefeuille": "BUS_B", "reports_ok": ["att", "mut", "model"], "reports_at": self.NOW},
        ])
        assert A._complete_accounts() == {"bus_a"}

    def test_an_extra_unknown_report_does_not_disqualify(self, monkeypatch):
        """A fifth report added later must not retire every account recorded before it."""
        _wire(monkeypatch, [{"portefeuille": "BUS_A", "reports_ok": [*ALL, "future"],
                             "reports_at": self.NOW}])
        assert A._complete_accounts() == {"bus_a"}

    def test_a_stale_verdict_does_not_count_as_complete(self, monkeypatch):
        """⚠ Only the NEWEST batch stamp counts. An account the latest refresh never reached keeps
        an older `reports_at` — it must not coast on last week's clean bill."""
        _wire(monkeypatch, [
            {"portefeuille": "BUS_A", "reports_ok": ALL, "reports_at": self.NOW},
            {"portefeuille": "BUS_OLD", "reports_ok": ALL, "reports_at": self.EARLIER},
        ])
        assert A._complete_accounts() == {"bus_a"}


class TestAbsenceIsNeverAnAssertion:
    def test_no_verdict_at_all_means_do_not_filter(self, monkeypatch):
        """⚠ `None`, NOT an empty set. On the deploy that adds the columns nothing has been
        measured, and treating that as "no account is whole" empties the portfolios page over a
        question never asked."""
        _wire(monkeypatch, [{"portefeuille": "BUS_A", "reports_ok": None, "reports_at": None}])
        assert A._complete_accounts() is None

    def test_an_empty_table_means_do_not_filter(self, monkeypatch):
        _wire(monkeypatch, [])
        assert A._complete_accounts() is None

    def test_a_read_failure_means_do_not_filter(self, monkeypatch):
        """A missing column or a transient error must show one row too many, never zero — the
        same rule `_hidden_accounts` and `_live_accounts` already follow."""
        _wire(monkeypatch, RuntimeError("column reports_ok does not exist"))
        assert A._complete_accounts() is None

    def test_a_refresh_where_nothing_came_back_whole_IS_an_empty_set(self, monkeypatch):
        """Different fact, different value: a refresh DID run and no account was complete. That
        must filter (to nothing), because it is a measurement, not a gap."""
        _wire(monkeypatch, [{"portefeuille": "BUS_A", "reports_ok": ["att"],
                             "reports_at": "2026-07-29T11:00:00Z"}])
        assert A._complete_accounts() == set()


class TestTheReportSetIsNotDuplicated:
    def test_the_gate_reads_the_refresh_s_own_list(self):
        """Two copies would drift the moment a fifth report is added — and the drift would show up
        as accounts silently missing from the page, not as an error.

        ⚠ THIS TEST USED TO END `assert set(REPORTS) == {"att", "volk", "mut", "model"}` — i.e. IT
        WAS THE SECOND COPY IT WARNS ABOUT, and it drifted exactly as predicted the day `trans` (a
        fifth report) was added. A literal here does not pin the gate to the list; it pins the list
        to a date. What is actually worth asserting is that there is ONE definition and the gate
        reads it, which is below and cannot rot.
        """
        import inspect

        assert "REPORTS" in inspect.getsource(A._complete_accounts)
        assert len(set(REPORTS)) == len(REPORTS), f"a report is listed twice: {REPORTS}"

    def test_an_account_is_complete_only_with_EVERY_report(self, monkeypatch):
        """The gate's behaviour, derived from `REPORTS` rather than restated — so adding a sixth
        report changes what this test demands without anyone editing it."""
        stamp = "2026-07-29T11:00:00Z"
        _wire(monkeypatch, [{"portefeuille": "BUS_A", "reports_ok": ALL, "reports_at": stamp},
                            *[{"portefeuille": f"SHORT_{r}", "reports_at": stamp,
                               "reports_ok": [x for x in ALL if x != r]} for r in REPORTS]])
        assert A._complete_accounts() == {"bus_a"}


class TestRetrievedIsNotTheSameAsNonEmpty:
    """⚠ THE REASON THE OUTCOME IS RECORDED RATHER THAN DERIVED.

    `_save_mutaties` returns a ROW COUNT, and a book with no transactions this year legitimately
    stores zero. Inferring "the report worked" from `rows > 0` would hide exactly the quiet,
    healthy accounts — so the refresh sets its own flag where the fetch succeeded.
    """

    def _wire(self, monkeypatch, *, mut_rows=0, model_rows=0, trans_rows=0, volk_raises=False):
        """Drive `scan_one` with every download stubbed — no network, no database.

        ⚠⚠ "EVERY" HAS TO MEAN EVERY, AND FOR A WHILE IT DID NOT. `REPORTS` gained a fifth leg
        (`trans`) when Transacties shipped on 2026-08-05 and this stub was never extended — so
        `_trans` ran for real, launching Playwright against live AirSPMS. It passed on a dev
        machine (browser installed, BROKER_* in .env.local) and failed on CI with
        `BrowserType.launch: Executable doesn't exist`, which is the worst possible split: the
        suite is green exactly where nobody is watching it.

        ⚠ AND THE FAILURE WAS NOT "TRANSACTIES IS MISSING". It cost `trans` from `reports_ok` AND
        added a second entry to `errors`, so the two assertions that broke were about the OTHER
        four reports — a missing stub reading as a bug in unrelated behaviour.

        ⚠ THE LAZY IMPORT IN `_trans` IS WHY PATCHING THE MODULE WORKS. It does
        `from routers._airs_transacties import _fetch_live, _store, ytd_window` INSIDE the
        function, so the attributes resolve at call time and a `setattr` here is seen. Patching
        `airs_vermogen` instead would do nothing.
        """
        import airs_scanner
        import portfolio

        import airs_vermogen as V
        from routers import _airs_transacties as T
        from routers import airs as R

        monkeypatch.setattr(T, "_fetch_live",
                            lambda *a, **k: type("Sheet", (), {"rows": [{}] * trans_rows})())
        monkeypatch.setattr(T, "_store", lambda *a, **k: None)

        monkeypatch.setattr(airs_scanner, "download_portfolio_sync", lambda *a, **k: b"x",
                            raising=False)
        monkeypatch.setattr(portfolio, "parse_airs_excel", lambda *a, **k: [], raising=False)
        monkeypatch.setattr(R, "_parse_att_excel", lambda *a, **k: [], raising=False)
        monkeypatch.setattr(R, "_save_performance_to_db", lambda *a, **k: None, raising=False)

        def _volk(*_a, **_k):
            if volk_raises:
                raise RuntimeError("no valued Vermogensoverzicht in the last 7 days")
            return "2026-07-28", b"x"

        monkeypatch.setattr(V, "_vermogen_most_recent", _volk)
        monkeypatch.setattr(V, "_save_holdings", lambda *a, **k: 12)
        monkeypatch.setattr(V, "_save_mutaties", lambda *a, **k: mut_rows)
        monkeypatch.setattr(V, "_save_model_weights", lambda *a, **k: model_rows)
        return V

    def test_zero_rows_still_counts_as_retrieved(self, monkeypatch):
        """A book with no transactions this year returns a valid EMPTY Mutaties report. If the
        verdict were inferred from the row count, the quietest healthy accounts would be the ones
        flagged as broken."""
        V = self._wire(monkeypatch, mut_rows=0, model_rows=0)
        res = V.scan_one("BUS_X", "2026-01-01", "2026-07-29")
        assert res["reports_ok"] == list(V.REPORTS)      # all four, despite two returning 0 rows
        assert res["mutaties"] == 0 and res["model_weights"] == 0
        assert res["errors"] == []

    def test_a_failed_report_drops_only_itself(self, monkeypatch):
        """The whole point of four independent steps: a book's dividends are worth having even
        when its valuation is unavailable."""
        V = self._wire(monkeypatch, mut_rows=7, volk_raises=True)
        res = V.scan_one("BUS_X", "2026-01-01", "2026-07-29")
        assert "volk" not in res["reports_ok"]
        assert {"att", "mut", "model"} <= set(res["reports_ok"])
        assert res["mutaties"] == 7
        # ⚠ STRUCTURED, so the fleet run can group 27 failures by CAUSE rather than regex-ing a
        # message it formatted itself one line earlier — see `summarise_errors`.
        assert len(res["errors"]) == 1
        assert res["errors"][0]["report"] == "Vermogensoverzicht"
        assert res["errors"][0]["account"] == "BUS_X"
        assert "no valued" in res["errors"][0]["message"]


class TestOneImplementation:
    """⚠ "Refresh all" AND the per-row "Refresh" MUST BE THE SAME CODE. They were two copies of the
    same four downloads and had already drifted — only one recorded which reports arrived, so a
    per-row retry could not clear the badge a fleet scan had set."""

    def test_both_entry_points_delegate_to_scan_one(self):
        import inspect

        import airs_vermogen as V

        for fn in (V.run_airs_vermogen_refresh_sync, V.refresh_one_portfolio):
            src = inspect.getsource(fn)
            assert "scan_one(" in src, fn.__name__
            # No second copy of the downloads hiding in either caller.
            assert "download_portfolio_sync" not in src, fn.__name__

    def test_scan_one_does_not_take_the_lock(self):
        """Both callers hold `_LOCK` already; taking it here would deadlock the fleet run against
        itself on its first account."""
        import inspect

        import airs_vermogen as V

        # Assert on the ACQUISITION, not the identifier — the docstring names `_LOCK` on purpose,
        # since it is the hazard being warned about.
        assert "_LOCK.acquire" not in inspect.getsource(V.scan_one)
        assert "_acquire_session" not in inspect.getsource(V.scan_one)
        # ...and both callers genuinely do take it, or nothing serialises the AirSPMS session.
        # ⚠ EITHER SPELLING COUNTS. `refresh_one_portfolio` now goes through `_acquire_session`
        # (which is `_LOCK.acquire` with an optional wait, added so a full refresh can QUEUE for
        # the session instead of abandoning a portfolio half-done). The property under test is
        # "this caller takes the session", not which of the two names it typed.
        for fn in (V.run_airs_vermogen_refresh_sync, V.refresh_one_portfolio):
            src = inspect.getsource(fn)
            assert "_LOCK.acquire" in src or "_acquire_session(" in src, fn.__name__

    def test_the_session_helper_is_the_only_way_in(self):
        """⚠ EVERY AirSPMS SCRAPE PASSES THROUGH ONE GATE, because there is one session and two
        threads driving it do not error — they interleave into each other's downloads.

        `_composition` (the model half, in `routers/_airs_portfolio_refresh`) took NO lock until
        2026-08-18 and said so in `routers/airs.py`: a documented, accepted gap, survivable only
        while exactly one human pressed one button. `refresh_many` fans out over both halves, so
        it stopped being survivable.
        """
        import inspect

        import airs_vermogen as V
        from routers import _airs_portfolio_refresh as R

        assert V._acquire_session(None) is True, "the lock was already held by something"
        V._LOCK.release()
        src = inspect.getsource(R._composition)
        assert "_acquire_session(" in src and "_LOCK.release()" in src

    def test_waiting_and_refusing_are_both_available(self):
        """`None` refuses at once — a BUTTON must answer, and "another refresh is running" is a
        true answer a person can act on. A number queues, for a caller already mid-job whose only
        other option is to leave half a portfolio refreshed."""
        import airs_vermogen as V

        assert V._acquire_session(None) is True
        try:
            # Held: the non-blocking form refuses rather than parking the caller.
            assert V._acquire_session(None) is False
            # ...and the waiting form gives up rather than hanging for ever.
            assert V._acquire_session(0.01) is False
        finally:
            V._LOCK.release()


class TestTheMarkerNamesTheGap:
    """What the list actually renders: not "hidden", but "here, and short THIS report"."""

    NOW = "2026-07-29T11:00:00Z"

    def test_a_whole_account_has_no_entry(self, monkeypatch):
        _wire(monkeypatch, [{"portefeuille": "BUS_A", "reports_ok": ALL, "reports_at": self.NOW}])
        assert A._missing_reports() == {}

    def test_it_names_exactly_what_did_not_arrive(self, monkeypatch):
        """The 13 accounts from the measurement: Rendement fine, Vermogensoverzicht absent.

        ⚠ THE MISSING REPORT IS REMOVED FROM `REPORTS`, NOT SPELLED OUT BESIDE IT. Listing the
        three that DID arrive is a second copy of the report set: when `trans` was added this read
        `{"bus_b": ["volk"]}` against an answer of `["volk", "trans"]` — a red test for a correct
        change, because the fixture claimed an account had every report but one and no longer did.
        """
        _wire(monkeypatch, [{"portefeuille": "BUS_B", "reports_at": self.NOW,
                             "reports_ok": [r for r in ALL if r != "volk"]}])
        assert A._missing_reports() == {"bus_b": ["volk"]}

    def test_several_missing_come_back_in_report_order(self, monkeypatch):
        """Display order, so two rows short of the same pair read identically.

        ⚠ `REPORTS`'s ORDER IS THE ASSERTION — `sorted()` here would pass while the page rendered
        them alphabetically, which is the one thing this test exists to prevent."""
        _wire(monkeypatch, [{"portefeuille": "BUS_C", "reports_ok": ["att"], "reports_at": self.NOW}])
        assert A._missing_reports()["bus_c"] == [r for r in REPORTS if r != "att"]

    def test_a_never_measured_account_is_not_reported_as_missing(self, monkeypatch):
        """⚠ Absence of evidence is not evidence of a gap — the same rule the filter had. A row
        with no verdict would otherwise wear a warning badge on the deploy that added the column."""
        _wire(monkeypatch, [{"portefeuille": "BUS_A", "reports_ok": None, "reports_at": None}])
        assert A._missing_reports() == {}

    def test_a_read_failure_marks_nothing(self, monkeypatch):
        # A missing column must not badge all 44 accounts as broken.
        _wire(monkeypatch, RuntimeError("column reports_ok does not exist"))
        assert A._missing_reports() == {}

    def test_an_older_scan_still_reports_what_it_found(self, monkeypatch):
        """⚠⚠ THIS ASSERTION WAS REVERSED (2026-08-19), so both sides are on the record.

        It used to demand `== {}` — "only the newest batch counts; an account the last scan never
        reached must not carry last week's badge". The intent was sound: a verdict from a stale
        scan might not describe the account now.

        What it cost was the badge itself. `airs_account_roster` is ONE ROW PER ACCOUNT with a
        per-account timestamp, so "the newest batch" is ONE account — measured on live data, 1 of
        45 rows matched, leaving 44 unbadgeable, and refreshing any single account silently cleared
        every other row's warning. The badge says "this account's LAST SCAN did not retrieve X",
        which is true whenever that scan happened; suppressing it makes a short row look whole,
        which is the more expensive mistake."""
        _wire(monkeypatch, [
            {"portefeuille": "BUS_A", "reports_ok": ALL, "reports_at": self.NOW},
            {"portefeuille": "BUS_OLD", "reports_ok": ["att"], "reports_at": "2026-07-22T11:00:00Z"},
        ])
        assert A._missing_reports() == {"bus_old": [c for c in ALL if c != "att"]}


@pytest.mark.parametrize("missing", ALL)
def test_every_single_missing_report_disqualifies(monkeypatch, missing):
    """No report is optional — including the two the old status message never mentioned."""
    _wire(monkeypatch, [{"portefeuille": "BUS_A",
                         "reports_ok": [r for r in ALL if r != missing],
                         "reports_at": "2026-07-29T11:00:00Z"}])
    assert A._complete_accounts() == set()


class TestEachAccountIsJudgedOnItsOwnScan:
    """⚠⚠ THE BUG THIS PINS, AS REPORTED: "⚠ Vermogensoverzicht is behind almost every portfolio,
    and when I refresh a single one nothing really gets fetched but all those warnings disappear."

    `_missing_reports` took the newest `reports_at` in the WHOLE table and skipped every row that
    did not match it exactly. `airs_account_roster` holds ONE ROW PER ACCOUNT, each stamped when
    that account was scanned — measured on the live data: 46 accounts, 7 distinct timestamps, and
    exactly 1 of 45 rows matched the newest. So 44 accounts could never be flagged, and refreshing
    any one account made ITS stamp the newest, which silently un-flagged everyone else.

    A global maximum cannot answer a per-row question.
    """

    OLD = "2026-08-17T13:15:11+00:00"
    NEW = "2026-08-18T07:30:19+00:00"

    def test_an_account_scanned_earlier_still_reports_its_gap(self, monkeypatch):
        # B was scanned later and is complete; A's gap must survive that.
        _wire(monkeypatch, [
            {"portefeuille": "BUS_B", "reports_ok": ALL, "reports_at": self.NEW},
            {"portefeuille": "BUS_A", "reports_ok": [c for c in ALL if c != "volk"],
             "reports_at": self.OLD},
        ])
        assert A._missing_reports() == {"bus_a": ["volk"]}

    def test_refreshing_ONE_account_does_not_clear_everyone_else(self, monkeypatch):
        """The exact reported symptom, as a before/after on the same data."""
        short = [c for c in ALL if c != "volk"]
        before = [
            {"portefeuille": "BUS_A", "reports_ok": short, "reports_at": self.OLD},
            {"portefeuille": "BUS_B", "reports_ok": short, "reports_at": self.OLD},
        ]
        _wire(monkeypatch, before)
        assert set(A._missing_reports()) == {"bus_a", "bus_b"}

        # Now BUS_C is refreshed on its own — a newer stamp, and it retrieved everything.
        after = [{"portefeuille": "BUS_C", "reports_ok": ALL, "reports_at": self.NEW}, *before]
        _wire(monkeypatch, after)
        # ⚠ A and B are untouched by C's refresh, so their badges must be untouched too.
        assert set(A._missing_reports()) == {"bus_a", "bus_b"}

    def test_a_complete_account_gets_no_entry_at_all(self, monkeypatch):
        # ⚠ Absent, not an empty list: the UI renders on truthiness, and `[]` would badge a row
        # with an empty gap list.
        _wire(monkeypatch, [{"portefeuille": "BUS_A", "reports_ok": ALL, "reports_at": self.NEW}])
        assert "bus_a" not in A._missing_reports()

    def test_the_newest_row_wins_if_an_account_ever_has_two(self, monkeypatch):
        # One row per account today. If that becomes a history, the OLD scan's gaps must not
        # resurface — the query orders `reports_at desc`, and the first row per account is kept.
        _wire(monkeypatch, [
            {"portefeuille": "BUS_A", "reports_ok": ALL, "reports_at": self.NEW},
            {"portefeuille": "BUS_A", "reports_ok": [c for c in ALL if c != "volk"],
             "reports_at": self.OLD},
        ])
        assert "bus_a" not in A._missing_reports()
