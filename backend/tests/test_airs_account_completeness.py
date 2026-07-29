"""An account is listed only when the last refresh retrieved ALL FOUR of its reports.

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
        as accounts silently missing from the page, not as an error."""
        import inspect

        assert "REPORTS" in inspect.getsource(A._complete_accounts)
        assert set(REPORTS) == {"att", "volk", "mut", "model"}


class TestRetrievedIsNotTheSameAsNonEmpty:
    """⚠ THE REASON THE OUTCOME IS RECORDED RATHER THAN DERIVED.

    `_save_mutaties` returns a ROW COUNT, and a book with no transactions this year legitimately
    stores zero. Inferring "the report worked" from `rows > 0` would hide exactly the quiet,
    healthy accounts — so the refresh sets its own flag where the fetch succeeded.
    """

    def test_zero_rows_still_counts_as_retrieved(self):
        import inspect

        from airs_vermogen import refresh_one_portfolio

        src = inspect.getsource(refresh_one_portfolio)
        # The flag is set inside the try, independent of the returned row count.
        assert "mutaties_ok = 1" in src
        assert "model_ok = 1" in src
        # ...and the verdict is built from the flags, never from the counts.
        assert '("mut", mutaties_ok), ("model", model_ok)' in src


@pytest.mark.parametrize("missing", ALL)
def test_every_single_missing_report_disqualifies(monkeypatch, missing):
    """No report is optional — including the two the old status message never mentioned."""
    _wire(monkeypatch, [{"portefeuille": "BUS_A",
                         "reports_ok": [r for r in ALL if r != missing],
                         "reports_at": "2026-07-29T11:00:00Z"}])
    assert A._complete_accounts() == set()
