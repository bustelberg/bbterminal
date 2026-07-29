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

    def _wire(self, monkeypatch, *, mut_rows=0, model_rows=0, volk_raises=False):
        """Drive `scan_one` with every download stubbed — no network, no database."""
        import airs_scanner
        import portfolio

        import airs_vermogen as V
        from routers import airs as R

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
        assert len(res["errors"]) == 1 and "Vermogensoverzicht" in res["errors"][0]


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
        # ...and both callers genuinely do take it, or nothing serialises the AirSPMS session.
        for fn in (V.run_airs_vermogen_refresh_sync, V.refresh_one_portfolio):
            assert "_LOCK.acquire" in inspect.getsource(fn), fn.__name__


class TestTheMarkerNamesTheGap:
    """What the list actually renders: not "hidden", but "here, and short THIS report"."""

    NOW = "2026-07-29T11:00:00Z"

    def test_a_whole_account_has_no_entry(self, monkeypatch):
        _wire(monkeypatch, [{"portefeuille": "BUS_A", "reports_ok": ALL, "reports_at": self.NOW}])
        assert A._missing_reports() == {}

    def test_it_names_exactly_what_did_not_arrive(self, monkeypatch):
        # The 13 accounts from the measurement: Rendement fine, Vermogensoverzicht absent.
        _wire(monkeypatch, [{"portefeuille": "BUS_B", "reports_ok": ["att", "mut", "model"],
                             "reports_at": self.NOW}])
        assert A._missing_reports() == {"bus_b": ["volk"]}

    def test_several_missing_come_back_in_report_order(self, monkeypatch):
        """Display order, so two rows short of the same pair read identically."""
        _wire(monkeypatch, [{"portefeuille": "BUS_C", "reports_ok": ["att"], "reports_at": self.NOW}])
        assert A._missing_reports()["bus_c"] == ["volk", "mut", "model"]

    def test_a_never_measured_account_is_not_reported_as_missing(self, monkeypatch):
        """⚠ Absence of evidence is not evidence of a gap — the same rule the filter had. A row
        with no verdict would otherwise wear a warning badge on the deploy that added the column."""
        _wire(monkeypatch, [{"portefeuille": "BUS_A", "reports_ok": None, "reports_at": None}])
        assert A._missing_reports() == {}

    def test_a_read_failure_marks_nothing(self, monkeypatch):
        # A missing column must not badge all 44 accounts as broken.
        _wire(monkeypatch, RuntimeError("column reports_ok does not exist"))
        assert A._missing_reports() == {}

    def test_a_stale_verdict_is_ignored(self, monkeypatch):
        """Only the newest batch counts, exactly as for `_complete_accounts` — an account the last
        scan never reached must not carry last week's badge."""
        _wire(monkeypatch, [
            {"portefeuille": "BUS_A", "reports_ok": ALL, "reports_at": self.NOW},
            {"portefeuille": "BUS_OLD", "reports_ok": ["att"], "reports_at": "2026-07-22T11:00:00Z"},
        ])
        assert A._missing_reports() == {}


@pytest.mark.parametrize("missing", ALL)
def test_every_single_missing_report_disqualifies(monkeypatch, missing):
    """No report is optional — including the two the old status message never mentioned."""
    _wire(monkeypatch, [{"portefeuille": "BUS_A",
                         "reports_ok": [r for r in ALL if r != missing],
                         "reports_at": "2026-07-29T11:00:00Z"}])
    assert A._complete_accounts() == set()
