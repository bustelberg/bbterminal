"""What the per-portfolio refresh JOB says it did — and the cancel it used to call a failure.

⚠⚠ THE REPORTED SYMPTOM (2026-09-03): "I tried to cancel a refresh but this happened —
`RuntimeError: BUS_Offensief_Dyn: AIRS scan failed — no reports returned`". Nothing had failed and
AIRS had not been asked anything. `refresh_portfolio_fully` stops at THREE boundaries and only one
of them can put a flag on the book result:

    before the book half   -> `full["cancelled_at"]`, and `full["book"]` is None
    inside the cascade     -> `full["book"]["cancelled_at"]`, naming the dependent book
    before the model half  -> `full["cancelled_at"]`, with a fully refreshed book beside it

The job body read the middle one alone. For the other two it fell through to `book or {}`, found
no `status == "ok"`, and raised — with an empty `errors` list, which is exactly why the message
could only reach its "no reports returned" fallback. A cancel rendered as a red vendor fault is
the worst of both: it hides that the button worked, and it invents an outage that did not happen.

⚠ WHY THESE TESTS CAN EXIST AT ALL. The wording was a closure inside the route, so covering it
meant standing up a job registry, an AirSPMS session and a database — which is to say it was not
covered. `job_verdict` is a pure function of one dict; these are plain dicts and no fake.

⚠ WHAT IS NOT PINNED HERE: WHERE the cancel is honoured. An account's four reports are downloaded
and stored as a unit and a press mid-scan waits for that unit — a deliberate constraint, argued in
`test_airs_refresh_cancel.py`. These tests are about what the card SAYS, not when it stops.
"""
from __future__ import annotations

import pytest

from jobs import JobCancelled
from routers._airs_full_refresh import job_verdict

BOOK = "BUS_Offensief_Dyn"


def _ok_book(**over) -> dict:
    return {"status": "ok", "portefeuille": BOOK, "holdings_rows": 32,
            "as_of": "2026-09-03", "cascaded": [], "stale_books": [],
            "cancelled_at": None, "errors": [], "reports_ok": ["att", "volk"], **over}


class TestACancelIsReportedAsACancel:
    def test_stopped_before_the_book_half_is_not_a_scan_failure(self):
        """⚠⚠ THE REPORTED BUG. Nothing was read, so there is no book result to carry the flag."""
        full = {"status": "cancelled", "cancelled_at": BOOK, "book": None,
                "book_status": "absent", "model_status": "absent"}
        with pytest.raises(JobCancelled) as e:
            job_verdict(BOOK, full)
        assert "nothing was read" in str(e.value)

    def test_and_it_is_not_a_runtimeerror(self):
        """The exact regression: `RuntimeError` paints the card RED and names a vendor fault.

        Asserted separately from the message because the TYPE is what the registry reads to decide
        amber against red — a JobCancelled whose text happened to say "cancelled" would still be
        the fix, and a RuntimeError whose text said so would not.
        """
        full = {"status": "cancelled", "cancelled_at": BOOK, "book": None,
                "book_status": "absent", "model_status": "absent"}
        with pytest.raises(JobCancelled):
            job_verdict(BOOK, full)

    def test_stopped_inside_the_cascade_names_the_books_left_stale(self):
        book = _ok_book(status="cancelled", cancelled_at="KID2",
                        stale_books=["KID2", "KID3"])
        with pytest.raises(JobCancelled) as e:
            job_verdict(BOOK, {"book": book, "book_status": "cancelled",
                               "model_status": "absent"})
        assert "cancelled before KID2" in str(e.value)
        assert "KID2, KID3" in str(e.value)

    def test_stopped_before_the_model_half_still_says_the_book_was_stored(self):
        """⚠ It used to return the ordinary success summary, so a reader who pressed Stop got a
        GREEN card and no statement that the model portfolio had been skipped."""
        full = {"status": "cancelled", "cancelled_at": "model 2015", "book": _ok_book(),
                "book_status": "ok", "model_status": "skipped"}
        with pytest.raises(JobCancelled) as e:
            job_verdict(BOOK, full)
        assert "model 2015" in str(e.value)
        assert "book was refreshed" in str(e.value)

    def test_the_books_own_flag_outranks_the_wrappers(self):
        """Both set: the book's is the more specific answer — it names what is stale."""
        book = _ok_book(status="cancelled", cancelled_at="KID1", stale_books=["KID1"])
        with pytest.raises(JobCancelled) as e:
            job_verdict(BOOK, {"book": book, "cancelled_at": "model 9",
                               "book_status": "cancelled", "model_status": "skipped"})
        assert "KID1" in str(e.value)
        assert "model 9" not in str(e.value)


class TestAFailureSaysWhichLayerFailed:
    def test_report_errors_are_listed(self):
        book = _ok_book(status="error", reports_ok=[],
                        errors=["Rendement: RuntimeError: Response too small"])
        with pytest.raises(RuntimeError) as e:
            job_verdict(BOOK, {"book": book, "book_status": "error", "model_status": "absent"})
        assert "Response too small" in str(e.value)

    def test_no_errors_at_all_is_its_own_finding(self):
        """⚠ Every report `scan_one` runs records a reason when it fails, so an EMPTY list means
        the scan did not run. "no reports returned" read as AIRS answering with nothing, which is
        a different fault in a different system from the one that actually occurred."""
        with pytest.raises(RuntimeError) as e:
            job_verdict(BOOK, {"book": {"status": "error", "errors": []},
                               "book_status": "error", "model_status": "absent"})
        msg = str(e.value)
        assert "no report errors were recorded" in msg
        assert "book status error" in msg
        assert "no reports returned" not in msg


class TestTheOrdinaryOutcomes:
    def test_busy_is_an_answer_not_a_failure(self):
        out = job_verdict(BOOK, {"book": {"status": "busy"}, "book_status": "busy",
                                 "model_status": "absent"})
        assert "another AIRS refresh is running" in out

    def test_a_clean_run_reports_its_holdings_and_date(self):
        out = job_verdict(BOOK, {"book": _ok_book(), "book_status": "ok",
                                 "model_status": "ok"})
        assert "32 holdings as of 2026-09-03" in out
        assert "model portfolio rebuilt" in out

    def test_a_failed_dependency_downgrades_the_summary(self):
        book = _ok_book(cascaded=[{"portefeuille": "KID1", "status": "error"},
                                  {"portefeuille": "KID2", "status": "ok"}])
        out = job_verdict(BOOK, {"book": book, "book_status": "ok", "model_status": "absent"})
        assert "also refreshed 2 book(s)" in out
        assert "1 FAILED" in out

    def test_a_model_half_that_did_not_rebuild_is_named(self):
        out = job_verdict(BOOK, {"book": _ok_book(), "book_status": "ok",
                                 "model_status": "error"})
        assert "NOT rebuilt (error)" in out
