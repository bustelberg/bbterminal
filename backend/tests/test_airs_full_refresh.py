"""ONE refresh function, both halves, every button.

⚠⚠ THE REPORTED SYMPTOM (2026-08-18): "there should be a single function which we call to fully
refresh a single portfolio, each refresh button needs to make use of the same function."

A portfolio is a PAIR in AIRS — the Fixed model (weights, ISINs, an effective date) and the
Dynamic book (real positions, real money) — and each had its own refresh path:

    /management-dashboard row   ->  refresh_one_portfolio   ->  the BOOK
    /portfolios row             ->  refresh_portfolio       ->  the MODEL
    the Analyse modal           ->  whichever panel opened it

So the same-looking button did different work depending on where the reader came from, and no
button on /management-dashboard could refresh a model at all. Neither half's docstring was wrong;
there was simply no object called "a portfolio refresh" for either of them to be half of.

⚠ WHAT THESE TESTS PIN IS THE COMPOSITION, NOT THE SCRAPING. Both halves are stubbed — their own
behaviour is covered by `test_airs_refresh_cancel.py` and `test_airs_portfolio_refresh.py`. What
can only be got wrong HERE is: both halves run, one failing does not cancel the other, an absent
half is reported rather than assumed, and the verdict is the worse of the two.
"""
from __future__ import annotations

import pytest

from routers import _airs_full_refresh as F

BOOK = "AITopSelectie OFF DYN"
MODEL_ID = 2015


def _book_result(name: str = BOOK, status: str = "ok") -> dict:
    return {"status": status, "portefeuille": name, "holdings_rows": 20, "as_of": "2026-08-15",
            "cascaded": [], "stale_books": [], "cancelled_at": None, "errors": [],
            "reports_ok": ["att", "volk"]}


@pytest.fixture
def halves(monkeypatch):
    """Both halves stubbed, with a record of which ran and with what."""
    calls: list[tuple[str, object]] = []

    def _book(portefeuille, cascade=True, on_step=None, should_stop=None, wait=None):
        calls.append(("book", portefeuille))
        return _book_result(portefeuille)

    def _model(portfolio_id, emit, wait=None):
        calls.append(("model", portfolio_id))
        emit("phase", phase="composition", message="1/5 Composition")
        return {"portfolio_id": portfolio_id, "ytd_pct": 44.1, "composition_datum": "2025-12-30"}

    monkeypatch.setattr("airs_vermogen.refresh_one_portfolio", _book)
    monkeypatch.setattr("routers._airs_portfolio_refresh.refresh_portfolio", _model)
    monkeypatch.setattr(F, "_pair", lambda pf, pid: (pf or BOOK, pid if pid is not None else MODEL_ID))
    return calls


class TestBothHalvesRun:
    def test_from_either_handle(self, halves):
        """⚠ THE TWO BUTTONS HOLD DIFFERENT HANDLES — the account row knows a `portefeuille`, the
        model row an `id`. Requiring one of them is how they ended up as two functions."""
        F.refresh_portfolio_fully(portefeuille=BOOK)
        assert halves == [("book", BOOK), ("model", MODEL_ID)]

        halves.clear()
        F.refresh_portfolio_fully(portfolio_id=MODEL_ID)
        assert halves == [("book", BOOK), ("model", MODEL_ID)]

    def test_the_book_comes_first(self, halves):
        """Not cosmetic: the model half re-prices, and doing that before re-reading the book would
        price a composition against holdings we are about to replace."""
        F.refresh_portfolio_fully(portefeuille=BOOK)
        assert [c[0] for c in halves] == ["book", "model"]

    def test_the_verdict_is_ok_only_when_both_are(self, halves):
        out = F.refresh_portfolio_fully(portefeuille=BOOK)
        assert out["status"] == "ok"
        assert out["book_status"] == "ok"
        assert out["model_status"] == "ok"


class TestOneHalfFailingDoesNotLoseTheOther:
    """⚠ THEY READ DIFFERENT SOURCES. AIRS being down says nothing about Yahoo, and stopping after
    a failed book scan would leave the half that WAS available stale for no reason at all."""

    def test_a_failed_book_still_runs_the_model(self, monkeypatch, halves):
        def _boom(*_a, **_k):
            raise RuntimeError("AirSPMS said no")
        monkeypatch.setattr("airs_vermogen.refresh_one_portfolio", _boom)

        out = F.refresh_portfolio_fully(portefeuille=BOOK)
        assert ("model", MODEL_ID) in halves
        assert out["book_status"] == "error"
        assert out["model_status"] == "ok"
        # ⚠ AND THE WHOLE THING IS NOT "ok". Half a refresh reported as a refresh is the claim
        # this module exists to stop.
        assert out["status"] == "error"

    def test_a_failed_model_does_not_discard_the_book(self, monkeypatch, halves):
        def _boom(*_a, **_k):
            raise RuntimeError("Yahoo returned nothing")
        monkeypatch.setattr("routers._airs_portfolio_refresh.refresh_portfolio", _boom)

        out = F.refresh_portfolio_fully(portefeuille=BOOK)
        assert out["book"]["holdings_rows"] == 20, "the book's stored work was thrown away"
        assert out["model_status"] == "error"
        assert out["status"] == "error"

    def test_a_busy_session_is_not_an_error(self, monkeypatch, halves):
        """`busy` is an ANSWER — the fleet scan holds the session, try again — and painting it red
        beside the real failures is how a reader learns to ignore both."""
        monkeypatch.setattr("airs_vermogen.refresh_one_portfolio",
                            lambda *a, **k: {"status": "busy", "portefeuille": BOOK})
        out = F.refresh_portfolio_fully(portefeuille=BOOK)
        assert out["status"] == "busy"


class TestAnAbsentHalfIsReportedNotAssumed:
    """⚠ 18 OF 51 ACCOUNTS HAVE NO MODEL, and a model can exist with no account running it. That
    is a normal state, not a failure — but it must not be reported as a completed half."""

    def test_a_book_with_no_model(self, monkeypatch, halves):
        monkeypatch.setattr(F, "_pair", lambda pf, pid: (BOOK, None))
        out = F.refresh_portfolio_fully(portefeuille=BOOK)
        assert [c[0] for c in halves] == ["book"]
        assert out["model_status"] == "absent"
        assert out["status"] == "ok", "an absent half must not drag the verdict down"

    def test_a_model_with_no_book(self, monkeypatch, halves):
        monkeypatch.setattr(F, "_pair", lambda pf, pid: (None, MODEL_ID))
        out = F.refresh_portfolio_fully(portfolio_id=MODEL_ID)
        assert [c[0] for c in halves] == ["model"]
        assert out["book_status"] == "absent"
        assert out["status"] == "ok"

    def test_neither_is_an_error_and_says_so(self, monkeypatch, halves):
        monkeypatch.setattr(F, "_pair", lambda pf, pid: (None, None))
        out = F.refresh_portfolio_fully(portefeuille="NOT_A_BOOK")
        assert out["status"] == "error"
        assert "no such portfolio" in out["message"]


class TestCancelStopsBetweenHalvesOnly:
    """Same rule the cascade follows: an account's reports — and a composition-and-reprice — are
    downloaded and stored as a UNIT. Stopping inside one leaves a book holding two fresh reports
    and two stale ones with nothing on it to say which."""

    def test_pressed_before_anything_ran(self, halves):
        out = F.refresh_portfolio_fully(portefeuille=BOOK, should_stop=lambda: True)
        assert halves == []
        assert out["status"] == "cancelled"

    def test_pressed_between_the_halves_keeps_the_book(self, halves):
        n = {"i": 0}

        def _stop() -> bool:
            n["i"] += 1
            return n["i"] > 1          # False at the book boundary, True at the model's

        out = F.refresh_portfolio_fully(portefeuille=BOOK, should_stop=_stop)
        assert [c[0] for c in halves] == ["book"]
        assert out["book"]["holdings_rows"] == 20, "the book's own scan was discarded"
        assert out["status"] == "cancelled"
        assert out["model_status"] == "skipped"


class TestTheTwoHooksAnswerTwoQuestions:
    """A JOB wants a bar; an SSE STREAM wants the frames. Collapsing the second onto the first was
    the first thing tried here and it silently cost the /portfolios console its bold phase lines —
    a formatting loss with no error anywhere to find it by."""

    def test_the_bar_has_one_denominator_across_both_halves(self, halves):
        steps: list[tuple[int, int]] = []
        F.refresh_portfolio_fully(portefeuille=BOOK, on_step=lambda d, t, _m: steps.append((d, t)))
        totals = {t for _d, t in steps}
        assert totals == {6}, f"the two halves disagree about the denominator: {totals}"
        # ⚠ MONOTONIC. Each half owned the bar before, so a full refresh ran 0->100% twice, which
        # on screen is indistinguishable from the job having restarted.
        assert [d for d, _t in steps] == sorted(d for d, _t in steps)
        assert steps[-1] == (6, 6)

    def test_the_stream_keeps_each_halfs_own_frame_kinds(self, halves):
        frames: list[tuple[str, str]] = []
        F.refresh_portfolio_fully(
            portefeuille=BOOK,
            on_event=lambda kind, **kw: frames.append((kind, str(kw.get("message") or ""))))
        kinds = [k for k, _m in frames]
        assert "phase" in kinds, "the model half's phase frames were flattened to progress"
        # The composition line the model half emits arrives verbatim, still a `phase`.
        assert ("phase", "1/5 Composition") in frames

    def test_a_caller_may_pass_neither(self, halves):
        """Nothing is required to watch. The scheduler and any script call this with no hooks."""
        assert F.refresh_portfolio_fully(portefeuille=BOOK)["status"] == "ok"


class TestTheBulkPathIsTheSingleOne:
    """⚠ `refresh_many` ADDS A THREAD POOL AND NOTHING ELSE. A second bulk implementation is the
    mistake `scan_one`'s docstring records having already been made one layer down — two copies of
    "scan an account" that had drifted, so only one of them recorded which reports arrived."""

    def test_every_portfolio_goes_through_the_single_function(self, monkeypatch, halves):
        seen: list[str] = []
        monkeypatch.setattr(F, "refresh_portfolio_fully",
                            lambda portefeuille=None, **k: (seen.append(portefeuille),
                                                            {"portefeuille": portefeuille,
                                                             "status": "ok"})[1])
        out = F.refresh_many(["A", "B", "C"], concurrency=2)
        assert sorted(seen) == ["A", "B", "C"]
        assert [r["status"] for r in out] == ["ok", "ok", "ok"]

    def test_results_come_back_in_the_order_asked_for(self, monkeypatch, halves):
        """The pool is unordered; the RESULTS must not be, or a caller pairing them with its own
        list by index reports one portfolio's outcome under another's name."""
        monkeypatch.setattr(F, "refresh_portfolio_fully",
                            lambda portefeuille=None, **k: {"portefeuille": portefeuille,
                                                            "status": "ok"})
        out = F.refresh_many(["A", "B", "C"], concurrency=3)
        assert [r["portefeuille"] for r in out] == ["A", "B", "C"]

    def test_one_throwing_does_not_abandon_the_rest(self, monkeypatch, halves):
        def _maybe(portefeuille=None, **_k):
            if portefeuille == "B":
                raise RuntimeError("that book will not scan")
            return {"portefeuille": portefeuille, "status": "ok"}
        monkeypatch.setattr(F, "refresh_portfolio_fully", _maybe)

        out = F.refresh_many(["A", "B", "C"], concurrency=2)
        assert [r["status"] for r in out] == ["ok", "error", "ok"]
        assert "that book will not scan" in out[1]["message"]

    def test_the_cascade_is_off_for_a_sweep(self, monkeypatch, halves):
        """⚠ THE ASYMMETRY IS DELIBERATE. A single press cascades so the books behind a
        certificate are re-read too; a sweep reaches those on their own turn, and leaving it on
        would pull nine accounts for TOPS_BEOFF_BEH_DYN alone at four downloads each."""
        kw: list[dict] = []
        monkeypatch.setattr(F, "refresh_portfolio_fully",
                            lambda **k: (kw.append(k), {"status": "ok"})[1])
        F.refresh_many(["A"], concurrency=1)
        assert kw[0]["cascade"] is False


class TestOneHalfOnly:
    """`halves` — the scope the 05:00 tick runs at.

    ⚠⚠ IT IS WHAT MAKES THAT HOUR SAFE. Nothing that scrapes the AIRS accounts may run before AIRS
    has valued the books: the fleet job forces and fires once, so an early pass stores YESTERDAY's
    valuation and nothing re-reads it until tomorrow — holdings a full day behind that look
    perfectly current. The MODEL half has no such hazard (a composition is a dated set of weights;
    its other steps talk to OpenFIGI, the ECB and Yahoo), so it is the half that can run early.

    ⚠ A SKIPPED HALF IS NOT AN ABSENT ONE. "We chose not to" and "there was none" are different
    facts, and the verdict must not read either as a failure.
    """

    def test_model_only_never_touches_the_book(self, halves):
        out = F.refresh_portfolio_fully(portefeuille=BOOK, halves=("model",))
        assert [c[0] for c in halves] == ["model"]
        assert out["book_status"] == "skipped"
        assert out["model_status"] == "ok"
        assert out["status"] == "ok", "a deliberate skip must not drag the verdict down"

    def test_book_only_never_touches_the_model(self, halves):
        out = F.refresh_portfolio_fully(portefeuille=BOOK, halves=("book",))
        assert [c[0] for c in halves] == ["book"]
        assert out["model_status"] == "skipped"
        assert out["status"] == "ok"

    def test_skipped_and_absent_are_told_apart(self, monkeypatch, halves):
        # No model exists at all -> absent. A model exists and we chose not to -> skipped.
        monkeypatch.setattr(F, "_pair", lambda pf, pid: (BOOK, None))
        assert F.refresh_portfolio_fully(portefeuille=BOOK)["model_status"] == "absent"
        monkeypatch.setattr(F, "_pair", lambda pf, pid: (BOOK, MODEL_ID))
        assert F.refresh_portfolio_fully(
            portefeuille=BOOK, halves=("book",))["model_status"] == "skipped"

    def test_a_failure_in_the_half_that_RAN_still_fails_the_verdict(self, monkeypatch, halves):
        # ⚠ The skip must not become a way for a real failure to read as ok.
        def _boom(*_a, **_k):
            raise RuntimeError("Yahoo returned nothing")
        monkeypatch.setattr("routers._airs_portfolio_refresh.refresh_portfolio", _boom)
        out = F.refresh_portfolio_fully(portefeuille=BOOK, halves=("model",))
        assert out["book_status"] == "skipped"
        assert out["model_status"] == "error"
        assert out["status"] == "error"

    def test_the_bar_counts_only_the_half_that_runs(self, halves):
        steps: list[tuple[int, int]] = []
        F.refresh_portfolio_fully(portefeuille=BOOK, halves=("model",),
                                  on_step=lambda d, t, _m: steps.append((d, t)))
        assert {t for _d, t in steps} == {5}, "the book's step is still in the denominator"

    def test_the_fan_out_passes_the_scope_through(self, monkeypatch, halves):
        kw: list[dict] = []
        monkeypatch.setattr(F, "refresh_portfolio_fully",
                            lambda **k: (kw.append(k), {"status": "ok"})[1])
        F.refresh_many(["A", "B"], halves=("model",), concurrency=2)
        assert all(k["halves"] == ("model",) for k in kw)
