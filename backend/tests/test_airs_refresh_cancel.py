"""Cancelling ONE portfolio's re-scan — the button that used to change nothing.

⚠⚠ THIS REVERSES A DOCUMENTED REFUSAL (2026-08-13). `airs_portfolio_refresh_job` passed no
`should_stop` and argued that stopping mid-cascade leaves the parent fresh against stale children,
"the state this endpoint exists to avoid". The argument proves too much — `cascade=False` produces
that same state deliberately, and it is a supported mode — while what the refusal actually bought
was a Cancel offered in THREE places (the row, the Analyse modal, the toast itself) that stopped
nothing: the card read "cancelling…" for the remaining minutes and then went green with `done`.

A control that does nothing is worse than a documented compromise, because it is not local: it
teaches the reader that Cancel is decorative on every other card too. So the scan stops between
accounts and the outcome NAMES what it left behind.

⚠ BETWEEN ACCOUNTS, NEVER INSIDE ONE. An account's reports are downloaded and stored as a unit;
stopping halfway would leave a book holding two fresh reports and two stale ones with nothing to
say which — the failure the `_LOCK`-and-unit design exists to prevent, and the one real constraint
that survives.
"""
from __future__ import annotations

import pytest

import airs_vermogen as V


def _scan_result(name: str) -> dict:
    return {"reports_ok": ["att", "volk"], "holdings": 3, "as_of": "2026-08-13",
            "mutaties": 0, "model_weights": 0, "transacties": 0, "errors": [],
            "portefeuille": name}


@pytest.fixture
def fleet(monkeypatch):
    """PARENT with three books behind it, and a record of every account actually downloaded."""
    scanned: list[str] = []

    def _scan_one(name, _van, _tot):
        scanned.append(name)
        return _scan_result(name)

    monkeypatch.setattr(V, "dependent_accounts", lambda _p: ["KID1", "KID2", "KID3"])
    monkeypatch.setattr(V, "scan_one", _scan_one)
    monkeypatch.setattr(V, "_record_reports", lambda *_a, **_k: None)
    return scanned


class TestCancelActuallyStops:
    def test_a_press_before_the_first_download_reads_nothing_at_all(self, fleet):
        """The misclick case, and the only boundary where stopping costs literally nothing —
        `dependent_accounts` is a lookup, so not one AIRS request has been made yet."""
        res = V.refresh_one_portfolio("PARENT", should_stop=lambda: True)

        assert fleet == []
        assert res["status"] == "cancelled"
        assert res["cancelled_at"] == "PARENT"
        # ⚠ THE BOOKS IT DID NOT REACH ARE NAMED. A cancelled refresh whose outcome does not say
        # what is now stale is indistinguishable on screen from one that finished.
        assert res["stale_books"] == ["KID1", "KID2", "KID3"]

    def test_it_stops_between_children_and_keeps_what_it_stored(self, fleet):
        """Pressed while the second book is queued: the parent and the first child are already
        downloaded and stored, and abandoning them would throw away the work the press was
        supposed to interrupt, not undo."""
        calls = {"n": 0}

        def _stop() -> bool:
            # False at the parent boundary and at KID1; True from KID2 on.
            calls["n"] += 1
            return calls["n"] > 2

        res = V.refresh_one_portfolio("PARENT", should_stop=_stop)

        assert fleet == ["PARENT", "KID1"]
        assert res["status"] == "cancelled"
        assert res["cancelled_at"] == "KID2"
        assert res["stale_books"] == ["KID2", "KID3"]
        # The parent's own scan is reported, not discarded — it is stored either way.
        assert res["holdings_rows"] == 3
        assert [c["portefeuille"] for c in res["cascaded"]] == ["KID1"]

    def test_cancelled_outranks_ok(self, fleet):
        """⚠ THE PARENT'S OWN REPORTS ARE FRESH, so `ok` is defensible on its own terms and
        completely misleading: the books its look-through figures are computed FROM were not
        re-read. One word for "we stopped"."""
        res = V.refresh_one_portfolio("PARENT", should_stop=lambda: True)
        assert res["status"] != "ok"


class TestNothingChangesWithoutTheHook:
    def test_no_should_stop_scans_the_whole_chain(self, fleet):
        """The plain POST and the scheduler call this with no hook at all; the default must be the
        behaviour they already had."""
        res = V.refresh_one_portfolio("PARENT")

        assert fleet == ["PARENT", "KID1", "KID2", "KID3"]
        assert res["status"] == "ok"
        assert res["cancelled_at"] is None
        assert res["stale_books"] == []

    def test_a_hook_that_never_fires_changes_nothing(self, fleet):
        res = V.refresh_one_portfolio("PARENT", should_stop=lambda: False)

        assert fleet == ["PARENT", "KID1", "KID2", "KID3"]
        assert res["status"] == "ok"

    def test_the_lock_is_released_on_the_cancel_path(self, fleet):
        """⚠ THE SCAN MUST REACH ITS OWN `finally`, which is why the job passes a FLAG rather than
        calling `ctx.check()` inside. Unwinding by exception would leave the shared AirSPMS session
        locked against every later refresh — a cancel that breaks the button for good."""
        V.refresh_one_portfolio("PARENT", should_stop=lambda: True)

        assert V._LOCK.acquire(blocking=False), "the AirSPMS lock was not released"
        V._LOCK.release()


class TestTheJobEndsCancelledNotDone:
    """`_work` returning a string — however carefully worded — is a `done` job to the registry, so
    the toast goes green and only the summary says otherwise. The status is what the card is
    coloured and labelled from."""

    def test_the_registry_carries_the_workers_own_account_of_where_it_stopped(self):
        """A cancelled card's whole value is what it left behind; `JobCancelled`'s message becomes
        the summary so that detail survives, while a bare raise still reads "cancelled"."""
        import jobs as job_registry

        assert job_registry.JobCancelled("cancelled before KID2").args[0] == "cancelled before KID2"
        assert str(job_registry.JobCancelled()) == ""
