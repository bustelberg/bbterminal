"""The fleet scan's narration reaches the job's progress line, not just `_STATUS["log"]`.

⚠⚠ THE REPORTED SYMPTOM: "Refresh all" sat on **"starting…" for a long time** before the first
portfolio appeared. Nothing was stuck. `run_airs_vermogen_refresh_sync` calls its `on_step` hook
only inside the ACCOUNT LOOP, and everything before it — a headless-browser sign-in, three menu
navigations, three filters, a paged scrape of the Front-Office list, then the roster and plan reads
— narrates exclusively through `_emit`, which appends to `_STATUS["log"]`.

⚠ THAT LOG USED TO BE POLLED. The portfolios panel read `/vermogen/status` every 2.5s and printed
it. When "Refresh all" became a job (2026-08-13) the poll went away and nothing re-pointed the log
at the toast, so the most detailed narration in this module started going nowhere — and the phase
it covers is the one with no denominator, where a bar cannot say anything either.

`_emit` now forwards to `_PROGRESS`. Two rules that are easy to get wrong and are pinned here:

  * it forwards the LINE and NOT a position, because these phases have no denominator — reporting
    `0/0` from a narration line mid-loop would blank a bar genuinely at 12/44;
  * the sink is module-level (safe: `_LOCK` serialises every writer) and MUST be cleared in the
    same `finally` that releases the lock, or the next run's lines land on a finished job's card.
"""
from __future__ import annotations

import pytest

import airs_vermogen as V


@pytest.fixture
def sink(monkeypatch):
    """Install a recording progress sink, and guarantee the module global is restored."""
    seen: list[tuple[int, int, str]] = []
    monkeypatch.setattr(V, "_PROGRESS", lambda d, t, m: seen.append((d, t, m)))
    monkeypatch.setattr(V, "_PROGRESS_AT", {"done": 0, "total": 0})
    monkeypatch.setattr(V, "_STATUS", dict(V._STATUS, log=[]))
    return seen


class TestEmitReachesTheToast:

    def test_a_narration_line_is_forwarded(self, sink):
        V._emit("discovery", step="login", message="Logged in successfully")
        assert sink == [(0, 0, "Logged in successfully")]

    def test_it_is_stripped(self, sink):
        """`_discover_portfolios` indents its relayed lines for the console log; a toast is one
        line of running text and a leading gap reads as a rendering fault."""
        V._emit("discovery", message="  Clicking Front-office...")
        assert sink[0][2] == "Clicking Front-office..."

    def test_an_event_with_no_message_is_not_forwarded(self, sink):
        """Some entries are structured-only (counts, name lists) — there is nothing to show."""
        V._emit("roster", count=44, names=["a", "b"])
        assert sink == []

    def test_it_still_appends_to_the_log(self, sink):
        """⚠ THE LOG IS NOT REPLACED BY THE TOAST. It is the only place the full roster and the
        per-report ✓/—/✗ breakdown appear, and the panel reads it once at the end."""
        V._emit("discovery", message="page 1: 25 new")
        assert (V._STATUS["log"] or [])[-1]["message"] == "page 1: 25 new"


class TestThePositionIsCarried:
    """⚠ THE BAR MUST NOT FLICKER BACK TO ZERO. `scan_one` narrates each of the four downloads
    under an account, so a mid-loop `_emit` reporting its own `0/0` would reset a bar at 12/44 four
    times per account."""

    def test_emit_reports_the_last_known_position(self, sink):
        V._say(12, 44, "12/44: BUS_X")
        V._emit("report", message="    Vermogensoverzicht: ok — 31 holdings")
        assert sink[-1][:2] == (12, 44)

    def test_say_is_what_moves_it(self, sink):
        V._say(3, 44, "3/44: BUS_A")
        V._say(4, 44, "4/44: BUS_B")
        assert [s[:2] for s in sink] == [(3, 44), (4, 44)]
        assert V._PROGRESS_AT == {"done": 4, "total": 44}

    def test_say_works_with_no_sink_installed(self, monkeypatch):
        """The scheduler and the plain POST call the scan with no hook at all."""
        monkeypatch.setattr(V, "_PROGRESS", None)
        monkeypatch.setattr(V, "_PROGRESS_AT", {"done": 0, "total": 0})
        V._say(1, 2, "no listener")                     # must not raise
        assert V._PROGRESS_AT == {"done": 1, "total": 2}


class TestAReporterNeverBreaksTheScan:
    """⚠ THE WORK IS THE SCAN; THE LINE ON SCREEN IS A COURTESY. A listener that raises must not
    lose a refresh that has already downloaded and stored 30 accounts."""

    def test_a_raising_sink_is_swallowed_by_emit(self, monkeypatch):
        def _boom(_d, _t, _m):
            raise RuntimeError("the client went away")
        monkeypatch.setattr(V, "_PROGRESS", _boom)
        monkeypatch.setattr(V, "_STATUS", dict(V._STATUS, log=[]))
        V._emit("discovery", message="still fine")
        assert (V._STATUS["log"] or [])[-1]["message"] == "still fine"

    def test_a_raising_sink_is_swallowed_by_say(self, monkeypatch):
        def _boom(_d, _t, _m):
            raise RuntimeError("the client went away")
        monkeypatch.setattr(V, "_PROGRESS", _boom)
        monkeypatch.setattr(V, "_PROGRESS_AT", {"done": 0, "total": 0})
        V._say(1, 44, "still fine")
        assert V._PROGRESS_AT["done"] == 1


class TestTheSinkIsClearedWithTheLock:
    """⚠ SET INSIDE THE LOCK HOLD, CLEARED IN THE SAME `finally`. Left dangling, the scheduler's
    05:00 tick — which passes no hook — would push its lines onto whatever toast the last manual
    press left behind."""

    def test_a_completed_run_clears_it(self, monkeypatch):
        monkeypatch.setattr(V, "_discover_portfolios", lambda: [])
        monkeypatch.setattr(V, "_record_roster", lambda _n: None)
        monkeypatch.setattr(V, "_roster_verdicts", lambda: {})
        monkeypatch.setattr(V, "accounts_to_scan",
                            lambda names, v, now, force=False: ([], list(names)))
        V.run_airs_vermogen_refresh_sync(triggered_by="test", on_step=lambda *_a: None)
        assert V._PROGRESS is None

    def test_a_run_that_RAISES_still_clears_it(self, monkeypatch):
        """The discovery failure path returns early; anything past it propagates. Either way the
        `finally` is the only thing standing between a fault and a stuck sink."""
        def _boom():
            raise RuntimeError("AirSPMS unreachable")
        monkeypatch.setattr(V, "_discover_portfolios", _boom)
        V.run_airs_vermogen_refresh_sync(triggered_by="test", on_step=lambda *_a: None)
        assert V._PROGRESS is None

    def test_the_lock_is_released_too(self, monkeypatch):
        monkeypatch.setattr(V, "_discover_portfolios", lambda: [])
        monkeypatch.setattr(V, "_record_roster", lambda _n: None)
        monkeypatch.setattr(V, "_roster_verdicts", lambda: {})
        monkeypatch.setattr(V, "accounts_to_scan",
                            lambda names, v, now, force=False: ([], list(names)))
        V.run_airs_vermogen_refresh_sync(triggered_by="test")
        assert V._LOCK.acquire(blocking=False)
        V._LOCK.release()


class TestThePerRowRefreshNarratesToo:
    """⚠⚠ THE SAME HOLE, ONE LEVEL DOWN, AND IT WAS ONE MISSING ARGUMENT. `scan_one` already
    narrates every download the moment it lands — `on_report`, with per-report timings — and the
    FLEET loop passes it. `refresh_one_portfolio` did not, so a single row's refresh emitted four
    lines total and the toast read "AITopSelectie OFF DYN — scanning AIRS reports" at 0% for the
    whole scan. With the cascade that is up to nine accounts at five downloads each: minutes of one
    unchanging line.
    """

    @staticmethod
    def _rig(monkeypatch, deps: list[str]):
        seen: list[tuple[int, int, str]] = []

        def _scan(name, _van, tot, on_report=None):
            for label in ("Rendement", "Vermogensoverzicht", "Mutaties", "Transacties", "Model"):
                if on_report:
                    on_report(name, label, "ok", "stored (1.2s)")
            return {"reports_ok": list(V.REPORTS), "holdings": 31, "mutaties": 0,
                    "transacties": 0, "model_weights": 0, "as_of": tot, "errors": []}

        monkeypatch.setattr(V, "scan_one", _scan)
        monkeypatch.setattr(V, "dependent_accounts", lambda _p: deps)
        monkeypatch.setattr(V, "_record_reports", lambda *_a, **_k: None)
        return seen

    def test_every_download_narrates(self, monkeypatch):
        seen = self._rig(monkeypatch, [])
        V.refresh_one_portfolio("ACC", on_step=lambda d, t, m: seen.append((d, t, m)))
        lines = [m for _d, _t, m in seen]
        for label in ("Rendement", "Vermogensoverzicht", "Mutaties", "Transacties", "Model"):
            assert any(label in ln for ln in lines), f"{label} was downloaded in silence"

    def test_the_cascade_narrates_per_book(self, monkeypatch):
        seen = self._rig(monkeypatch, ["CHILD_A", "CHILD_B"])
        V.refresh_one_portfolio("ACC", on_step=lambda d, t, m: seen.append((d, t, m)))
        assert any("CHILD_A" in m and "Vermogensoverzicht" in m for _d, _t, m in seen)
        assert any("CHILD_B" in m and "Vermogensoverzicht" in m for _d, _t, m in seen)

    def test_a_per_download_line_does_not_reset_the_bar(self, monkeypatch):
        """⚠ THE POSITION IS CARRIED. Five downloads per account, so a `0/0` from each would drag
        the bar back to the start five times per book."""
        seen = self._rig(monkeypatch, ["CHILD_A"])
        V.refresh_one_portfolio("ACC", on_step=lambda d, t, m: seen.append((d, t, m)))
        after = [(d, t) for d, t, m in seen if "CHILD_A" in m and "Rendement" in m]
        assert after and after[0] == (1, 2), (
            f"a download line reported {after} instead of the account position 1/2")

    def test_it_speaks_before_the_dependency_walk(self, monkeypatch):
        """`dependent_accounts` walks the certificate chain through `resolve_account_isins` per
        book — a lookup, but over up to nine accounts, and it ran before any line was emitted."""
        order: list[str] = []
        monkeypatch.setattr(V, "scan_one", lambda *_a, **_k: {
            "reports_ok": list(V.REPORTS), "holdings": 0, "mutaties": 0, "transacties": 0,
            "model_weights": 0, "as_of": "2026-01-01", "errors": []})
        monkeypatch.setattr(V, "_record_reports", lambda *_a, **_k: None)

        def _deps(_p):
            order.append("<walk>")
            return []
        monkeypatch.setattr(V, "dependent_accounts", _deps)
        V.refresh_one_portfolio("ACC", on_step=lambda _d, _t, m: order.append(m))
        assert order and order.index("<walk>") > 0, "the walk ran before the first line"

    def test_the_sink_is_cleared(self, monkeypatch):
        self._rig(monkeypatch, [])
        V.refresh_one_portfolio("ACC", on_step=lambda *_a: None)
        assert V._PROGRESS is None


class TestTheFirstLineIsImmediate:
    """⚠ THE POINT OF THE WHOLE CHANGE. The press must produce a line before any work, naming the
    slow thing — otherwise the reader's only evidence for a minutes-long browser login is a toast
    that says "starting…"."""

    def test_the_run_narrates_before_it_discovers(self, monkeypatch):
        seen: list[str] = []

        def _discover():
            seen.append("<discovery ran>")
            return []
        monkeypatch.setattr(V, "_discover_portfolios", _discover)
        monkeypatch.setattr(V, "_record_roster", lambda _n: None)
        monkeypatch.setattr(V, "_roster_verdicts", lambda: {})
        monkeypatch.setattr(V, "accounts_to_scan",
                            lambda names, v, now, force=False: ([], list(names)))
        V.run_airs_vermogen_refresh_sync(
            triggered_by="test", on_step=lambda _d, _t, m: seen.append(m))
        assert seen, "the run said nothing at all"
        assert seen.index("<discovery ran>") > 0, (
            "the first progress line came AFTER discovery — the reader watches 'starting…' "
            "through the slowest phase of the run")
        assert "AirSPMS" in seen[0]
