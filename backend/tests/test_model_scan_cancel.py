"""Cancelling the model-portfolio scan — phase two of the portfolios page's "Refresh all".

⚠⚠ THE BUG THIS PINS PRESENTED AS "REFRESH ALL IS SILENT, AND CANCEL DOESN'T WORK". That button is
two operations in sequence: the account scan (a job since 2026-08-13 — toast, `i/n`, working
Cancel) and then this, the model-portfolio scan, which streamed straight into `console.warn`. For
the MINUTES it runs — an edit-page GET plus an XLS download for each of ~58 fixed portfolios — the
only thing on screen was the button's own label, and there was nothing to press to stop it.

Two things had to become true for it to be a job, and both are here:

  1. It must be STOPPABLE at a boundary that leaves the data consistent — `should_stop`.
  2. Its progress must be READABLE AS DATA, not parsed back out of a sentence — `i`/`n` on the
     `count` event, which is what drives the toast's bar.

⚠ THE BOUNDARY IS BETWEEN PORTFOLIOS, NEVER INSIDE ONE. A portfolio's XLS is downloaded, counted
and persisted as a unit (`on_positions` writes the very rows the count was taken from). Stopping
midway would leave a row with positions stored and no count, or a count standing against positions
that were never written — and nothing on the row to say which. Between rows every portfolio is
either fully done or untouched.
"""
from __future__ import annotations

import airs_scanner as scanner


def _portfolios(n: int) -> list[dict]:
    """`n` portfolios that all have a fixed model, so every one of them is real work."""
    return [{"id": 100 + i, "name": f"BUS_MODEL_{i}", "fixed": "fixed (0)"} for i in range(n)]


def _rig(monkeypatch, *, fails: set[int] | None = None):
    """Run the counter against a fake AIRS. Returns (events, fetched_ids, saved_ids)."""
    fetched: list[int] = []
    saved: list[int] = []
    events: list[tuple[str, dict]] = []

    def _fetch(portfolio_id: int):
        fetched.append(portfolio_id)
        if fails and portfolio_id in fails:
            raise RuntimeError("AIRS said no")
        return {"datum": "2026-08-01", "dates": ["2026-08-01"],
                "rows": [{"ISINCode": "US0378331005"}, {"ISINCode": "NL0011794037"},
                         # Cash — no ISIN, not an instrument. Present so the count under
                         # cancellation is a real count and not a row tally.
                         {"ISINCode": None}]}

    monkeypatch.setattr(scanner, "fetch_portfolio_positions_sync", _fetch)
    return events, fetched, saved


class TestItStopsBetweenPortfolios:

    def test_no_hook_means_it_runs_to_the_end(self, monkeypatch):
        """⚠ THE DEFAULT MUST BE UNCHANGED. The scheduler's two ticks and
        `scripts/add_portfolio_isins.py` all call this with no `should_stop`."""
        events, fetched, saved = _rig(monkeypatch)
        rows = _portfolios(5)
        scanner.count_model_portfolio_holdings_sync(
            rows, lambda kind, **kw: events.append((kind, kw)),
            on_positions=lambda pid, *_a: saved.append(pid))
        assert len(fetched) == 5
        assert all(p["holdings"] == 2 for p in rows)

    def test_it_stops_before_the_next_portfolio(self, monkeypatch):
        """Stop after two: the third must never be FETCHED at all. A hook honoured only after the
        download is a hook that costs the reader the very minutes they pressed Cancel to save."""
        events, fetched, saved = _rig(monkeypatch)
        rows = _portfolios(6)
        scanner.count_model_portfolio_holdings_sync(
            rows, lambda kind, **kw: events.append((kind, kw)),
            on_positions=lambda pid, *_a: saved.append(pid),
            should_stop=lambda: len(fetched) >= 2)
        assert fetched == [100, 101], "it kept downloading after the stop was requested"

    def test_everything_already_counted_is_kept(self, monkeypatch):
        """⚠ A CANCEL IS NOT A ROLLBACK. Two portfolios were really read and really stored; the
        rest keep whatever a previous scan left on them, which is why the event names the count."""
        events, fetched, saved = _rig(monkeypatch)
        rows = _portfolios(6)
        scanner.count_model_portfolio_holdings_sync(
            rows, lambda kind, **kw: events.append((kind, kw)),
            on_positions=lambda pid, *_a: saved.append(pid),
            should_stop=lambda: len(fetched) >= 2)
        assert saved == [100, 101]
        assert [p.get("holdings") for p in rows] == [2, 2, None, None, None, None]

    def test_a_cancel_before_the_first_one_reads_nothing(self, monkeypatch):
        events, fetched, _saved = _rig(monkeypatch)
        scanner.count_model_portfolio_holdings_sync(
            _portfolios(4), lambda kind, **kw: events.append((kind, kw)),
            should_stop=lambda: True)
        assert fetched == []

    def test_the_cancellation_names_what_is_left(self, monkeypatch):
        """"Cancelled" with no count is indistinguishable from "cancelled before it started"."""
        events, fetched, _saved = _rig(monkeypatch)
        scanner.count_model_portfolio_holdings_sync(
            _portfolios(6), lambda kind, **kw: events.append((kind, kw)),
            should_stop=lambda: len(fetched) >= 2)
        stop = [kw for kind, kw in events if kind == "cancelled"]
        assert len(stop) == 1
        assert stop[0]["i"] == 2 and stop[0]["n"] == 6
        assert stop[0]["account"] == "BUS_MODEL_2"


class TestTheProgressIsDataNotProse:
    """⚠ THE JOB WRAPPER TURNS THESE INTO THE TOAST'S BAR. Regex-ing `2/58` back out of a message
    we formatted one line earlier is the same mistake `summarise_errors` exists to avoid — and it
    breaks silently the first time a portfolio's name contains a slash."""

    def test_every_count_event_carries_its_position(self, monkeypatch):
        events, _f, _s = _rig(monkeypatch)
        scanner.count_model_portfolio_holdings_sync(
            _portfolios(3), lambda kind, **kw: events.append((kind, kw)))
        counts = [kw for kind, kw in events if kind == "count"]
        assert [(c["i"], c["n"]) for c in counts] == [(1, 3), (2, 3), (3, 3)]

    def test_the_denominator_is_the_FIXED_portfolios_only(self, monkeypatch):
        """⚠ NOT `len(portfolios)`. A `normaal`/`meervoudig` book has no composition to count and
        is skipped entirely — a bar denominated on all 95 would stall at 58/95 for ever and read as
        a scan that died with a third of the work outstanding."""
        events, fetched, _s = _rig(monkeypatch)
        rows = _portfolios(2) + [{"id": 900, "name": "BENCH", "fixed": "normaal"},
                                 {"id": 901, "name": "WRAP", "fixed": "meervoudig"}]
        scanner.count_model_portfolio_holdings_sync(
            rows, lambda kind, **kw: events.append((kind, kw)))
        assert fetched == [100, 101]
        assert {kw["n"] for kind, kw in events if kind == "count"} == {2}

    def test_a_failed_portfolio_still_advances_the_bar(self, monkeypatch):
        """⚠ ONE BAD PORTFOLIO MUST NOT STALL THE COUNTER. It is reported (`error` on the event,
        `holdings` left unset — never 0) and the scan moves on, so the bar keeps meaning "how far
        through the list are we" rather than "how many succeeded"."""
        events, _f, _s = _rig(monkeypatch, fails={101})
        rows = _portfolios(3)
        scanner.count_model_portfolio_holdings_sync(
            rows, lambda kind, **kw: events.append((kind, kw)))
        counts = [kw for kind, kw in events if kind == "count"]
        assert [(c["i"], c["n"]) for c in counts] == [(1, 3), (2, 3), (3, 3)]
        assert counts[1]["error"] and counts[1]["holdings"] is None
        assert "holdings" not in rows[1], "a count we failed to take was written as a number"
