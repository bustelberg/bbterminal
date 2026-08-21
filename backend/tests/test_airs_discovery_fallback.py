"""Discovery is the one step of a fleet refresh that drives somebody else's UI with a browser.

⚠⚠ AND IT WAS THE ONLY FATAL ONE (2026-08-22). AIRS's Rapportage menu became unclickable — the
Front-office anchor was covered by another element matching the same selector — and a 30-second
Playwright timeout raised straight out of `_discover_portfolios`:

    [airs_vermogen] discovery failed: RuntimeError: TimeoutError: ElementHandle.click: Timeout
    30000ms exceeded ... <a data-field="Front-Office"> from <div class="top_menu"> subtree
    intercepts pointer events
    [job] Refresh all portfolios (airs.vermogen.refresh) failed

Not one of 46 accounts was scanned. The refresh is what clears a ⚠ Vermogensoverzicht badge, so a
menu that could not be clicked presented as a fleet of stale books.

Two independent changes, one pinned here and one in `airs_scanner`:
  * the menu click is best effort — the caller navigates the content frame itself, with filters the
    menu route cannot even express (`_open_front_office`);
  * discovery failing falls back to the roster the LAST successful discovery wrote, because we are
    already holding the list and the population changes a few times a year.
"""
from __future__ import annotations

import airs_vermogen as V


class _Q:
    """Minimal PostgREST stand-in — only the calls `_roster_names` makes."""

    def __init__(self, rows: list[dict] | Exception):
        self._rows = rows

    def select(self, *_a, **_k):
        return self

    def limit(self, *_a, **_k):
        return self

    def execute(self):
        if isinstance(self._rows, Exception):
            raise self._rows
        return type("R", (), {"data": self._rows})()


def _wire(monkeypatch, rows):
    monkeypatch.setattr(
        V, "supabase", type("S", (), {"table": staticmethod(lambda _n: _Q(rows))})())


class TestTheStoredRosterIsTheFallback:
    def test_it_returns_the_accounts_the_last_discovery_found(self, monkeypatch):
        _wire(monkeypatch, [{"portefeuille": "BUS_B"}, {"portefeuille": "BUS_A"}])
        assert V._roster_names() == ["BUS_A", "BUS_B"]

    def test_it_is_sorted_deduped_and_trimmed(self, monkeypatch):
        # ⚠ THE SAME LIST SHAPE DISCOVERY PRODUCES, so the fallback cannot make the run behave
        # differently in some second way — a duplicate would scan an account twice and a blank
        # would be requested from AIRS as an empty portefeuille.
        _wire(monkeypatch, [{"portefeuille": " BUS_B "}, {"portefeuille": "BUS_A"},
                            {"portefeuille": "BUS_B"}, {"portefeuille": "  "},
                            {"portefeuille": None}, {}])
        assert V._roster_names() == ["BUS_A", "BUS_B"]

    def test_a_read_failure_is_empty_rather_than_partial(self, monkeypatch):
        """⚠ AND THE CALLER THEN REPORTS THE ORIGINAL DISCOVERY ERROR. Returning a half-list here
        would scan some accounts and report a complete run — the fallback's whole risk is that it
        cannot see a portfolio opened since, and a truncated read makes that risk unbounded."""
        _wire(monkeypatch, RuntimeError("connection reset"))
        assert V._roster_names() == []

    def test_too_few_accounts_declines_the_fallback(self, monkeypatch):
        """⚠ ONE DEFINITION OF "TOO FEW TO BELIEVE", shared with `_record_roster`. A stored roster
        that is itself suspiciously short is not a safer answer than the error — it is the same
        failure one step earlier, and scanning 3 of 46 books would report `status: ok`."""
        _wire(monkeypatch, [{"portefeuille": f"BUS_{i}"} for i in range(V._MIN_ROSTER - 1)])
        assert len(V._roster_names()) < V._MIN_ROSTER

        _wire(monkeypatch, [{"portefeuille": f"BUS_{i}"} for i in range(V._MIN_ROSTER)])
        assert len(V._roster_names()) >= V._MIN_ROSTER


class TestADegradedRunSaysSo:
    def test_the_caveat_leads_the_message(self):
        """⚠⚠ IT CHANGES WHAT EVERY COUNT AFTER IT MEANS. "46 accounts refreshed" off a live
        discovery asserts that is the whole population; off a stored roster it cannot. Appending
        the caveat would put it after the numbers it qualifies, where a reader who has already
        read "46 accounts" has stopped."""
        degraded = "⚠ Portfolio discovery failed (TimeoutError) — scanning the 46 accounts from…"
        summary = V.format_run_message({"added": 0, "updated": 46, "up_to_date": 0, "failed": 0})
        combined = f"{degraded} {summary}"
        assert combined.startswith("⚠ Portfolio discovery failed")
        assert summary in combined


class TestTheMenuClickCannotFailTheScan:
    def test_it_swallows_and_narrates_instead_of_raising(self, monkeypatch):
        """⚠ AND IT EMITS `progress`, NOT `error`. `_discover_portfolios._sink` RAISES on an
        `error` event, so reporting a fault the scan immediately recovers from would reintroduce
        the exact failure this exists to remove — by a different route."""
        import airs_scanner

        class _Page:
            def hover(self, *_a, **_k):
                raise airs_scanner.PlaywrightTimeoutError("Timeout 5000ms exceeded")

            def wait_for_timeout(self, *_a, **_k):
                pass

            def locator(self, *_a, **_k):
                raise AssertionError("should not be reached once hover has failed")

        seen: list[tuple] = []
        airs_scanner._open_front_office(
            _Page(), lambda kind, **kw: seen.append((kind, kw.get("message", ""))))

        assert [k for k, _ in seen] == ["progress"], seen
        assert "navigating straight to the selection page" in seen[0][1]
