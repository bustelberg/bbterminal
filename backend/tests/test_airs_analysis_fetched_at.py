"""The Analyse modal must carry the SECOND date, or it cannot agree with the row that opened it.

⚠⚠ THE REPORTED SYMPTOM (2026-08-18): "this row shows it's up to date but when I click Analyse I
see different out of date numbers, and a refresh should get a portfolio fully up to date so these
rows and modal numbers cannot be out of sync."

They were reading the SAME dates and reaching OPPOSITE verdicts. `Provenance` needs two facts to
colour its badge — `as_of` (the day AIRS valued the book) and `fetched_at` (the moment we last read
it) — because only their combination says whose lag a stale date is. The overview row passes both,
so it stays quiet on AIRS's own batch lag. The modal's payload carried only the first, so
`lagOwner` returned null, `stale` could not rule out that the gap was ours, and every ⓘ inside the
modal went amber on a book the row called current.

⚠ AND NO REFRESH COULD CLEAR IT, which is the part that makes this worse than a wrong colour. A
refresh updates `airs_account_roster.reports_at` — exactly the fact that would have silenced the
badge — and the modal never received it. Pressing the button changed nothing visible, so the
button read as broken.

Measured on AITopSelectie OFF DYN, straight out of the database:

    model composition (`as_of`)        2025-12-30   AIRS has published nothing newer
    book snapshot (`holdings_as_of`)   2026-08-15   AIRS's own valuation date
    we last read it (`fetched_at`)     2026-08-17   a successful scan, two days ago
    row's YTD == modal's YTD           44.138731    the NUMBERS never disagreed

Same class of bug as the one the row was fixed for on 2026-08-17; the modal simply never got the
second date. This pins the loader, not the colour — the colour is `lib/lagOwner.test.ts`.
"""
from __future__ import annotations

from routers import _airs_accounts
from routers._airs_portfolio_analysis import _book_fetched_at
from tests._fake_supabase import FakeSupabase

SCANNED = "2026-08-17T13:26:51.782222+00:00"


def _roster(monkeypatch, rows):
    monkeypatch.setattr(_airs_accounts, "supabase", FakeSupabase({"airs_account_roster": rows}))


class TestItReadsTheSameFactTheRowReads:
    def test_the_books_own_scan_time(self, monkeypatch):
        _roster(monkeypatch, [
            {"portefeuille": "AITopSelectie OFF DYN", "reports_at": SCANNED},
            {"portefeuille": "BUS_Offensief_Dyn", "reports_at": "2026-08-11T09:00:00+00:00"},
        ])
        assert _book_fetched_at("AITopSelectie OFF DYN") == SCANNED

    def test_it_matches_the_row_on_case_and_padding(self, monkeypatch):
        """⚠ `_fetched_at` KEYS ON THE LOWER-CASED, STRIPPED NAME and the payload carries AIRS's
        own casing. A lookup that missed on case would return None — which is not a loud failure
        here, it is the amber badge coming back for the books whose names happen to differ."""
        _roster(monkeypatch, [{"portefeuille": "  AITopSelectie OFF DYN  ", "reports_at": SCANNED}])
        assert _book_fetched_at("aitopselectie off dyn") == SCANNED


class TestItDeclinesRatherThanGuesses:
    """⚠ None IS AN ANSWER — `Provenance` treats it exactly as an absent prop and says nothing
    about whose lag it is, which is the honest outcome when there is no book to have scanned."""

    def test_an_unpaired_portfolio_has_no_book_and_therefore_no_scan(self, monkeypatch):
        _roster(monkeypatch, [{"portefeuille": "AITopSelectie OFF DYN", "reports_at": SCANNED}])
        assert _book_fetched_at(None) is None
        assert _book_fetched_at("") is None

    def test_a_book_we_have_never_scanned(self, monkeypatch):
        _roster(monkeypatch, [{"portefeuille": "SOMETHING_ELSE_DYN", "reports_at": SCANNED}])
        assert _book_fetched_at("AITopSelectie OFF DYN") is None
