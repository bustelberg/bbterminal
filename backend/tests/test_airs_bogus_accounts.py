"""Which books the fleet scan is allowed to skip — and, more importantly, for how long.

⚠⚠ THE SKIP IS A COST DECISION AND IT HAD BECOME A PERMANENT EXEMPTION. A book under
`MIN_REAL_HOLDINGS` is not re-downloaded, which saves ~60 downloads a run on benchmarks and shells
nobody opens. But a book that is NEVER re-read has an `as_of` that can never move, so:

  * its row wears the amber "N trading days old" badge for ever;
  * `lagOwner` reports the lag as OURS — the one verdict that tells a reader a Refresh fixes it;
  * and every Refresh skips it again.

Measured 2026-08-17: 45 accounts on the page, 27 amber. Twenty-three were AIRS's own lag (read that
same afternoon, AIRS had simply published nothing newer). The other FOUR were the `BUS_BM_*`
benchmarks — one holding each, last read 2026-07-30, twelve trading days earlier. They were the only
rows a refresh could have fixed and the only rows every refresh refused to touch, which is exactly
what "Refresh all says everything is up to date yet we have stale info icons" looks like from here.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airs_vermogen import MIN_REAL_HOLDINGS, bogus_accounts

NOW = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
FRESH = (NOW - timedelta(days=2)).isoformat()
STALE = (NOW - timedelta(days=18)).isoformat()      # 2026-07-30, the measured case
FULL = ["att", "volk", "mut", "model"]


def _verdicts(**rows: str) -> dict[str, dict]:
    """{account: last read at}, all of them having yielded a Vermogensoverzicht."""
    return {name: {"reports_ok": FULL, "reports_at": at} for name, at in rows.items()}


class TestTheSizeRule:
    def test_a_small_book_read_recently_is_skipped(self):
        v = _verdicts(bench=FRESH)
        assert bogus_accounts({"bench": 1}, v, now=NOW, visible={"bench"}) == {"bench"}

    def test_a_real_book_is_never_skipped(self):
        v = _verdicts(real=FRESH)
        assert bogus_accounts({"real": MIN_REAL_HOLDINGS}, v, now=NOW, visible={"real"}) == set()

    def test_a_book_we_have_never_fetched_holdings_for_is_unknown_not_empty(self):
        """⚠ ZERO AND UNKNOWN LOOK IDENTICAL IN `counts`. Without `volk` in the roster, an absent
        count means "never asked" — skipping it would strand a new account for ever, since it could
        never acquire the holdings that would rescue it."""
        v = {"newbook": {"reports_ok": ["att"], "reports_at": FRESH}}
        assert bogus_accounts({}, v, now=NOW, visible={"newbook"}) == set()


class TestTheSkipIsBounded:
    def test_a_small_book_unread_for_a_fortnight_is_re_admitted(self):
        """The four `BUS_BM_*` benchmarks, in one assertion."""
        v = _verdicts(bench=STALE)
        assert bogus_accounts({"bench": 1}, v, now=NOW, visible={"bench"}) == set()

    def test_the_clock_can_be_switched_off_for_the_pure_size_question(self):
        v = _verdicts(bench=STALE)
        assert bogus_accounts({"bench": 1}, v, now=NOW, visible={"bench"},
                              max_stale_hours=None) == {"bench"}

    def test_a_book_with_no_read_timestamp_is_not_re_admitted_by_the_clock(self):
        """It cannot be stale-by-time if we have no time for it; `volk` already proved we fetched
        it once, so the size answer stands."""
        v = {"bench": {"reports_ok": FULL, "reports_at": None}}
        assert bogus_accounts({"bench": 1}, v, now=NOW, visible={"bench"}) == {"bench"}

    def test_an_unparseable_timestamp_does_not_re_admit_the_whole_fleet(self):
        """⚠ A BAD VALUE MUST NOT QUIETLY UNDO THE SAVING. Treating "not a date" as "very old" would
        re-admit every skipped book on every run and nothing on screen would say why the scan got
        slower."""
        v = {"bench": {"reports_ok": FULL, "reports_at": "not-a-date"}}
        assert bogus_accounts({"bench": 1}, v, now=NOW, visible={"bench"}) == {"bench"}


class TestOnlyBooksAReaderCanSee:
    def test_an_invisible_stale_book_stays_skipped(self):
        """⚠⚠ THE JUSTIFICATION FOR RE-READING IS THAT A ROW SHOWS A BADGE NOBODY CAN CLEAR — so a
        book with no row buys nothing. Without this, the fix re-admitted fourteen books at once
        (measured), including `wts test 1-4 fx` and the `_MV` shells."""
        v = _verdicts(ghost=STALE)
        assert bogus_accounts({"ghost": 0}, v, now=NOW, visible=set()) == {"ghost"}

    def test_with_no_visibility_list_the_age_rule_applies_to_all(self):
        """`visible=None` means "we could not read the list". Failing toward doing the work matches
        `_roster_verdicts`, which scans everything when the roster read fails."""
        v = _verdicts(ghost=STALE)
        assert bogus_accounts({"ghost": 0}, v, now=NOW, visible=None) == set()

    def test_visibility_does_not_rescue_a_book_that_is_merely_small(self):
        """Being on the page is not a reason to re-read a book we read this morning."""
        v = _verdicts(bench=FRESH)
        assert bogus_accounts({"bench": 1}, v, now=NOW, visible={"bench"}) == {"bench"}


class TestKeying:
    def test_counts_may_be_keyed_by_the_accounts_real_case(self):
        """⚠ `airs_holding.portefeuille` keeps AIRS's own casing while the skip set is lower-cased.
        A lookup that missed would read every book as zero-holdings and skip the entire fleet —
        measured while diagnosing this: 45 of 46 books "bogus" against a documented 15."""
        v = {"BUS_FTS_OFF_DYN": {"reports_ok": FULL, "reports_at": FRESH}}
        assert bogus_accounts({"BUS_FTS_OFF_DYN": 24}, v, now=NOW,
                              visible={"bus_fts_off_dyn"}) == set()
