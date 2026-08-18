"""The refresh's one line: what it did to our copy of the fleet.

It used to read "30/44 accounts complete — 0 already current, 44 scanned: Rendement 44/44,
Vermogensoverzicht 44/44 (710 holdings), Mutaties 972 rows, Model 699 rows; 14 report(s) failed" —
five report names, six ratios and two row counts, and still no answer to "did it work". The
breakdown is now log/console detail; the page gets added / updated / already up to date.

Two things have to hold or the sentence lies:
  - "added" is decided against the roster AS IT WAS BEFORE the run, or the run's own writes make
    every account look known and nothing is ever new again;
  - an account that was visited but stored NOTHING is `failed`, not `updated` — it is the number
    somebody reads to decide whether to press the button again.
"""
from __future__ import annotations

from airs_vermogen import count_outcomes, format_run_message

REPORTS_OK = ["att", "volk", "mut", "model"]


class TestItCountsWhatTheRunDid:
    def test_an_account_with_no_roster_row_is_added(self):
        out = count_outcomes([], known=set(), outcomes={"new_book": REPORTS_OK})
        assert out == {"added": 1, "updated": 0, "up_to_date": 0, "failed": 0, "too_small": 0}

    def test_an_account_we_already_had_is_updated(self):
        out = count_outcomes([], known={"old_book"}, outcomes={"old_book": REPORTS_OK})
        assert out["updated"] == 1
        assert out["added"] == 0

    def test_skipped_accounts_are_already_up_to_date(self):
        """Being complete is WHY they were skipped — they are not failures and not work."""
        out = count_outcomes(["a", "b", "c"], known={"a", "b", "c"}, outcomes={})
        assert out == {"added": 0, "updated": 0, "up_to_date": 3, "failed": 0, "too_small": 0}

    def test_an_account_that_stored_nothing_is_failed_not_updated(self):
        """⚠ THE ONE THAT MATTERS. Every report can fail while the account is still visited.
        Counting the visit as an update reports work that did not happen."""
        out = count_outcomes([], known={"old"}, outcomes={"old": []})
        assert out["failed"] == 1
        assert out["updated"] == 0

    def test_a_partial_scan_still_counts_as_work(self):
        """Some reports arrived, so something was written — that is an update, not a failure.
        WHICH reports are short is the row's own badge and the console's business."""
        out = count_outcomes([], known={"old"}, outcomes={"old": ["att"]})
        assert out == {"added": 0, "updated": 1, "up_to_date": 0, "failed": 0, "too_small": 0}

    def test_a_brand_new_account_that_failed_is_not_added(self):
        """Nothing was stored, so nothing was added — "added" must mean rows exist now."""
        out = count_outcomes([], known=set(), outcomes={"new": []})
        assert out == {"added": 0, "updated": 0, "up_to_date": 0, "failed": 1, "too_small": 0}

    def test_the_counts_partition_the_fleet(self):
        """No account may fall between the five buckets — a reader adds them up."""
        skipped = ["s1", "s2"]
        outcomes = {"new": REPORTS_OK, "old": REPORTS_OK, "dead": [], "part": ["volk"]}
        out = count_outcomes(skipped, known={"old", "dead", "part", "s1", "s2"}, outcomes=outcomes)
        assert sum(out.values()) == len(skipped) + len(outcomes)
        assert out == {"added": 1, "updated": 2, "up_to_date": 2, "failed": 1, "too_small": 0}

    def test_books_the_run_never_looked_at_are_counted_too(self):
        """⚠⚠ THE PARTITION HAD QUIETLY STOPPED BEING ONE, AND THAT IS THE WHOLE BUG BEHIND
        "Refresh all says everything is up to date yet the rows show stale".

        `bogus_accounts` drops books under the holdings floor from `todo` AFTER `accounts_to_scan`
        has split the fleet, so they landed in neither `skipped` nor `outcomes` — they were in no
        count at all. Measured 2026-08-17: 45 accounts on the page, 16 dropped, and the summary
        described 29 of them. Four of the missing sixteen had not been read in twelve trading days.
        """
        skipped, small = ["s1"], ["tiny1", "tiny2", "tiny3"]
        outcomes = {"a": REPORTS_OK, "b": REPORTS_OK}
        out = count_outcomes(skipped, known={"a", "b", "s1"}, outcomes=outcomes, small=small)
        assert out["too_small"] == 3
        assert sum(out.values()) == len(skipped) + len(small) + len(outcomes)


class TestTheLineItPrints:
    def test_it_says_only_the_three_counts_when_nothing_failed(self):
        msg = format_run_message(
            {"added": 3, "updated": 41, "up_to_date": 0, "failed": 0})
        assert msg == ("3 portfolios added, 41 re-read, "
                       "0 skipped (we read them within 20h)")

    def test_it_names_no_report_and_no_row_count(self):
        """The whole point: no "Rendement 44/44", no holdings/mutaties/model row totals."""
        msg = format_run_message(
            {"added": 0, "updated": 44, "up_to_date": 0, "failed": 0})
        for jargon in ("Rendement", "Vermogensoverzicht", "Mutaties", "Model", "holdings", "/44"):
            assert jargon not in msg

    def test_failures_are_stated_in_one_word_never_hidden(self):
        """The banner turns amber when reports failed; a colour with no reason beside it tells the
        reader only that something is wrong."""
        msg = format_run_message(
            {"added": 0, "updated": 30, "up_to_date": 0, "failed": 14})
        assert msg.endswith(", 14 failed")   # still last when no valuation date rides along

    def test_a_single_portfolio_is_not_pluralised(self):
        assert format_run_message(
            {"added": 1, "updated": 0, "up_to_date": 0, "failed": 0}
        ).startswith("1 portfolio added")

    def test_zero_counts_still_show_so_the_shape_is_fixed(self):
        """A line that drops its clauses has to be parsed before it can be read."""
        msg = format_run_message({"added": 0, "updated": 0, "up_to_date": 44, "failed": 0})
        assert msg == ("0 portfolios added, 0 re-read, "
                       "44 skipped (we read them within 20h)")


class TestItNeverClaimsTheDATAIsCurrent:
    """⚠⚠ THE LINE SAID "44 ALREADY UP TO DATE" WHILE THE ROWS SAID "3 TRADING DAYS OLD", AND BOTH
    WERE TRUE. Every count here is about OUR COPY — what we fetched and when. The ⓘ on each row
    measures AIRS's VALUATION DATE. Read side by side they are flatly contradictory, and the reader
    is right to believe the pessimistic one.

    Measured 2026-08-17 on `DealmakersTopSelectie Offensief`: fetched 13:15 that day, all five
    reports retrieved, `as_of` 2026-08-12 — complete, current, and three trading days old, all at
    once. AIRS had simply not valued the book since the 12th.

    So the sentence now says what it means, and carries the DATA's own date beside it: a reader who
    sees "newest AIRS valuation 2026-08-15" cannot read the counts as a claim about today.
    """

    def test_the_phrase_up_to_date_is_gone(self):
        msg = format_run_message({"added": 0, "updated": 0, "up_to_date": 44, "failed": 0})
        assert "up to date" not in msg, (
            "the summary claims the DATA is current; it only knows when we last fetched it")

    def test_the_skipped_clause_says_why_they_were_skipped(self):
        msg = format_run_message({"added": 0, "updated": 0, "up_to_date": 44, "failed": 0})
        assert "skipped" in msg and "we read them" in msg

    def test_the_valuation_date_rides_along_when_known(self):
        msg = format_run_message(
            {"added": 3, "updated": 28, "up_to_date": 14, "failed": 0}, "2026-08-15")
        assert msg.endswith("· newest AIRS valuation 2026-08-15")

    def test_it_is_omitted_when_the_run_read_nothing(self):
        """⚠ A SKIP-EVERYTHING RUN LEARNED NO VALUATION DATE. Printing the stored one would state
        a finding this run did not make — the same rule `count_outcomes` follows for `added`."""
        msg = format_run_message({"added": 0, "updated": 0, "up_to_date": 44, "failed": 0})
        assert "AIRS valuation" not in msg

    def test_failed_still_comes_before_the_valuation_date(self):
        """The amber reason must not be pushed past a neutral fact — it is why the banner is amber."""
        msg = format_run_message(
            {"added": 0, "updated": 30, "up_to_date": 0, "failed": 2}, "2026-08-15")
        assert msg.index("2 failed") < msg.index("newest AIRS valuation")

    def test_the_books_it_never_looked_at_are_named_in_the_line(self):
        """⚠⚠ THE OTHER HALF OF "up to date, yet the rows show stale". The valuation clause explained
        the rows we DID read; this one accounts for the rows we did not. Without it the sentence
        describes a subset of the fleet in the voice of the whole of it — measured 2026-08-17, 29 of
        45 accounts, with four of the unmentioned sixteen twelve trading days behind."""
        msg = format_run_message(
            {"added": 0, "updated": 28, "up_to_date": 1, "failed": 0, "too_small": 16})
        assert "16 not re-read" in msg and "under 5 holdings" in msg
        # ⚠ AND IT SAYS THE SKIP IS BOUNDED. "not re-read" alone reads as "never", which is what it
        # used to be and is the reason four rows rotted for twelve trading days.
        assert "14 days" in msg

    def test_that_clause_is_absent_when_every_book_was_considered(self):
        """A run that skipped nothing must not carry a clause about nothing — the three fixed counts
        are the shape; this one is a property of the fleet on that particular run."""
        msg = format_run_message({"added": 0, "updated": 44, "up_to_date": 0, "failed": 0})
        assert "not re-read" not in msg
        assert format_run_message(
            {"added": 0, "updated": 44, "up_to_date": 0, "failed": 0, "too_small": 0}) == msg

    def test_the_small_clause_comes_before_failed(self):
        """Same rule as the valuation date: the amber reason stays last of the counts, so it is not
        buried behind a neutral one."""
        msg = format_run_message(
            {"added": 0, "updated": 20, "up_to_date": 0, "failed": 2, "too_small": 5}, "2026-08-15")
        assert msg.index("5 not re-read") < msg.index("2 failed") < msg.index("newest AIRS")
