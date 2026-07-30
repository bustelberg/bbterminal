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
        assert out == {"added": 1, "updated": 0, "up_to_date": 0, "failed": 0}

    def test_an_account_we_already_had_is_updated(self):
        out = count_outcomes([], known={"old_book"}, outcomes={"old_book": REPORTS_OK})
        assert out["updated"] == 1
        assert out["added"] == 0

    def test_skipped_accounts_are_already_up_to_date(self):
        """Being complete is WHY they were skipped — they are not failures and not work."""
        out = count_outcomes(["a", "b", "c"], known={"a", "b", "c"}, outcomes={})
        assert out == {"added": 0, "updated": 0, "up_to_date": 3, "failed": 0}

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
        assert out == {"added": 0, "updated": 1, "up_to_date": 0, "failed": 0}

    def test_a_brand_new_account_that_failed_is_not_added(self):
        """Nothing was stored, so nothing was added — "added" must mean rows exist now."""
        out = count_outcomes([], known=set(), outcomes={"new": []})
        assert out == {"added": 0, "updated": 0, "up_to_date": 0, "failed": 1}

    def test_the_counts_partition_the_fleet(self):
        """No account may fall between the four buckets — a reader adds them up."""
        skipped = ["s1", "s2"]
        outcomes = {"new": REPORTS_OK, "old": REPORTS_OK, "dead": [], "part": ["volk"]}
        out = count_outcomes(skipped, known={"old", "dead", "part", "s1", "s2"}, outcomes=outcomes)
        assert sum(out.values()) == len(skipped) + len(outcomes)
        assert out == {"added": 1, "updated": 2, "up_to_date": 2, "failed": 1}


class TestTheLineItPrints:
    def test_it_says_only_the_three_counts_when_nothing_failed(self):
        msg = format_run_message(
            {"added": 3, "updated": 41, "up_to_date": 0, "failed": 0})
        assert msg == "3 portfolios added, 41 updated, 0 already up to date"

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
        assert msg.endswith(", 14 failed")

    def test_a_single_portfolio_is_not_pluralised(self):
        assert format_run_message(
            {"added": 1, "updated": 0, "up_to_date": 0, "failed": 0}
        ).startswith("1 portfolio added")

    def test_zero_counts_still_show_so_the_shape_is_fixed(self):
        """A line that drops its clauses has to be parsed before it can be read."""
        msg = format_run_message({"added": 0, "updated": 0, "up_to_date": 44, "failed": 0})
        assert msg == "0 portfolios added, 0 updated, 44 already up to date"
