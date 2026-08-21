"""AirSPMS says "no data" with an HTML FRAGMENT, and we reported it as 16 unreadable bytes.

Measured in production 2026-07-30. A fleet scan reached all 44 accounts and got Rendement 44/44,
Vermogensoverzicht 44/44 and Model 30/44 — the 14 misses all came back as:

    RuntimeError: MODEL for 'BUS_Defensief_Kl_MV' … is not a spreadsheet:
                  177 bytes starting b'<br>\\n30-07-2026 '

Every one of those 14 is an `_MV` (meervoudig), `_BM_` (benchmark) or `WTS test` book — exactly the
types that HAVE no fixed model, which the module docstring already records as "an empty table there
is an answer, not a failure". So AIRS was answering the question and the answer was thrown away:
`_looks_like_html` only recognised a DOCUMENT (`<!doctype`, `<html>`), the fragment fell through to
the raw-bytes branch, and the operator saw sixteen bytes of a sentence.

Recognising the fragment routes it to `_describe_non_excel`, which prints the status, content-type
and a 300-character excerpt — so the next run states what AIRS said rather than hinting at it.
"""
from __future__ import annotations

from airs_scanner import _looks_like_html

# The real body, as far as the production error surfaced it.
AIRS_NO_DATA = b"<br>\n30-07-2026 Er zijn geen gegevens gevonden voor deze periode."


class TestItRecognisesAFragmentNotJustADocument:
    def test_the_airs_no_data_reply_is_html(self):
        assert _looks_like_html(AIRS_NO_DATA) is True

    def test_a_document_still_is(self):
        for body in (b"<!doctype html><html>", b"  <HTML>", b"<!-- x -->", b"<head>", b"<?php"):
            assert _looks_like_html(body) is True, body

    def test_other_fragment_shapes_count_too(self):
        for tag in (b"<p>", b"<div>", b"<span>", b"<b>", b"<font ", b"<table>"):
            assert _looks_like_html(tag + b"geen gegevens") is True, tag


class TestItDoesNotRelabelABrokenDownload:
    """⚠ THE REASON THERE IS A SIZE BOUND. A real spreadsheet never begins with a tag, but a
    TRUNCATED or corrupted binary might — and calling that "an HTML page" would dress a genuine
    transport fault in a tidier diagnosis and send the next investigation to the wrong place."""

    def test_a_real_xlsx_is_not_html(self):
        assert _looks_like_html(b"PK\x03\x04" + b"x" * 5000) is False

    def test_a_real_xls_is_not_html(self):
        assert _looks_like_html(b"\xd0\xcf\x11\xe0" + b"x" * 5000) is False

    def test_a_LARGE_body_beginning_with_a_tag_is_not_called_a_fragment(self):
        """A message is short. Four kilobytes of anything is not a message."""
        assert _looks_like_html(b"<br" + b"x" * 9000) is False

    def test_a_large_body_with_a_DOCUMENT_marker_still_is(self):
        """A full error page is genuinely HTML however long it runs — only the FRAGMENT rule is
        size-bounded, because only the fragment rule is ambiguous."""
        assert _looks_like_html(b"<html>" + b"x" * 9000) is True


class TestAirsNoDataIsAnAnswerNotAFailure:
    """⚠ 14 OF 44 ACCOUNTS FAILED THEIR MODEL REPORT ON EVERY SINGLE RUN, AND NONE OF THEM WAS
    BROKEN. AIRS answers a report it has nothing for with a ~170-byte fragment; that was reported
    as a hard error, so those books never counted as COMPLETE — which meant they wore a permanent
    ⚠, and (the expensive part) an account that can never be complete is never skipped as fresh, so
    they were the ONLY accounts the incremental scan ever visited: "1/14: BUS_WTS_SterkeMerken_Fx…"
    while the 30 real books were correctly skipped.

    ⚠ WHAT MAKES THIS SAFE TO CLASSIFY IS THE OTHER THREE REPORTS. A dead session or an IP block
    breaks all four; measured 2026-07-30 the same accounts in the same session returned Rendement
    44/44, Vermogensoverzicht 44/44, Mutaties 44/44 and Model 30/44. Per-REPORT failure is a fact
    about the report. The 14 are all `_MV` (meervoudig), `_BM_`, `WTS test` or `_Fx` books — the
    types that have no fixed model at all.

    So the test that matters is not "does it spot the message" — it is "does it REFUSE to spot one
    in anything that could be a real fault".
    """

    def test_the_airs_no_data_fragment_is_recognised(self):
        from airs_scanner import _is_no_data

        assert _is_no_data(AIRS_NO_DATA + b" Er zijn geen gegevens gevonden.") is True

    def test_a_login_page_is_NEVER_no_data(self):
        """The failure that must still be loud: reporting an expired session as "no model" hides
        the one thing a human has to act on."""
        from airs_scanner import _is_no_data

        assert _is_no_data(b"<!doctype html><html><body><form name=login></form></body></html>") is False

    def test_a_full_document_mentioning_it_is_still_an_error(self):
        """A real page that happens to contain the phrase is not AIRS's terse no-data reply."""
        from airs_scanner import _is_no_data

        assert _is_no_data(b"<html><body>geen gegevens</body></html>") is False

    def test_an_unrecognised_short_fragment_stays_an_error(self):
        from airs_scanner import _is_no_data

        assert _is_no_data(b"<br>\n30-07-2026 Onbekende fout") is False

    def test_a_long_body_is_never_no_data(self):
        from airs_scanner import _is_no_data

        assert _is_no_data(b"<br>geen gegevens" + b"x" * 5000) is False

    def test_a_spreadsheet_is_never_no_data(self):
        from airs_scanner import _is_no_data

        assert _is_no_data(b"PK\x03\x04" + b"x" * 400) is False

    def test_the_step_wrapper_counts_it_as_retrieved(self):
        """⚠ THE POINT OF THE WHOLE CHANGE. `reports_ok` is what `accounts_to_scan` reads to decide
        an account is complete and can be skipped — so a no-data report must land in `ok`, not in
        `errors`, or the account is re-scanned for ever."""
        import inspect

        import airs_vermogen

        src = inspect.getsource(airs_vermogen.scan_one)
        assert "AirsNoData" in src
        # It appends to `ok` in that branch rather than recording an error.
        branch = src.split("except AirsNoData")[1].split("except Exception")[0]
        assert "ok.append(code)" in branch
        assert "errors.append" not in branch


class TestTheRosterIsCheckedAgainstAirsOwnCount:
    """⚠ THE THREE FILTERS ARE SENT AND NOTHING CONFIRMS THEY APPLIED.

    `actief=actief&portefeuilleIntern=1&metConsolidatie=0` defines the Front-Office population, and
    the response to a wrong combination is a perfectly normal table with the wrong rows in it. The
    only independent check is the count AIRS itself prints — "44 Items in selectie". Measured
    2026-07-30 the scan reported 46 and there was no way to tell whether a filter had stopped
    applying, the pager had walked into another selection, or AIRS's roster had genuinely grown.
    Reading the page's own number makes those three separable.
    """

    def test_it_reads_the_count_airs_prints(self):
        from airs_scanner import _SELECTIE_RE

        assert _SELECTIE_RE.search("44 Items in selectie").group(1) == "44"

    def test_a_thousands_separator_survives(self):
        from airs_scanner import _SELECTIE_RE

        assert _SELECTIE_RE.search("1.234 Items in selectie").group(1) == "1.234"

    def test_it_does_not_invent_a_count(self):
        """None must mean "the page did not say", never a guess — an unreadable count has to leave
        the scrape alone rather than veto it."""
        from airs_scanner import _SELECTIE_RE

        for text in ("Item in selectie", "geen items", "", "Items in selectie"):
            assert _SELECTIE_RE.search(text) is None, text

    def test_the_scraper_dedupes_and_stops_when_a_page_adds_nothing(self):
        """⚠ AirSPMS CLAMPS an out-of-range page instead of returning nothing — the trap the
        model-portfolio list already documents. A pager that trusts the "next" arrow re-reads the
        last page, and appending without dedupe turns that into extra portfolios rather than an
        error."""
        import inspect

        import airs_scanner

        src = inspect.getsource(airs_scanner.scan_portfolios_sync)
        assert "seen" in src and "dupes" in src
        assert "len(portfolios) == before" in src, "must stop when a page adds no new names"


class TestTheValuationDateIsDiscoveredOncePerRun:
    """AirSPMS values end-of-day in ONE batch, so a day it never valued is a fact about the DAY.
    `_vermogen_most_recent` used to re-discover that per account, starting at today: measured
    2026-07-30 the day's valuation had not run, so all ~25 books with holdings paid a wasted request
    before landing on the 29th — on a Monday it is three (Mon, Sun, Sat) before Friday.

    ⚠⚠ THE MEMO THAT FIXED IT THEN BROKE 29 OF 46 BOOKS (2026-08-21), and this class now pins the
    rule that makes it sound. A single failure was taken as proof the DATE was dead, and a failure is
    only ever proof about the BOOK that made it — books are valued on different cadences, so one book
    a week behind ruled out six good dates on its way back to its own, and every account scanned
    afterwards skipped them. It compounds: the more books walk back, the fewer dates remain, until
    the rest skip their whole horizon without making one request and raise.

    Measured on the real fleet (29 books, valuation dates spread over 8 days):

        one failure rules a date out, 7-day walk     refreshed 22   badged 7   downloads  29
        quorum of 3 alone                            refreshed 22   badged 7   downloads  64
        no memo at all                               refreshed 29   badged 0   downloads 139
        quorum of 3, and only ABOVE the newest hit   refreshed 29   badged 0   downloads 113

    ⚠ THE QUORUM ALONE IS NOT ENOUGH, which is why the second row is in that table: books SHARE
    cadences, so three books that are all a week behind rule out the very date a fourth one needs.
    What makes it sound is the second condition — a book cannot be valued AHEAD of the newest batch
    that has run, so a date is only dead once some account has successfully fetched an OLDER one.
    """

    def _patch(self, monkeypatch, unvalued_from: str, behind: dict[str, str] | None = None):
        """`unvalued_from` = dates >= this return nothing for everyone. `behind` = per-account
        override: that account is valued only ON that date, whatever the fleet is doing."""
        import airs_scanner
        import airs_vermogen

        calls: list[tuple[str, str]] = []
        behind = behind or {}

        def fake(name, van, tot):
            calls.append((name, tot))
            if name in behind:
                if tot != behind[name]:
                    raise RuntimeError("Response too small")
                return b"ok"
            if tot >= unvalued_from:
                raise RuntimeError("Response too small")
            return b"ok"

        monkeypatch.setattr(airs_scanner, "download_vermogensoverzicht_sync", fake)
        # ⚠ ALL THREE, so a test starts from a known memo rather than from whatever the previous
        # one left in the module. The TTL stamp matters as much as the two it guards: leaving it
        # unset would make the first call in each test reset the memo again, which is fine here but
        # would quietly hide a test that MEANT to carry state across a phase (see the inheritance
        # regression below, which does exactly that on purpose).
        airs_vermogen._reset_valuation_memo()
        return calls

    def test_a_dead_date_is_ruled_out_once_a_quorum_agrees(self, monkeypatch):
        """The saving the memo exists for: three books pay for the discovery, the rest skip it."""
        import airs_vermogen
        from datetime import date

        today = date.today().isoformat()
        calls = self._patch(monkeypatch, unvalued_from=today)
        for acct in ("A", "B", "C", "D", "E"):
            airs_vermogen._vermogen_most_recent(acct, "2026-01-01")
        tried_today = [c[0] for c in calls if c[1] == today]
        # ⚠ EXACTLY THE QUORUM, THEN NEVER AGAIN. Fewer would mean one book can speak for the
        # fleet (the bug); more would mean the memo never pays for itself.
        assert tried_today == ["A", "B", "C"], tried_today
        assert [c[0] for c in calls].count("E") == 1, "E should have skipped straight to the hit"

    def test_every_account_still_gets_its_OWN_valued_date(self, monkeypatch):
        """The memo must not pin the fleet to one answer — only rule dates OUT."""
        import airs_vermogen
        from datetime import date

        today = date.today().isoformat()
        self._patch(monkeypatch, unvalued_from=today)
        dates = {a: airs_vermogen._vermogen_most_recent(a, "2026-01-01")[0] for a in ("A", "B")}
        assert dates["A"] == dates["B"] < today

    def test_a_book_that_is_simply_BEHIND_does_not_rule_dates_out_for_the_fleet(self, monkeypatch):
        """⚠⚠ THE REGRESSION. This is the production failure, in five accounts.

        `slow` is valued a week ago, so it fails on six dates the fleet is perfectly valued on. Under
        the old rule those six were dead for everyone after it, and the books that needed them wore a
        permanent "⚠ Vermogensoverzicht" on /management-dashboard while their stored holdings sat
        days out of date. Every one of them must still get its own snapshot.
        """
        import airs_vermogen
        from datetime import date, timedelta

        day = lambda b: (date.today() - timedelta(days=b)).isoformat()  # noqa: E731
        # The fleet is valued up to yesterday; `slow` only ever on day 7.
        self._patch(monkeypatch, unvalued_from=day(0), behind={"slow": day(7)})

        # The offender goes FIRST, which is the worst case: it poisons before anyone succeeds.
        assert airs_vermogen._vermogen_most_recent("slow", "2026-01-01")[0] == day(7)
        for acct in ("A", "B", "C", "D"):
            got, _blob = airs_vermogen._vermogen_most_recent(acct, "2026-01-01")
            assert got == day(1), f"{acct} got {got}, not the fleet's newest valued date"

    def test_the_horizon_reaches_a_book_valued_more_than_a_week_ago(self, monkeypatch):
        """⚠ THE SECOND, INDEPENDENT DEFECT. The walk was `range(0, 7)`, and two of the 29 badged
        books were last valued 8 days back — unreachable even with an empty memo."""
        import airs_vermogen
        from datetime import date, timedelta

        day = lambda b: (date.today() - timedelta(days=b)).isoformat()  # noqa: E731
        self._patch(monkeypatch, unvalued_from=day(0), behind={"stale": day(8)})
        assert airs_vermogen._vermogen_most_recent("stale", "2026-01-01")[0] == day(8)
        assert airs_vermogen._WALK_BACK_DAYS > 8

    def test_a_dead_session_still_raises_rather_than_returning_a_wrong_date(self, monkeypatch):
        """⚠ EXHAUSTION IS STILL A FAILURE. An auth failure returns the same empty body on EVERY
        date; widening the horizon must not turn that into a silent success on some old date."""
        import airs_vermogen
        import pytest as _pytest

        self._patch(monkeypatch, unvalued_from="0000-00-00")   # nothing is ever valued
        with _pytest.raises(RuntimeError, match="no valued Vermogensoverzicht"):
            airs_vermogen._vermogen_most_recent("A", "2026-01-01")

    def test_a_later_refresh_does_not_inherit_a_finished_runs_ruled_out_dates(self, monkeypatch):
        """⚠⚠ THE PRODUCTION FAILURE, ONE LEVEL UP FROM THE CASCADE (2026-08-22).

        The memo is process-global and `run_airs_vermogen_refresh_sync` was the only caller that
        ever reset it. `refresh_one_portfolio` — the per-row Refresh button, the Analyse modal's
        Refresh, and `refresh_many` under the 05:00 model-prices job — did not, and the backend is
        a long-lived process.

        The dates a fleet run rules out are by construction the NEWEST ones (today, and the weekend
        behind it), which are exactly the dates AirSPMS has since valued by the time anybody presses
        Refresh. So the button walked past the date it needed, landed older or exhausted its
        horizon, and the row kept its ⚠ Vermogensoverzicht — reported as "I still see this behind
        most portfolios" after the cascade fix had shipped.

        This is the whole bug in two phases and one process, and it is deliberately written against
        BEHAVIOUR rather than source text — see the test below it for why.
        """
        import airs_scanner
        import airs_vermogen
        from datetime import date, timedelta

        day = lambda b: (date.today() - timedelta(days=b)).isoformat()  # noqa: E731

        # Phase 1 — a fleet pass at a moment AirSPMS has not yet valued today. Four books is enough
        # to meet the quorum and have the fourth skip today, which is the state that then persists.
        calls = self._patch(monkeypatch, unvalued_from=day(0))
        for acct in ("A", "B", "C", "D"):
            airs_vermogen._vermogen_most_recent(acct, "2026-01-01")
        assert airs_vermogen._UNVALUED_DATES.get(day(0)) == {"A", "B", "C"}

        # Phase 2 — the batch runs, today becomes valued, and somebody presses Refresh on a row.
        # ⚠ NO RESET BETWEEN THE PHASES, deliberately: not resetting is precisely what the per-row
        # button did, and a test that reset here would be testing the fleet path a second time.
        airs_vermogen._MEMO_STARTED_AT = (
            airs_vermogen._time.monotonic() - airs_vermogen._MEMO_TTL_S - 1)
        calls.clear()

        def now_valued(name, van, tot):
            calls.append((name, tot))
            return b"ok"

        monkeypatch.setattr(airs_scanner, "download_vermogensoverzicht_sync", now_valued)

        got, _blob = airs_vermogen._vermogen_most_recent("A", "2026-01-01")
        # ⚠ THE ASSERTION THAT FAILS WITHOUT THE FIX: it comes back with day(1), the newest date the
        # PREVIOUS run had proved, having skipped today without asking.
        assert got == day(0), (
            f"the refresh got {got}, not today — it skipped a date that has since been valued")
        assert calls == [("A", day(0))], calls

    def test_the_memo_expires_on_a_clock_not_on_a_caller_remembering(self):
        """⚠⚠ WHY THE TTL LIVES ON THE READ PATH. The rule this replaces was "the fleet run clears
        it at the top", pinned by a test that grepped `run_airs_vermogen_refresh_sync` for the
        string `_UNVALUED_DATES.clear()`. That assertion was true for the entire time production was
        broken: the clear was there, and two other entry points reached the memo without it.

        A source-text assertion can only ever confirm that one caller does the right thing, which is
        the exact shape of the bug. So the invariant is stated where it can be checked for ALL
        callers — reaching the walk with a memo older than the TTL leaves it empty, whoever called.
        """
        import airs_vermogen

        airs_vermogen._UNVALUED_DATES["2026-08-20"] = {"A", "B", "C"}
        airs_vermogen._NEWEST_VALUED = "2026-08-19"
        airs_vermogen._MEMO_STARTED_AT = (
            airs_vermogen._time.monotonic() - airs_vermogen._MEMO_TTL_S - 1)

        airs_vermogen._expire_valuation_memo()

        assert airs_vermogen._UNVALUED_DATES == {}
        # ⚠ BOTH HALVES, OR THE PAIR IS INCOHERENT — `_NEWEST_VALUED` is the LICENCE to rule a date
        # out, so keeping it while dropping the misses licences one run's answer against another's
        # evidence.
        assert airs_vermogen._NEWEST_VALUED is None

    def test_the_memo_survives_within_one_run(self):
        """The saving still has to happen — an expiry on every call would be no memo at all."""
        import airs_vermogen

        airs_vermogen._reset_valuation_memo()
        airs_vermogen._UNVALUED_DATES["2026-08-20"] = {"A", "B", "C"}
        airs_vermogen._expire_valuation_memo()
        assert airs_vermogen._UNVALUED_DATES == {"2026-08-20": {"A", "B", "C"}}


class TestBooksTooSmallToBePortfoliosAreNotRescanned:
    """⚠ THE FLEET SPENT 60 DOWNLOADS A RUN ON BOOKS NOBODY LOOKS AT. Of 46 accounts, 5 are AIRS
    benchmarks carrying exactly 1 holding and 10 are `_MV` / `WTS test` shells carrying none —
    against 10-29 for every real book. Each cost four reports on every pass, and because several
    could never be complete the freshness skip never caught them either: they were the ONLY
    accounts an incremental scan ever visited.

    ⚠ THE HARD PART IS THAT ZERO AND UNKNOWN LOOK IDENTICAL. A book storing no holdings has no rows
    in `airs_holding`, so it is simply absent from the counts — exactly like one never scanned.
    Treating absence as bogus would strand a brand-new account for ever (it could never acquire the
    holdings that would rescue it); treating it as unknown misses the emptiest books, which are the
    ones worth skipping. The roster's `reports_ok` settles it: `volk` present means we DID fetch the
    Vermogensoverzicht, so absent-and-fetched is a MEASURED zero.
    """

    def _verdicts(self, **got):
        return {name: {"reports_ok": list(reports)} for name, reports in got.items()}

    def test_a_measured_small_count_is_bogus(self):
        from airs_vermogen import bogus_accounts

        v = self._verdicts(BM_1=("att", "volk", "mut", "model"))
        assert bogus_accounts({"BM_1": 1}, v) == {"bm_1"}

    def test_a_measured_ZERO_is_bogus_even_though_it_has_no_rows(self):
        """The `_MV` shells: fetched, and the Vermogensoverzicht was empty."""
        from airs_vermogen import bogus_accounts

        v = self._verdicts(BUS_Neutraal_Kl_MV=("att", "volk", "mut", "model"))
        assert bogus_accounts({}, v) == {"bus_neutraal_kl_mv"}

    def test_an_account_whose_holdings_were_NEVER_FETCHED_is_not_bogus(self):
        """⚠ THE ONE THAT WOULD BE PERMANENT. Skipping it means never fetching it, which means it
        can never stop being skipped."""
        from airs_vermogen import bogus_accounts

        v = self._verdicts(BrandNew=("att",))       # no `volk` yet
        assert bogus_accounts({}, v) == set()

    def test_a_real_book_is_never_bogus(self):
        from airs_vermogen import bogus_accounts

        v = self._verdicts(AITopSelectie=("att", "volk", "mut", "model"))
        assert bogus_accounts({"AITopSelectie": 21}, v) == set()

    def test_the_threshold_boundary_is_inclusive_of_real(self):
        from airs_vermogen import MIN_REAL_HOLDINGS, bogus_accounts

        v = self._verdicts(Edge=("volk",))
        assert bogus_accounts({"Edge": MIN_REAL_HOLDINGS}, v) == set()
        assert bogus_accounts({"Edge": MIN_REAL_HOLDINGS - 1}, v) == {"edge"}

    def test_force_bypasses_it_entirely(self):
        """A forced re-scan must re-check every book, or a mis-classification is unfixable."""
        import inspect

        import airs_vermogen

        src = inspect.getsource(airs_vermogen.run_airs_vermogen_refresh_sync)
        assert "if not force:" in src
        assert "bogus_accounts(" in src

    def test_the_freshness_window_clears_a_daily_valuation(self):
        """AIRS values once a day and the job ticks ~24h apart; the window only has to be shorter
        than that gap, and longer than the interval between two presses."""
        from airs_vermogen import AIRS_FRESH_HOURS

        assert 12 < AIRS_FRESH_HOURS < 24
