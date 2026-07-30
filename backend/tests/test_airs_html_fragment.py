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
