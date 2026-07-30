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
