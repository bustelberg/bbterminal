"""Every AIRS report download goes through ONE funnel, and it has to survive AirSPMS's junk byte.

MEASURED 2026-07-29, on a full fleet scan:

    14x Model              — Excel file format cannot be determined, you must specify an engine manually.
    13x Vermogensoverzicht — Excel file format cannot be determined, you must specify an engine manually.
     0x Rendement          (44/44 fine)

That message is pandas', and it is what pandas says for BOTH of the two things that actually
happen here: a spreadsheet with a stray leading byte, and an HTML error page. The codebase already
knew about the first — `_strip_spreadsheet_preamble` was written for the model-portfolio LIST
export, where AirSPMS prepends an APOSTROPHE before the zip magic — but the front-office report
funnel never used it, so the same vendor quirk arrived as a mystery on a different endpoint.

⚠ THE POINT OF THE SECOND CHECK IS THAT A REMAINING FAILURE MUST NAME ITSELF. The old guard only
caught a body beginning exactly `<!doctype`; leading whitespace, a bare `<html>` or a BOM sailed
through into pandas, where an expired session, a "no data for this period" page and an IP block
were all one indistinguishable engine error.
"""
from __future__ import annotations

import pytest

import airs_scanner as S

XLSX = bytes([0x50, 0x4B, 0x03, 0x04])
XLS = bytes([0xD0, 0xCF, 0x11, 0xE0])


def _wire(monkeypatch, body: bytes):
    monkeypatch.setattr(S, "_session", type("X", (), {"get": staticmethod(lambda _u: body)})())


def _call():
    return S._download_report_sync("BUS_X", "2026-01-01", "2026-07-29", "VOLK")


class TestTheStrayByte:
    def test_an_apostrophe_before_the_zip_magic_is_cut(self, monkeypatch):
        """The exact failure: pandas rejects the whole file over one junk byte."""
        _wire(monkeypatch, b"'" + XLSX + b"x" * 200)
        assert _call().startswith(XLSX)

    def test_an_old_style_xls_is_handled_too(self, monkeypatch):
        _wire(monkeypatch, b"'" + XLS + b"x" * 200)
        assert _call().startswith(XLS)

    def test_a_clean_file_is_untouched(self, monkeypatch):
        body = XLSX + b"y" * 200
        _wire(monkeypatch, body)
        assert _call() == body


class TestAFailureNamesItself:
    def test_html_is_reported_as_html_with_a_diagnosis(self, monkeypatch):
        _wire(monkeypatch, b"  <html><head><title>Sessie verlopen</title></head><body>" + b"z" * 200)
        with pytest.raises(RuntimeError) as ei:
            _call()
        msg = str(ei.value)
        # Names the report and the account — 27 identical messages told you neither.
        assert "VOLK" in msg and "BUS_X" in msg
        # ...and what the page actually was, rather than a pandas engine error.
        assert "Sessie verlopen" in msg

    def test_leading_whitespace_no_longer_slips_through(self, monkeypatch):
        """The old guard tested `content[:15].lower().startswith(b'<!doctype')` — a single space
        in front of the doctype defeated it and the body went to pandas."""
        _wire(monkeypatch, b"   <!DOCTYPE html><title>Geen data</title>" + b"z" * 200)
        with pytest.raises(RuntimeError, match="not a spreadsheet"):
            _call()

    def test_non_html_junk_shows_its_first_bytes(self, monkeypatch):
        # Neither Excel nor HTML: print the head so the next investigation starts from the bytes.
        _wire(monkeypatch, b"\x00\x01rubbish" + b"q" * 200)
        with pytest.raises(RuntimeError) as ei:
            _call()
        assert "starting" in str(ei.value)

    def test_a_short_body_is_still_refused_first(self, monkeypatch):
        # An unknown `rapport_types` returns an EMPTY body rather than an error; that check must
        # stay ahead of the magic test, or the message becomes "not a spreadsheet: 12 bytes".
        _wire(monkeypatch, b"tiny")
        with pytest.raises(RuntimeError, match="too small"):
            _call()


def test_every_report_shares_this_funnel():
    """ATT / VOLK / MUT / MODEL all download through `_download_report_sync`, so the fix lands on
    all four at once — the reason it was applied here rather than at each parse site."""
    import inspect

    for fn in (S.download_portfolio_sync, S.download_vermogensoverzicht_sync,
               S.download_mutaties_sync, S.download_model_sync):
        assert "_download_report_sync" in inspect.getsource(fn), fn.__name__
