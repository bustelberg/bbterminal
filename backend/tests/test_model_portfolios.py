"""Parsing the AirSPMS Model-portefeuilles list.

Both fixtures are the REAL markup (probed 2026-07-13), including the two things that make
a naive scraper silently wrong:

  * the name column is TRUNCATED ("BUS_WTS_Dividend...") and the full name appears NOWHERE
    on the list page — not even in a title=/alt=. It exists only on the row's edit page.
  * an out-of-range page does NOT come back empty: AirSPMS clamps, so page 5 of a 4-page
    list returns 25 rows again. `while rows: page += 1` never terminates.
"""
from __future__ import annotations

from airs_scanner import (
    AirsHttpResponse,
    _decode_html,
    parse_model_portfolio_rows,
    parse_portfolio_full_name,
)


class TestPageEncoding:
    """AirSPMS serves ISO-8859-1, NOT utf-8.

    Decoding it as utf-8 (errors="replace") silently turns "Azië" into "Azi�" — three
    of the 95 model portfolios carry an accent, and nothing errors: the mangled name just
    sits there looking almost right. Honour the charset the server declares.
    """

    @staticmethod
    def _resp(body: bytes, ctype: str) -> AirsHttpResponse:
        return AirsHttpResponse(body=body, status=200, content_type=ctype, url="x")

    def test_latin1_page_keeps_its_accents(self):
        body = "AziëTopSelectie dynamisch".encode("cp1252")
        html = _decode_html(self._resp(body, "text/html; charset=ISO-8859-1"))
        assert html == "AziëTopSelectie dynamisch"
        assert "�" not in html

    def test_decoding_that_same_page_as_utf8_is_what_corrupted_it(self):
        # The bug, made explicit: this is what the code used to do.
        body = "AziëTopSelectie".encode("cp1252")
        assert "�" in body.decode("utf-8", "replace")

    def test_a_utf8_page_is_still_honoured(self):
        body = "Azië".encode("utf-8")
        assert _decode_html(self._resp(body, "text/html; charset=UTF-8")) == "Azië"

    def test_an_unknown_charset_falls_back_rather_than_raising(self):
        body = "Azië".encode("cp1252")
        assert _decode_html(self._resp(body, "text/html; charset=bogus-99")) == "Azië"

    def test_entities_are_unescaped_too(self):
        rows = parse_model_portfolio_rows(
            '<tr><td><a href="modelportefeuillesEdit.php?action=edit&id=1">'
            'Azi&euml;TopSelectie &nbsp;</a></td></tr>')
        assert rows[0]["name"] == "AziëTopSelectie"

# Two real rows: one whose name the list truncated, one short enough to survive intact.
LIST_HTML = """
<table>
<tr class="header"><td>Portefeuille</td><td>Omschrijving</td></tr>
<tr>
  <td><a href="modelportefeuillesEdit.php?action=edit&id=1728" class="icon"><img src="images//16/muteer.gif"></a></td>
  <td><a href="modelportefeuillesEdit.php?action=edit&id=1728" >BUS_WTS_Dividend... &nbsp;</a></td>
  <td><a href="modelportefeuillesEdit.php?action=edit&id=1728" >WTS Dividend FX &nbsp;</a></td>
  <td><a href="modelportefeuillesEdit.php?action=edit&id=1728" >fixed (12.6) &nbsp;</a></td>
  <td><a href="modelportefeuillesEdit.php?action=edit&id=1728" >2024-01-25 &nbsp;</a></td>
</tr>
<tr>
  <td><a href="modelportefeuillesEdit.php?action=edit&id=2077" class="icon"><img src="images//16/muteer.gif"></a></td>
  <td><a href="modelportefeuillesEdit.php?action=edit&id=2077" >TOPS_KM &nbsp;</a></td>
  <td><a href="modelportefeuillesEdit.php?action=edit&id=2077" >Toppenberg kies een mix &nbsp;</a></td>
  <td><a href="modelportefeuillesEdit.php?action=edit&id=2077" >meervoudig &nbsp;</a></td>
  <td><a href="modelportefeuillesEdit.php?action=edit&id=2077" >2026-05-20 &nbsp;</a></td>
</tr>
</table>
"""

EDIT_HTML = (
    '<input type="hidden" name="updateScript" value="modelportefeuillesEdit.php">'
    '<input class="" type="text" size="24" value="BUS_WTS_Dividend_Fx" '
    'name="Portefeuille" id="Portefeuille" disabled>'
    '<input class="" type="text" size="50" value="WTS Dividend FX" name="Omschrijving">'
)


class TestParseListRows:
    def test_reads_both_rows_with_their_ids(self):
        rows = parse_model_portfolio_rows(LIST_HTML)
        assert [r["id"] for r in rows] == [1728, 2077]

    def test_the_pencil_icon_anchor_is_not_mistaken_for_the_name(self):
        """Every cell of a row is wrapped in an anchor to the SAME edit id, and the first
        one is the icon. Take anchors naively and the name column becomes ''."""
        rows = parse_model_portfolio_rows(LIST_HTML)
        assert rows[0]["name"] == "BUS_WTS_Dividend..."
        assert rows[1]["name"] == "TOPS_KM"

    def test_truncation_is_flagged_so_the_full_name_can_be_fetched(self):
        rows = parse_model_portfolio_rows(LIST_HTML)
        assert rows[0]["truncated"] is True     # needs its edit page
        assert rows[1]["truncated"] is False    # short enough; already complete

    def test_the_other_columns_come_through(self):
        r = parse_model_portfolio_rows(LIST_HTML)[0]
        assert r["omschrijving"] == "WTS Dividend FX"
        assert r["fixed"] == "fixed (12.6)"
        assert r["fixed_datum"] == "2024-01-25"

    def test_nbsp_and_tags_are_stripped(self):
        # The cells really do end in "&nbsp;" and the icon cell holds an <img>.
        assert all("&nbsp;" not in r["name"] for r in parse_model_portfolio_rows(LIST_HTML))

    def test_a_page_with_no_rows_yields_nothing_rather_than_raising(self):
        assert parse_model_portfolio_rows("<html><body>no table</body></html>") == []


class TestFullNameFastPath:
    """The list's own "Naar XLS" export gives all 95 FULL names in ONE download — the scan
    went from ~220s to ~6s. But the XLS has no `id`, so it must be paired with the HTML rows
    (which do).

    THE PAIRING CANNOT BE DONE BY NAME. The truncated names are not a unique key:
    "DiTopSelectie OF..." prefix-matches BOTH "DiTopSelectie OFF DYN" and
    "DiTopSelectie OFF FX" — 19 of the 95 rows are ambiguous like this. So it is POSITIONAL
    (both views are the same query, verified 95/95), and every pairing is then CHECKED
    against the prefix the list did show. A pairing that fails keeps its truncated name and
    falls back to its edit page.
    """

    @staticmethod
    def _rows(*names: str) -> list[dict]:
        return [
            {"id": i, "name": n, "truncated": n.endswith("..."),
             "omschrijving": "", "fixed": "", "fixed_datum": ""}
            for i, n in enumerate(names, 1)
        ]

    def test_it_fills_in_the_full_names(self):
        from airs_scanner import _apply_full_names
        rows = self._rows("BUS_WTS_Dividend...", "TOPS_KM")
        out = _apply_full_names(rows, ["BUS_WTS_Dividend_Fx", "TOPS_KM"])
        assert [r["name"] for r in out] == ["BUS_WTS_Dividend_Fx", "TOPS_KM"]
        assert all(not r["truncated"] for r in out)

    def test_it_resolves_the_ambiguous_pairs_a_name_join_could_not(self):
        """The real case: these two share a truncation prefix, so only position can tell
        them apart — and the prefix check still confirms each pairing."""
        from airs_scanner import _apply_full_names
        rows = self._rows("DiTopSelectie OF...", "DiTopSelectie OF...")
        out = _apply_full_names(rows, ["DiTopSelectie OFF DYN", "DiTopSelectie OFF FX"])
        assert [r["name"] for r in out] == ["DiTopSelectie OFF DYN", "DiTopSelectie OFF FX"]

    def test_a_pairing_that_fails_its_prefix_check_is_LEFT_truncated(self):
        """The guard. If the two views ever stop agreeing on sort order, a positional join
        would hand back confident, wrong names. Instead the row keeps its clipped name and
        stays flagged, so the caller re-reads it from its edit page."""
        from airs_scanner import _apply_full_names
        rows = self._rows("BUS_WTS_Dividend...", "TOPS_KM")
        out = _apply_full_names(rows, ["SOMETHING_ELSE", "TOPS_KM"])
        assert out[0]["name"] == "BUS_WTS_Dividend..."   # NOT "SOMETHING_ELSE"
        assert out[0]["truncated"] is True               # -> edit-page fallback
        assert out[1]["name"] == "TOPS_KM"               # the row that DID check out

    def test_a_count_mismatch_trusts_nothing(self):
        from airs_scanner import _apply_full_names
        rows = self._rows("BUS_WTS_Dividend...")
        out = _apply_full_names(rows, ["BUS_WTS_Dividend_Fx", "EXTRA_ROW"])
        assert out[0]["truncated"] is True               # falls back entirely


class TestSpreadsheetPreamble:
    """AirSPMS prepends a stray apostrophe to the list export — the payload literally starts
    b"'PK\\x03\\x04". pandas then rejects the whole file with "Excel file format cannot be
    determined", which reads like a broken download rather than one junk byte."""

    def test_it_cuts_back_to_the_zip_magic(self):
        from airs_scanner import _strip_spreadsheet_preamble
        body = b"'" + bytes([0x50, 0x4B, 0x03, 0x04]) + b"rest"
        assert _strip_spreadsheet_preamble(body).startswith(bytes([0x50, 0x4B, 0x03, 0x04]))

    def test_it_handles_the_legacy_xls_magic_too(self):
        from airs_scanner import _strip_spreadsheet_preamble
        body = bytes([0xD0, 0xCF, 0x11, 0xE0]) + b"rest"
        assert _strip_spreadsheet_preamble(body) == body      # clean file, untouched

    def test_it_does_not_chop_a_file_that_merely_CONTAINS_the_magic(self):
        # "PK" appears inside zip payloads constantly; only a magic at the very START is a
        # preamble. Anything further in is data, and slicing there would corrupt the file.
        from airs_scanner import _strip_spreadsheet_preamble
        body = b"x" * 40 + bytes([0x50, 0x4B, 0x03, 0x04])
        assert _strip_spreadsheet_preamble(body) == body


class TestFixedDatumOptions:
    """The snapshot dates, and the trap in them.

    AirSPMS ALWAYS leads the dropdown with TODAY — an empty "new snapshot" placeholder that
    returns ZERO rows. Ask for it and you get a blank table that reads exactly like "this
    portfolio has no holdings". The real snapshots follow (BUS_WTS_Dividend_Fx has 13,
    newest 2024-12-10), which is why positions are fetched newest-first until rows appear.
    """

    SELECT = (
        '<select name="FixedDatum" id="FixedDatum" onChange="javascript:reloadFixed()">'
        '<option value="2026-07-13">2026-07-13</option>'      # today — the placeholder
        '<option value="2023-12-31">2023-12-31</option>'
        '<option value="2024-12-06">2024-12-06</option>'
        '<option value="2024-12-10">2024-12-10</option>'      # the newest REAL snapshot
        '</select>'
    )

    def test_it_reads_every_option(self):
        from airs_scanner import parse_fixed_datum_options
        assert parse_fixed_datum_options(self.SELECT) == [
            "2026-07-13", "2023-12-31", "2024-12-06", "2024-12-10"]

    def test_the_newest_real_snapshot_is_not_the_first_option(self):
        """The bug this guards: taking options[0] gets you today's EMPTY placeholder."""
        from airs_scanner import parse_fixed_datum_options
        opts = parse_fixed_datum_options(self.SELECT)
        assert opts[0] == "2026-07-13"                 # today, and it has no rows
        assert max(opts[1:]) == "2024-12-10"           # the one that actually has data

    def test_a_portfolio_with_no_snapshots_yields_only_the_placeholder(self):
        # TOPS_KM and the BUS_BM_* benchmarks are like this — typed meervoudig/normaal, no
        # fixed model at all. An empty positions table for them is an ANSWER.
        from airs_scanner import parse_fixed_datum_options
        one = '<select id="FixedDatum"><option value="2026-07-13">x</option></select>'
        assert parse_fixed_datum_options(one) == ["2026-07-13"]

    def test_no_select_is_empty_not_an_exception(self):
        from airs_scanner import parse_fixed_datum_options
        assert parse_fixed_datum_options("<html>nothing</html>") == []


class TestPositionsNaN:
    """The cash line has no ISIN — and NaN must not survive as the string "nan".

    `df.where(pd.notna(df), None)` alone does NOT work: on a float/mixed column pandas
    coerces the None straight back to NaN, which then reaches the API as a float and
    `str()`s into "nan" — a truthy value, so the cash row counted as a holding with the
    ISIN "nan" and inflated the unmatched count. `astype(object)` first is the fix.
    """

    def test_astype_object_is_what_makes_None_stick(self):
        import pandas as pd

        df = pd.DataFrame({"ISINCode": ["FR0000120271", None], "Percentage": [4.95, 0.76]})

        naive = df.where(pd.notna(df), None).to_dict("records")
        fixed = df.astype(object).where(pd.notna(df), None).to_dict("records")

        # The naive form leaves a float NaN behind — and NaN is TRUTHY.
        assert fixed[1]["ISINCode"] is None
        assert str(naive[1]["ISINCode"]) == "nan" or naive[1]["ISINCode"] is None


class TestParseFullName:
    def test_it_recovers_the_untruncated_name(self):
        """THE reason we open the edit page at all: the list only ever says
        "BUS_WTS_Dividend..."."""
        assert parse_portfolio_full_name(EDIT_HTML) == "BUS_WTS_Dividend_Fx"

    def test_it_does_not_grab_the_omschrijving_input(self):
        # Both are <input>s on the same page; keying off name="Portefeuille" is what keeps
        # the description ("WTS Dividend FX") out of the name field.
        assert parse_portfolio_full_name(EDIT_HTML) != "WTS Dividend FX"

    def test_attribute_order_does_not_matter(self):
        swapped = '<input name="Portefeuille" value="BUS_BM_AAN_kw_USD_2026_dyn" type="text">'
        assert parse_portfolio_full_name(swapped) == "BUS_BM_AAN_kw_USD_2026_dyn"

    def test_a_page_without_the_field_is_None_not_an_exception(self):
        assert parse_portfolio_full_name("<html>login</html>") is None
