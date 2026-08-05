"""The Transacties sheet parser — pure, over in-memory Excel bytes. No AIRS, no DB, no network.

⚠ THESE TESTS PIN BEHAVIOUR, NOT A SCHEMA, and that is the point. No column of the real TRANS
report has been measured yet (see `airs_transacties`), so there is nothing truthful to assert
about `Bedrag` or `Fonds`. What CAN be pinned is that whatever arrives survives the trip intact:
every column kept, every blank an actual null rather than the truthy string "nan", every date an
ISO day rather than a timestamp.
"""
from __future__ import annotations

from io import BytesIO

import pandas as pd
import pytest

from airs_transacties import KIND_DATE, KIND_NUMBER, KIND_TEXT, parse_transacties


def _xlsx(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    df.to_excel(buf, index=False)
    return buf.getvalue()


class TestNothingIsLost:
    def test_every_column_survives_in_the_sheets_own_order(self):
        sheet = parse_transacties(_xlsx(pd.DataFrame({
            "Boekdatum": ["2026-03-12"], "Fonds": ["ASML"], "Aantal": [10],
            "SomeColumnWeHaveNeverSeen": ["x"],
        })))
        # ⚠ The report's order, not ours. A reader comparing this against the AIRS export must not
        # have to re-find the columns.
        assert sheet.columns == ["Boekdatum", "Fonds", "Aantal", "SomeColumnWeHaveNeverSeen"]
        assert sheet.rows[0]["SomeColumnWeHaveNeverSeen"] == "x"

    def test_an_unnamed_column_is_kept_and_named_readably(self):
        # pandas calls a header-less column "Unnamed: 1". Dropping it could lose the one field that
        # matters; printing pandas' placeholder tells the reader about pandas, not about AIRS.
        raw = _xlsx(pd.DataFrame({"Fonds": ["ASML"], "": ["kept"]}))
        sheet = parse_transacties(raw)
        assert len(sheet.columns) == 2
        assert "(column 2)" in sheet.columns
        assert sheet.rows[0]["(column 2)"] == "kept"

    def test_header_whitespace_is_stripped(self):
        # " Fonds" and "Fonds" are different keys to everything downstream.
        sheet = parse_transacties(_xlsx(pd.DataFrame({" Fonds ": ["ASML"]})))
        assert sheet.columns == ["Fonds"]
        assert sheet.rows[0]["Fonds"] == "ASML"


class TestTheNanTrap:
    """A blank Excel cell is float NaN. `str()` renders it "nan", which is TRUTHY — the same trap
    that once counted a cash line as a holding."""

    def test_a_blank_text_cell_is_none_not_the_string_nan(self):
        sheet = parse_transacties(_xlsx(pd.DataFrame({"Fonds": ["ASML", None]})))
        assert sheet.rows[1]["Fonds"] is None
        assert sheet.rows[1]["Fonds"] != "nan"

    def test_a_blank_number_cell_is_none_not_nan(self):
        sheet = parse_transacties(_xlsx(pd.DataFrame({"Aantal": [10.0, None]})))
        v = sheet.rows[1]["Aantal"]
        assert v is None
        # NaN != NaN — if a NaN leaked through, this is what would catch it.
        assert not (isinstance(v, float) and v != v)

    def test_the_literal_string_nan_is_also_treated_as_blank(self):
        sheet = parse_transacties(_xlsx(pd.DataFrame({"Fonds": ["nan"]})))
        assert sheet.rows[0]["Fonds"] is None


class TestTyping:
    def test_a_date_column_becomes_an_iso_day_not_a_timestamp(self):
        # AIRS books a transaction to a DAY; "2026-03-12T00:00:00" would imply a precision the
        # report does not carry.
        sheet = parse_transacties(_xlsx(pd.DataFrame({
            "Boekdatum": pd.to_datetime(["2026-03-12"])})))
        assert sheet.kinds["Boekdatum"] == KIND_DATE
        assert sheet.rows[0]["Boekdatum"] == "2026-03-12"

    def test_a_numeric_column_stays_a_number(self):
        sheet = parse_transacties(_xlsx(pd.DataFrame({"Bedrag": [-1234.56]})))
        assert sheet.kinds["Bedrag"] == KIND_NUMBER
        assert sheet.rows[0]["Bedrag"] == pytest.approx(-1234.56)

    def test_a_number_exported_as_TEXT_stays_text(self):
        # ⚠ THE DTYPE DECIDES, NEVER THE NAME. A column called "Bedrag" that AIRS exported as text
        # is a visible fact about the export; coercing it would hide that, and a value that only
        # sometimes parses is how a total silently omits rows.
        sheet = parse_transacties(_xlsx(pd.DataFrame({"Bedrag": ["1.234,56"]})))
        assert sheet.kinds["Bedrag"] == KIND_TEXT
        assert sheet.rows[0]["Bedrag"] == "1.234,56"

    def test_a_sign_is_never_flipped(self):
        # Whatever convention AIRS books a sale under, it is reported as reported. Same rule as
        # `airs_mutaties`, where re-deriving the sign from Debet/Credit is the documented way to
        # double-count.
        sheet = parse_transacties(_xlsx(pd.DataFrame({"Bedrag eur": [-500.0, 500.0]})))
        assert [r["Bedrag eur"] for r in sheet.rows] == [-500.0, 500.0]


class TestEmptyIsAnAnswer:
    def test_a_sheet_with_headers_and_no_rows_parses_to_zero_rows(self):
        # A book that has not traded this year. The caller treats a RAISE as a failure, so this
        # must not raise — "no transactions" and "we could not ask" must not look alike.
        sheet = parse_transacties(_xlsx(pd.DataFrame({"Fonds": pd.Series([], dtype=object)})))
        assert sheet.rows == []
        assert sheet.columns == ["Fonds"]

    def test_a_wholly_blank_row_is_a_spacer_and_is_dropped(self):
        # AIRS pads some exports with blank lines between sections; counting them would report
        # trades that never happened.
        sheet = parse_transacties(_xlsx(pd.DataFrame({
            "Fonds": ["ASML", None, "MSFT"], "Aantal": [1.0, None, 2.0]})))
        assert [r["Fonds"] for r in sheet.rows] == ["ASML", "MSFT"]

    def test_a_partly_blank_row_is_kept(self):
        # Only a WHOLLY empty row is a spacer. A real row with one missing field is a real row.
        sheet = parse_transacties(_xlsx(pd.DataFrame({
            "Fonds": ["ASML", "MSFT"], "Aantal": [1.0, None]})))
        assert len(sheet.rows) == 2
        assert sheet.rows[1]["Aantal"] is None
