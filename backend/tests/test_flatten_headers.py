"""Unit tests for the pure header/cell helpers in `ingest/flatten.py` —
the logic that turns LongEquity's grouped two-row Excel headers into flat,
schema-safe column names. No Excel I/O; just the string transforms.
"""
from __future__ import annotations

import pandas as pd

from ingest.flatten import (
    _clean_header_cell,
    _make_unique,
    _normalize_column_name,
    _strip_unicode_text,
)


class TestCleanHeaderCell:
    def test_none_and_nan_and_unnamed_blanked(self):
        assert _clean_header_cell(None) == ""
        assert _clean_header_cell("nan") == ""
        assert _clean_header_cell("None") == ""
        assert _clean_header_cell("Unnamed: 3") == ""

    def test_whitespace_collapsed_and_stripped(self):
        assert _clean_header_cell("  Market   Cap ") == "Market Cap"

    def test_plain_value_passthrough(self):
        assert _clean_header_cell("Ticker") == "Ticker"


class TestMakeUnique:
    def test_duplicates_get_numeric_suffix(self):
        assert _make_unique(["a", "a", "a"]) == ["a", "a_2", "a_3"]

    def test_empty_names_become_col(self):
        assert _make_unique(["", ""]) == ["col", "col_2"]

    def test_distinct_names_unchanged(self):
        assert _make_unique(["x", "y", "z"]) == ["x", "y", "z"]


class TestNormalizeColumnName:
    def test_empty(self):
        assert _normalize_column_name("") == ""

    def test_group_separator_to_underscore(self):
        assert _normalize_column_name("Valuation - P/E") == "valuation_pe"

    def test_percent_and_bn_tokens(self):
        assert _normalize_column_name("Margin %") == "margin_pct"
        # "(bn)" → "bn", and the preceding space collapses to an underscore.
        assert _normalize_column_name("Revenue (bn)") == "revenue_bn"

    def test_special_chars_stripped_and_underscores_collapsed(self):
        assert _normalize_column_name("EPS (diluted) $$$") == "eps_diluted"

    def test_dots_removed(self):
        assert _normalize_column_name("E.B.I.T.D.A") == "ebitda"


class TestStripUnicodeText:
    def test_non_ascii_removed(self):
        assert _strip_unicode_text("Café — Ω") == "Caf  "

    def test_nan_passthrough(self):
        v = float("nan")
        assert pd.isna(_strip_unicode_text(v))

    def test_non_string_passthrough(self):
        assert _strip_unicode_text(42) == 42
        assert _strip_unicode_text(None) is None
