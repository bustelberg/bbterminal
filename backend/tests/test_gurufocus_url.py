"""Unit tests for `ingest/gurufocus_url.py` — the single canonical builder
for GuruFocus summary URLs. A mismatched URL (US name with a prefix, or a
foreign name without one) 404s, so this helper is the one place the rule
lives. Pure; mirrors `frontend/lib/gurufocusUrl.ts`.
"""
from __future__ import annotations

import pytest

from ingest.gurufocus_url import gurufocus_url, pad_hkse_ticker


class TestPadHkseTicker:
    def test_pads_numeric_hkse_to_five(self):
        assert pad_hkse_ticker("1", "HKSE") == "00001"
        assert pad_hkse_ticker("700", "HKSE") == "00700"

    def test_already_padded_unchanged(self):
        assert pad_hkse_ticker("00700", "HKSE") == "00700"

    def test_lowercase_exchange(self):
        assert pad_hkse_ticker("5", "hkse") == "00005"

    def test_non_numeric_hkse_passes_through(self):
        assert pad_hkse_ticker("ABC", "HKSE") == "ABC"

    def test_other_exchange_untouched(self):
        assert pad_hkse_ticker("700", "XTKS") == "700"

    def test_none_and_blank(self):
        assert pad_hkse_ticker(None, "HKSE") == ""
        assert pad_hkse_ticker("  1  ", "HKSE") == "00001"


class TestUsListings:
    @pytest.mark.parametrize("exchange", ["NYSE", "NASDAQ", "AMEX", "CBOE", "CBOE BZX", "US"])
    def test_us_exchanges_produce_bare_url(self, exchange):
        assert gurufocus_url("AAPL", exchange) == "https://www.gurufocus.com/stock/AAPL/summary"

    def test_empty_exchange_treated_as_us(self):
        assert gurufocus_url("AAPL", "") == "https://www.gurufocus.com/stock/AAPL/summary"
        assert gurufocus_url("AAPL", None) == "https://www.gurufocus.com/stock/AAPL/summary"

    def test_lowercase_us_code_normalized(self):
        # exchange is upper-cased before the US-set check.
        assert gurufocus_url("AAPL", "nasdaq") == "https://www.gurufocus.com/stock/AAPL/summary"


class TestForeignListings:
    def test_prefixed_url(self):
        assert gurufocus_url("NESN", "XSWX") == "https://www.gurufocus.com/stock/XSWX:NESN/summary"

    def test_exchange_uppercased(self):
        assert gurufocus_url("NESN", "xswx") == "https://www.gurufocus.com/stock/XSWX:NESN/summary"


class TestHkseZeroPad:
    def test_short_numeric_padded_to_five(self):
        # CK Hutchison: HKSE ticker "1" must become 00001 or GuruFocus 404s.
        assert gurufocus_url("1", "HKSE") == "https://www.gurufocus.com/stock/HKSE:00001/summary"

    def test_three_digit_padded(self):
        assert gurufocus_url("700", "HKSE") == "https://www.gurufocus.com/stock/HKSE:00700/summary"

    def test_already_padded_unchanged(self):
        assert gurufocus_url("00700", "HKSE") == "https://www.gurufocus.com/stock/HKSE:00700/summary"

    def test_lowercase_hkse_padded(self):
        assert gurufocus_url("5", "hkse") == "https://www.gurufocus.com/stock/HKSE:00005/summary"

    def test_non_numeric_hkse_ticker_not_padded(self):
        # Defensive: a non-numeric HKSE ticker is left alone (no zfill on letters).
        assert gurufocus_url("ABC", "HKSE") == "https://www.gurufocus.com/stock/HKSE:ABC/summary"

    def test_other_exchanges_not_padded(self):
        # Padding is HKSE-only — numeric tickers elsewhere stay as-is.
        assert gurufocus_url("700", "XTKS") == "https://www.gurufocus.com/stock/XTKS:700/summary"


class TestNoneCases:
    def test_missing_ticker_returns_none(self):
        assert gurufocus_url(None, "NYSE") is None
        assert gurufocus_url("", "NYSE") is None

    def test_whitespace_only_ticker_returns_none(self):
        assert gurufocus_url("   ", "XSWX") is None

    def test_ticker_is_trimmed(self):
        assert gurufocus_url("  AAPL  ", "NASDAQ") == "https://www.gurufocus.com/stock/AAPL/summary"
