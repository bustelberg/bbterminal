"""Unit tests for `ingest/gurufocus_url.py` — the single canonical builder
for GuruFocus summary URLs. A mismatched URL (US name with a prefix, or a
foreign name without one) 404s, so this helper is the one place the rule
lives. Pure; mirrors `frontend/lib/gurufocusUrl.ts`.
"""
from __future__ import annotations

import pytest

from ingest.gurufocus_url import gurufocus_url


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


class TestNoneCases:
    def test_missing_ticker_returns_none(self):
        assert gurufocus_url(None, "NYSE") is None
        assert gurufocus_url("", "NYSE") is None

    def test_whitespace_only_ticker_returns_none(self):
        assert gurufocus_url("   ", "XSWX") is None

    def test_ticker_is_trimmed(self):
        assert gurufocus_url("  AAPL  ", "NASDAQ") == "https://www.gurufocus.com/stock/AAPL/summary"
