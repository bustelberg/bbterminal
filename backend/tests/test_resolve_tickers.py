"""Unit tests for the pure helpers in `ingest/resolve_tickers.py` — the
OpenFIGI ticker resolution path. The gnarly one is `_best_match`: OpenFIGI
returns every listing for an ISIN, and picking the wrong one routes a
company to an OTC sub-class (the Baidu BIDU→BIDUN / Baloise BALN→BLON
bug). No network — these feed `_best_match` synthetic OpenFIGI arrays.
"""
from __future__ import annotations

import json

import pandas as pd

from ingest.resolve_tickers import (
    _best_match,
    _exchcode_to_exchange,
    _normalize_ticker_for_gurufocus,
    detect_unknown_tickers,
)


class TestExchcodeToExchange:
    def test_empty_is_unknown(self):
        assert _exchcode_to_exchange(None) == "UNKNOWN"
        assert _exchcode_to_exchange("") == "UNKNOWN"

    def test_mapped_codes(self):
        assert _exchcode_to_exchange("HK") == "HKSE"
        assert _exchcode_to_exchange("SW") == "XSWX"
        assert _exchcode_to_exchange("JP") == "TSE"

    def test_stockholm_ss_is_not_shanghai(self):
        # Bloomberg "SS" = Stockholm, NOT Shanghai (out of scope) — a
        # mapping mistake here would misroute Nordic names to China.
        assert _exchcode_to_exchange("SS") == "OSTO"

    def test_unmapped_code_passthrough(self):
        assert _exchcode_to_exchange("ZZ") == "ZZ"


class TestNormalizeTickerForGurufocus:
    def test_nordic_class_dot_to_space(self):
        assert _normalize_ticker_for_gurufocus("NOVO.B", "OCSE") == "NOVO B"

    def test_nordic_class_dash_to_space(self):
        assert _normalize_ticker_for_gurufocus("ATCO-A", "OSTO") == "ATCO A"

    def test_non_nordic_untouched(self):
        assert _normalize_ticker_for_gurufocus("BRK.B", "NYSE") == "BRK.B"

    def test_nordic_without_class_suffix_untouched(self):
        assert _normalize_ticker_for_gurufocus("VOLV", "OSTO") == "VOLV"


class TestBestMatch:
    def test_empty_results_is_none(self):
        assert _best_match([]) is None

    def test_prefers_common_stock_on_mapped_exchange(self):
        # The sub-class on an unmapped exchange must lose to the primary
        # Common Stock on a mapped one (HK).
        results = [
            {"securityType": "Common Stock", "exchCode": "ZZ", "ticker": "SUB"},
            {"securityType": "Common Stock", "exchCode": "HK", "ticker": "PRIM"},
        ]
        assert _best_match(results)["ticker"] == "PRIM"

    def test_ordinary_shares_also_qualify(self):
        results = [
            {"securityType": "ETP", "exchCode": "HK", "ticker": "ETF"},
            {"securityType": "Ordinary Shares", "exchCode": "SW", "ticker": "BALN"},
        ]
        assert _best_match(results)["ticker"] == "BALN"

    def test_falls_back_to_any_common_stock_when_no_mapped_exchange(self):
        results = [
            {"securityType": "Preferred", "exchCode": "HK", "ticker": "PFD"},
            {"securityType": "Common Stock", "exchCode": "ZZ", "ticker": "ANY"},
        ]
        assert _best_match(results)["ticker"] == "ANY"

    def test_falls_back_to_first_result_when_no_common_stock(self):
        results = [
            {"securityType": "Warrant", "exchCode": "ZZ", "ticker": "WT"},
            {"securityType": "Bond", "exchCode": "ZZ", "ticker": "BD"},
        ]
        assert _best_match(results)["ticker"] == "WT"


class TestDetectUnknownTickers:
    def _df(self, rows):
        return pd.DataFrame(rows)

    def test_known_tickers_filtered_out(self, tmp_path):
        fill = tmp_path / "fill.json"
        fill.write_text(json.dumps([{"ticker": "AAPL"}]), encoding="utf-8")
        df = self._df([
            {"ticker": "AAPL", "country": "USA", "exchange": "NASDAQ"},
            {"ticker": "ZZZZ", "country": "USA", "exchange": "NYSE"},
        ])
        unknowns = detect_unknown_tickers(df, fill_path=fill)
        assert [u["ticker"] for u in unknowns] == ["ZZZZ"]

    def test_db_overrides_count_as_known(self, tmp_path):
        fill = tmp_path / "fill.json"
        fill.write_text("[]", encoding="utf-8")
        df = self._df([{"ticker": "ZZZZ", "country": "USA", "exchange": "NYSE"}])
        unknowns = detect_unknown_tickers(df, fill_path=fill, db_overrides=[{"ticker": "ZZZZ"}])
        assert unknowns == []

    def test_dedup_case_and_delimiter_insensitive(self, tmp_path):
        fill = tmp_path / "fill.json"
        fill.write_text("[]", encoding="utf-8")
        # "NOVO-B", "novo.b", "NOVO B" all normalize to the same key.
        df = self._df([
            {"ticker": "NOVO-B", "country": "Denmark", "exchange": "OCSE"},
            {"ticker": "novo.b", "country": "Denmark", "exchange": "OCSE"},
            {"ticker": "NOVO B", "country": "Denmark", "exchange": "OCSE"},
        ])
        unknowns = detect_unknown_tickers(df, fill_path=fill)
        assert len(unknowns) == 1
