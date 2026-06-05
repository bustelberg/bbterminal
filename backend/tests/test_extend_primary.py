"""Unit tests for `ingest/extend_primary.py` — resolves each flattened
row to (gurufocus_ticker, gurufocus_exchange) via the override→fill→
existing→ticker priority chain. Pure (pandas + a tmp fill JSON).
"""
from __future__ import annotations

import json

import pandas as pd
import pytest

from ingest.extend_primary import (
    _norm_ticker,
    enrich_flattened_df_with_primary_listing,
    load_fill_df,
)


class TestNormTicker:
    def test_delimiters_to_dot_and_uppercased(self):
        assert _norm_ticker("novo-b") == "NOVO.B"
        assert _norm_ticker("novo b") == "NOVO.B"
        assert _norm_ticker("NOVO.B") == "NOVO.B"


class TestLoadFillDf:
    def test_rejects_case_insensitive_duplicates(self, tmp_path):
        fill = tmp_path / "fill.json"
        fill.write_text(json.dumps([{"ticker": "AAPL"}, {"ticker": "aapl"}]), encoding="utf-8")
        with pytest.raises(ValueError, match="Duplicate ticker"):
            load_fill_df(fill)

    def test_rejects_non_list(self, tmp_path):
        fill = tmp_path / "fill.json"
        fill.write_text(json.dumps({"ticker": "AAPL"}), encoding="utf-8")
        with pytest.raises(ValueError, match="must be a JSON list"):
            load_fill_df(fill)

    def test_loads_and_adds_normalized_key(self, tmp_path):
        fill = tmp_path / "fill.json"
        fill.write_text(json.dumps([{"ticker": "novo-b"}]), encoding="utf-8")
        df = load_fill_df(fill)
        assert df["_ticker_upper"].tolist() == ["NOVO.B"]


class TestEnrich:
    def _empty_fill(self, tmp_path):
        fill = tmp_path / "fill.json"
        fill.write_text("[]", encoding="utf-8")
        return fill

    def test_requires_ticker_column(self, tmp_path):
        with pytest.raises(ValueError, match="must contain a 'ticker' column"):
            enrich_flattened_df_with_primary_listing(
                pd.DataFrame({"x": [1]}), fill_path=self._empty_fill(tmp_path)
            )

    def test_ticker_fallback_when_no_mapping(self, tmp_path):
        df = pd.DataFrame({"ticker": ["AAPL"]})
        out = enrich_flattened_df_with_primary_listing(
            df, fill_path=self._empty_fill(tmp_path), default_exchange="UNKNOWN"
        )
        assert str(out["gurufocus_ticker"].iloc[0]) == "AAPL"
        assert str(out["gurufocus_exchange"].iloc[0]) == "UNKNOWN"

    def test_extra_overrides_take_precedence_over_fill(self, tmp_path):
        # fill says NASDAQ:AAPL, but the DB override remaps to NYSE:APLE.
        fill = tmp_path / "fill.json"
        fill.write_text(
            json.dumps([{"ticker": "AAPL", "exchange": "NASDAQ", "gurufocus_ticker": "AAPL", "gurufocus_exchange": "NASDAQ"}]),
            encoding="utf-8",
        )
        df = pd.DataFrame({"ticker": ["AAPL"]})
        out = enrich_flattened_df_with_primary_listing(
            df,
            fill_path=fill,
            extra_overrides=[{"ticker": "AAPL", "gurufocus_ticker": "APLE", "gurufocus_exchange": "NYSE"}],
        )
        assert str(out["gurufocus_ticker"].iloc[0]) == "APLE"
        assert str(out["gurufocus_exchange"].iloc[0]) == "NYSE"

    def test_fill_mapping_applied_case_insensitively(self, tmp_path):
        fill = tmp_path / "fill.json"
        fill.write_text(
            json.dumps([{"ticker": "nesn", "exchange": "XSWX", "gurufocus_ticker": "NESN", "gurufocus_exchange": "XSWX"}]),
            encoding="utf-8",
        )
        df = pd.DataFrame({"ticker": ["NESN"]})
        out = enrich_flattened_df_with_primary_listing(df, fill_path=fill)
        assert str(out["gurufocus_exchange"].iloc[0]) == "XSWX"
