"""Unit tests for `ingest/transformation.py` — value-type inference, as-of
date normalization, and the `prepare_flattened_for_schema` reshape that
splits a flat LongEquity DF into a company dimension + a long metric_data
frame. Pure (pandas only).
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from ingest.transformation import (
    _infer_value_type,
    _normalize_as_of_date,
    prepare_flattened_for_schema,
)


class TestInferValueType:
    def test_bool_is_text(self):
        assert _infer_value_type(pd.Series([True, False])) == "text"

    def test_datetime_is_text(self):
        assert _infer_value_type(pd.to_datetime(pd.Series(["2026-01-01", "2026-02-01"]))) == "text"

    def test_numeric_is_number(self):
        assert _infer_value_type(pd.Series([1.0, 2.5, 3.0])) == "number"

    def test_object_is_text(self):
        assert _infer_value_type(pd.Series(["a", "b"])) == "text"


class TestNormalizeAsOfDate:
    def test_none_defaults_to_today(self):
        assert _normalize_as_of_date(None) == pd.Timestamp(date.today()).normalize()

    def test_string_parsed(self):
        assert _normalize_as_of_date("2026-03-15") == pd.Timestamp("2026-03-15")

    def test_timestamp_time_component_stripped(self):
        assert _normalize_as_of_date(pd.Timestamp("2026-03-15 14:37:00")) == pd.Timestamp("2026-03-15")


class TestPrepareFlattenedForSchema:
    def _df(self):
        return pd.DataFrame({
            "ticker": ["AAPL", "MSFT"],
            "company": ["Apple", "Microsoft"],
            "country": ["USA", "USA"],
            "gurufocus_ticker": ["AAPL", "MSFT"],
            "gurufocus_exchange": ["NASDAQ", "NASDAQ"],
            "pe_ratio": [30.1, 35.4],
        })

    def test_happy_path_company_and_metrics(self):
        out = prepare_flattened_for_schema(self._df(), source_code="longequity", as_of_date="2026-03-15")
        assert out.source_code == "longequity"
        assert out.target_date == date(2026, 3, 15)
        assert len(out.company) == 2
        # The metric column lands in the long metric_data frame.
        assert "pe_ratio" in set(out.metric_data["metric_code"])

    def test_dedup_on_gurufocus_identity(self):
        df = self._df()
        # Same (gf_ticker, gf_exchange) twice → one company row.
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        out = prepare_flattened_for_schema(df, as_of_date="2026-03-15")
        assert len(out.company) == 2

    def test_missing_required_columns_raises(self):
        df = self._df().drop(columns=["country"])
        with pytest.raises(ValueError, match="Missing required columns"):
            prepare_flattened_for_schema(df, as_of_date="2026-03-15")

    def test_empty_df_raises(self):
        with pytest.raises(ValueError, match="empty"):
            prepare_flattened_for_schema(pd.DataFrame(), as_of_date="2026-03-15")

    def test_empty_gurufocus_exchange_raises(self):
        df = self._df()
        df.loc[0, "gurufocus_exchange"] = ""
        with pytest.raises(ValueError, match="empty values"):
            prepare_flattened_for_schema(df, as_of_date="2026-03-15")
