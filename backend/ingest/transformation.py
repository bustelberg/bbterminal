from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
    is_string_dtype,
)


@dataclass(frozen=True)
class PreparedForSchema:
    company: pd.DataFrame
    metric: pd.DataFrame
    snapshot: pd.DataFrame
    source: pd.DataFrame
    facts_number: pd.DataFrame
    facts_text: pd.DataFrame


def _infer_value_type(series: pd.Series) -> Literal["number", "text", "bool", "date"]:
    if is_bool_dtype(series.dtype):
        return "bool"
    if is_datetime64_any_dtype(series.dtype):
        return "date"
    if is_numeric_dtype(series.dtype):
        return "number"
    return "text"


def _to_text_series(series: pd.Series) -> pd.Series:
    if is_string_dtype(series.dtype) or is_object_dtype(series.dtype):
        return series.astype("string")
    return series.astype("string")


def _normalize_as_of_date(d: date | str | pd.Timestamp | None) -> pd.Timestamp:
    if d is None:
        d = date.today()
    return pd.Timestamp(d).normalize()


def prepare_flattened_for_schema(
    df_flat: pd.DataFrame,
    *,
    source_code: str = "longequity",
    as_of_date: date | str | pd.Timestamp | None = None,
) -> PreparedForSchema:
    if df_flat is None or df_flat.empty:
        raise ValueError("df_flat is empty; nothing to prepare.")

    df = df_flat.copy()

    if as_of_date is None:
        as_of_date = df.attrs.get("as_of_date") or df.attrs.get("asof_date")
    as_of_ts = _normalize_as_of_date(as_of_date)

    required = {"ticker", "company", "country", "primary_ticker", "primary_exchange"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    for c in ["ticker", "company", "country", "primary_ticker", "primary_exchange"]:
        df[c] = _to_text_series(df[c])

    for c in ["primary_ticker", "primary_exchange"]:
        if df[c].isna().any():
            raise ValueError(f"Found NULL values in required column '{c}'.")
        if (df[c].astype("string").str.strip() == "").any():
            raise ValueError(f"Found empty values in required column '{c}'.")

    if "sector" in df.columns:
        df["sector"] = _to_text_series(df["sector"])

    # Company dimension
    company_dict: dict = {
        "longequity_ticker": df["ticker"],
        "primary_ticker": df["primary_ticker"],
        "primary_exchange": df["primary_exchange"],
        "country": df["country"],
        "company_name": df["company"],
    }
    if "sector" in df.columns:
        company_dict["sector"] = df["sector"]

    company = pd.DataFrame(company_dict)
    company = company.dropna(subset=["primary_ticker", "primary_exchange"])
    company = company.drop_duplicates(subset=["primary_ticker", "primary_exchange"]).reset_index(drop=True)

    company_cols = ["longequity_ticker", "primary_ticker", "primary_exchange", "country", "company_name"]
    if "sector" in company.columns:
        company_cols.append("sector")
    for col in company_cols:
        company[col] = company[col].astype("string")

    # Metric definitions
    non_metric_cols = {"ticker", "company", "country", "primary_ticker", "primary_exchange"}
    if "sector" in df.columns:
        non_metric_cols.add("sector")
    metric_cols = [c for c in df.columns if c not in non_metric_cols]

    metric = pd.DataFrame([
        {"metric_code": str(col), "value_type": _infer_value_type(df[col])}
        for col in metric_cols
    ]).drop_duplicates(subset=["metric_code"]).reset_index(drop=True)
    metric["metric_code"] = metric["metric_code"].astype("string")
    metric["value_type"] = metric["value_type"].astype("string")

    # Snapshot + source
    snapshot = pd.DataFrame({"as_of_date": pd.to_datetime([as_of_ts]).normalize()})
    source = pd.DataFrame({"source_code": pd.Series([source_code], dtype="string")})

    # Long-form facts
    long = df[["ticker", "primary_ticker", "primary_exchange"] + metric_cols].melt(
        id_vars=["ticker", "primary_ticker", "primary_exchange"],
        var_name="metric_code",
        value_name="metric_value_raw",
    )
    long["longequity_ticker"] = _to_text_series(long["ticker"])
    long["primary_ticker"] = _to_text_series(long["primary_ticker"])
    long["primary_exchange"] = _to_text_series(long["primary_exchange"])
    long["as_of_date"] = pd.to_datetime(as_of_ts).normalize()
    long["source_code"] = source_code
    long = long.drop(columns=["ticker"])

    metric_type_map = dict(zip(metric["metric_code"], metric["value_type"]))
    long["value_type"] = long["metric_code"].map(metric_type_map).fillna("text")

    num_mask = long["value_type"].eq("number")

    facts_number = long.loc[num_mask, [
        "longequity_ticker", "primary_ticker", "primary_exchange",
        "metric_code", "as_of_date", "source_code", "metric_value_raw",
    ]].copy()
    facts_number["metric_value"] = pd.to_numeric(facts_number["metric_value_raw"], errors="coerce")
    facts_number = facts_number.drop(columns=["metric_value_raw"])

    facts_text = long.loc[~num_mask, [
        "longequity_ticker", "primary_ticker", "primary_exchange",
        "metric_code", "as_of_date", "source_code", "metric_value_raw",
    ]].copy()
    facts_text["metric_value"] = _to_text_series(facts_text["metric_value_raw"]).astype("string")
    facts_text = facts_text.drop(columns=["metric_value_raw"])

    pk = ["primary_ticker", "primary_exchange", "metric_code", "as_of_date", "source_code"]
    for df_out in (facts_number, facts_text):
        for col in ["longequity_ticker", "primary_ticker", "primary_exchange", "metric_code", "source_code"]:
            df_out[col] = df_out[col].astype("string")
        df_out["as_of_date"] = pd.to_datetime(df_out["as_of_date"]).dt.normalize()
    facts_number = facts_number.drop_duplicates(subset=pk).reset_index(drop=True)
    facts_text = facts_text.drop_duplicates(subset=pk).reset_index(drop=True)

    facts_number["is_prediction"] = False
    facts_number = facts_number[[
        "longequity_ticker", "primary_ticker", "primary_exchange",
        "metric_code", "as_of_date", "source_code", "metric_value", "is_prediction",
    ]]
    facts_number["is_prediction"] = facts_number["is_prediction"].astype("bool")

    facts_text = facts_text[[
        "longequity_ticker", "primary_ticker", "primary_exchange",
        "metric_code", "as_of_date", "source_code", "metric_value",
    ]]

    base_company_cols = ["longequity_ticker", "primary_ticker", "primary_exchange", "country", "company_name"]
    if "sector" in company.columns:
        base_company_cols.append("sector")
    company = company[base_company_cols]

    return PreparedForSchema(
        company=company,
        metric=metric,
        snapshot=snapshot,
        source=source,
        facts_number=facts_number,
        facts_text=facts_text,
    )
