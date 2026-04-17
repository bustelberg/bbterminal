from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_string_dtype,
    is_object_dtype,
)


@dataclass(frozen=True)
class PreparedForSchema:
    company: pd.DataFrame
    metric_data: pd.DataFrame  # unified: company columns + metric_code, target_date, source_code, numeric_value, text_value
    target_date: date
    source_code: str


def _infer_value_type(series: pd.Series) -> str:
    if is_bool_dtype(series.dtype):
        return "text"
    if is_datetime64_any_dtype(series.dtype):
        return "text"
    if is_numeric_dtype(series.dtype):
        return "number"
    return "text"


def _to_text_series(series: pd.Series) -> pd.Series:
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
    target_date_val = as_of_ts.date()

    required = {"ticker", "company", "country", "gurufocus_ticker", "gurufocus_exchange"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    for c in ["ticker", "company", "country", "gurufocus_ticker", "gurufocus_exchange"]:
        df[c] = _to_text_series(df[c])

    for c in ["gurufocus_ticker", "gurufocus_exchange"]:
        if df[c].isna().any():
            raise ValueError(f"Found NULL values in required column '{c}'.")
        if (df[c].astype("string").str.strip() == "").any():
            raise ValueError(f"Found empty values in required column '{c}'.")

    # Company dimension — sector and universe_ticker are stored per-universe,
    # not on the company row.  We still carry them through the pipeline so
    # the caller (load_into_supabase) can create universe_membership rows.
    company_dict: dict = {
        "universe_ticker": df["ticker"],
        "gurufocus_ticker": df["gurufocus_ticker"],
        "gurufocus_exchange": df["gurufocus_exchange"],
        "country": df["country"],
        "company_name": df["company"],
    }
    if "sector" in df.columns:
        company_dict["sector"] = _to_text_series(df["sector"])

    company = pd.DataFrame(company_dict)
    company = company.dropna(subset=["gurufocus_ticker", "gurufocus_exchange"])
    company = company.drop_duplicates(subset=["gurufocus_ticker", "gurufocus_exchange"]).reset_index(drop=True)

    company_cols = list(company.columns)
    for col in company_cols:
        company[col] = company[col].astype("string")

    # Identify metric columns
    non_metric_cols = {"ticker", "company", "country", "gurufocus_ticker", "gurufocus_exchange"}
    if "sector" in df.columns:
        non_metric_cols.add("sector")
    metric_cols = [c for c in df.columns if c not in non_metric_cols]

    # Determine value type per metric column
    metric_type_map = {col: _infer_value_type(df[col]) for col in metric_cols}

    # Melt to long form
    long = df[["gurufocus_ticker", "gurufocus_exchange"] + metric_cols].melt(
        id_vars=["gurufocus_ticker", "gurufocus_exchange"],
        var_name="metric_code",
        value_name="raw_value",
    )

    long["value_type"] = long["metric_code"].map(metric_type_map).fillna("text")

    # Split into numeric and text values
    num_mask = long["value_type"].eq("number")
    long["numeric_value"] = None
    long["text_value"] = None

    long.loc[num_mask, "numeric_value"] = pd.to_numeric(long.loc[num_mask, "raw_value"], errors="coerce")
    long.loc[~num_mask, "text_value"] = long.loc[~num_mask, "raw_value"].astype("string")

    # Drop rows where both values are null
    long = long[long["numeric_value"].notna() | (long["text_value"].notna() & (long["text_value"] != "<NA>"))].copy()

    long["source_code"] = source_code
    long["target_date"] = target_date_val
    long["is_prediction"] = False

    # Deduplicate
    pk = ["gurufocus_ticker", "gurufocus_exchange", "metric_code", "source_code", "target_date"]
    long = long.drop_duplicates(subset=pk).reset_index(drop=True)

    metric_data = long[[
        "gurufocus_ticker", "gurufocus_exchange", "metric_code",
        "source_code", "target_date", "numeric_value", "text_value", "is_prediction",
    ]]

    return PreparedForSchema(
        company=company,
        metric_data=metric_data,
        target_date=target_date_val,
        source_code=source_code,
    )
