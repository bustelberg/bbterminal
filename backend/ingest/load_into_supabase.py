from __future__ import annotations

import json
from dataclasses import dataclass

import pandas as pd
from supabase import Client

from .transformation import PreparedForSchema

_BATCH_SIZE = 500


@dataclass
class LoadResult:
    company_inserted: int
    metric_data_inserted: int


def _df_to_rows(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to JSON-safe list of dicts (NaN -> None, datetime -> ISO date string)."""
    return json.loads(df.to_json(orient="records", date_format="iso", date_unit="s"))


def _upsert_batched(
    supabase: Client,
    table: str,
    rows: list[dict],
    on_conflict: str,
) -> int:
    """Upsert rows in batches, ignoring conflicts. Returns count of newly inserted rows."""
    if not rows:
        return 0
    inserted = 0
    for i in range(0, len(rows), _BATCH_SIZE):
        batch = rows[i : i + _BATCH_SIZE]
        resp = supabase.table(table).upsert(
            batch, on_conflict=on_conflict, ignore_duplicates=True
        ).execute()
        inserted += len(resp.data)
    return inserted



def get_ticker_overrides(supabase: Client) -> list[dict]:
    """
    Fetch all rows from the ticker_override table.
    Returns list of {ticker, primary_ticker, primary_exchange, source}.
    """
    resp = supabase.table("ticker_override").select("ticker,primary_ticker,primary_exchange,source").limit(10000).execute()
    return resp.data or []


def save_ticker_overrides(supabase: Client, overrides: list[dict]) -> int:
    """
    Upsert resolved ticker overrides into the ticker_override table.
    Returns count of newly inserted rows.
    """
    if not overrides:
        return 0
    rows = [
        {
            "ticker": o["ticker"],
            "primary_ticker": o["primary_ticker"],
            "primary_exchange": o["primary_exchange"],
            "source": o.get("source", "openfigi"),
        }
        for o in overrides
    ]
    resp = supabase.table("ticker_override").upsert(rows, on_conflict="ticker", ignore_duplicates=True).execute()
    return len(resp.data)


def fix_company_primary_keys(supabase: Client, corrections: list[dict]) -> int:
    """
    For each correction {ticker, primary_ticker, primary_exchange}, update any company
    row that was loaded with the fallback primary_exchange='UNKNOWN' for that ticker.
    Returns total rows updated.
    """
    fixed = 0
    for c in corrections:
        try:
            resp = (
                supabase.table("company")
                .update({
                    "primary_ticker": c["primary_ticker"],
                    "primary_exchange": c["primary_exchange"],
                })
                .eq("longequity_ticker", c["ticker"])
                .eq("primary_exchange", "UNKNOWN")
                .execute()
            )
            fixed += len(resp.data)
        except Exception:
            pass  # skip if uniqueness conflict with an already-correct row
    return fixed


def load_prepared_into_supabase(prepared: PreparedForSchema, supabase: Client) -> LoadResult:
    """
    Load a PreparedForSchema into Supabase.
    Upserts companies, then resolves company_ids and upserts metric_data.
    """
    # ------------------------------------------------------------------ #
    # 1. COMPANY
    # ------------------------------------------------------------------ #
    company_rows = _df_to_rows(prepared.company)
    company_inserted = _upsert_batched(
        supabase, "company", company_rows, on_conflict="primary_ticker,primary_exchange"
    )

    # Fetch company_id lookup map
    all_companies = (
        supabase.table("company")
        .select("company_id,primary_ticker,primary_exchange")
        .limit(10000)
        .execute()
    )
    company_id_map: dict[tuple[str, str], int] = {
        (r["primary_ticker"], r["primary_exchange"]): r["company_id"]
        for r in all_companies.data
    }

    # ------------------------------------------------------------------ #
    # 2. METRIC_DATA
    # ------------------------------------------------------------------ #
    md_rows: list[dict] = []
    for _, row in prepared.metric_data.iterrows():
        cid = company_id_map.get((row["primary_ticker"], row["primary_exchange"]))
        if cid is None:
            continue

        nv = row.get("numeric_value")
        tv = row.get("text_value")

        # Skip if both values are null
        has_numeric = nv is not None and not (isinstance(nv, float) and pd.isna(nv))
        has_text = tv is not None and str(tv) not in ("", "<NA>", "None") and not (isinstance(tv, float) and pd.isna(tv))
        if not has_numeric and not has_text:
            continue

        entry: dict = {
            "company_id": cid,
            "metric_code": str(row["metric_code"]),
            "source_code": str(row["source_code"]),
            "target_date": str(row["target_date"]),
            "is_prediction": bool(row.get("is_prediction", False)),
        }
        if has_numeric:
            entry["numeric_value"] = float(nv)
        if has_text:
            entry["text_value"] = str(tv)

        md_rows.append(entry)

    metric_data_inserted = _upsert_batched(
        supabase, "metric_data", md_rows,
        on_conflict="company_id,metric_code,source_code,target_date",
    )

    return LoadResult(
        company_inserted=company_inserted,
        metric_data_inserted=metric_data_inserted,
    )
