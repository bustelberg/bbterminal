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
    metric_inserted: int
    snapshot_inserted: int
    source_inserted: int
    facts_number_inserted: int
    facts_text_inserted: int


def _df_to_rows(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to JSON-safe list of dicts (NaN → None, datetime → ISO date string)."""
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
    Load a PreparedForSchema into Supabase using the supabase-py client.
    Each table is upserted with ON CONFLICT DO NOTHING semantics.
    Dimension IDs are resolved by querying back after upsert.
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
    # 2. METRIC
    # ------------------------------------------------------------------ #
    metric_rows = _df_to_rows(prepared.metric)
    metric_inserted = _upsert_batched(
        supabase, "metric", metric_rows, on_conflict="metric_code"
    )

    all_metrics = (
        supabase.table("metric")
        .select("metric_id,metric_code")
        .limit(10000)
        .execute()
    )
    metric_id_map: dict[str, int] = {
        r["metric_code"]: r["metric_id"] for r in all_metrics.data
    }

    # ------------------------------------------------------------------ #
    # 3. SNAPSHOT
    # ------------------------------------------------------------------ #
    snapshot_df = prepared.snapshot.copy()
    as_of_str = snapshot_df["as_of_date"].iloc[0].strftime("%Y-%m-%d")
    snapshot_upsert_rows = [{"target_date": as_of_str, "published_at": as_of_str}]
    snapshot_inserted = _upsert_batched(
        supabase, "snapshot", snapshot_upsert_rows, on_conflict="target_date,published_at"
    )

    snap_resp = (
        supabase.table("snapshot")
        .select("snapshot_id,target_date,published_at")
        .eq("target_date", as_of_str)
        .eq("published_at", as_of_str)
        .execute()
    )
    snapshot_id: int = snap_resp.data[0]["snapshot_id"]

    # ------------------------------------------------------------------ #
    # 4. SOURCE
    # ------------------------------------------------------------------ #
    source_code = str(prepared.source["source_code"].iloc[0])
    source_inserted = _upsert_batched(
        supabase, "source", [{"source_code": source_code}], on_conflict="source_code"
    )

    src_resp = (
        supabase.table("source")
        .select("source_id,source_code")
        .eq("source_code", source_code)
        .execute()
    )
    source_id: int = src_resp.data[0]["source_id"]

    # ------------------------------------------------------------------ #
    # 5. FACTS_NUMBER
    # ------------------------------------------------------------------ #
    fn_rows: list[dict] = []
    for _, row in prepared.facts_number.iterrows():
        cid = company_id_map.get((row["primary_ticker"], row["primary_exchange"]))
        mid = metric_id_map.get(row["metric_code"])
        if cid is None or mid is None:
            continue
        if pd.isna(row["metric_value"]):
            continue
        fn_rows.append({
            "company_id": cid,
            "metric_id": mid,
            "snapshot_id": snapshot_id,
            "source_id": source_id,
            "metric_value": float(row["metric_value"]),
            "is_prediction": bool(row["is_prediction"]),
        })

    facts_number_inserted = _upsert_batched(
        supabase, "facts_number", fn_rows,
        on_conflict="company_id,metric_id,snapshot_id,source_id",
    )

    # ------------------------------------------------------------------ #
    # 6. FACTS_TEXT
    # ------------------------------------------------------------------ #
    ft_rows: list[dict] = []
    for _, row in prepared.facts_text.iterrows():
        cid = company_id_map.get((row["primary_ticker"], row["primary_exchange"]))
        mid = metric_id_map.get(row["metric_code"])
        if cid is None or mid is None:
            continue
        mv = row["metric_value"]
        ft_rows.append({
            "company_id": cid,
            "metric_id": mid,
            "snapshot_id": snapshot_id,
            "source_id": source_id,
            "metric_value": None if pd.isna(mv) else str(mv),
        })

    facts_text_inserted = _upsert_batched(
        supabase, "facts_text", ft_rows,
        on_conflict="company_id,metric_id,snapshot_id,source_id",
    )

    return LoadResult(
        company_inserted=company_inserted,
        metric_inserted=metric_inserted,
        snapshot_inserted=snapshot_inserted,
        source_inserted=source_inserted,
        facts_number_inserted=facts_number_inserted,
        facts_text_inserted=facts_text_inserted,
    )
