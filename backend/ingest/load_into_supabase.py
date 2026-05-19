from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import pandas as pd
from supabase import Client

from .transformation import PreparedForSchema

_BATCH_SIZE = 500
_logger = logging.getLogger(__name__)


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


def _get_exchange_id_map(supabase: Client) -> dict[str, int]:
    """Fetch exchange_code → exchange_id mapping from gurufocus_exchange table."""
    resp = supabase.table("gurufocus_exchange").select("exchange_id,exchange_code").limit(1000).execute()
    return {r["exchange_code"]: r["exchange_id"] for r in (resp.data or [])}


def get_ticker_overrides(supabase: Client) -> list[dict]:
    """
    Fetch all rows from the ticker_override table.
    Returns list of {ticker, gurufocus_ticker, gurufocus_exchange, source}.
    """
    resp = supabase.table("ticker_override").select("ticker,gurufocus_ticker,gurufocus_exchange,source").limit(10000).execute()
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
            "gurufocus_ticker": o["gurufocus_ticker"],
            "gurufocus_exchange": o["gurufocus_exchange"],
            "source": o.get("source", "openfigi"),
        }
        for o in overrides
    ]
    resp = supabase.table("ticker_override").upsert(rows, on_conflict="ticker", ignore_duplicates=True).execute()
    return len(resp.data)


def fix_company_primary_keys(supabase: Client, corrections: list[dict]) -> int:
    """
    For each correction {ticker, gurufocus_ticker, gurufocus_exchange}, update any company
    row that was loaded with exchange_id=NULL (unknown exchange) for that ticker.
    Returns total rows updated.
    """
    exchange_map = _get_exchange_id_map(supabase)
    fixed = 0
    for c in corrections:
        new_exchange_id = exchange_map.get(c["gurufocus_exchange"])
        if new_exchange_id is None:
            continue
        try:
            resp = (
                supabase.table("company")
                .update({
                    "gurufocus_ticker": c["gurufocus_ticker"],
                    "exchange_id": new_exchange_id,
                })
                .eq("gurufocus_ticker", c["ticker"])
                .is_("exchange_id", "null")
                .execute()
            )
            fixed += len(resp.data)
        except Exception:
            pass  # skip if uniqueness conflict with an already-correct row
    return fixed


def merge_duplicate_companies(supabase: Client) -> list[str]:
    """Find companies with the same (company_name, exchange_id) and merge them.

    Keeps the company with the lowest company_id, reassigns metric_data and
    portfolio_weight rows, then deletes the duplicate.
    Returns list of log messages describing what was merged.
    """
    logs: list[str] = []
    resp = supabase.table("company").select("company_id,company_name,gurufocus_ticker,exchange_id").limit(10000).execute()
    companies = resp.data or []

    from collections import defaultdict
    groups: dict[tuple[str, int | None], list[dict]] = defaultdict(list)
    for c in companies:
        name = (c.get("company_name") or "").strip().lower()
        exchange_id = c.get("exchange_id")
        if name:
            groups[(name, exchange_id)].append(c)

    for key, group in groups.items():
        if len(group) < 2:
            continue

        group.sort(key=lambda c: c["company_id"])
        keep = group[0]
        for dup in group[1:]:
            keep_id = keep["company_id"]
            dup_id = dup["company_id"]
            msg = f"Merging duplicate: {dup['gurufocus_ticker']} (id={dup_id}) into {keep['gurufocus_ticker']} (id={keep_id}) — both \"{keep.get('company_name')}\""
            logs.append(msg)
            _logger.info(msg)

            try:
                supabase.rpc("merge_company_data", {
                    "p_from_id": dup_id,
                    "p_to_id": keep_id,
                }).execute()
            except Exception:
                try:
                    supabase.table("metric_data").delete().eq("company_id", dup_id).execute()
                except Exception as e2:
                    logs.append(f"  Warning: could not clean metric_data for id={dup_id}: {e2}")

            try:
                supabase.table("portfolio_weight").delete().eq("company_id", dup_id).execute()
            except Exception:
                pass

            try:
                supabase.table("company_source").delete().eq("company_id", dup_id).execute()
            except Exception:
                pass

            try:
                supabase.table("universe_membership").delete().eq("company_id", dup_id).execute()
            except Exception:
                pass

            try:
                supabase.table("company").delete().eq("company_id", dup_id).execute()
            except Exception as e:
                logs.append(f"  Warning: could not delete duplicate company id={dup_id}: {e}")

    return logs


def _ensure_company_source(supabase: Client, company_id: int, source_code: str) -> None:
    """Insert a company_source row if it doesn't already exist."""
    try:
        supabase.table("company_source").upsert(
            {"company_id": company_id, "source_code": source_code},
            on_conflict="company_id,source_code",
            ignore_duplicates=True,
        ).execute()
    except Exception:
        pass


def load_prepared_into_supabase(
    prepared: PreparedForSchema,
    supabase: Client,
    *,
    universe_label: str | None = None,
) -> LoadResult:
    """
    Load a PreparedForSchema into Supabase.
    Upserts companies (with exchange_id resolution), tags company_source,
    creates universe_membership rows, then upserts metric_data.
    """
    exchange_map = _get_exchange_id_map(supabase)

    # ------------------------------------------------------------------ #
    # 1. COMPANY
    # ------------------------------------------------------------------ #
    company_rows = _df_to_rows(prepared.company)

    # Resolve gurufocus_exchange → exchange_id for each company row.
    # Rows whose gurufocus_exchange doesn't resolve to a known
    # exchange_id are SKIPPED — committing NULL exchange_id is the
    # bug behind empty-exchange columns in /backtest, broken GuruFocus
    # links, and the upcoming NOT NULL constraint on company.exchange_id
    # would reject them anyway. Surface the skip count + a sample so
    # the operator knows which exchanges to add to gurufocus_exchange
    # before re-ingesting.
    skipped_no_exchange: list[dict] = []
    resolved_rows: list[dict] = []
    for row in company_rows:
        gf_exchange = row.pop("gurufocus_exchange", None)
        exchange_id = exchange_map.get(gf_exchange) if gf_exchange else None
        # Remove fields that don't belong on the company table.
        row.pop("universe_ticker", None)
        row.pop("sector", None)
        row.pop("country", None)
        if exchange_id is None:
            skipped_no_exchange.append({
                "gurufocus_ticker": row.get("gurufocus_ticker"),
                "gurufocus_exchange": gf_exchange,
                "reason": (
                    "Empty gurufocus_exchange field on the source row"
                    if not gf_exchange
                    else f"Exchange code {gf_exchange!r} not found in gurufocus_exchange table"
                ),
            })
            continue
        row["exchange_id"] = exchange_id
        resolved_rows.append(row)

    if skipped_no_exchange:
        sample = ", ".join(
            f"{s['gurufocus_ticker']}@{s['gurufocus_exchange']!r}"
            for s in skipped_no_exchange[:10]
        )
        more = f" (+{len(skipped_no_exchange) - 10} more)" if len(skipped_no_exchange) > 10 else ""
        import logging  # noqa: PLC0415
        logging.getLogger(__name__).warning(
            "[ingest] Skipped %s company row(s) with unresolvable exchange. "
            "Add the missing codes to gurufocus_exchange then re-ingest. Sample: %s%s",
            len(skipped_no_exchange), sample, more,
        )

    company_rows = resolved_rows
    company_inserted = _upsert_batched(
        supabase, "company", company_rows, on_conflict="gurufocus_ticker,exchange_id"
    )

    # Fetch company_id lookup map: (gurufocus_ticker, exchange_id) → company_id
    all_companies = (
        supabase.table("company")
        .select("company_id,gurufocus_ticker,exchange_id")
        .limit(10000)
        .execute()
    )
    company_id_map: dict[tuple[str, int | None], int] = {
        (r["gurufocus_ticker"], r["exchange_id"]): r["company_id"]
        for r in all_companies.data
    }

    # Tag company_source for all ingested companies
    source_code = prepared.source_code
    for row in company_rows:
        cid = company_id_map.get((row["gurufocus_ticker"], row["exchange_id"]))
        if cid is not None:
            _ensure_company_source(supabase, cid, source_code)

    # ------------------------------------------------------------------ #
    # 2. UNIVERSE MEMBERSHIP (if universe_label provided)
    # ------------------------------------------------------------------ #
    if universe_label:
        # Ensure universe exists
        existing = supabase.table("universe").select("universe_id").eq("label", universe_label).limit(1).execute()
        if existing.data:
            universe_id = existing.data[0]["universe_id"]
        else:
            resp = supabase.table("universe").insert({"label": universe_label}).execute()
            universe_id = resp.data[0]["universe_id"]

        target_month = str(prepared.target_date)[:7]  # "YYYY-MM"

        # Build membership rows from the original prepared.company DataFrame
        # which still has universe_ticker and sector
        orig_company_rows = _df_to_rows(prepared.company)
        membership_rows = []
        for row in orig_company_rows:
            gf_ticker = row.get("gurufocus_ticker")
            gf_exchange = row.get("gurufocus_exchange")
            eid = exchange_map.get(gf_exchange) if gf_exchange else None
            cid = company_id_map.get((gf_ticker, eid))
            if cid is None:
                continue
            membership_rows.append({
                "universe_id": universe_id,
                "company_id": cid,
                "target_month": target_month,
                "universe_ticker": row.get("universe_ticker"),
                "sector": row.get("sector"),
            })

        # Clear any existing rows for this (universe, target_month) before
        # writing fresh ones. Re-ingesting a month can produce a different
        # company_id for the same universe_ticker (e.g. when exchange_id
        # resolution changes between runs). Without this delete, stale rows
        # linger because the PK is (universe_id, company_id, target_month) —
        # a new cid doesn't conflict with the old one, so counts inflate.
        supabase.table("universe_membership").delete().eq(
            "universe_id", universe_id
        ).eq("target_month", target_month).execute()

        if membership_rows:
            _upsert_batched(
                supabase, "universe_membership", membership_rows,
                on_conflict="universe_id,company_id,target_month",
            )

    # ------------------------------------------------------------------ #
    # 3. METRIC_DATA
    # ------------------------------------------------------------------ #
    # Build a reverse lookup: gurufocus_exchange code → exchange_id so we can
    # resolve metric_data rows which carry (gurufocus_ticker, gurufocus_exchange)
    md_rows: list[dict] = []
    for _, row in prepared.metric_data.iterrows():
        gf_ticker = row["gurufocus_ticker"]
        gf_exchange = row["gurufocus_exchange"]
        eid = exchange_map.get(gf_exchange)
        cid = company_id_map.get((gf_ticker, eid))
        if cid is None:
            continue

        nv = row.get("numeric_value")
        tv = row.get("text_value")

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
