"""Backfill `universe_membership.sector` for LongEquity from the source Excels.

The LongEquity report Excels carry a 100%-populated "sector" column (e.g. A O
Smith → "Industrials"). The current ingest extracts it correctly, but
historical ingests (before sector extraction worked) left ~30% of the
LongEquity membership with empty sectors, and `rebuild_longequity_universe`'s
carry-forward reads the universe it rebuilds — so once a sector is missing it
stays missing.

This re-derives the per-month sector straight from each raw Excel in the
`longequity-raw` Storage bucket and writes it into the per-month LongEquity
membership, then runs the normal rebuild (carry-forward) and re-freezes the
union so the frozen snapshot picks the sectors up too. Idempotent.

Usage:
    uv run python -m index_universe.backfill_longequity_sectors
"""
from __future__ import annotations

import io
import logging

from supabase import Client

log = logging.getLogger(__name__)


def _company_map(supabase: Client) -> dict[tuple[str, str], int]:
    """{(gurufocus_ticker, exchange_code): company_id} — the same key the
    ingest resolves a report row to."""
    out: dict[tuple[str, str], int] = {}
    offset, page = 0, 1000
    for _ in range(50):
        resp = (
            supabase.table("company")
            .select("company_id, gurufocus_ticker, gurufocus_exchange:gurufocus_exchange(exchange_code)")
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        for c in batch:
            tk = c.get("gurufocus_ticker")
            ex = (c.get("gurufocus_exchange") or {}).get("exchange_code")
            if tk and ex:
                out[(tk, ex)] = int(c["company_id"])
        if len(batch) < page:
            break
        offset += page
    return out


def backfill_longequity_sectors(supabase: Client, *, on_progress=None) -> dict:
    from deps import chunked  # noqa: PLC0415
    from ingest.flatten import flatten_excel  # noqa: PLC0415
    from ingest.extend_primary import enrich_flattened_df_with_primary_listing  # noqa: PLC0415
    from ingest.transformation import prepare_flattened_for_schema  # noqa: PLC0415
    from ingest.longequity_universe import (  # noqa: PLC0415
        freeze_longequity_union,
        rebuild_longequity_universe,
    )
    from routers.longequity import _as_of_date_from_filename  # noqa: PLC0415

    def emit(msg: str) -> None:
        log.info(msg)
        if on_progress:
            on_progress(msg)

    u = supabase.table("universe").select("universe_id").eq("label", "LongEquity").limit(1).execute()
    if not u.data:
        emit("No LongEquity universe — nothing to backfill.")
        return {"updated": 0, "files": 0}
    le_uid = u.data[0]["universe_id"]

    cmap = _company_map(supabase)
    files = [
        f["name"] for f in supabase.storage.from_("longequity-raw").list()
        if f.get("name", "").lower().endswith((".xlsx", ".xls"))
    ]
    emit(f"{len(files)} report files; resolving sectors via {len(cmap)} known companies…")

    total_rows = 0
    for fn in sorted(files):
        try:
            month = _as_of_date_from_filename(fn).strftime("%Y-%m")
        except Exception:
            emit(f"  skip (unparseable month): {fn}")
            continue
        data = supabase.storage.from_("longequity-raw").download(fn)
        prep = prepare_flattened_for_schema(
            enrich_flattened_df_with_primary_listing(flatten_excel(io.BytesIO(data)))
        )
        rows: list[dict] = []
        for r in prep.company.to_dict("records"):
            cid = cmap.get((r.get("gurufocus_ticker"), r.get("gurufocus_exchange")))
            sec = (r.get("sector") or "").strip() if r.get("sector") is not None else ""
            if cid and sec:
                rows.append({"universe_id": le_uid, "company_id": cid, "target_month": month, "sector": sec})
        for chunk in chunked(rows, 500):
            # on_conflict updates ONLY `sector` on the existing membership row.
            supabase.table("universe_membership").upsert(
                chunk, on_conflict="universe_id,company_id,target_month"
            ).execute()
        total_rows += len(rows)
        emit(f"  {month}: {len(rows)} sectors")

    # Carry-forward fills any month a company lacked its own sector, then the
    # frozen union is re-created so the snapshot reflects the new sectors.
    emit("Rebuilding LongEquity (carry-forward)…")
    rebuild_longequity_universe(supabase)
    emit("Re-freezing the union…")
    # Drop today's frozen union (if present) so the re-freeze picks up sectors.
    today = supabase.table("universe").select("universe_id, label").eq("frozen_from", "LongEquity").execute()
    for row in (today.data or []):
        supabase.table("universe_membership").delete().eq("universe_id", row["universe_id"]).execute()
        supabase.table("universe").delete().eq("universe_id", row["universe_id"]).execute()
    freeze_longequity_union(supabase)

    return {"updated": total_rows, "files": len(files)}


if __name__ == "__main__":
    from deps import supabase  # noqa: PLC0415  -- importing deps loads backend/.env

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    res = backfill_longequity_sectors(supabase, on_progress=print)
    print(f"\nDone: {res['updated']} membership sectors set across {res['files']} files.")
