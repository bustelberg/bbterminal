"""Universe label CRUD + per-month detail endpoints.

Endpoints:
    GET    /api/universe/labels                  list universes with full stats
    GET    /api/universe/months                  per-month counts for one label
    GET    /api/universe/months/{month}          companies for one (label, month)
    DELETE /api/universe/months/{month}          drop one month's memberships
    DELETE /api/universe/labels/{label}          drop a label + its children
    PUT    /api/universe/labels/{label}          rename a label
    DELETE /api/universe/labels                  drop EVERY universe (destructive)
"""
from __future__ import annotations

import asyncio
from collections import Counter

from fastapi import APIRouter, HTTPException

from deps import supabase, IN_CHUNK_SIZE

from ._models import UniverseRenameRequest

router = APIRouter(tags=["universe"])


@router.get("/api/universe/labels")
async def universe_labels():
    """List all universes with stats. Aggregation runs in Postgres via the
    `universe_full_stats` RPC so this is two round trips regardless of
    universe size."""
    def _run():
        u_rows = (
            supabase.table("universe")
            .select("universe_id, label, description, created_at, parent_universe_id, filter_config")
            .order("label")
            .execute()
            .data or []
        )
        stats_rows = supabase.rpc("universe_full_stats").execute().data or []
        stats_by_id: dict[int, dict] = {r["universe_id"]: r for r in stats_rows}

        label_by_id = {u["universe_id"]: u["label"] for u in u_rows}

        result = []
        for u in u_rows:
            uid = u["universe_id"]
            s = stats_by_id.get(uid, {})
            monthly = s.get("monthly_counts") or []
            sectors = s.get("sector_counts") or []

            total_rows = s.get("total_rows", 0)
            month_count = s.get("month_count", 0)
            avg_per_month = round(total_rows / month_count, 1) if month_count else 0

            parent_id = u.get("parent_universe_id")
            result.append({
                "universe_id": uid,
                "label": u["label"],
                "description": u.get("description"),
                "created_at": u.get("created_at"),
                "parent_universe_id": parent_id,
                "parent_label": label_by_id.get(parent_id) if parent_id else None,
                "filter_config": u.get("filter_config"),
                "is_derived": parent_id is not None,
                "start_month": s.get("start_month"),
                "end_month": s.get("end_month"),
                "month_count": month_count,
                "total_rows": total_rows,
                "unique_companies": s.get("unique_companies", 0),
                "unique_tickers": s.get("unique_tickers", 0),
                "avg_per_month": avg_per_month,
                "first_month_count": monthly[0]["count"] if monthly else 0,
                "last_month_count": monthly[-1]["count"] if monthly else 0,
                "monthly_counts": monthly,
                "sectors": sectors,
            })

        return result

    return await asyncio.to_thread(_run)


@router.get("/api/universe/months")
async def universe_months(label: str = "default"):
    """Distinct months + per-month row counts for one universe label."""
    def _run():
        u_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]
        resp = supabase.table("universe_membership").select("target_month").eq("universe_id", universe_id).limit(10000).execute()
        rows = resp.data or []
        counts = Counter(r["target_month"] for r in rows)
        return [{"target_month": m, "count": c} for m, c in sorted(counts.items())]

    return await asyncio.to_thread(_run)


@router.get("/api/universe/months/{month}")
async def universe_month_detail(month: str, label: str = "default"):
    """All companies for a specific (label, month) pair."""
    def _run():
        u_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]

        resp = supabase.table("universe_membership").select(
            "company_id, universe_ticker, sector"
        ).eq("universe_id", universe_id).eq("target_month", month).limit(10000).execute()
        membership_rows = resp.data or []

        if not membership_rows:
            return []

        cids = [r["company_id"] for r in membership_rows]
        company_map: dict[int, dict] = {}
        for i in range(0, len(cids), IN_CHUNK_SIZE):
            batch = cids[i:i + IN_CHUNK_SIZE]
            cr = supabase.table("company").select(
                "company_id, gurufocus_ticker, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
            ).in_("company_id", batch).execute()
            for c in (cr.data or []):
                exch_info = c.pop("gurufocus_exchange", None) or {}
                country_info = exch_info.pop("country", None) or {}
                c["gurufocus_exchange"] = exch_info.get("exchange_code")
                c["country"] = country_info.get("country_name")
                company_map[c["company_id"]] = c

        result = []
        for r in membership_rows:
            info = company_map.get(r["company_id"], {})
            result.append({
                "company_id": r["company_id"],
                "ticker": info.get("gurufocus_ticker", ""),
                "exchange": info.get("gurufocus_exchange", ""),
                "company_name": info.get("company_name", ""),
                "sector": r.get("sector", ""),
                "country": info.get("country", ""),
                "universe_ticker": r.get("universe_ticker", ""),
            })
        return result

    return await asyncio.to_thread(_run)


@router.delete("/api/universe/months/{month}")
async def universe_delete_month(month: str, label: str = "default"):
    """Delete one month's memberships from a universe."""
    def _run():
        u_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
        if not u_resp.data:
            return {"deleted": month}
        universe_id = u_resp.data[0]["universe_id"]
        supabase.table("universe_membership").delete().eq(
            "universe_id", universe_id
        ).eq("target_month", month).execute()
        return {"deleted": month}
    return await asyncio.to_thread(_run)


@router.delete("/api/universe/labels/{label}")
async def universe_delete_label(label: str):
    """Delete a universe and its memberships.

    If the target is a base universe, its derived children (tight variants)
    are deleted first — the `parent_universe_id` FK is ON DELETE SET NULL,
    not CASCADE, so callers would otherwise leave orphans.
    """
    def _run():
        resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
        if not resp.data:
            return {"deleted": label, "children": []}
        uid = resp.data[0]["universe_id"]

        child_resp = (
            supabase.table("universe")
            .select("universe_id, label")
            .eq("parent_universe_id", uid)
            .execute()
        )
        children = child_resp.data or []
        if children:
            child_ids = [c["universe_id"] for c in children]
            supabase.table("universe").delete().in_("universe_id", child_ids).execute()

        supabase.table("universe").delete().eq("universe_id", uid).execute()
        return {"deleted": label, "children": [c["label"] for c in children]}
    return await asyncio.to_thread(_run)


@router.put("/api/universe/labels/{label}")
async def universe_rename_label(label: str, req: UniverseRenameRequest):
    """Rename a universe label."""
    def _run():
        new_label = (req.new_label or "").strip()
        if not new_label:
            raise HTTPException(status_code=400, detail="new_label is required")
        existing = supabase.table("universe").select("universe_id").eq("label", new_label).limit(1).execute()
        if existing.data:
            raise HTTPException(status_code=409, detail=f"Label '{new_label}' already exists")
        supabase.table("universe").update({"label": new_label}).eq("label", label).execute()
        return {"old_label": label, "new_label": new_label}
    return await asyncio.to_thread(_run)


@router.delete("/api/universe/labels")
async def universe_delete_all():
    """Delete EVERY universe + memberships (cascade)."""
    def _run():
        resp = supabase.table("universe").select("universe_id").limit(100000).execute()
        ids = [r["universe_id"] for r in (resp.data or [])]
        if not ids:
            return {"deleted": 0}
        for i in range(0, len(ids), IN_CHUNK_SIZE):
            batch = ids[i:i + IN_CHUNK_SIZE]
            supabase.table("universe").delete().in_("universe_id", batch).execute()
        return {"deleted": len(ids)}
    return await asyncio.to_thread(_run)
