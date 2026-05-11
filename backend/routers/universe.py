"""Criteria-driven universe screening + derived universes.

Endpoints:
    GET    /api/universe/criteria                       LongEquity criteria spec for the UI
    POST   /api/universe/screen                         SSE: screen all companies
    POST   /api/universe/build                          SSE: build per-month universes for a range
    GET    /api/universe/labels                         list universes with full stats
    GET    /api/universe/months                         per-month counts for one label
    GET    /api/universe/months/{month}                 companies for one (label, month)
    DELETE /api/universe/months/{month}                 drop one month's memberships
    DELETE /api/universe/labels/{label}                 drop a label + its children
    PUT    /api/universe/labels/{label}                 rename a label
    DELETE /api/universe/labels                         drop EVERY universe (destructive)
    GET    /api/universe/derived-metrics/criteria       derived-metric specs + defaults
    GET    /api/universe/derived-metrics/status         "how many companies have metrics"
    POST   /api/universe/derived-metrics/recompute      SSE: recompute derived metrics
    POST   /api/universe/derive/preview                 dry-run: row counts per month
    POST   /api/universe/derive                         SSE: create a derived (tightened) universe
    GET    /api/universe/validate                       compare screen results vs LongEquity

"Derived" universes tighten a base universe via quality-metric thresholds —
e.g., "longequity_cumulative + ROIC>10% + FCF growth>5%". The base + child
relation is stored via `universe.parent_universe_id`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import queue as _queue
import time
from collections import Counter
from datetime import date

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from deps import supabase
from universe.criteria import CRITERIA_DESCRIPTIONS, CRITERIA_MIN_YEARS, CRITERIA_NAMES
from universe.screen import (
    build_and_store_universes,
    screen_universe,
    validate_vs_longequity,
)

router = APIRouter(tags=["universe"])


# ─── Helpers ───────────────────────────────────────────────────────────────

def _cutoff_for_target_month(target_month: str) -> date:
    """Latest fiscal-year-end date to consider for a given target month.
    Matches the convention in screen.py: target month 'YYYY-MM' uses the
    previous calendar year's FY data → cutoff (YYYY-1)-12-31."""
    year = int(target_month[:4])
    return date(year - 1, 12, 31)


def _load_derived_metrics(
    company_ids: list[int],
    metric_codes: list[str],
) -> dict[int, list[tuple[date, dict[str, float]]]]:
    """Fetch derived metric rows for the given companies + codes.

    Returns {company_id -> [(fy_end_date, {code: value}), …]}, sorted by
    date asc. Batched in chunks of 50 company_ids (Cloudflare 502 avoidance)."""
    out: dict[int, dict[str, dict[str, float]]] = {}  # cid -> {iso_date -> {code -> value}}
    for i in range(0, len(company_ids), 50):
        batch = company_ids[i:i + 50]
        resp = (
            supabase.table("metric_data")
            .select("company_id, metric_code, target_date, numeric_value")
            .in_("company_id", batch)
            .eq("source_code", "derived")
            .in_("metric_code", metric_codes)
            .limit(100000)
            .execute()
        )
        for row in (resp.data or []):
            cid = row["company_id"]
            d = row["target_date"]
            code = row["metric_code"]
            v = row["numeric_value"]
            if v is None:
                continue
            out.setdefault(cid, {}).setdefault(d, {})[code] = float(v)

    result: dict[int, list[tuple[date, dict[str, float]]]] = {}
    for cid, by_date in out.items():
        rows: list[tuple[date, dict[str, float]]] = []
        for iso, metrics in by_date.items():
            try:
                rows.append((date.fromisoformat(iso), metrics))
            except ValueError:
                continue
        rows.sort(key=lambda x: x[0])
        result[cid] = rows
    return result


def _applicable_metrics(
    rows: list[tuple[date, dict[str, float]]],
    cutoff: date,
) -> dict[str, float]:
    """Merged view of all derived metric values as of `cutoff`.

    Walks FYs in ascending order and overlays each, so later FYs overwrite
    earlier ones. Any code seen up to the cutoff is returned — needed
    because a given FY entry may not include every metric."""
    merged: dict[str, float] = {}
    for d, metrics in rows:
        if d > cutoff:
            break
        merged.update(metrics)
    return merged


# ─── Request models ────────────────────────────────────────────────────────

class ScreenRequest(BaseModel):
    as_of_year: str | None = None  # e.g. "2025-12"
    force_refresh: bool = False


class BuildUniverseRequest(BaseModel):
    start_month: str  # "YYYY-MM"
    end_month: str    # "YYYY-MM"
    label: str = "default"
    max_companies: int = 5


class UniverseRenameRequest(BaseModel):
    new_label: str


class DeriveUniverseRequest(BaseModel):
    base_universe_id: int
    label: str | None = None  # required for non-preview
    description: str | None = None
    filter_config: dict


class RecomputeRequest(BaseModel):
    universe_ids: list[int] | None = None  # None = all companies in any universe


# ─── Screening ─────────────────────────────────────────────────────────────

@router.get("/api/universe/criteria")
async def universe_criteria():
    """LongEquity quality criteria list with descriptions + min_years."""
    return [
        {
            "key": key,
            "label": label,
            "description": CRITERIA_DESCRIPTIONS.get(key, ""),
            "min_years": CRITERIA_MIN_YEARS.get(key, 1),
        }
        for key, label in CRITERIA_NAMES
    ]


@router.post("/api/universe/screen")
async def universe_screen(body: ScreenRequest = ScreenRequest()):
    """Screen all companies against LongEquity criteria. SSE stream."""
    as_of = body.as_of_year
    force = body.force_refresh

    def _run(q: _queue.Queue):
        resp = supabase.table("company").select(
            "company_id, gurufocus_ticker, exchange_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
        ).limit(10000).execute()
        companies = []
        for c in (resp.data or []):
            exch_info = c.pop("gurufocus_exchange", None) or {}
            country_info = exch_info.pop("country", None) or {}
            c["gurufocus_exchange"] = exch_info.get("exchange_code")
            c["country"] = country_info.get("country_name")
            companies.append(c)
        label = f" as of {as_of}" if as_of else ""
        q.put(json.dumps({"type": "progress", "message": f"Found {len(companies)} companies to screen{label}."}))

        for event in screen_universe(supabase, companies, as_of_year=as_of, force_refresh=force):
            q.put(json.dumps(event))
        q.put(None)

    async def generate():
        yield ": keepalive\n\n"
        q: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, q)

        while True:
            try:
                msg = await asyncio.to_thread(q.get, timeout=0.15)
            except Exception:
                if task.done():
                    while not q.empty():
                        msg = q.get_nowait()
                        if msg is not None:
                            yield f"data: {msg}\n\n"
                    break
                continue
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@router.post("/api/universe/build")
async def universe_build(body: BuildUniverseRequest):
    """Build monthly universes for a date range and persist them. SSE stream."""
    start = body.start_month
    end = body.end_month
    lbl = body.label
    max_co = body.max_companies

    def _run(q: _queue.Queue):
        resp = supabase.table("company").select(
            "company_id, gurufocus_ticker, exchange_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
        ).limit(10000).execute()
        companies = []
        for c in (resp.data or []):
            exch_info = c.pop("gurufocus_exchange", None) or {}
            country_info = exch_info.pop("country", None) or {}
            c["gurufocus_exchange"] = exch_info.get("exchange_code")
            c["country"] = country_info.get("country_name")
            companies.append(c)

        for event in build_and_store_universes(supabase, companies, start, end, label=lbl, max_companies=max_co):
            q.put(json.dumps(event))
        q.put(None)

    async def generate():
        yield ": keepalive\n\n"
        q: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, q)

        while True:
            try:
                msg = await asyncio.to_thread(q.get, timeout=0.15)
            except Exception:
                if task.done():
                    while not q.empty():
                        msg = q.get_nowait()
                        if msg is not None:
                            yield f"data: {msg}\n\n"
                    break
                continue
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


# ─── Universe label CRUD ───────────────────────────────────────────────────

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
        for i in range(0, len(cids), 50):
            batch = cids[i:i + 50]
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
        for i in range(0, len(ids), 50):
            batch = ids[i:i + 50]
            supabase.table("universe").delete().in_("universe_id", batch).execute()
        return {"deleted": len(ids)}
    return await asyncio.to_thread(_run)


# ─── Derived universes (tightened bases via quality-metric thresholds) ────

@router.get("/api/universe/derived-metrics/criteria")
async def universe_derived_criteria():
    """Criterion specs + default filter_config for the /universe UI."""
    from universe.derived_metrics import CRITERIA_SPECS, default_filter_config

    specs = []
    for s in CRITERIA_SPECS:
        entry: dict = {
            "key": s.key,
            "label": s.label,
            "default_threshold": s.default_threshold,
            "default_enabled": s.default_enabled,
        }
        if s.components is not None:
            entry["components"] = [
                {"label": label, "code": code, "default": default}
                for label, code, default in s.components
            ]
        else:
            entry["metric"] = s.metric
            entry["op"] = s.op
        specs.append(entry)
    return {"specs": specs, "default_filter_config": default_filter_config()}


@router.get("/api/universe/derived-metrics/status")
async def universe_derived_status():
    """How many companies have at least one derived metric row stored."""
    def _run():
        resp = (
            supabase.table("metric_data")
            .select("company_id", count="exact")
            .eq("source_code", "derived")
            .limit(1)
            .execute()
        )
        total_rows = resp.count or 0

        # Distinct companies — pull in pages (Supabase has no DISTINCT here).
        seen: set[int] = set()
        offset = 0
        page = 1000
        while True:
            r = (
                supabase.table("metric_data")
                .select("company_id")
                .eq("source_code", "derived")
                .range(offset, offset + page - 1)
                .execute()
            )
            batch = r.data or []
            for row in batch:
                seen.add(row["company_id"])
            if len(batch) < page:
                break
            offset += page
        return {"companies_with_derived_metrics": len(seen), "total_rows": total_rows}

    return await asyncio.to_thread(_run)


@router.post("/api/universe/derived-metrics/recompute")
async def universe_derived_recompute(body: RecomputeRequest = RecomputeRequest()):
    """Recompute derived metric values from cached GuruFocus annuals. SSE."""
    from universe.derived_metrics import precompute_for_companies

    def _collect_companies() -> list[dict]:
        cid_set: set[int] = set()
        offset = 0
        page = 1000
        q = supabase.table("universe_membership").select("company_id")
        if body.universe_ids:
            q = q.in_("universe_id", body.universe_ids)
        while True:
            r = q.range(offset, offset + page - 1).execute()
            batch = r.data or []
            for row in batch:
                if row.get("company_id"):
                    cid_set.add(row["company_id"])
            if len(batch) < page:
                break
            offset += page

        ids = sorted(cid_set)
        if not ids:
            return []

        companies: list[dict] = []
        for i in range(0, len(ids), 50):
            batch = ids[i:i + 50]
            r = supabase.table("company").select(
                "company_id, gurufocus_ticker, company_name, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            ).in_("company_id", batch).execute()
            for c in (r.data or []):
                exch = c.pop("gurufocus_exchange", None) or {}
                c["gurufocus_exchange"] = exch.get("exchange_code")
                companies.append(c)
        return companies

    def _run(q: _queue.Queue):
        companies = _collect_companies()
        q.put(json.dumps({
            "type": "progress",
            "message": f"Found {len(companies)} companies across selected universes.",
        }))
        for event in precompute_for_companies(supabase, companies):
            q.put(json.dumps(event))
        q.put(None)

    async def generate():
        yield ": keepalive\n\n"
        qq: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, qq)
        while True:
            try:
                msg = await asyncio.to_thread(qq.get, timeout=0.15)
            except Exception:
                if task.done():
                    while not qq.empty():
                        m = qq.get_nowait()
                        if m is not None:
                            yield f"data: {m}\n\n"
                    break
                continue
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@router.post("/api/universe/derive/preview")
async def universe_derive_preview(body: DeriveUniverseRequest):
    """Count how many companies per month would survive the filter. No writes."""
    from universe.derived_metrics import company_passes, required_metric_codes

    def _run():
        base_resp = supabase.table("universe").select(
            "universe_id, label"
        ).eq("universe_id", body.base_universe_id).limit(1).execute()
        if not base_resp.data:
            raise HTTPException(status_code=404, detail="base universe not found")

        rows: list[dict] = []
        offset = 0
        page = 1000
        while True:
            r = (
                supabase.table("universe_membership")
                .select("target_month, company_id")
                .eq("universe_id", body.base_universe_id)
                .range(offset, offset + page - 1)
                .execute()
            )
            batch = r.data or []
            rows.extend(batch)
            if len(batch) < page:
                break
            offset += page

        if not rows:
            return {"monthly_counts": [], "base_rows": 0, "passed_rows": 0, "missing_metrics": 0}

        codes = required_metric_codes(body.filter_config)
        cids = sorted({r["company_id"] for r in rows})

        if not codes:
            # Nothing enabled → filter is a no-op, every base row passes.
            by_month: dict[str, int] = {}
            for r in rows:
                by_month[r["target_month"]] = by_month.get(r["target_month"], 0) + 1
            return {
                "monthly_counts": [
                    {"month": m, "count": c} for m, c in sorted(by_month.items())
                ],
                "base_rows": len(rows),
                "passed_rows": len(rows),
                "missing_metrics": 0,
                "base_label": base_resp.data[0]["label"],
            }

        metrics_by_cid = _load_derived_metrics(cids, codes)

        missing = 0
        pass_by_month: dict[str, int] = {}
        base_by_month: dict[str, int] = {}
        for row in rows:
            m = row["target_month"]
            cid = row["company_id"]
            base_by_month[m] = base_by_month.get(m, 0) + 1
            fy_rows = metrics_by_cid.get(cid, [])
            if not fy_rows:
                missing += 1
                continue
            cutoff = _cutoff_for_target_month(m)
            applicable = _applicable_metrics(fy_rows, cutoff)
            if company_passes(body.filter_config, applicable):
                pass_by_month[m] = pass_by_month.get(m, 0) + 1

        months_sorted = sorted(base_by_month.keys())
        return {
            "monthly_counts": [
                {"month": m, "count": pass_by_month.get(m, 0), "base_count": base_by_month[m]}
                for m in months_sorted
            ],
            "base_rows": len(rows),
            "passed_rows": sum(pass_by_month.values()),
            "missing_metrics": missing,
            "base_label": base_resp.data[0]["label"],
        }

    return await asyncio.to_thread(_run)


@router.post("/api/universe/derive")
async def universe_derive_create(body: DeriveUniverseRequest):
    """Create a derived (tightened) universe. SSE: precompute → filter → insert."""
    from universe.derived_metrics import (
        _fmt_duration as _fmt_dur,
        company_passes,
        precompute_for_companies,
        required_metric_codes,
    )

    def _run(q: _queue.Queue):
        def emit(step: str, status: str, message: str, **extra):
            q.put(json.dumps({
                "type": "progress", "step": step, "status": status, "message": message, **extra,
            }))

        try:
            label = (body.label or "").strip()
            if not label:
                q.put(json.dumps({"type": "error", "message": "label is required"}))
                return

            emit("validate", "in_progress", "Validating inputs...")
            base_resp = supabase.table("universe").select(
                "universe_id, label"
            ).eq("universe_id", body.base_universe_id).limit(1).execute()
            if not base_resp.data:
                q.put(json.dumps({"type": "error", "message": "base universe not found"}))
                return
            base_label = base_resp.data[0]["label"]

            dup = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
            if dup.data:
                q.put(json.dumps({"type": "error", "message": f"label '{label}' already exists"}))
                return
            emit("validate", "done", f"Base: {base_label} → new label: {label}")

            emit("load_base", "in_progress", f"Loading memberships from {base_label}...")
            rows: list[dict] = []
            offset = 0
            page = 1000
            while True:
                r = (
                    supabase.table("universe_membership")
                    .select("target_month, company_id, universe_ticker, sector")
                    .eq("universe_id", body.base_universe_id)
                    .range(offset, offset + page - 1)
                    .execute()
                )
                batch = r.data or []
                rows.extend(batch)
                if len(batch) < page:
                    break
                offset += page
            months = sorted({r["target_month"] for r in rows if r.get("target_month")})
            cids = sorted({r["company_id"] for r in rows})
            emit(
                "load_base", "done",
                f"Loaded {len(rows):,} memberships across {len(months)} months, {len(cids)} companies.",
            )

            codes = required_metric_codes(body.filter_config)
            if codes and cids:
                emit(
                    "precompute", "in_progress",
                    f"Precomputing derived metrics for {len(cids)} companies...",
                )
                companies: list[dict] = []
                for i in range(0, len(cids), 50):
                    batch = cids[i:i + 50]
                    r = supabase.table("company").select(
                        "company_id, gurufocus_ticker, company_name, "
                        "gurufocus_exchange:gurufocus_exchange(exchange_code)"
                    ).in_("company_id", batch).execute()
                    for c in (r.data or []):
                        exch = c.pop("gurufocus_exchange", None) or {}
                        c["gurufocus_exchange"] = exch.get("exchange_code")
                        companies.append(c)

                last_done_summary = ""
                for ev in precompute_for_companies(supabase, companies):
                    etype = ev.get("type")
                    msg = ev.get("message", "")
                    if etype == "progress_update":
                        emit("precompute", "in_progress", msg)
                    elif etype == "done":
                        last_done_summary = msg
                emit("precompute", "done", last_done_summary or "Derived metrics up to date.")
            else:
                emit("precompute", "done", "No filters enabled; skipping precompute.")

            emit("filter", "in_progress", f"Applying filter to {len(rows):,} rows...")
            metrics_by_cid = _load_derived_metrics(cids, codes) if codes else {}

            kept: list[dict] = []
            missing = 0
            for row in rows:
                cid = row["company_id"]
                if not codes:
                    kept.append(row)
                    continue
                fy_rows = metrics_by_cid.get(cid, [])
                if not fy_rows:
                    missing += 1
                    continue
                cutoff = _cutoff_for_target_month(row["target_month"])
                applicable = _applicable_metrics(fy_rows, cutoff)
                if company_passes(body.filter_config, applicable):
                    kept.append(row)
            emit(
                "filter", "done",
                f"{len(kept):,} / {len(rows):,} rows pass"
                + (f" ({missing:,} excluded for missing metrics)." if missing else "."),
            )

            if not kept:
                q.put(json.dumps({
                    "type": "error",
                    "message": "Filter matches zero rows — adjust thresholds or precompute metrics.",
                }))
                return

            emit("create", "in_progress", "Creating universe row...")
            created = supabase.table("universe").insert({
                "label": label,
                "description": body.description,
                "parent_universe_id": body.base_universe_id,
                "filter_config": body.filter_config,
            }).execute()
            new_id = created.data[0]["universe_id"]
            emit("create", "done", f"Universe created (id={new_id}).")

            # Dedup defensively on (company_id, target_month). The base may
            # carry stale duplicate rows from prior runs; the universe_membership
            # PK would reject them mid-insert otherwise.
            seen_keys: set[tuple] = set()
            payload: list[dict] = []
            dropped_dupes = 0
            for r in kept:
                key = (r["company_id"], r["target_month"])
                if key in seen_keys:
                    dropped_dupes += 1
                    continue
                seen_keys.add(key)
                payload.append({
                    "universe_id": new_id,
                    "company_id": r["company_id"],
                    "target_month": r["target_month"],
                    "universe_ticker": r.get("universe_ticker"),
                    "sector": r.get("sector"),
                })
            if dropped_dupes:
                emit(
                    "filter", "done",
                    f"{len(kept):,} rows passed filter; dropped {dropped_dupes:,} duplicate "
                    f"(company_id, target_month) row(s) before insert.",
                )
            batch_size = 500
            total_inserted = 0
            total_batches = (len(payload) + batch_size - 1) // batch_size
            insert_started = time.monotonic()
            emit(
                "insert", "in_progress",
                f"Inserting {len(payload):,} rows in {total_batches} batches...",
            )
            for bi, i in enumerate(range(0, len(payload), batch_size), start=1):
                chunk = payload[i:i + batch_size]
                elapsed = time.monotonic() - insert_started
                rate = (bi - 1) / elapsed if elapsed > 0 and bi > 1 else 0
                remaining = (total_batches - bi + 1) / rate if rate > 0 else 0
                emit(
                    "insert", "in_progress",
                    f"Starting batch {bi}/{total_batches} ({len(chunk):,} rows) · "
                    f"{_fmt_dur(elapsed)} elapsed · ~{_fmt_dur(remaining)} left",
                )
                try:
                    resp = supabase.table("universe_membership").insert(chunk).execute()
                    total_inserted += len(resp.data or [])
                except Exception as batch_exc:
                    emit(
                        "insert", "in_progress",
                        f"Batch {bi}/{total_batches} failed: {batch_exc}. Retrying once...",
                    )
                    resp = supabase.table("universe_membership").insert(chunk).execute()
                    total_inserted += len(resp.data or [])
                elapsed = time.monotonic() - insert_started
                rate = bi / elapsed if elapsed > 0 else 0
                remaining = (total_batches - bi) / rate if rate > 0 else 0
                emit(
                    "insert", "in_progress",
                    f"Batch {bi}/{total_batches} done · {total_inserted:,}/{len(payload):,} rows · "
                    f"{_fmt_dur(elapsed)} elapsed · ~{_fmt_dur(remaining)} left",
                )
            emit("insert", "done", f"Inserted {total_inserted:,} rows in {_fmt_dur(time.monotonic() - insert_started)}.")

            q.put(json.dumps({
                "type": "done",
                "message": f"Created '{label}' from {base_label} with {total_inserted:,} rows.",
                "data": {
                    "universe_id": new_id,
                    "label": label,
                    "rows_inserted": total_inserted,
                    "base_universe_id": body.base_universe_id,
                    "base_label": base_label,
                },
            }))
        except Exception as exc:
            logging.getLogger(__name__).exception("universe/derive failed")
            q.put(json.dumps({"type": "error", "message": str(exc)}))
        finally:
            q.put(None)

    async def generate():
        yield ": keepalive\n\n"
        qq: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, qq)
        while True:
            try:
                msg = await asyncio.to_thread(qq.get, timeout=0.15)
            except Exception:
                if task.done():
                    while not qq.empty():
                        m = qq.get_nowait()
                        if m is not None:
                            yield f"data: {m}\n\n"
                    break
                continue
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@router.get("/api/universe/validate")
async def universe_validate():
    """Compare current screening results against LongEquity snapshots."""
    def _run():
        resp = supabase.table("company").select(
            "company_id, gurufocus_ticker, exchange_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
        ).limit(10000).execute()
        companies = []
        for c in (resp.data or []):
            exch_info = c.pop("gurufocus_exchange", None) or {}
            country_info = exch_info.pop("country", None) or {}
            c["gurufocus_exchange"] = exch_info.get("exchange_code")
            c["country"] = country_info.get("country_name")
            companies.append(c)

        results = []
        for event in screen_universe(supabase, companies):
            if event["type"] == "done":
                results = event["data"]["results"]
                break

        return validate_vs_longequity(supabase, results)

    return await asyncio.to_thread(_run)
