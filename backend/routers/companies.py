"""Company CRUD + universe-membership lookup + field-options.

Endpoints:
    GET    /api/companies                       full company list with exchange/country
    GET    /api/companies/memberships           {company_id: [universe labels]} aggregate
    POST   /api/companies                       create
    PUT    /api/companies/{company_id}          update
    DELETE /api/companies/{company_id}          delete (cascades dependent rows)
    GET    /api/companies/field-options         dropdown source for exchange/country/sector

`/memberships` is split off from the main list so a slow aggregate can't
block the table render — the frontend kicks both off in parallel.
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel

from routers._cache_headers import CACHE_USER

from deps import supabase

router = APIRouter(tags=["companies"])


class CreateCompanyRequest(BaseModel):
    company_name: str
    gurufocus_ticker: str
    gurufocus_exchange: str  # exchange_code, resolved to exchange_id
    # When the dupe check fires, the frontend re-submits with
    # force=True to override (e.g. the user inspected the existing
    # match and confirmed this is a genuinely different security).
    force: bool = False


class UpdateCompanyRequest(BaseModel):
    company_name: str | None = None
    gurufocus_ticker: str | None = None
    gurufocus_exchange: str | None = None  # exchange_code


def _resolve_exchange_id(exchange_code: str) -> int | None:
    """Look up exchange_id from an exchange_code (case-insensitive)."""
    resp = (
        supabase.table("gurufocus_exchange")
        .select("exchange_id")
        .eq("exchange_code", exchange_code.upper())
        .limit(1)
        .execute()
    )
    return resp.data[0]["exchange_id"] if resp.data else None


@router.get("/api/companies/field-options")
async def get_company_field_options(response: Response):
    """Dropdown options for the companies page — exchanges, countries, sectors."""
    response.headers["Cache-Control"] = CACHE_USER
    exch_resp = supabase.table("gurufocus_exchange").select("exchange_code").limit(1000).execute()
    exchanges = sorted({r["exchange_code"] for r in (exch_resp.data or [])})
    country_resp = supabase.table("country").select("country_name").limit(1000).execute()
    countries = sorted({r["country_name"] for r in (country_resp.data or [])})
    sector_resp = supabase.table("universe_membership").select("sector").limit(10000).execute()
    sectors = sorted({r["sector"] for r in (sector_resp.data or []) if r.get("sector") and r["sector"].strip()})
    return {"exchanges": exchanges, "countries": countries, "sectors": sectors}


@router.get("/api/companies")
async def list_companies():
    """Company list. Memberships are fetched separately via
    /api/companies/memberships so a slow aggregate can't block the table render.

    Pagination: PostgREST `db-max-rows` caps the cloud project at 1000 by
    default — `.limit(10000)` alone won't lift the cap. We page via
    `.range()` so the company list comes through whole even when the
    project setting hasn't been bumped to match local (10000 in
    `supabase/config.toml`). See `project_postgrest_max_rows_trap`."""
    def _query():
        # Fast path: one direct-Postgres COPY when SUPABASE_DB_URL is set.
        # Self-healing — returns None (→ PostgREST pager below) when
        # unconfigured or on any error, so behaviour is unchanged without it.
        from momentum.data._pg import load_companies_via_copy  # noqa: PLC0415
        fast = load_companies_via_copy()
        if fast is not None:
            return fast

        rows: list[dict] = []
        page = 1000
        offset = 0
        # 20-page hard cap (~20k companies) — universe is ~2800 today,
        # the cap is purely a safety against an infinite loop if
        # `.range()` is ever silently ignored.
        for _attempt in range(20):
            resp = (
                supabase.table("company")
                .select(
                    "company_id,company_name,gurufocus_ticker,exchange_id,isin,"
                    "delisted_at,gurufocus_lookup_failed_at,"
                    "out_of_scope_at,out_of_scope_reason,"
                    "gurufocus_exchange:gurufocus_exchange("
                    "exchange_code,currency_code,country:country(country_name))"
                )
                # `company_id` tiebreaker so range() pagination stays stable
                # across pages even when company_name has duplicates.
                .order("company_name")
                .order("company_id")
                .range(offset, offset + page - 1)
                .execute()
            )
            batch = resp.data or []
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < page:
                break
            offset += page
        for r in rows:
            exch_info = r.pop("gurufocus_exchange", None) or {}
            country_info = exch_info.pop("country", None) or {}
            r["gurufocus_exchange"] = exch_info.get("exchange_code")
            r["currency"] = exch_info.get("currency_code")
            r["country"] = country_info.get("country_name")
        return rows

    return await asyncio.to_thread(_query)


@router.get("/api/companies/memberships")
async def list_company_memberships():
    """Distinct universe labels per company. Computed server-side via the
    `company_universe_labels` RPC; backed by an index on
    universe_membership(company_id) so the aggregate is fast even when the
    table holds millions of per-month rows. Returns
    `{memberships: {company_id: [labels]}}`.

    Pagination: the cloud Supabase project caps PostgREST responses at
    `db-max-rows=1000` per request (local Docker Supabase is bumped to
    10000 in `supabase/config.toml`, which hides the cap during dev). The
    RPC returns ~2800 rows in prod, so without paging we'd silently miss
    the membership chips for every company past offset 1000 — see the
    2G Energy AG / cid=4875 incident on 2026-05-22. The loop here pages
    via `.range()` until a partial page comes back."""
    def _query():
        try:
            page = 1000
            offset = 0
            collected: dict[str, list[str]] = {}
            # Hard cap at 20 pages = 20k companies — far above the
            # current universe (~2800). Guards against an infinite loop
            # if `.range()` is ever silently ignored on the server.
            for _attempt in range(20):
                resp = (
                    supabase.rpc("company_universe_labels")
                    .range(offset, offset + page - 1)
                    .execute()
                )
                batch = resp.data or []
                if not batch:
                    break
                # If `.range()` is honored the batch is the next slice
                # and `seen` dedups by company_id either way; the guard
                # below also breaks once we see a slice with no NEW ids.
                added = 0
                for r in batch:
                    cid = str(r["company_id"])
                    if cid in collected:
                        continue
                    collected[cid] = r.get("labels") or []
                    added += 1
                if added == 0 or len(batch) < page:
                    break
                offset += page
            return collected
        except Exception:
            # Migration may not be applied yet — return empty rather than 500.
            return {}

    memberships = await asyncio.to_thread(_query)
    return {"memberships": memberships}


@router.get("/api/companies/check-duplicates")
async def check_duplicates(name: str = "", ticker: str = "", exchange: str = ""):
    """Pre-add dupe probe. Returns any existing rows that would
    canonically match the proposed `(name, ticker, exchange)` triple —
    HKSE zero-pad collapsed, name case-insensitive, also catches the
    cross-exchange H-share/A-share/GDR collisions.

    The frontend calls this on input change (debounced) to surface
    warnings inline before the user clicks Add."""
    from ingest.dedupe import find_canonical_match, canonical_ticker  # noqa: PLC0415

    def _query():
        matches = find_canonical_match(supabase, name, ticker, exchange)
        return {
            "matches": [
                {
                    "company_id": m.company_id,
                    "company_name": m.company_name,
                    "gurufocus_ticker": m.gurufocus_ticker,
                    "gurufocus_exchange": m.exchange_code,
                }
                for m in matches
            ],
            "canonical_ticker": canonical_ticker(ticker, exchange),
        }

    return await asyncio.to_thread(_query)


@router.post("/api/companies")
async def create_company(req: CreateCompanyRequest):
    # Reject unresolvable exchanges loudly instead of silently inserting
    # `exchange_id = NULL`. NULL-exchange rows render as blank columns in
    # /backtest / /schedule and the frontend's GuruFocus link falls back
    # to a bare-ticker URL that resolves to the wrong security for
    # non-US listings — root cause of past ENI-style breakage. Use the
    # /api/admin/companies/missing-exchange endpoint to triage existing
    # NULL rows.
    if not req.gurufocus_exchange or not req.gurufocus_exchange.strip():
        raise HTTPException(
            status_code=400,
            detail="gurufocus_exchange is required; companies must always carry an exchange.",
        )
    exchange_id = _resolve_exchange_id(req.gurufocus_exchange)
    if exchange_id is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown exchange_code {req.gurufocus_exchange!r}; add it to "
                f"gurufocus_exchange first or use one of the known codes "
                f"(GET /api/companies/field-options)."
            ),
        )

    # Canonicalize the ticker before insert so dupes can't accumulate
    # through ticker-format drift (HKSE `700` vs `00700` was the
    # original culprit — see backend/ingest/dedupe.py).
    from ingest.dedupe import (  # noqa: PLC0415
        canonical_ticker,
        find_canonical_match,
    )
    norm_ticker = canonical_ticker(req.gurufocus_ticker, req.gurufocus_exchange)

    # Block obvious dupes unless `force=True` is explicit. The frontend
    # is expected to call /api/companies/check-duplicates first and
    # only POST with force=True after a user explicitly chooses to
    # create a new row rather than reuse the existing match.
    if not req.force:
        def _check():
            return find_canonical_match(
                supabase, req.company_name, norm_ticker, req.gurufocus_exchange,
            )
        matches = await asyncio.to_thread(_check)
        if matches:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": (
                        f"A company with this name or canonical ticker already "
                        f"exists ({len(matches)} match(es)). Pass force=true to "
                        f"override after reviewing."
                    ),
                    "matches": [
                        {
                            "company_id": m.company_id,
                            "company_name": m.company_name,
                            "gurufocus_ticker": m.gurufocus_ticker,
                            "gurufocus_exchange": m.exchange_code,
                        }
                        for m in matches
                    ],
                    "canonical_ticker": norm_ticker,
                },
            )

    row = {
        "company_name": req.company_name,
        "gurufocus_ticker": norm_ticker,
        "exchange_id": exchange_id,
    }
    resp = supabase.table("company").insert(row).execute()
    if not resp.data:
        raise HTTPException(status_code=400, detail="Insert failed")
    return resp.data[0]


@router.put("/api/companies/{company_id}")
async def update_company(company_id: int, req: UpdateCompanyRequest):
    updates: dict = {}
    if req.company_name is not None:
        updates["company_name"] = req.company_name
    if req.gurufocus_ticker is not None:
        updates["gurufocus_ticker"] = req.gurufocus_ticker.upper()
    if req.gurufocus_exchange is not None:
        # Same loud-rejection rule as create_company — an explicit
        # PUT with `gurufocus_exchange` set must resolve to a real
        # exchange_id. Pass null in the request body to deliberately
        # leave the field unchanged (Pydantic distinguishes
        # `None`-default vs explicit `null` if the schema is set up
        # for it; here None means "not provided", so this branch
        # only fires when the caller passes a value).
        if not req.gurufocus_exchange.strip():
            raise HTTPException(
                status_code=400,
                detail="gurufocus_exchange cannot be empty; omit the field to leave it unchanged.",
            )
        exchange_id = _resolve_exchange_id(req.gurufocus_exchange)
        if exchange_id is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Unknown exchange_code {req.gurufocus_exchange!r}; "
                    f"add it to gurufocus_exchange first or use a known code."
                ),
            )
        updates["exchange_id"] = exchange_id
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    resp = (
        supabase.table("company")
        .update(updates)
        .eq("company_id", company_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(status_code=404, detail="Company not found")
    return resp.data[0]


@router.delete("/api/companies/{company_id}")
async def delete_company(company_id: int):
    """Manual cascade because the FKs predate ON DELETE CASCADE in places."""
    supabase.table("portfolio_weight").delete().eq("company_id", company_id).execute()
    supabase.table("metric_data").delete().eq("company_id", company_id).execute()
    supabase.table("company_source").delete().eq("company_id", company_id).execute()
    supabase.table("universe_membership").delete().eq("company_id", company_id).execute()
    supabase.table("company").delete().eq("company_id", company_id).execute()
    return {"ok": True}
