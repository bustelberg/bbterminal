"""CSV-vs-universe ISIN comparison.

Backs the /isin-compare page: the user uploads a CSV, picks the ISIN column,
and compares that set against a selected (frozen) universe. This endpoint takes
the cleaned ISIN list + the universe label and partitions the universe's
members into:
  * `intersection`            — members whose ISIN appears in the CSV;
  * `in_universe_not_in_csv`  — members whose ISIN is NOT in the CSV (incl.
                                members that have no stored ISIN to match on);
and reports `csv_isins_not_in_universe` (uploaded ISINs no member matched).

Admin-gated automatically by the /api auth middleware (POST, not a user-read
prefix). Pure read — resolves the universe's latest stored month membership
joined to `company` for ISIN/name/ticker/exchange.
"""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from deps import IN_CHUNK_SIZE, chunked, fetch_in_chunks, supabase

router = APIRouter(tags=["universe"])


class IsinCompareRequest(BaseModel):
    universe_label: str
    isins: list[str]


class MemberRow(BaseModel):
    company_id: int
    company_name: str | None = None
    ticker: str | None = None
    exchange: str | None = None
    isin: str | None = None


class IsinCompareResponse(BaseModel):
    universe_label: str
    target_month: str | None = None
    universe_member_count: int
    csv_isin_count: int            # distinct, non-empty ISINs uploaded
    matched_count: int
    unmatched_count: int
    intersection: list[MemberRow]
    in_universe_not_in_csv: list[MemberRow]
    csv_isins_not_in_universe: list[str]


def _norm(s: str | None) -> str:
    return (s or "").strip().upper()


@router.post("/api/isin-compare", response_model=IsinCompareResponse)
async def isin_compare(req: IsinCompareRequest):
    label = req.universe_label.strip()
    if not label:
        raise HTTPException(400, "universe_label is required")
    csv_set = {_norm(x) for x in req.isins}
    csv_set.discard("")

    def _members() -> tuple[list[dict], str | None]:
        u = (
            supabase.table("universe")
            .select("universe_id")
            .eq("label", label)
            .limit(1)
            .execute()
        )
        if not u.data:
            raise HTTPException(404, f"Universe '{label}' not found")
        uid = u.data[0]["universe_id"]
        # Frozen universes are single-month; use the latest stored month.
        lm = (
            supabase.table("universe_membership")
            .select("target_month")
            .eq("universe_id", uid)
            .order("target_month", desc=True)
            .limit(1)
            .execute()
        )
        if not lm.data:
            return [], None
        month = lm.data[0]["target_month"]

        mem: list[dict] = []
        offset = 0
        while True:
            r = (
                supabase.table("universe_membership")
                .select("company_id, universe_ticker")
                .eq("universe_id", uid)
                .eq("target_month", month)
                .range(offset, offset + 999)
                .execute()
            )
            batch = r.data or []
            mem.extend(batch)
            if len(batch) < 1000:
                break
            offset += 1000

        cids = list({m["company_id"] for m in mem if m.get("company_id") is not None})
        uticker = {m["company_id"]: m.get("universe_ticker") for m in mem}

        companies: dict[int, dict] = {}
        for c in fetch_in_chunks(
            cids,
            lambda chunk: supabase.table("company")
            .select(
                "company_id, company_name, gurufocus_ticker, isin, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            )
            .in_("company_id", chunk)
            .execute(),
        ):
            companies[c["company_id"]] = c

        members: list[dict] = []
        for cid in cids:
            c = companies.get(cid, {})
            gfx = c.get("gurufocus_exchange") or {}
            members.append({
                "company_id": cid,
                "company_name": c.get("company_name"),
                "ticker": c.get("gurufocus_ticker") or uticker.get(cid),
                "exchange": gfx.get("exchange_code"),
                "isin": c.get("isin"),
            })
        return members, month

    members, month = await asyncio.to_thread(_members)

    intersection: list[dict] = []
    only: list[dict] = []
    matched_isins: set[str] = set()
    for m in members:
        ni = _norm(m.get("isin"))
        if ni and ni in csv_set:
            intersection.append(m)
            matched_isins.add(ni)
        else:
            only.append(m)

    def _key(m: dict) -> str:
        return (m.get("company_name") or m.get("ticker") or "").lower()

    intersection.sort(key=_key)
    only.sort(key=_key)

    return IsinCompareResponse(
        universe_label=label,
        target_month=month,
        universe_member_count=len(members),
        csv_isin_count=len(csv_set),
        matched_count=len(intersection),
        unmatched_count=len(only),
        intersection=[MemberRow(**m) for m in intersection],
        in_universe_not_in_csv=[MemberRow(**m) for m in only],
        csv_isins_not_in_universe=sorted(csv_set - matched_isins),
    )


class PruneRequest(BaseModel):
    universe_label: str
    # Company ids to remove from the universe (the "in universe, not in CSV"
    # set from a compare run).
    drop_company_ids: list[int]


class PruneResponse(BaseModel):
    universe_label: str
    dropped: int
    remaining_member_count: int


@router.post("/api/isin-compare/prune", response_model=PruneResponse)
async def prune_universe(req: PruneRequest):
    """Drop the given companies from a universe's membership — used to prune a
    universe down to the CSV intersection (remove the members not in the
    uploaded list). Deletes the `universe_membership` rows across every stored
    month of that universe, so the companies leave the universe entirely.
    Destructive + admin-gated; the frontend confirms first."""
    label = req.universe_label.strip()
    drop_ids = sorted({int(c) for c in req.drop_company_ids})
    if not label:
        raise HTTPException(400, "universe_label is required")
    if not drop_ids:
        raise HTTPException(400, "drop_company_ids is empty")

    def _work() -> tuple[int, int]:
        u = (
            supabase.table("universe")
            .select("universe_id")
            .eq("label", label)
            .limit(1)
            .execute()
        )
        if not u.data:
            raise HTTPException(404, f"Universe '{label}' not found")
        uid = u.data[0]["universe_id"]

        dropped = 0
        for chunk in chunked(drop_ids, IN_CHUNK_SIZE):
            resp = (
                supabase.table("universe_membership")
                .delete()
                .eq("universe_id", uid)
                .in_("company_id", chunk)
                .execute()
            )
            dropped += len(resp.data or [])

        # Remaining distinct members in the universe's latest stored month.
        lm = (
            supabase.table("universe_membership")
            .select("target_month")
            .eq("universe_id", uid)
            .order("target_month", desc=True)
            .limit(1)
            .execute()
        )
        remaining = 0
        if lm.data:
            month = lm.data[0]["target_month"]
            seen: set[int] = set()
            offset = 0
            while True:
                r = (
                    supabase.table("universe_membership")
                    .select("company_id")
                    .eq("universe_id", uid)
                    .eq("target_month", month)
                    .range(offset, offset + 999)
                    .execute()
                )
                batch = r.data or []
                seen.update(x["company_id"] for x in batch if x.get("company_id") is not None)
                if len(batch) < 1000:
                    break
                offset += 1000
            remaining = len(seen)
        return dropped, remaining

    dropped, remaining = await asyncio.to_thread(_work)
    return PruneResponse(universe_label=label, dropped=dropped, remaining_member_count=remaining)
