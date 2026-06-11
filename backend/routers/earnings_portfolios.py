"""Earnings-dashboard portfolios: CRUD + aggregated metrics.

A portfolio is a named basket of existing companies with weights. The
`/{id}/metrics` endpoint aggregates each member's `metric_data` into a single
synthesized MetricRow[] (weighted mean per metric per date, currency-denominated
metrics converted to EUR) so the /earnings charts render a portfolio exactly
like a single company.

Auth: paths live under /api/earnings/portfolios so they inherit the earnings
tier — GET is allowed for any authenticated user; POST/PUT/DELETE are admin-only
(the gate only user-allows `/refresh*` writes under /api/earnings). See
routers/_auth_middleware.py.
"""
from __future__ import annotations

import asyncio
import bisect
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from deps import supabase
from routers.earnings import load_company_metric_rows

router = APIRouter(tags=["earnings"])


# ── Models ──────────────────────────────────────────────────────────────────

class PortfolioMemberIn(BaseModel):
    company_id: int
    weight: float | None = None  # omitted on all members → equal weight


class PortfolioCreate(BaseModel):
    name: str
    members: list[PortfolioMemberIn] = []


class PortfolioUpdate(BaseModel):
    name: str | None = None
    members: list[PortfolioMemberIn] | None = None  # None = leave members unchanged


# ── Helpers ─────────────────────────────────────────────────────────────────

def _fetch_members(portfolio_id: int) -> list[dict]:
    """Members with display labels (ticker, name) for a portfolio."""
    resp = (
        supabase.table("earnings_portfolio_member")
        .select("company_id, weight, company:company(gurufocus_ticker, company_name)")
        .eq("portfolio_id", portfolio_id)
        .execute()
    )
    out: list[dict] = []
    for r in resp.data or []:
        c = r.get("company") or {}
        out.append({
            "company_id": r["company_id"],
            "weight": r["weight"],
            "ticker": c.get("gurufocus_ticker"),
            "name": c.get("company_name"),
        })
    out.sort(key=lambda m: (m.get("name") or m.get("ticker") or "").lower())
    return out


def _serialize(p: dict) -> dict:
    return {
        "id": p["id"],
        "name": p["name"],
        "updated_at": p.get("updated_at"),
        "members": _fetch_members(p["id"]),
    }


def _replace_members(portfolio_id: int, members: list[PortfolioMemberIn]) -> None:
    """Delete + re-insert the member set. Equal-weight when no member carries
    an explicit weight; otherwise the given weights (a missing one → 0)."""
    supabase.table("earnings_portfolio_member").delete().eq("portfolio_id", portfolio_id).execute()
    if not members:
        return
    any_weight = any(m.weight is not None for m in members)
    n = len(members)
    rows = [
        {
            "portfolio_id": portfolio_id,
            "company_id": m.company_id,
            "weight": (float(m.weight) if m.weight is not None else 0.0) if any_weight else 1.0 / n,
        }
        for m in members
    ]
    supabase.table("earnings_portfolio_member").insert(rows).execute()


# ── CRUD ────────────────────────────────────────────────────────────────────

@router.get("/api/earnings/portfolios")
async def list_portfolios():
    """All portfolios with their members + display labels."""
    def _q():
        resp = supabase.table("earnings_portfolio").select("id, name, updated_at").order("name").execute()
        return [_serialize(p) for p in (resp.data or [])]
    return await asyncio.to_thread(_q)


@router.post("/api/earnings/portfolios")
async def create_portfolio(body: PortfolioCreate):
    """Create a portfolio. Weights default to equal when none are supplied."""
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name is required")

    def _q():
        ins = supabase.table("earnings_portfolio").insert({"name": name}).execute()
        pid = ins.data[0]["id"]
        _replace_members(pid, body.members)
        row = supabase.table("earnings_portfolio").select("id, name, updated_at").eq("id", pid).limit(1).execute()
        return _serialize(row.data[0])
    return await asyncio.to_thread(_q)


@router.put("/api/earnings/portfolios/{portfolio_id}")
async def update_portfolio(portfolio_id: int, body: PortfolioUpdate):
    """Rename and/or replace the members of a portfolio."""
    def _q():
        existing = supabase.table("earnings_portfolio").select("id").eq("id", portfolio_id).limit(1).execute()
        if not existing.data:
            raise HTTPException(status_code=404, detail="Portfolio not found")
        update: dict = {"updated_at": datetime.now(timezone.utc).isoformat()}
        if body.name is not None:
            nm = body.name.strip()
            if not nm:
                raise HTTPException(status_code=400, detail="Name cannot be empty")
            update["name"] = nm
        supabase.table("earnings_portfolio").update(update).eq("id", portfolio_id).execute()
        if body.members is not None:
            _replace_members(portfolio_id, body.members)
        row = supabase.table("earnings_portfolio").select("id, name, updated_at").eq("id", portfolio_id).limit(1).execute()
        return _serialize(row.data[0])
    return await asyncio.to_thread(_q)


@router.delete("/api/earnings/portfolios/{portfolio_id}")
async def delete_portfolio(portfolio_id: int):
    """Delete a portfolio (members cascade)."""
    def _q():
        supabase.table("earnings_portfolio").delete().eq("id", portfolio_id).execute()
        return {"deleted": True}
    return await asyncio.to_thread(_q)


# ── Aggregated metrics ──────────────────────────────────────────────────────

def _is_currency_metric(code: str) -> bool:
    """True for native-currency absolute metrics that must be EUR-normalized
    before cross-company aggregation (per-share data, price, the per-share
    estimates). Unit-less ratios (P/E, %s) pass through unconverted."""
    if code == "close_price":
        return True
    if "Per Share Data" in code:  # annuals__/quarterly__ Per Share Data __ *
        return True
    return code in ("annual_eps_nri_estimate", "annual_dividend_estimate")


def _load_fx_asof(currency: str) -> list[tuple[str, float]]:
    """Sorted (rate_date, rate) for a currency from `fx_rate` (units per 1 EUR)."""
    rows: list[tuple[str, float]] = []
    offset, page = 0, 1000
    while True:
        resp = (
            supabase.table("fx_rate")
            .select("rate_date, rate")
            .eq("currency_code", currency)
            .order("rate_date")
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        rows.extend((r["rate_date"], float(r["rate"])) for r in batch if r.get("rate"))
        if len(batch) < page:
            break
        offset += page
    return rows


def _asof_rate(series: list[tuple[str, float]], date: str) -> float | None:
    """Latest rate on or before `date` (string ISO compare). If `date` predates
    the earliest stored rate, fall back to that earliest rate rather than
    returning None.

    The `fx_rate` table only spans the last couple of years, but companies have
    decades of price/EPS history. Returning None here would make the caller drop
    every pre-FX-history row, truncating a multi-currency portfolio's charts to
    just the recent window. A flat oldest-rate fallback keeps the full history —
    exact for indexed/relative charts (a constant scale cancels) and a small
    approximation for absolute levels. Proper fix: backfill `fx_rate` with full
    ECB history."""
    if not series:
        return None
    idx = bisect.bisect_right([d for d, _ in series], date) - 1
    return series[idx][1] if idx >= 0 else series[0][1]


def _members_currency_fx(members: list[dict]) -> tuple[dict[int, str], dict[str, list[tuple[str, float]]]]:
    """Resolve each member's native currency + load the per-currency FX series
    (once per distinct non-EUR currency). Shared by the aggregate + per-member
    endpoints so both convert identically."""
    cids = [m["company_id"] for m in members]
    cur_resp = (
        supabase.table("company")
        .select("company_id, gurufocus_exchange:gurufocus_exchange(currency_code)")
        .in_("company_id", cids)
        .execute()
    )
    currency_by_cid: dict[int, str] = {}
    for r in cur_resp.data or []:
        exch = r.get("gurufocus_exchange") or {}
        currency_by_cid[r["company_id"]] = (exch.get("currency_code") or "EUR").upper()
    fx_cache: dict[str, list[tuple[str, float]]] = {
        cur: _load_fx_asof(cur) for cur in set(currency_by_cid.values()) if cur != "EUR"
    }
    return currency_by_cid, fx_cache


def _member_eur_rows(company_id: int, currency: str, fx: list[tuple[str, float]] | None) -> list[dict]:
    """One member's dashboard rows with currency-denominated metrics converted
    to EUR (unit-less ratios pass through). Drops null values."""
    out: list[dict] = []
    for row in load_company_metric_rows(company_id):
        v = row.get("numeric_value")
        if v is None:
            continue
        code, date = row["metric_code"], row["target_date"]
        val = float(v)
        if currency != "EUR" and _is_currency_metric(code):
            rate = _asof_rate(fx, date) if fx else None
            if not rate:
                continue
            val = val / rate
        out.append({
            "metric_code": code,
            "target_date": date,
            "numeric_value": val,
            "is_prediction": bool(row.get("is_prediction")),
        })
    return out


@router.get("/api/earnings/portfolios/{portfolio_id}/metrics")
async def portfolio_metrics(portfolio_id: int):
    """Aggregated MetricRow[] for the portfolio — weighted mean per (metric,
    date) over members holding data there (weights renormalized to those
    present), currency-denominated metrics converted to EUR first. Same shape
    as /api/earnings/{company_id}/metrics, so every chart consumes it directly."""
    def _q():
        members = _fetch_members(portfolio_id)
        if not members:
            return []
        currency_by_cid, fx_cache = _members_currency_fx(members)

        # acc[(code, date)] = [sum_wv, sum_w, is_prediction]
        acc: dict[tuple[str, str], list] = {}
        for m in members:
            w = float(m["weight"] or 0.0)
            if w <= 0:
                continue
            cur = currency_by_cid.get(m["company_id"], "EUR")
            for row in _member_eur_rows(m["company_id"], cur, fx_cache.get(cur)):
                code, date, val = row["metric_code"], row["target_date"], row["numeric_value"]
                slot = acc.get((code, date))
                if slot is None:
                    acc[(code, date)] = [w * val, w, row["is_prediction"]]
                else:
                    slot[0] += w * val
                    slot[1] += w
                    slot[2] = slot[2] or row["is_prediction"]

        out = [
            {"metric_code": code, "target_date": date, "numeric_value": sum_wv / sum_w, "is_prediction": is_pred}
            for (code, date), (sum_wv, sum_w, is_pred) in acc.items()
            if sum_w > 0
        ]
        out.sort(key=lambda r: r["target_date"])
        return out

    try:
        return await asyncio.to_thread(_q)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregation failed: {e}")


@router.get("/api/earnings/portfolios/{portfolio_id}/member-metrics")
async def portfolio_member_metrics(portfolio_id: int):
    """Per-member metrics (EUR-converted, same as the aggregate) so the charts
    can show each holding's own value for a metric and rank them by impact in
    the tooltip. Returns `[{company_id, ticker, name, weight, metrics: [...]}]`."""
    def _q():
        members = _fetch_members(portfolio_id)
        if not members:
            return []
        currency_by_cid, fx_cache = _members_currency_fx(members)
        out = []
        for m in members:
            cur = currency_by_cid.get(m["company_id"], "EUR")
            out.append({
                "company_id": m["company_id"],
                "ticker": m["ticker"],
                "name": m["name"],
                "weight": m["weight"],
                "metrics": _member_eur_rows(m["company_id"], cur, fx_cache.get(cur)),
            })
        return out

    try:
        return await asyncio.to_thread(_q)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Member metrics failed: {e}")
