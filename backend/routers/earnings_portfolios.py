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
import re
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from deps import supabase, chunked

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


# ── AIRS import (Vermogensoverzicht → earnings portfolio) ────────────────────
#
# Import a stored AIRS portfolio's holdings (the daily Vermogensoverzicht in
# `airs_holding`) as an earnings portfolio. AIRS exports only the fund NAME (no
# ticker/ISIN), so each "Fonds" is fuzzy-matched (rapidfuzz) to a company for the
# user to confirm / change / drop, then saved via the normal create endpoint.

_NAME_SUFFIXES = {
    "inc", "incorporated", "corp", "corporation", "co", "company", "ltd",
    "limited", "plc", "ag", "nv", "sa", "se", "spa", "oyj", "ab", "asa", "as",
    "group", "holding", "holdings", "lp", "llc", "ord", "the",
}


def _normalize_name(s: str | None) -> str:
    """Match key for a company / fund name: lowercased, punctuation -> space,
    whitespace collapsed, trailing corporate-form tokens stripped (so
    'Advanced Micro Devices' == 'Advanced Micro Devices Inc'). Falls back to the
    punctuation-stripped form if stripping removes everything."""
    if not s:
        return ""
    n = re.sub(r"[^\w\s]", " ", s.lower())
    n = re.sub(r"\s+", " ", n).strip()
    tokens = n.split()
    while tokens and tokens[0] == "the":
        tokens.pop(0)
    while tokens and tokens[-1] in _NAME_SUFFIXES:
        tokens.pop()
    return " ".join(tokens) if tokens else n


def _load_companies_for_match() -> list[dict]:
    """All companies as `[{company_id, company_name, ticker, exchange}]` for
    name matching (paginated to bypass the PostgREST row cap)."""
    from deps import paginate  # noqa: PLC0415

    out: list[dict] = []
    for r in paginate(
        lambda lo, hi: supabase.table("company")
        .select("company_id, company_name, gurufocus_ticker, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)")
        .range(lo, hi).execute()
    ):
        out.append({
            "company_id": r["company_id"],
            "company_name": r.get("company_name"),
            "ticker": r.get("gurufocus_ticker"),
            "exchange": (r.get("gurufocus_exchange") or {}).get("exchange_code"),
        })
    return out


def _match_names(names: list[str], companies: list[dict]) -> dict[str, list[dict]]:
    """For each holding name, the top company candidates by fuzzy score (0-100).
    Uses rapidfuzz WRatio over normalized names; ties broken by score order."""
    from rapidfuzz import fuzz, process  # noqa: PLC0415

    choices = {c["company_id"]: _normalize_name(c["company_name"] or c.get("ticker"))
               for c in companies}
    info = {c["company_id"]: c for c in companies}
    out: dict[str, list[dict]] = {}
    for name in names:
        if name in out:
            continue
        q = _normalize_name(name)
        cands: list[dict] = []
        if q:
            for _, score, cid in process.extract(q, choices, scorer=fuzz.WRatio, limit=6):
                c = info[cid]
                cands.append({
                    "company_id": cid, "name": c["company_name"],
                    "ticker": c["ticker"], "exchange": c["exchange"],
                    "score": round(float(score), 1),
                })
        out[name] = cands
    return out


def _latest_airs_date(portfolio_name: str) -> str | None:
    latest = (
        supabase.table("airs_holding").select("as_of_date")
        .eq("portefeuille", portfolio_name).order("as_of_date", desc=True)
        .limit(1).execute()
    ).data
    return latest[0]["as_of_date"] if latest else None


@router.get("/api/earnings/airs-portfolios")
async def list_airs_portfolios():
    """Available AIRS Vermogensoverzicht portfolios to import — one entry per
    `portefeuille` (its latest snapshot date + holding count) for the /earnings
    'Import from AIRS' picker."""
    def _q():
        rows = (
            supabase.table("airs_holding")
            .select("portefeuille, as_of_date")
            .order("as_of_date", desc=True)
            .execute()
        ).data or []
        seen: dict[str, dict] = {}
        for r in rows:
            p = r["portefeuille"]
            if p not in seen:
                seen[p] = {"portfolio_name": p, "as_of_date": r["as_of_date"], "holdings": 0}
            if r["as_of_date"] == seen[p]["as_of_date"]:
                seen[p]["holdings"] += 1
        return sorted(seen.values(), key=lambda x: (x["portfolio_name"] or "").lower())
    return await asyncio.to_thread(_q)


@router.get("/api/earnings/airs-portfolios/{portfolio_name}/match")
async def match_airs_portfolio(portfolio_name: str, as_of: str | None = None):
    """Pull an AIRS portfolio's holdings (latest snapshot, or `as_of`) and
    fuzzy-match each Fonds name to a company for the import review UI. Returns
    each holding's AIRS value/weight, the best company match (None when even the
    best score is weak), and a shortlist of alternatives to pick from."""
    def _q():
        date_q = as_of or _latest_airs_date(portfolio_name)
        if not date_q:
            return {"portfolio_name": portfolio_name, "as_of_date": None, "rows": []}
        holdings = (
            supabase.table("airs_holding")
            .select("holding_name, currency, weight, current_value_eur")
            .eq("portefeuille", portfolio_name).eq("as_of_date", date_q)
            .order("current_value_eur", desc=True).execute()
        ).data or []
        matches = _match_names([h["holding_name"] for h in holdings], _load_companies_for_match())
        rows = []
        for h in holdings:
            cands = matches.get(h["holding_name"], [])
            # Default-select the top candidate only when it's a confident match;
            # weak best scores start unmatched so the user must pick.
            best = cands[0] if cands and cands[0]["score"] >= 75 else None
            rows.append({
                "holding_name": h["holding_name"],
                "currency": h.get("currency"),
                "current_value_eur": h.get("current_value_eur"),
                "weight": h.get("weight"),
                "match": best,
                "candidates": cands,
            })
        return {"portfolio_name": portfolio_name, "as_of_date": date_q, "rows": rows}
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
    currency_by_cid: dict[int, str] = {}
    # Chunked: a single `.in_()` over a big union (e.g. two large universes in
    # the attribution matrix, ~2k+ ids) overflows the request URI (PostgREST
    # 414 "URI too long").
    for chunk in chunked(cids):
        cur_resp = (
            supabase.table("company")
            .select("company_id, gurufocus_exchange:gurufocus_exchange(currency_code)")
            .in_("company_id", chunk)
            .execute()
        )
        for r in cur_resp.data or []:
            exch = r.get("gurufocus_exchange") or {}
            currency_by_cid[r["company_id"]] = (exch.get("currency_code") or "EUR").upper()
    fx_cache: dict[str, list[tuple[str, float]]] = {
        cur: _load_fx_asof(cur) for cur in set(currency_by_cid.values()) if cur != "EUR"
    }
    return currency_by_cid, fx_cache


def _load_members_eur_rows(
    members: list[dict],
    currency_by_cid: dict[int, str],
    fx_cache: dict[str, list[tuple[str, float]]],
    *,
    start_year: int | None = None,
) -> dict[int, list[dict]]:
    """Batched EUR-converted dashboard rows for EVERY member, keyed by
    company_id. Mirrors `routers.earnings.load_company_metric_rows` (the same
    four source queries) but fetches all members at once via chunked
    `.in_(company_id)` reads instead of one round-trip per company — so a
    frozen-universe basket of thousands of companies aggregates without
    thousands of sequential queries. Currency-denominated metrics are converted
    to EUR (unit-less ratios pass through); null values dropped.

    `start_year` floors the date filter (default 1998). The /earnings dashboard
    can never display data before its year-selector floor (2015), so passing
    that floor skips loading/transferring ~17 years of unshowable history per
    member — a big saving for a 400+-company universe basket."""
    from routers.earnings import (  # noqa: PLC0415
        _DASHBOARD_METRIC_CODES,
        _LONGEQUITY_METRIC_CODES,
    )

    date_floor = f"{start_year}-01-01" if start_year else "1998-01-01"
    cids = [m["company_id"] for m in members]
    raw: dict[int, list[dict]] = {c: [] for c in cids}

    def _collect(builder_factory, chunk: list[int]) -> None:
        # Paginate one source query for one company chunk. Order by
        # (company_id, target_date, metric_code) so range-pagination is stable
        # across page boundaries — a date-only order can shuffle ties when many
        # companies share a date.
        offset, page = 0, 1000
        while True:
            resp = builder_factory(chunk).range(offset, offset + page - 1).execute()
            batch = resp.data or []
            for r in batch:
                raw.setdefault(r["company_id"], []).append(r)
            if len(batch) < page:
                break
            offset += page

    non_price_codes = [c for c in _DASHBOARD_METRIC_CODES if c != "close_price"]
    sel = "company_id,metric_code,target_date,numeric_value,is_prediction"
    for chunk in chunked(cids):
        _collect(lambda ch: (
            supabase.table("metric_data").select(sel)
            .in_("company_id", ch).eq("source_code", "gurufocus")
            .gte("target_date", date_floor).in_("metric_code", non_price_codes)
            .order("company_id").order("target_date").order("metric_code")
        ), chunk)
        _collect(lambda ch: (
            supabase.table("metric_data").select(sel)
            .in_("company_id", ch).eq("source_code", "gurufocus")
            .eq("metric_code", "close_price").gte("target_date", date_floor)
            .order("company_id").order("target_date")
        ), chunk)
        _collect(lambda ch: (
            supabase.table("metric_data").select(sel)
            .in_("company_id", ch).eq("source_code", "gurufocus")
            .eq("is_prediction", True).gte("target_date", date_floor)
            .like("metric_code", "annual_%")
            .order("company_id").order("target_date").order("metric_code")
        ), chunk)
        _collect(lambda ch: (
            supabase.table("metric_data").select(sel)
            .in_("company_id", ch).eq("source_code", "longequity")
            .in_("metric_code", _LONGEQUITY_METRIC_CODES)
            .order("company_id").order("target_date").order("metric_code")
        ), chunk)

    out: dict[int, list[dict]] = {}
    for c in cids:
        cur = currency_by_cid.get(c, "EUR")
        fx = fx_cache.get(cur)
        conv: list[dict] = []
        for row in raw.get(c, []):
            v = row.get("numeric_value")
            if v is None:
                continue
            code, date = row["metric_code"], row["target_date"]
            val = float(v)
            if cur != "EUR" and _is_currency_metric(code):
                rate = _asof_rate(fx, date) if fx else None
                if not rate:
                    continue
                val = val / rate
            conv.append({
                "metric_code": code,
                "target_date": date,
                "numeric_value": val,
                "is_prediction": bool(row.get("is_prediction")),
            })
        out[c] = conv
    return out


def _aggregate_members(members: list[dict], *, start_year: int | None = None) -> list[dict]:
    """Weighted mean per (metric, date) over members holding data there (weights
    renormalized to those present), currency metrics already EUR. Same shape as
    /api/earnings/{company_id}/metrics, so every chart consumes it directly.
    Shared by the portfolio + frozen-universe basket endpoints."""
    if not members:
        return []
    currency_by_cid, fx_cache = _members_currency_fx(members)
    rows_by_cid = _load_members_eur_rows(members, currency_by_cid, fx_cache, start_year=start_year)

    # acc[(code, date)] = [sum_wv, sum_w, is_prediction]
    acc: dict[tuple[str, str], list] = {}
    for m in members:
        w = float(m["weight"] or 0.0)
        if w <= 0:
            continue
        for row in rows_by_cid.get(m["company_id"], []):
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


def _member_metrics_payload(members: list[dict], *, start_year: int | None = None) -> list[dict]:
    """Per-member EUR-converted metrics (same conversion as the aggregate) so the
    charts can show each holding's own value and rank holdings by impact in the
    tooltip. `[{company_id, ticker, name, weight, metrics: [...]}]`."""
    if not members:
        return []
    currency_by_cid, fx_cache = _members_currency_fx(members)
    rows_by_cid = _load_members_eur_rows(members, currency_by_cid, fx_cache, start_year=start_year)
    return [
        {
            "company_id": m["company_id"],
            "ticker": m["ticker"],
            "name": m["name"],
            "weight": m["weight"],
            "metrics": rows_by_cid.get(m["company_id"], []),
        }
        for m in members
    ]


@router.get("/api/earnings/portfolios/{portfolio_id}/metrics")
async def portfolio_metrics(portfolio_id: int, start_year: int | None = None):
    """Aggregated MetricRow[] for the portfolio — weighted mean per (metric,
    date), currency-denominated metrics converted to EUR. Same shape as
    /api/earnings/{company_id}/metrics, so every chart consumes it directly.
    `start_year` floors the history loaded (the dashboard passes its 2015 floor)."""
    try:
        return await asyncio.to_thread(lambda: _aggregate_members(_fetch_members(portfolio_id), start_year=start_year))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregation failed: {e}")


@router.get("/api/earnings/portfolios/{portfolio_id}/member-metrics")
async def portfolio_member_metrics(portfolio_id: int, start_year: int | None = None):
    """Per-member metrics (EUR-converted, same as the aggregate) for the ranked
    holdings breakdown in chart tooltips. `start_year` floors the history loaded."""
    try:
        return await asyncio.to_thread(lambda: _member_metrics_payload(_fetch_members(portfolio_id), start_year=start_year))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Member metrics failed: {e}")


# ── Frozen-universe baskets ──────────────────────────────────────────────────
# A frozen universe snapshot (universe.frozen_at set, template_key NULL — the
# "ACWI (as of 2026-06)" / "LEONTEQ (as of …)" rows the Freeze button creates)
# can be treated as an equal-weighted basket and aggregated through the exact
# same machinery as a portfolio, so the /earnings dropdown can chart a universe
# next to hand-built portfolios. Endpoints live under /api/earnings so they
# inherit the earnings auth tier (the /static-universes listing is admin-only).

def _universe_members(universe_id: int) -> list[dict]:
    """Equal-weighted members of a universe's latest membership month, shaped
    like `_fetch_members` (company_id, weight, ticker, name) so the aggregation
    reuses verbatim."""
    lm = (
        supabase.table("universe_membership").select("target_month")
        .eq("universe_id", universe_id).order("target_month", desc=True).limit(1).execute()
    )
    if not lm.data:
        return []
    month = lm.data[0]["target_month"]
    cids: list[int] = []
    offset, page = 0, 1000
    while True:
        resp = (
            supabase.table("universe_membership").select("company_id")
            .eq("universe_id", universe_id).eq("target_month", month)
            .range(offset, offset + page - 1).execute()
        )
        batch = resp.data or []
        cids.extend(r["company_id"] for r in batch)
        if len(batch) < page:
            break
        offset += page
    cids = sorted(set(cids))
    if not cids:
        return []
    labels: dict[int, dict] = {}
    for chunk in chunked(cids):
        resp = (
            supabase.table("company")
            .select("company_id, gurufocus_ticker, company_name")
            .in_("company_id", chunk).execute()
        )
        for r in resp.data or []:
            labels[r["company_id"]] = r
    w = 1.0 / len(cids)
    out = [
        {
            "company_id": c,
            "weight": w,
            "ticker": (labels.get(c) or {}).get("gurufocus_ticker"),
            "name": (labels.get(c) or {}).get("company_name"),
        }
        for c in cids
    ]
    out.sort(key=lambda m: (m.get("name") or m.get("ticker") or "").lower())
    return out


@router.get("/api/earnings/universes")
async def list_earnings_universes():
    """Frozen universe snapshots selectable as equal-weighted baskets in the
    earnings Portfolio dropdown. `[{universe_id, label, count}]`, newest first."""
    def _q():
        resp = (
            supabase.table("universe")
            .select("universe_id, label, frozen_at")
            .not_.is_("frozen_at", "null")
            .order("frozen_at", desc=True)
            .execute()
        )
        out = []
        for u in resp.data or []:
            uid = u["universe_id"]
            lm = (
                supabase.table("universe_membership").select("target_month")
                .eq("universe_id", uid).order("target_month", desc=True).limit(1).execute()
            )
            count = 0
            if lm.data:
                c_resp = (
                    supabase.table("universe_membership")
                    .select("company_id", count="exact")
                    .eq("universe_id", uid).eq("target_month", lm.data[0]["target_month"])
                    .limit(0).execute()
                )
                count = getattr(c_resp, "count", 0) or 0
            out.append({"universe_id": uid, "label": u["label"], "count": count})
        return out
    return await asyncio.to_thread(_q)


@router.get("/api/earnings/universes/{universe_id}/metrics")
async def universe_metrics(universe_id: int, start_year: int | None = None):
    """Aggregated MetricRow[] for a frozen universe treated as an equal-weighted
    basket — same shape + machinery as the portfolio aggregate. `start_year`
    floors the history loaded (the dashboard passes its 2015 floor)."""
    try:
        return await asyncio.to_thread(lambda: _aggregate_members(_universe_members(universe_id), start_year=start_year))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregation failed: {e}")


@router.get("/api/earnings/universes/{universe_id}/member-metrics")
async def universe_member_metrics(universe_id: int, start_year: int | None = None):
    """Per-member metrics for a frozen-universe basket (drives the ranked
    holdings breakdown in chart tooltips). `start_year` floors the history loaded."""
    try:
        return await asyncio.to_thread(lambda: _member_metrics_payload(_universe_members(universe_id), start_year=start_year))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Member metrics failed: {e}")


# ── Cross-portfolio sector attribution (Brinson-style) ───────────────────────
# A 2x2 matrix mixing one portfolio's sector ALLOCATION (weights) with another's
# sector SELECTION (returns): cell(row i, col j) = Σ_sectors Wᵢ(s)·Rⱼ(s).
# Diagonal = each portfolio's own one-year return; off-diagonal = counterfactual
# (i's weights × j's stock-picking), isolating allocation vs selection alpha.

def _universe_sector_map(label: str) -> dict[int, str]:
    """`{company_id: sector}` from a universe's latest membership month."""
    u = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
    if not u.data:
        return {}
    uid = u.data[0]["universe_id"]
    lm = (
        supabase.table("universe_membership").select("target_month")
        .eq("universe_id", uid).order("target_month", desc=True).limit(1).execute()
    )
    if not lm.data:
        return {}
    month = lm.data[0]["target_month"]
    out: dict[int, str] = {}
    offset, page = 0, 1000
    while True:
        resp = (
            supabase.table("universe_membership")
            .select("company_id, sector")
            .eq("universe_id", uid).eq("target_month", month)
            .range(offset, offset + page - 1).execute()
        )
        batch = resp.data or []
        for r in batch:
            if r.get("sector"):
                out[r["company_id"]] = r["sector"]
        if len(batch) < page:
            break
        offset += page
    return out


def _company_sector_map(cids: list[int]) -> dict[int, str]:
    """`{company_id: sector}` for the given cids using each company's OWN sector
    classification — the same `company_sector_with_source` RPC the /companies
    page shows (Leonteq-preferred, else the most recent month from any universe).

    Used as the attribution sector FALLBACK: a holding outside the chosen
    reference sector-universe still gets its real sector instead of dropping into
    'Unclassified'. Best-effort — a failure (e.g. RPC missing on an un-migrated
    prod) just yields no fallback. The RPC returns one row per company (DISTINCT
    ON), so paginate the full set and keep the ones we need."""
    want = set(cids)
    out: dict[int, str] = {}
    if not want:
        return out
    try:
        offset, page = 0, 1000
        for _ in range(50):
            resp = (
                supabase.rpc("company_sector_with_source")
                .range(offset, offset + page - 1).execute()
            )
            rows = resp.data or []
            if not rows:
                break
            for r in rows:
                cid, sec = r.get("company_id"), r.get("sector")
                if cid in want and sec:
                    out[int(cid)] = sec
            if len(rows) < page or len(out) >= len(want):
                break
            offset += page
    except Exception:
        pass  # best-effort fallback; missing → 'Unclassified' as before
    return out


def _asof_price(series: list[tuple[str, float]], date: str) -> float | None:
    """Latest EUR price on or before `date` (series sorted ascending)."""
    if not series:
        return None
    idx = bisect.bisect_right([d for d, _ in series], date) - 1
    return series[idx][1] if idx >= 0 else None


def _load_eur_close_prices(cids: list[int], start_date: str, end_date: str) -> dict[int, list[tuple[str, float]]]:
    """EUR-converted daily close prices per company over [start, end]."""
    currency_by_cid, fx_cache = _members_currency_fx([{"company_id": c} for c in cids])
    out: dict[int, list[tuple[str, float]]] = {c: [] for c in cids}
    for chunk in chunked(cids):
        offset, page = 0, 1000
        while True:
            resp = (
                supabase.table("metric_data")
                .select("company_id, target_date, numeric_value")
                .eq("source_code", "gurufocus").eq("metric_code", "close_price")
                .in_("company_id", chunk)
                .gte("target_date", start_date).lte("target_date", end_date)
                .order("target_date")
                .range(offset, offset + page - 1).execute()
            )
            batch = resp.data or []
            for r in batch:
                v = r.get("numeric_value")
                if v is None:
                    continue
                c = r["company_id"]
                cur = currency_by_cid.get(c, "EUR")
                val = float(v)
                if cur != "EUR":
                    rate = _asof_rate(fx_cache.get(cur), r["target_date"])
                    if not rate:
                        continue
                    val = val / rate
                out[c].append((r["target_date"], val))
            if len(batch) < page:
                break
            offset += page
    return out


def _latest_close_year(cids: list[int]) -> int | None:
    """Calendar year of the most recent close price across `cids`."""
    for chunk in chunked(cids):
        resp = (
            supabase.table("metric_data").select("target_date")
            .eq("source_code", "gurufocus").eq("metric_code", "close_price")
            .in_("company_id", chunk).order("target_date", desc=True).limit(1).execute()
        )
        if resp.data:
            return int(resp.data[0]["target_date"][:4])
    return None


def _portfolio_sectors(weighted: list[tuple[int, float]], sector_of, ret: dict[int, float | None]):
    """Per-sector weight Wᵢ(s) + within-sector weighted return Rᵢ(s) (over the
    priced holdings; None when the sector has no priced holding)."""
    sec_w: dict[str, float] = {}
    ret_num: dict[str, float] = {}
    ret_w: dict[str, float] = {}
    for cid, w in weighted:
        s = sector_of(cid)
        sec_w[s] = sec_w.get(s, 0.0) + w
        r = ret.get(cid)
        if r is not None:
            ret_num[s] = ret_num.get(s, 0.0) + w * r
            ret_w[s] = ret_w.get(s, 0.0) + w
    sec_r = {s: (ret_num[s] / ret_w[s] if ret_w.get(s, 0.0) > 0 else None) for s in sec_w}
    return sec_w, sec_r


@router.get("/api/earnings/sector-universes")
async def sector_universes():
    """Universes that carry a sector classification (for the attribution sector
    picker). Leonteq first (the default)."""
    def _q():
        resp = supabase.table("universe").select("label").order("label").execute()
        labels = [r["label"] for r in (resp.data or [])]
        with_sectors = []
        for lbl in labels:
            uid_resp = supabase.table("universe").select("universe_id").eq("label", lbl).limit(1).execute()
            if not uid_resp.data:
                continue
            uid = uid_resp.data[0]["universe_id"]
            s = (
                supabase.table("universe_membership").select("company_id")
                .eq("universe_id", uid).not_.is_("sector", "null").limit(1).execute()
            )
            if s.data:
                with_sectors.append(lbl)
        with_sectors.sort(key=lambda x: (x != "Leonteq", x))  # Leonteq first
        return {"universes": with_sectors}
    return await asyncio.to_thread(_q)


def _basket_members(kind: str, bid: int) -> list[dict]:
    """Members of a basket — a saved portfolio (`kind='portfolio'`, weighted) or
    a frozen universe snapshot (`kind='universe'`, equal-weighted). Same shape
    either way so attribution treats them identically."""
    return _universe_members(bid) if kind == "universe" else _fetch_members(bid)


def _basket_name(kind: str, bid: int) -> str | None:
    """Display name for a basket: a universe's `label` or a portfolio's `name`."""
    if kind == "universe":
        r = supabase.table("universe").select("label").eq("universe_id", bid).limit(1).execute().data
        return (r or [{}])[0].get("label")
    r = supabase.table("earnings_portfolio").select("name").eq("id", bid).limit(1).execute().data
    return (r or [{}])[0].get("name")


@router.get("/api/earnings/portfolios/attribution")
async def portfolio_attribution(
    a: int,
    b: int,
    a_kind: str = "portfolio",
    b_kind: str = "portfolio",
    universe: str = "Leonteq",
    year: int | None = None,
):
    """Cross-basket sector attribution over one calendar year. Each side is a
    basket — a saved portfolio (`*_kind=portfolio`) or a frozen-universe snapshot
    (`*_kind=universe`, equal-weighted) — so the /earnings portfolio mode can
    compare two universes, two portfolios, or one of each. Returns the 2x2
    matrix + the per-sector weights/returns behind it."""
    def _q():
        pa, pb = _basket_members(a_kind, a), _basket_members(b_kind, b)
        if not pa or not pb:
            raise HTTPException(status_code=400, detail="Both baskets need members")

        def norm(members):
            tot = sum((m["weight"] or 0.0) for m in members) or 1.0
            return [(m["company_id"], (m["weight"] or 0.0) / tot) for m in members]

        wa, wb = norm(pa), norm(pb)
        cids = sorted({cid for cid, _ in wa} | {cid for cid, _ in wb})

        yr = year if year is not None else (_latest_close_year(cids) or 0)
        prices = _load_eur_close_prices(cids, f"{yr - 1}-01-01", f"{yr}-12-31")
        ret: dict[int, float | None] = {}
        for cid in cids:
            series = prices.get(cid) or []
            p_end = _asof_price(series, f"{yr}-12-31")
            p_start = _asof_price(series, f"{yr - 1}-12-31")
            ret[cid] = (p_end / p_start - 1.0) if (p_start and p_end and p_start != 0) else None

        # Reference sector taxonomy = the chosen universe's membership sectors;
        # fall back to each company's OWN sector for holdings outside it, so an
        # imported portfolio full of non-universe names still attributes instead
        # of collapsing into 'Unclassified'.
        sector_by_cid = _universe_sector_map(universe)
        own_sector = _company_sector_map(cids)
        def sector_of(cid):
            return sector_by_cid.get(cid) or own_sector.get(cid) or "Unclassified"

        def members_detail(members, normed):
            """Per-holding rows for the 'show holdings' view: normalized weight,
            resolved sector, one-year return (None = no price coverage), priced
            flag. Sorted weight-desc."""
            nw = {cid: w for cid, w in normed}
            rows = [
                {
                    "company_id": m["company_id"],
                    "ticker": m.get("ticker"),
                    "name": m.get("name"),
                    "weight": round(nw.get(m["company_id"], 0.0), 6),
                    "sector": sector_of(m["company_id"]),
                    "ret": (round(ret.get(m["company_id"]), 4)
                            if ret.get(m["company_id"]) is not None else None),
                    "priced": ret.get(m["company_id"]) is not None,
                }
                for m in members
            ]
            rows.sort(key=lambda x: -x["weight"])
            return rows

        wa_s, ra_s = _portfolio_sectors(wa, sector_of, ret)
        wb_s, rb_s = _portfolio_sectors(wb, sector_of, ret)
        sectors = sorted(set(wa_s) | set(wb_s))

        def rget(rmap, s):  # missing sector → 0% (per spec)
            v = rmap.get(s)
            return v if v is not None else 0.0

        def cell(wi, rj):
            return sum(wi.get(s, 0.0) * rget(rj, s) for s in sectors)

        # rows = whose weights (a, b); cols = whose returns (a, b)
        matrix = [
            [cell(wa_s, ra_s), cell(wa_s, rb_s)],
            [cell(wb_s, ra_s), cell(wb_s, rb_s)],
        ]

        def side(wmap, rmap):
            return {
                "name": None,  # filled below
                "sector_weights": {s: wmap.get(s, 0.0) for s in sectors},
                "sector_returns": {s: rmap.get(s) for s in sectors},
            }

        a_name = _basket_name(a_kind, a)
        b_name = _basket_name(b_kind, b)
        sa, sb = side(wa_s, ra_s), side(wb_s, rb_s)
        sa["name"], sb["name"] = a_name, b_name
        sa["members"] = members_detail(pa, wa)
        sb["members"] = members_detail(pb, wb)
        return {
            "year": yr,
            "universe": universe,
            "sectors": sectors,
            "a": sa,
            "b": sb,
            "matrix": matrix,
        }

    try:
        return await asyncio.to_thread(_q)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Attribution failed: {e}")
