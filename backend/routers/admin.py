"""Admin-only programmatic API.

Purpose: let an external script (e.g. a local IBKR re-balancer) pull the
latest scheduled-strategy portfolio + monitor pipeline health without
opening the BBTerminal web UI. All endpoints under `/api/admin/` require
the caller's Bearer JWT to have `app_metadata.role == 'admin'` — same
gate the UI's admin pages use. Sign-in:

    curl -X POST "$SUPABASE_URL/auth/v1/token?grant_type=password" \
        -H "apikey: $SUPABASE_ANON_KEY" \
        -H "Content-Type: application/json" \
        -d '{"email":"admin@example.com","password":"…"}'
    → {access_token, refresh_token, expires_at}

Then call admin endpoints with:

    curl -H "Authorization: Bearer $ACCESS_TOKEN" \
         "https://<backend>/api/admin/schedules"

Endpoints — the IBKR buy flow is just three:
    GET /api/admin/schedules         — list strategies + each one's next rebalance date
                                        (lightweight; no holdings)
    GET /api/admin/schedules/{id}    — one strategy's CURRENT holdings (order-ready:
                                        ticker/exchange/country/currency/isin/company_name/
                                        weight/side/prices) + as_of_date
    GET /api/admin/health            — composite go/no-go; gate trades on is_healthy_strict

Universe explorer (same per-company shape as holdings):
    GET /api/admin/universes         — list every universe + its id / kind / month range
    GET /api/admin/universes/{id}    — full membership for a month (default latest;
                                        ?month=YYYY-MM), each member with
                                        ticker/exchange/country/currency/isin/sector +
                                        latest close (native + EUR)

The remaining endpoints are data-maintenance tools (GuruFocus exchange resolution,
companies missing/flagged), separate from the buy flow.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, Header, HTTPException
from postgrest.exceptions import APIError
from pydantic import BaseModel

from deps import fetch_in_chunks, supabase
from routers._admin_health import _max_target_date, _now_utc, _trading_day_age
from routers._admin_payloads import (
    _build_portfolio_payload,
    _enrich_universe_members,
    _fetch_latest_snapshots_for,
)
from routers.auth import _require_admin

router = APIRouter(tags=["admin"])


# ─── Portfolio ─────────────────────────────────────────────────────


class _GuruFocusExchangeSearchRequest(BaseModel):
    """One request entry for the bulk exchange-search diagnostic."""
    ticker: str
    current_exchange: str | None = None  # informational only -- gets echoed back


class _GuruFocusExchangeSearchBody(BaseModel):
    tickers: list[_GuruFocusExchangeSearchRequest]
    # Candidate exchange codes to probe in order. Defaults to a list
    # covering the markets GuruFocus's subscription includes (USA +
    # Europe + Asia per FEASIBLE_GF_EXCHANGES). Caller can override to
    # narrow the search or add a vendor-specific code we missed.
    candidate_exchanges: list[str] | None = None


class _GfCompanyNameBody(BaseModel):
    ticker: str
    exchange: str | None = None


class _IlliquidBody(BaseModel):
    company_id: int
    illiquid: bool = True


@router.post("/api/admin/company-illiquid")
async def set_company_illiquid(body: _IlliquidBody, authorization: str = Header(...)):
    """Mark / unmark a company as **illiquid** — a listing that trades
    infrequently, so GuruFocus serves stale prices for it (e.g. Telecom Italia
    savings shares MIL:TITR). Sets `company.illiquid_at` (now / NULL). Illiquid
    companies are still priced (they occasionally trade) but are excluded from
    the price-coverage freshness measure so their perpetually-behind close can't
    masquerade as the 'oldest' active price. Admin only."""
    _require_admin(authorization)

    def _q() -> dict:
        val = datetime.now(timezone.utc).isoformat() if body.illiquid else None
        supabase.table("company").update({"illiquid_at": val}).eq("company_id", body.company_id).execute()
        return {"company_id": body.company_id, "illiquid": body.illiquid}

    return await asyncio.to_thread(_q)


@router.post("/api/admin/gurufocus-company-name")
async def gurufocus_company_name(body: _GfCompanyNameBody, authorization: str = Header(...)):
    """Fetch the company name GuruFocus reports for a (ticker, exchange) — so a
    mislabeled row can be corrected to what its GuruFocus link actually shows
    (e.g. a row stored as "TSMC" whose `TSE:2330` listing GuruFocus calls
    "Forside Co Ltd"). One GuruFocus call. Returns `{name, found, symbol, log}`;
    the caller confirms + writes the rename via PUT /api/companies/{id}."""
    _require_admin(authorization)
    from index_universe.backfill_market_cap import gf_company_name_for  # noqa: PLC0415
    return await asyncio.to_thread(gf_company_name_for, body.ticker, body.exchange)


class _PriceRefreshBody(BaseModel):
    company_id: int
    # When set, re-price this strategy's held basket after the fetch so the
    # Current-portfolio card + monthly-returns heatmap reflect the fresh close
    # immediately (a new price_update snapshot).
    strategy_id: int | None = None


@router.post("/api/admin/company-price-refresh")
async def company_price_refresh(body: _PriceRefreshBody, authorization: str = Header(...)):
    """Force-refresh ONE holding's prices from GuruFocus (bypassing cache) and
    return a COMPACT view of the actual API request + response — for clearing a
    single stale row straight from the /schedule Price-update / Current-portfolio
    views.

    Handles both a real company (positive `company_id` → `metric_data`) and an
    ETF overlay (negative `company_id` = `-benchmark_id` → `benchmark_price`,
    priced as a US listing like the /benchmarks refresh). When `strategy_id` is
    supplied, re-prices that strategy's held basket (a new price_update snapshot)
    so the card + heatmap update on the next reload. Admin only."""
    _require_admin(authorization)
    import os  # noqa: PLC0415
    from urllib.parse import quote  # noqa: PLC0415

    from ingest.api_usage import track_api_call  # noqa: PLC0415
    from ingest.constants import DATA_CUTOFF  # noqa: PLC0415
    from ingest.prices import (  # noqa: PLC0415
        _build_symbol,
        _fetch_price_from_api,
        _mask_url,
        _parse_price_series,
        ensure_prices_for_company,
    )

    def _q() -> dict:
        cid = body.company_id
        if cid == 0:
            raise HTTPException(400, "Cash is not a priceable security.")

        # ── ETF overlay: negative company_id = -benchmark_id (benchmark_price) ──
        if cid < 0:
            bid = -cid
            b = (
                supabase.table("benchmark")
                .select("benchmark_id, ticker, name")
                .eq("benchmark_id", bid)
                .limit(1)
                .execute()
            )
            if not b.data:
                raise HTTPException(404, f"Benchmark #{bid} not found")
            brow = b.data[0]
            ticker = brow.get("ticker") or ""
            if not ticker:
                raise HTTPException(400, f"Benchmark #{bid} has no ticker to fetch")
            exchange = "NYSE"  # ETFs price as US listings (matches /benchmarks refresh)

            def _edge(desc: bool, n: int = 2) -> list[str]:
                r = (
                    supabase.table("benchmark_price")
                    .select("target_date")
                    .eq("benchmark_id", bid)
                    .order("target_date", desc=desc)
                    .limit(n)
                    .execute()
                )
                return [str(x["target_date"])[:10] for x in (r.data or [])]

            before = (_edge(True, 1) or [None])[0]
            data, api_log, http_status = _fetch_price_from_api(ticker, exchange)
            track_api_call(supabase, exchange)
            parsed = _parse_price_series(data) if data is not None else []
            rows_loaded = 0
            if parsed:
                rows = [
                    {"benchmark_id": bid, "target_date": d.isoformat(), "price": p}
                    for d, p in parsed if d >= DATA_CUTOFF
                ]
                for i in range(0, len(rows), 500):
                    supabase.table("benchmark_price").upsert(
                        rows[i:i + 500], on_conflict="benchmark_id,target_date"
                    ).execute()
                rows_loaded = len(rows)
            symbol = _build_symbol(ticker, exchange)
            base = os.environ.get("GURUFOCUS_BASE_URL", "").strip().rstrip("/")
            if base.endswith("/data"):
                base = base[: -len("/data")]
            key = os.environ.get("GURUFOCUS_API_KEY", "")
            info = {
                "company_name": brow.get("name"),
                "ticker": ticker, "exchange": exchange, "resolved_exchange": None,
                "request_url": _mask_url(f"{base}/public/user/{key}/stock/{quote(symbol, safe=':')}/price"),
                "symbol": symbol,
                "http_status": http_status,
                "source": "api" if parsed else "none",
                "points": len(parsed),
                "excerpt": None,
                "error": None if parsed else (api_log or "no prices parsed"),
                "is_delisted": False, "is_forbidden": False,
                "rows_loaded": rows_loaded, "api_calls": 1,
                "before": before, "newest": _edge(True), "oldest": _edge(False),
                "logs": [api_log] if api_log else [],
            }
        # ── Real company: positive company_id (metric_data) ──
        else:
            resp = (
                supabase.table("company")
                .select(
                    "company_id, company_name, gurufocus_ticker, "
                    "gurufocus_exchange:gurufocus_exchange(exchange_code)"
                )
                .eq("company_id", cid)
                .limit(1)
                .execute()
            )
            if not resp.data:
                raise HTTPException(404, f"Company #{cid} not found")
            row = resp.data[0]
            ticker = row.get("gurufocus_ticker") or ""
            exchange = (row.get("gurufocus_exchange") or {}).get("exchange_code") or ""
            if not ticker or not exchange:
                raise HTTPException(400, f"Company #{cid} has no ticker/exchange to fetch")

            def _edge(desc: bool, n: int = 2) -> list[str]:
                r = (
                    supabase.table("metric_data")
                    .select("target_date")
                    .eq("company_id", cid)
                    .eq("metric_code", "close_price")
                    .order("target_date", desc=desc)
                    .limit(n)
                    .execute()
                )
                return [str(x["target_date"])[:10] for x in (r.data or [])]

            before = (_edge(True, 1) or [None])[0]
            result = ensure_prices_for_company(
                supabase, cid, ticker, exchange, force_refresh=True,
            )
            info = {
                "company_name": row.get("company_name"),
                "ticker": ticker, "exchange": exchange,
                "resolved_exchange": result.resolved_exchange,
                "request_url": result.request_url, "symbol": f"{exchange}:{ticker}",
                "http_status": result.http_status,
                "source": result.source, "points": result.total_prices,
                "excerpt": result.response_excerpt, "error": result.error,
                "is_delisted": result.is_delisted, "is_forbidden": result.is_forbidden,
                "rows_loaded": result.rows_loaded, "api_calls": result.api_calls,
                "before": before, "newest": _edge(True), "oldest": _edge(False),
                "logs": result.logs,
            }

        after = info["newest"][0] if info["newest"] else None

        repriced = False
        if body.strategy_id is not None:
            try:
                from routers._schedule_snapshots import (  # noqa: PLC0415
                    compute_and_save_price_update,
                )
                compute_and_save_price_update(body.strategy_id, ingest_run_id=None)
                repriced = True
            except Exception:
                repriced = False

        return {
            "company_id": cid,
            "company_name": info["company_name"],
            "ticker": info["ticker"],
            "exchange": info["exchange"],
            "resolved_exchange": info["resolved_exchange"],
            "request": {
                "method": "GET",
                "url": info["request_url"],
                "symbol": info["symbol"],
            },
            "response": {
                # None on a cache-only path (API not hit) — surfaced as "cache".
                "http_status": info["http_status"],
                "source": info["source"],
                "points": info["points"],
                "excerpt": info["excerpt"],
                "error": info["error"],
                "is_delisted": info["is_delisted"],
                "is_forbidden": info["is_forbidden"],
            },
            "db": {
                "rows_loaded": info["rows_loaded"],
                "latest_before": info["before"],
                "latest_after": after,
                "advanced": bool(after and (info["before"] is None or after > info["before"])),
            },
            # The 2 newest + 2 oldest close dates now stored (= what the fetch
            # returned, clamped to the 1998 cutoff) — the compact single-line view.
            "dates": {"newest": info["newest"], "oldest": info["oldest"]},
            "api_calls": info["api_calls"],
            "logs": info["logs"],
            "repriced": repriced,
        }

    return await asyncio.to_thread(_q)


@router.post("/api/admin/gurufocus-exchange-search")
async def gurufocus_exchange_search(
    body: _GuruFocusExchangeSearchBody,
    authorization: str = Header(...),
):
    """Probe GuruFocus to find which exchange code ACTUALLY resolves for
    each ticker. Use case: `company.gurufocus_lookup_failed_at` is set on
    a row whose `exchange_id` is wrong (e.g. NYSE:ASND when the listing
    is really NASDAQ:ASND). This endpoint tries each candidate exchange
    in turn until one returns price data, then reports the match per
    ticker.

    Cost: O(tickers × candidates_per_ticker) GuruFocus API calls in the
    worst case. The probe short-circuits to the first hit, so a ticker
    that lives on its first-tried exchange is cheap (1 call). Bound
    `candidate_exchanges` to a small list if you're searching across
    many tickers.

    Response: one entry per ticker with `{ticker, current_exchange,
    found_exchange, status, candidates_tried, error}`. `found_exchange`
    is non-null only when an exchange resolved to a 200 with parseable
    data.
    """
    _require_admin(authorization)

    DEFAULT_CANDIDATES = [
        # Largest US exchanges first -- the most common wrong-NYSE/NASDAQ
        # slip gets caught in the first 2 probes.
        "NAS", "NYSE", "AMEX", "OTCBB",
        # Major European exchanges (matches FEASIBLE_GF_EXCHANGES in acwi/exchange_map).
        "XTER", "XPAR", "AMS", "OBOM", "MIL", "MAD", "WBO",
        "STO", "OSL", "HEL", "CSE", "LSE", "SWX",
        # Asia.
        "TSE", "HKEX", "KSE", "BOM", "NSE", "SGX", "TPE",
    ]
    candidates = body.candidate_exchanges or DEFAULT_CANDIDATES

    from ingest._gurufocus_http import cf_get  # noqa: PLC0415
    import os as _os  # noqa: PLC0415

    base_url = (_os.environ.get("GURUFOCUS_BASE_URL", "").strip().rstrip("/"))
    if base_url.endswith("/data"):
        base_url = base_url[: -len("/data")]
    api_key = _os.environ.get("GURUFOCUS_API_KEY", "")
    if not base_url or not api_key:
        raise HTTPException(500, "GURUFOCUS_BASE_URL / GURUFOCUS_API_KEY not set")

    def _build_symbol(ticker: str, exch: str) -> str:
        # Mirror ingest.prices._build_symbol's US-vs-non-US convention.
        us = {"NAS", "NASDAQ", "NYSE", "AMEX", "CBOE"}
        return ticker if exch.upper() in us else f"{exch}:{ticker}"

    def _probe_one(ticker: str, current: str | None) -> dict:
        tried: list[dict] = []
        # Try the company's CURRENT exchange first (free signal -- if it
        # actually works the caller already would've gotten data). Then
        # the candidates in order, skipping any equal to the current
        # exchange.
        order: list[str] = []
        if current:
            order.append(current)
        for c in candidates:
            if c.upper() != (current or "").upper():
                order.append(c)
        for exch in order:
            symbol = _build_symbol(ticker, exch)
            url = f"{base_url}/public/user/{api_key}/stock/{symbol}/price"
            r = cf_get(url, headers={"Accept": "application/json"}, timeout=30)
            short_body = (r.text or "")[:120].replace("\n", " ")
            tried.append({
                "exchange": exch,
                "status_code": r.status_code,
                "ok": r.ok,
                "body_excerpt": short_body,
            })
            if r.ok:
                # Sanity-check: GuruFocus sometimes returns 200 with an
                # error string in the body. The price endpoint emits a
                # JSON array on success; treat a leading `[` as the
                # positive signal.
                if r.text and r.text.lstrip().startswith("["):
                    return {
                        "ticker": ticker,
                        "current_exchange": current,
                        "found_exchange": exch,
                        "status": "found",
                        "candidates_tried": tried,
                        "error": None,
                    }
        return {
            "ticker": ticker,
            "current_exchange": current,
            "found_exchange": None,
            "status": "not_found",
            "candidates_tried": tried,
            "error": f"No candidate exchange resolved {ticker}. Tried {len(tried)} exchanges.",
        }

    def _q() -> list[dict]:
        return [_probe_one(t.ticker, t.current_exchange) for t in body.tickers]

    return await asyncio.to_thread(_q)


@router.get("/api/admin/gurufocus-probe")
async def gurufocus_probe(
    authorization: str = Header(...),
    symbol: str = "AAPL",
    endpoint: str = "price",
):
    """One-shot diagnostic: hit a single GuruFocus URL through the same
    `cf_get` + impersonation ladder the ingest pipeline uses, and return
    the FULL response (status, response headers, body excerpt, attempted
    fingerprints) so we can confirm whether a failure is actually a
    Cloudflare IP block or something else (revoked key, vendor 403,
    nginx misconfig, etc.).

    Query params:
        symbol   GuruFocus symbol form, e.g. "AAPL" or "XAMS:ABN" (default AAPL)
        endpoint One of "price", "financials", "analyst_estimate",
                 "forward_pe_ratio" (default "price")

    Look for these in `headers` to confirm Cloudflare:
        cf-ray            present → Cloudflare touched the response
        server=cloudflare same signal
        cf-mitigated      explicit "challenge" / "block" verdict
    If those are absent on a 403, it's NOT Cloudflare — investigate the
    upstream (likely a GuruFocus auth/quota issue).
    """
    _require_admin(authorization)
    import os as _os  # noqa: PLC0415

    from ingest._gurufocus_http import cf_get, ladder, current_preferred_target  # noqa: PLC0415

    base_url = (_os.environ.get("GURUFOCUS_BASE_URL", "").strip().rstrip("/"))
    if base_url.endswith("/data"):
        base_url = base_url[: -len("/data")]
    api_key = _os.environ.get("GURUFOCUS_API_KEY", "")
    if not base_url or not api_key:
        raise HTTPException(500, "GURUFOCUS_BASE_URL / GURUFOCUS_API_KEY not set")

    safe_endpoint = endpoint.strip().lstrip("/")
    url = f"{base_url}/public/user/{api_key}/stock/{symbol}/{safe_endpoint}"
    masked_url = url.replace(api_key, api_key[:4] + "***") if api_key else url

    def _q() -> dict:
        resp = cf_get(
            url,
            headers={"Accept": "application/json"},
            timeout=30,
        )
        return {
            "url": masked_url,
            "status_code": resp.status_code,
            "used_target": resp.used_target,
            "attempted": resp.attempted,
            "ladder": ladder(),
            "current_preferred": current_preferred_target(),
            "error": resp.error,
            "is_cloudflare_block": resp.is_cloudflare_block,
            "diagnostic_headers": resp.diagnostic_headers(),
            "all_response_headers": resp.headers,
            "body_excerpt": (resp.text or "")[:2000],
            "body_length": len(resp.text or ""),
            "proxy_set": bool(_os.environ.get("GURUFOCUS_PROXY") or _os.environ.get("HTTPS_PROXY")),
            "observed_at": datetime.now(timezone.utc).isoformat(),
        }

    return await asyncio.to_thread(_q)


@router.get("/api/admin/egress-ip")
async def get_egress_ip(authorization: str = Header(...)):
    """Return the IP this backend currently appears to egress from.

    Why: AirSPMS allowlists by IP, Railway hobby/free egress IPs CAN
    rotate across deploys/restarts. Hit this endpoint a few times over
    a day to see whether the IP is stable enough to allowlist (or to
    discover the value to plug into the allowlist + Railway's paid
    static-egress add-on).

    Returns: {ip, source, observed_at, headers_seen}. Uses ifconfig.me
    as the reflector; falls back to a couple alternates if it 4xx/5xxs
    so a single reflector outage doesn't blind us.
    """
    _require_admin(authorization)

    reflectors = [
        "https://ifconfig.me/all.json",
        "https://api.ipify.org?format=json",
        "https://ifconfig.co/json",
    ]

    def _q() -> dict:
        import requests as _req  # noqa: PLC0415
        for url in reflectors:
            try:
                r = _req.get(url, timeout=10)
                if not r.ok:
                    continue
                data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                ip = (
                    data.get("ip")
                    or data.get("ip_addr")
                    or (r.text.strip() if "ipify" in url else None)
                )
                if ip:
                    return {
                        "ip": ip,
                        "source": url,
                        "observed_at": datetime.now(timezone.utc).isoformat(),
                        "raw": data,
                    }
            except Exception:
                # Per-reflector failure is non-fatal — the loop tries
                # the next one. If every reflector fails we 502 below.
                continue
        raise HTTPException(502, "all egress-ip reflectors failed")

    return await asyncio.to_thread(_q)


@router.get("/api/admin/network-diagnostics")
async def network_diagnostics(authorization: str = Header(...), guru_method: str = "curl"):
    """Reachability report for every external service the terminal depends on
    — backs the /network page. Returns this backend's egress IP, the live
    GuruFocus Cloudflare circuit-breaker state, and per-source verdicts (DNS
    IP, latency, status, and a plain-language reason). `guru_method=curl`
    (default) probes GuruFocus through the real curl_cffi impersonation ladder
    so the verdict matches what the ingest pipeline experiences in prod;
    `guru_method=plain` uses a bare requests.get to show whether the API still
    bot-challenges fingerprint-less clients. See routers/_network_diag.py."""
    _require_admin(authorization)
    from routers._network_diag import run_diagnostics  # noqa: PLC0415

    return await run_diagnostics(guru_method)


@router.get("/api/admin/copy-status")
async def copy_status(authorization: str = Header(...)):
    """Diagnose the direct-Postgres COPY fast path the heavy loaders use
    (backtests, /companies, FX, freshness). When SUPABASE_DB_URL is unset
    OR the connection fails, those loaders SILENTLY fall back to PostgREST,
    which then times out (57014) on large universes like LEONTEQ.

    Returns whether the path is enabled, the connection target (password
    masked, so you can see the host/port — e.g. pooler :5432 vs :6543 vs
    the IPv6-only direct host), and the result of an ACTUAL test COPY with
    the EXACT exception when it fails. Hit this to stop guessing why a
    backtest times out in prod."""
    _require_admin(authorization)

    from momentum.data._pg import _db_url  # noqa: PLC0415

    def _mask(url: str) -> str:
        import re  # noqa: PLC0415
        # postgresql://user:pass@host:port/db -> postgresql://user:***@host:port/db
        return re.sub(r"(://[^:/@]+:)[^@]*(@)", r"\1***\2", url)

    def _q() -> dict:
        url = _db_url()
        out: dict = {
            "copy_path_enabled": bool(url),
            "db_url_target": _mask(url) if url else None,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        if not url:
            out["copy_works"] = False
            out["status"] = (
                "DISABLED — neither SUPABASE_DB_URL nor DATABASE_URL is visible to "
                "this process. Every heavy load falls back to PostgREST and times "
                "out on big universes. Set SUPABASE_DB_URL on the backend service "
                "and redeploy."
            )
            return out
        try:
            import psycopg  # noqa: PLC0415
        except ImportError:
            out["copy_works"] = False
            out["status"] = "psycopg not installed — falling back to PostgREST."
            return out
        # Trivial COPY round-trip over a fresh connection — capture the EXACT
        # failure reason instead of the silent None that _run_copy returns.
        try:
            with psycopg.connect(url, connect_timeout=10) as conn:
                with conn.cursor() as cur:
                    with cur.copy("COPY (SELECT 1) TO STDOUT WITH (FORMAT csv)") as cp:
                        b"".join(cp)
            out["copy_works"] = True
            out["status"] = "OK — direct COPY connection works; the fast path is active."
        except Exception as e:  # noqa: BLE001 — surface the reason, never raise
            out["copy_works"] = False
            out["status"] = (
                f"FAILED — direct COPY raised {type(e).__name__}: {e}. THIS is why "
                "heavy loads fall back to PostgREST and time out. Common causes: "
                "SUPABASE_DB_URL points at the IPv6-only direct host "
                "(db.<ref>.supabase.co) that Railway (IPv4) can't reach, or the "
                "transaction pooler (:6543). Use the Session pooler host on :5432."
            )
        return out

    return await asyncio.to_thread(_q)


# ─── Schedules ─────────────────────────────────────────────────────


@router.get("/api/admin/schedules")
async def list_schedules(
    enabled_only: bool = True,
    authorization: str = Header(...),
):
    """List every scheduled strategy with its next rebalance date. Admin
    only. Lightweight (no holdings) — the discovery call: find the
    `strategy_id` to drill into, see when each next rebalances, and how
    fresh its holdings are.

    `next_rebalance_at` is the UTC tick at which the strategy will next
    re-select its holdings (NULL = a never-run strategy, rebalances on the
    next tick). `as_of_date` / `latest_price_date` / `holdings_count` come
    from its most recent snapshot (absent until the strategy first runs).

    Query: `enabled_only=true` (default) hides paused strategies; pass
    `false` to see everything.

    Response: `[{strategy_id, name, enabled, frequency, next_rebalance_at,
    last_run_at, as_of_date, latest_price_date, holdings_count}]`."""
    _require_admin(authorization)

    def _query() -> list[dict]:
        q = supabase.table("scheduled_strategy").select("*").order("created_at")
        if enabled_only:
            q = q.eq("enabled", True)
        try:
            rows = q.execute().data or []
        except APIError as e:
            raise HTTPException(500, f"DB read failed: {e}")
        latest = _fetch_latest_snapshots_for([r["id"] for r in rows])
        out: list[dict] = []
        for r in rows:
            snap = latest.get(r["id"])
            out.append({
                "strategy_id": r["id"],
                "name": r.get("name") or f"Strategy #{r['id']}",
                "enabled": r.get("enabled", True),
                "frequency": r.get("frequency"),
                "next_rebalance_at": r.get("next_due_at"),
                "last_run_at": r.get("last_run_at"),
                "as_of_date": snap.get("as_of_date") if snap else None,
                "latest_price_date": snap.get("latest_price_date") if snap else None,
                "holdings_count": len(snap.get("holdings") or []) if snap else 0,
            })
        return out

    return await asyncio.to_thread(_query)


@router.get("/api/admin/schedules/{strategy_id}")
async def get_schedule(strategy_id: int, authorization: str = Header(...)):
    """One scheduled strategy's CURRENT holdings — the order-ready call your
    IBKR buyer makes. Admin only.

    Holdings come from the strategy's most recent `current_picks_snapshot`;
    `as_of_date` is the date they were selected and `latest_price_date` the
    most recent close priced into them — gate on these (or `/api/admin/health`)
    so you never trade on stale data. A strategy with no snapshot yet returns
    an empty `holdings` list. 404 when the strategy doesn't exist.

    Each holding carries everything needed to place an order + the full set of
    per-position marks shown on the /schedule Current-portfolio table:
        company_id, ticker, exchange, country, currency, isin, company_name,
        sector, side, is_cash, score,
        target_weight, current_weight (drift-renormalized),
        entry_price_local, exit_price_local, entry_price_eur, exit_price_eur,
        entry_date, exit_date, entry_fx_rate_eur, exit_fx_rate_eur,
        return_eur_pct

    Response: `{strategy_id, name, enabled, frequency, next_rebalance_at,
    last_run_at, as_of_date, latest_price_date, holdings_count, holdings:[…]}`."""
    _require_admin(authorization)

    def _query() -> dict:
        resp = (
            supabase.table("scheduled_strategy")
            .select("*")
            .eq("id", strategy_id)
            .limit(1)
            .execute()
        )
        if not resp.data:
            raise HTTPException(404, f"Scheduled strategy #{strategy_id} not found")
        strat = resp.data[0]
        snap = _fetch_latest_snapshots_for([strategy_id]).get(strategy_id)
        payload = _build_portfolio_payload(snap) if snap else None
        return {
            "strategy_id": strat["id"],
            "name": strat.get("name") or f"Strategy #{strat['id']}",
            "enabled": strat.get("enabled", True),
            "frequency": strat.get("frequency"),
            "next_rebalance_at": strat.get("next_due_at"),
            "last_run_at": strat.get("last_run_at"),
            "as_of_date": payload.get("as_of_date") if payload else None,
            "latest_price_date": payload.get("latest_price_date") if payload else None,
            "holdings_count": len(payload.get("holdings")) if payload else 0,
            "holdings": payload.get("holdings") if payload else [],
        }

    return await asyncio.to_thread(_query)


def _load_strategy_row(strategy_id: int) -> dict:
    """Fetch one scheduled_strategy row or raise 404. Shared by the
    risk-metrics + performance endpoints."""
    resp = (
        supabase.table("scheduled_strategy")
        .select("*")
        .eq("id", strategy_id)
        .limit(1)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, f"Scheduled strategy #{strategy_id} not found")
    return resp.data[0]


def _strategy_snapshots(strategy_id: int) -> list[dict]:
    """The strategy's `current_picks_snapshot` rows in curve order — the live
    source `_extended_curve` / `_returns_from_backtest` walk to mark the
    frozen backtest curve to market through the latest priced day. Same query
    `/api/scheduled-strategies/{id}/runs` uses to build its live_curve."""
    return (
        supabase.table("current_picks_snapshot")
        .select("kind, as_of_date, latest_price_date, period_return_pct, created_at")
        .eq("scheduled_strategy_id", strategy_id)
        .order("latest_price_date", desc=False)
        .order("created_at", desc=False)
        .execute()
    ).data or []


def _daily_returns_since(pts: list[tuple[str, float]], inception_iso: str) -> list[dict]:
    """Per-day % returns from a cumulative-return curve `[(date, cum_pct)]`,
    starting at `inception_iso`. Day N's return is the close-to-close move from
    the prior curve point — identical to the /schedule monthly-returns heatmap
    drill-down (`(1+cum[i])/(1+cum[i-1]) - 1`). Period-boundary dates (prior
    exit == next entry) are de-duped keeping the last cumulative, matching the
    UI, so no spurious 0% boundary days appear."""
    if not pts:
        return []
    # De-dup by date (keep last cumulative), preserving ascending order.
    seen: dict[str, int] = {}
    deduped: list[tuple[str, float]] = []
    for d, cum in pts:
        if d in seen:
            deduped[seen[d]] = (d, cum)
        else:
            seen[d] = len(deduped)
            deduped.append((d, cum))
    # Baseline = last point on/before inception; the first reported day is the
    # next point after it (its return spans inception → that day).
    anchor_idx: int | None = None
    for i, (d, _cum) in enumerate(deduped):
        if d <= inception_iso:
            anchor_idx = i
        else:
            break
    start_i = (anchor_idx + 1) if anchor_idx is not None else 1
    out: list[dict] = []
    for i in range(start_i, len(deduped)):
        f0 = 1 + deduped[i - 1][1] / 100.0
        f1 = 1 + deduped[i][1] / 100.0
        if f0 <= 0:
            continue
        out.append({"date": deduped[i][0], "return_pct": round((f1 / f0 - 1) * 100.0, 4)})
    return out


@router.get("/api/admin/schedules/{strategy_id}/risk-metrics")
async def get_schedule_risk_metrics(strategy_id: int, authorization: str = Header(...)):
    """A scheduled strategy's BACKTESTED risk-adjusted metrics — Sharpe +
    Sortino — and the period they were computed over. Admin only.

    Both ratios come from the strategy's source `backtest_run` summary
    (annualized, risk-free = 0, computed off the closed-period daily curve so
    they're comparable across rebalance cadences — see
    `momentum/backtest/_summary.py`). The `period` is the actual span of that
    backtest's daily curve (first → last dated point), i.e. exactly the data
    the ratios were measured over (not the requested config range, which can
    extend past the data). `annualized_return_pct` + `max_drawdown_pct` round
    out the risk picture. 404 if the strategy doesn't exist; null metrics when
    it has no saved backtest. Response:
        {strategy_id, name, backtest_run_id, sharpe_ratio, sortino_ratio,
         annualized_return_pct, max_drawdown_pct,
         period: {start_date, end_date}}"""
    _require_admin(authorization)

    def _query() -> dict:
        from routers._schedule_hydration import _curve_stats, _load_backtest_pts  # noqa: PLC0415
        from routers.momentum.backtest_crud import load_backtest_result_sync  # noqa: PLC0415

        strat = _load_strategy_row(strategy_id)
        run_id = strat.get("backtest_run_id")
        cash_pct = float((strat.get("config") or {}).get("cash_pct") or 0.0)
        base = {
            "strategy_id": strat["id"],
            "name": strat.get("name") or f"Strategy #{strat['id']}",
            "backtest_run_id": run_id,
            "sharpe_ratio": None,
            "sortino_ratio": None,
            "annualized_return_pct": None,
            "max_drawdown_pct": None,
            "period": {"start_date": None, "end_date": None},
        }
        if not run_id:
            return base
        result = load_backtest_result_sync(int(run_id)) or {}
        summary = result.get("summary") or {}
        pts = _load_backtest_pts(int(run_id))
        if pts:
            period = {"start_date": pts[0][0], "end_date": pts[-1][0]}
        else:
            cfg = strat.get("config") or {}
            period = {"start_date": cfg.get("start_date"), "end_date": cfg.get("end_date")}
        base.update({
            # Sharpe/Sortino are cash-INVARIANT (mean & vol both scale by
            # (1-cash), so the ratio is unchanged) — use the stored values.
            "sharpe_ratio": summary.get("sharpe_ratio"),
            "sortino_ratio": summary.get("sortino_ratio"),
            "annualized_return_pct": summary.get("annualized_return_pct"),
            "max_drawdown_pct": summary.get("max_drawdown_pct"),
            "period": period,
        })
        # Cash drag DOES scale annualized return + max drawdown. Recompute
        # annualized off the cash-scaled curve; scale the stored max-drawdown by
        # the magnitude ratio (preserves its sign convention). No-op at cash=0.
        if cash_pct > 0 and pts and len(pts) >= 2:
            scaled = _load_backtest_pts(int(run_id), cash_pct)
            ann_scaled, mdd_scaled = _curve_stats(scaled)
            _, mdd_base = _curve_stats(pts)
            if ann_scaled is not None:
                base["annualized_return_pct"] = round(ann_scaled, 2)
            stored_mdd = summary.get("max_drawdown_pct")
            if stored_mdd is not None and mdd_base and mdd_scaled is not None:
                base["max_drawdown_pct"] = round(stored_mdd * (mdd_scaled / mdd_base), 2)
        return base

    return await asyncio.to_thread(_query)


@router.get("/api/admin/schedules/{strategy_id}/performance")
async def get_schedule_performance(strategy_id: int, authorization: str = Header(...)):
    """A scheduled strategy's LIVE performance since go-live. Admin only.

    Returns the strategy's inception (go-live) date, its return since
    inception, the month-to-date return, the latest date the data is current
    through, and the full per-day return series since inception.

    All figures track the live held portfolio: the frozen backtest curve is
    extended with the snapshot tail the price-update job marks to market
    through the latest priced day (`_extended_curve`), then read at the
    relevant anchors (`_returns_from_backtest`). `daily_returns` is the
    per-day close-to-close series off that same curve from inception onward —
    the same numbers behind the /schedule 'daily returns' table, but for every
    day rather than one month. Returns are GROSS (no fee model on the live
    path). Inception = the strategy's `start_date`, or `created_at` when unset.

    404 if the strategy doesn't exist; null returns + empty `daily_returns`
    when it has no saved backtest / no live data yet. Response:
        {strategy_id, name, inception_date, as_of_date,
         since_inception_return_pct, mtd_return_pct,
         daily_returns: [{date, return_pct}, ...]}"""
    _require_admin(authorization)

    def _query() -> dict:
        from routers._schedule_hydration import (  # noqa: PLC0415
            _extended_curve,
            _returns_from_backtest,
        )

        strat = _load_strategy_row(strategy_id)
        run_id = strat.get("backtest_run_id")
        # Inception = explicit go-live date, else the creation timestamp.
        inception_iso = (
            str(strat["start_date"])[:10]
            if strat.get("start_date")
            else str(strat.get("created_at") or "")[:10]
        )
        base = {
            "strategy_id": strat["id"],
            "name": strat.get("name") or f"Strategy #{strat['id']}",
            "inception_date": inception_iso or None,
            "as_of_date": None,
            "since_inception_return_pct": None,
            "mtd_return_pct": None,
            "daily_returns": [],
        }
        if not run_id:
            return base
        cash_pct = float((strat.get("config") or {}).get("cash_pct") or 0.0)
        snapshots = _strategy_snapshots(strategy_id)
        rets = _returns_from_backtest(
            int(run_id), inception_iso, _now_utc().date(), snapshots, cash_pct=cash_pct
        )
        if rets:
            base.update({
                "as_of_date": rets.get("as_of_date"),
                "since_inception_return_pct": rets.get("since_inception_pct"),
                "mtd_return_pct": rets.get("mtd_return_pct"),
            })
        base["daily_returns"] = _daily_returns_since(
            _extended_curve(int(run_id), snapshots, cash_pct), inception_iso
        )
        return base

    return await asyncio.to_thread(_query)


# ─── Universes ─────────────────────────────────────────────────────


@router.get("/api/admin/universes")
async def list_universes(
    include_all: bool = False,
    authorization: str = Header(...),
):
    """List the **frozen** universes — the discovery call for the membership
    endpoint below. Admin only.

    By default returns ONLY frozen static snapshots (`frozen_at` set) — the
    reproducible "X (as of YYYY-MM)" universes that are the canonical, usable
    sets across the app. Pass `?include_all=true` to also list the live
    template-managed canonicals (`template_key` set), the LongEquity
    time-series universe, criteria-derived universes (`parent_universe_id`),
    and imported index universes.

    Single-set model: each universe is a frozen set as of `as_of_date`.
    `is_monthly` is true only for the LongEquity time-series universe; the
    `start_month` / `end_month` / `month_count` fields are populated only when
    `is_monthly` (null otherwise). `member_count` comes from the
    `universe_stats` materialized view (a refreshed-on-pipeline hint; may
    lag). Pick a `universe_id` and pass it to `GET /api/admin/universes/{id}`.

    Response: `{count, universes:[{universe_id, label, description, kind,
    template_key, frozen_at, parent_universe_id, created_at,
    last_refreshed_at, as_of_date, is_monthly, member_count, start_month,
    end_month, month_count}]}`."""
    _require_admin(authorization)

    def _query() -> dict:
        q = (
            supabase.table("universe")
            .select(
                "universe_id, label, description, template_key, frozen_at, "
                "parent_universe_id, created_at, last_refreshed_at, "
                "as_of_date, is_monthly"
            )
        )
        if not include_all:
            q = q.not_.is_("frozen_at", "null")
        rows = q.order("label").execute().data or []

        # Aggregates from the materialized view (best-effort — it may be
        # unpopulated / stale; the membership endpoint computes the live count).
        stats: dict[int, dict] = {}
        try:
            srows = (
                supabase.table("universe_stats")
                .select("universe_id, start_month, end_month, month_count, total_unique_tickers")
                .execute()
            ).data or []
            stats = {int(s["universe_id"]): s for s in srows}
        except Exception:
            stats = {}

        out: list[dict] = []
        for r in rows:
            uid = int(r["universe_id"])
            s = stats.get(uid, {})
            if r.get("template_key"):
                kind = "template"
            elif r.get("frozen_at"):
                kind = "frozen"
            elif r.get("parent_universe_id"):
                kind = "derived"
            else:
                kind = "index"
            out.append({
                "universe_id": uid,
                "label": r.get("label"),
                "description": r.get("description"),
                "kind": kind,
                "template_key": r.get("template_key"),
                "frozen_at": r.get("frozen_at"),
                "parent_universe_id": r.get("parent_universe_id"),
                "created_at": r.get("created_at"),
                "last_refreshed_at": r.get("last_refreshed_at"),
                # Single-set model: as_of_date is the snapshot date; is_monthly
                # is true only for the LongEquity time-series universe (the
                # month-range fields below are only meaningful when true).
                "as_of_date": r.get("as_of_date"),
                "is_monthly": bool(r.get("is_monthly")),
                "member_count": s.get("total_unique_tickers"),
                "start_month": s.get("start_month") if r.get("is_monthly") else None,
                "end_month": s.get("end_month") if r.get("is_monthly") else None,
                "month_count": s.get("month_count") if r.get("is_monthly") else None,
            })
        return {"count": len(out), "universes": out}

    return await asyncio.to_thread(_query)


@router.get("/api/admin/universes/{universe_id}")
async def get_universe(
    universe_id: int,
    month: str | None = None,
    authorization: str = Header(...),
):
    """Full membership of one universe, each member enriched with the same
    per-company attributes the holdings endpoint returns. Admin only.

    Almost every universe is a single frozen set (one `target_month`), so by
    default you get that set and the `month` param does nothing. The ONE
    exception is the live, multi-month **LongEquity** time-series universe
    (`is_monthly=true`, reachable via `?include_all=true` on the list): there
    `?month=YYYY-MM` selects a historical snapshot, defaulting to its latest
    month. For any single-month universe `month` is IGNORED (you always get
    the frozen set, never an empty wrong-month result). 404 when the universe
    doesn't exist; empty `members` when it has no membership.

    Each member carries:
        company_id, ticker, exchange, country, currency, isin,
        company_name, sector, industry,
        latest_close_local, latest_close_eur, latest_close_date,
        fx_rate_per_eur

    Same descriptive fields as a scheduled strategy's holdings; the
    position-specific fields (side / target_weight / score / entry_date)
    don't apply to a universe member, and the holding's entry price becomes
    the latest close (native + EUR).

    Response: `{universe_id, label, template_key, frozen_at, is_monthly,
    target_month, member_count, members:[…]}`."""
    _require_admin(authorization)

    def _query() -> dict:
        urow = (
            supabase.table("universe")
            .select("universe_id, label, description, template_key, frozen_at, parent_universe_id, is_monthly")
            .eq("universe_id", universe_id)
            .limit(1)
            .execute()
        ).data
        if not urow:
            raise HTTPException(404, f"Universe #{universe_id} not found")
        u = urow[0]

        # `month` is only meaningful for the multi-month LongEquity universe;
        # for a single-month frozen set it's ignored so a stray ?month= can't
        # return empty members. Target month = the override (monthly only),
        # else the universe's latest.
        is_monthly = bool(u.get("is_monthly"))
        target_month = month if is_monthly else None
        if not target_month:
            latest = (
                supabase.table("universe_membership")
                .select("target_month")
                .eq("universe_id", universe_id)
                .order("target_month", desc=True)
                .limit(1)
                .execute()
            ).data
            target_month = latest[0]["target_month"] if latest else None

        # Pull the whole month's membership — paginated, since a broad
        # universe (e.g. Leonteq ~1.6k names) exceeds PostgREST's single-
        # response cap.
        members_raw: list[dict] = []
        if target_month:
            offset, page = 0, 1000
            while True:
                chunk = (
                    supabase.table("universe_membership")
                    .select("company_id, universe_ticker, sector, industry")
                    .eq("universe_id", universe_id)
                    .eq("target_month", target_month)
                    .order("company_id")
                    .range(offset, offset + page - 1)
                    .execute()
                ).data or []
                members_raw.extend(chunk)
                if len(chunk) < page:
                    break
                offset += page

        members = _enrich_universe_members(members_raw)
        return {
            "universe_id": u["universe_id"],
            "label": u.get("label"),
            "template_key": u.get("template_key"),
            "frozen_at": u.get("frozen_at"),
            "is_monthly": is_monthly,
            "target_month": target_month,
            "member_count": len(members),
            "members": members,
        }

    return await asyncio.to_thread(_query)


@router.get("/api/admin/etfs")
async def list_etfs(authorization: str = Header(...)):
    """Every ETF that carries an ISIN, enriched like a universe member. Admin only.

    ETFs live in the `benchmark` table (the same rows the diversifier + sector
    overlays reference — an ETF is a benchmark with a tradeable ISIN). This
    returns ONLY benchmarks with an `isin` set — the identifiable, tradeable
    instruments — each with its latest close (native + EUR via the same fx_rate
    source the /fx-rates page + universe members use). Index-only benchmarks
    (no ISIN) are excluded.

    Each ETF carries the universe-member-style shape (minus the fields that
    don't apply to a fund — exchange/country/industry):

        benchmark_id, ticker, name, isin, currency, sector,
        latest_close_local, latest_close_eur, latest_close_date, fx_rate_per_eur

    Response: `{count, etfs:[…]}`, sorted by ticker."""
    _require_admin(authorization)

    def _query() -> dict:
        rows = (
            supabase.table("benchmark")
            .select("benchmark_id, ticker, name, isin, currency, sector")
            .not_.is_("isin", "null")
            .order("ticker")
            .execute()
        ).data or []
        if not rows:
            return {"count": 0, "etfs": []}

        # Latest close per benchmark from benchmark_price. ETFs number in the
        # dozens, so a per-row limit-1 (newest date) is cheap.
        latest: dict[int, dict] = {}
        for r in rows:
            bid = int(r["benchmark_id"])
            pr = (
                supabase.table("benchmark_price")
                .select("target_date, price")
                .eq("benchmark_id", bid)
                .order("target_date", desc=True)
                .limit(1)
                .execute()
            ).data
            if pr:
                latest[bid] = {"date": pr[0].get("target_date"), "price": pr[0].get("price")}

        # Latest {ccy}/EUR rate per currency — same source as _enrich_universe_members.
        fx: dict[str, float] = {}
        try:
            from fx_rates import fetch_latest_from_db  # noqa: PLC0415
            for r in fetch_latest_from_db(supabase):
                code, rate = r.get("currency"), r.get("rate")
                if code and rate:
                    fx[code] = float(rate)
        except Exception:
            fx = {}

        out: list[dict] = []
        for r in rows:
            bid = int(r["benchmark_id"])
            cur = r.get("currency")
            lc = latest.get(bid, {})
            raw = lc.get("price")
            local = float(raw) if raw is not None else None
            rate = 1.0 if cur == "EUR" else (fx.get(cur) if cur else None)
            eur = (
                round(local / rate, 4)
                if rate and local is not None and rate > 0
                else None
            )
            out.append({
                "benchmark_id": bid,
                "ticker": r.get("ticker"),
                "name": r.get("name"),
                "isin": r.get("isin"),
                "currency": cur,
                "sector": r.get("sector"),
                "latest_close_local": local,
                "latest_close_eur": eur,
                "latest_close_date": lc.get("date"),
                "fx_rate_per_eur": rate,
            })
        return {"count": len(out), "etfs": out}

    return await asyncio.to_thread(_query)


# ─── Health ────────────────────────────────────────────────────────


@router.get("/api/admin/scheduled-jobs")
async def admin_scheduled_jobs(authorization: str = Header(...)):
    """EVERY JOB THAT IS SUPPOSED TO RUN BY ITSELF — declared, registered, and last actually run.

    ⚠⚠ IT ANSWERS "IS ANYTHING MISSING", WHICH NOTHING ELSE COULD. `/schedule` shows the ingest
    pipeline's own history and `scheduler.list_scheduled_jobs()` shows what APScheduler is holding
    right now — and BOTH look healthy in the one case that matters, a job that is not registered at
    all. `list_scheduled_jobs()` is empty under `DISABLE_SCHEDULER`, empty before startup finishes,
    and empty of any job whose `add_job` threw; none of those is distinguishable from an idle
    scheduler by looking at the list. The declaration in `scheduled_jobs.py` is what makes an
    absence visible, and this endpoint is the join.

    ⚠ THE READ IS PER-PROCESS AND SAYS SO. The scheduler is in-process by design (one instance,
    `DISABLE_SCHEDULER=1` on any replica), so `registered`/`next_run_at` describe *the container
    that served this request* — which is the honest scope, and the reason `scheduler_running` is
    reported rather than inferred from an empty list.

    ⚠ SIX OF THE EIGHT JOBS REPORT `unknown`, ON PURPOSE. They leave no durable record — only a log
    line that scrolls away — so "did it run?" genuinely has no answer for them yet. Green would be a
    fabrication and red would cry wolf; either teaches the reader to stop reading the page. They say
    so, and `record_run` is what will fill them in.
    """
    _require_admin(authorization)

    # ⚠⚠ THE ASSEMBLY MOVED TO `scheduler.job_health` SO THE WATCHDOG SHARES IT. It was a closure
    # here; the self-healing tick needs the same verdict, and a second copy of "is this job
    # overdue" is the one thing that must not exist — the page would say `ok` while the watchdog
    # re-fired, or the reverse, and the surface built to report what is wrong would be wrong about
    # itself.
    import os  # noqa: PLC0415

    from routers._scheduled_jobs_status import summarize  # noqa: PLC0415
    from scheduler import job_health  # noqa: PLC0415

    health = await asyncio.to_thread(job_health)
    rows, running, now = health["rows"], health["running"], health["now"]
    if health["history_error"]:
        # ⚠ THE PAGE STILL RENDERS. Losing the history costs the "did it run" column; it must not
        # cost the "is it registered" one, which needs no database at all.
        return {"jobs": rows, "summary": summarize(rows), "scheduler_running": running,
                "checked_at": now.isoformat(), "history_error": health["history_error"]}

    return {
        "jobs": rows,
        "summary": summarize(rows),
        "scheduler_running": running,
        "disable_scheduler": os.environ.get("DISABLE_SCHEDULER", ""),
        "checked_at": now.isoformat(),
    }


@router.post("/api/admin/scheduled-jobs/{job_id}/run")
async def admin_run_scheduled_job(job_id: str, authorization: str = Header(...)):
    """Kick one declared job off NOW, as a cancellable registry job with a progress toast.

    ⚠ THE SAME BODY THE SCHEDULER TICK RUNS (`scheduler.JOB_BODIES`), never a second copy — a
    button that ran its own implementation would drift from the thing the schedule does, and the
    drift would only ever surface as the button disagreeing with the nightly result.

    ⚠⚠ CANCELLATION IS COOPERATIVE, AND ITS LATENCY DIFFERS PER JOB. The AIRS scan stops between
    ACCOUNTS (an account's four reports are stored as a unit); the drift probe stops between
    COMPANIES; the FX, CRM and size jobs are seconds long and have no useful boundary at all.
    "Stops immediately" is not on offer for a scraper mid-download, and a Cancel that claimed it
    would be the decorative control this codebase has already removed once. The UI says which is
    which rather than implying they are the same.

    ⚠ A JOB WITH NO BODY IS 404, WHICH IS AN ANSWER. The 20-second queue worker has nothing worth
    triggering, and the two pipeline jobs already own a richer Run-now with a live console tail —
    `runnable` on `/api/admin/scheduled-jobs` says so per row, so the button is simply absent
    rather than present-and-failing.
    """
    _require_admin(authorization)

    from scheduler import start_job_now  # noqa: PLC0415

    try:
        job = await asyncio.to_thread(start_job_now, job_id)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"{job_id} cannot be run by hand — it has no body in scheduler.JOB_BODIES",
        ) from None
    return {"job_id": job.id, "label": job.label, "kind": job.kind}


@router.get("/api/admin/db-growth")
async def admin_db_growth(days: int = 7, authorization: str = Header(...)):
    """HOW FAST THE DATABASE IS GROWING, PER TABLE — bytes on disk, over a window.

    ⚠⚠ BYTES, NOT ROWS WRITTEN, AND THE DIFFERENCE INVERTS THE RANKING. Asking each job to count
    its own inserts would put `crm_relaties_refresh` — which OVERWRITES its table, thousands of rows
    written and zero growth — above the month-end price refresh. Several jobs here are
    delete-then-insert snapshots or upserts. A row count is also blind to INDEXES and BLOAT, which
    on an 18 GB table are most of the disk.

    ⚠ IT ANSWERS "WHAT GREW", NEVER "WHO GREW IT". The measurement is taken from outside every job,
    which is what makes it impossible for a job to forget to report or to drift — and is exactly
    why it cannot attribute. Per-job attribution is a separate, lossier measurement.

    ⚠ `delta` IS NULL, NOT 0, UNTIL THE HISTORY REACHES BACK `days`. A fresh install has sizes and
    no growth; rendering that as "0 MB added" would present an unmeasured database as a static one.
    `has_baseline` says which of the two you are looking at.

    ⚠ SUPABASE STORAGE IS NOT COUNTED — the `gurufocus-raw` bucket of cached vendor JSON is not in
    Postgres. Reconciling this against the hosting's disk figure will show a gap; that is the gap.
    """
    _require_admin(authorization)

    from db_growth import growth  # noqa: PLC0415

    # ⚠ CLAMPED. `days` reaches a SQL `make_interval`, and a silly value is a silly window rather
    # than an error — but an unbounded one invites a negative, which would make `earlier` newer
    # than `latest` and report shrinkage.
    window = max(1, min(int(days), 365))
    return await asyncio.to_thread(growth, window)


@router.get("/api/admin/health")
async def get_health(authorization: str = Header(...)):
    """Composite go/no-go. Returns a single boolean `is_healthy` plus
    the list of checks that failed. Threshold defaults are
    intentionally permissive — we're guarding against "something is
    obviously broken" cases, not micro-staleness.

    Checks:
      - DB reachable
      - close_price max date is within the last 6 trading days
      - most recent ingest_run is within the last 8 days
      - that run isn't 'running' for more than 2 hours (a stuck job)
      - that run's status is 'ok' (allows a single transient failure
        downstream — see `is_healthy_strict` for the stricter variant)
    """
    _require_admin(authorization)

    def _query() -> dict:
        problems: list[str] = []
        # 1. DB reachable
        try:
            ping = supabase.table("ingest_run").select("run_id").limit(1).execute()
            _ = ping.data  # noqa: F841 — just want the call to round-trip
        except Exception as e:
            return {
                "is_healthy": False,
                "is_healthy_strict": False,
                "checks": {"db_reachable": False},
                "problems": [f"DB unreachable: {type(e).__name__}: {e}"],
            }

        # 2. close_price freshness
        latest_close = _max_target_date("close_price")
        close_age = _trading_day_age(latest_close)
        close_fresh = close_age is not None and close_age <= 6
        if not close_fresh:
            problems.append(
                f"close_price stale ({close_age} trading days behind; latest={latest_close})"
            )

        # 3. Pipeline-run freshness
        last_run_resp = (
            supabase.table("ingest_run")
            .select("run_id, status, started_at, finished_at")
            .order("started_at", desc=True)
            .limit(1)
            .execute()
        )
        last_run = last_run_resp.data[0] if last_run_resp.data else None
        run_fresh = False
        run_succeeded = False
        run_not_stuck = True
        if last_run is None:
            problems.append("No pipeline runs have happened yet")
        else:
            try:
                started = datetime.fromisoformat(last_run["started_at"].replace("Z", "+00:00"))
                run_age_days = (_now_utc() - started).total_seconds() / 86400
                run_fresh = run_age_days <= 8
                if not run_fresh:
                    problems.append(
                        f"Last pipeline run is {run_age_days:.1f} days old "
                        f"(run_id={last_run['run_id']})"
                    )
                if last_run["status"] == "running":
                    age_hours = (_now_utc() - started).total_seconds() / 3600
                    if age_hours > 2:
                        run_not_stuck = False
                        problems.append(
                            f"Pipeline run #{last_run['run_id']} has been 'running' "
                            f"for {age_hours:.1f}h — likely stuck"
                        )
                run_succeeded = last_run["status"] == "ok"
                if not run_succeeded and run_not_stuck:
                    problems.append(
                        f"Last pipeline run ended with status='{last_run['status']}' "
                        f"(run_id={last_run['run_id']})"
                    )
            except Exception as e:
                problems.append(f"Failed to interpret last run timestamps: {e}")

        # 4. Latest snapshot exists (don't gate on health — just inform)
        snap_resp = (
            supabase.table("current_picks_snapshot")
            .select("snapshot_id, created_at")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        has_snapshot = bool(snap_resp.data)
        if not has_snapshot:
            problems.append(
                "No current_picks_snapshot exists yet — pick a scheduled strategy on /schedule"
            )

        checks = {
            "db_reachable": True,
            "close_price_fresh": close_fresh,
            "pipeline_run_fresh": run_fresh,
            "pipeline_run_not_stuck": run_not_stuck,
            "has_snapshot": has_snapshot,
        }
        # Loose check: tolerate a single failed run (data may still be
        # usable). Strict check: every signal must be green.
        is_healthy = checks["db_reachable"] and close_fresh and run_fresh and run_not_stuck
        is_healthy_strict = is_healthy and run_succeeded and has_snapshot
        return {
            "is_healthy": is_healthy,
            "is_healthy_strict": is_healthy_strict,
            "checks": checks,
            "problems": problems,
        }

    return await asyncio.to_thread(_query)


# ─── Data integrity: companies missing exchange ───────────────────


@router.get("/api/admin/companies/missing-exchange")
async def list_companies_missing_exchange(authorization: str = Header(None)):
    """Companies whose `exchange_id` is NULL — they show up with an empty
    exchange column in /backtest + /schedule + /companies, and the
    frontend's GuruFocus link falls back to a bare-ticker URL that
    silently lands on the wrong security (or 404s) for non-US names.
    Returns `{count, companies: [{company_id, name, ticker, country}]}`.
    The country comes from any universe-membership rows on the row;
    useful as a hint for the bulk-resolve endpoint's OpenFIGI call."""
    _require_admin(authorization)

    def _query() -> dict:
        # company table has no native country; look it up via the
        # company's most-recent universe_membership row when the
        # exchange-derived country (via gurufocus_exchange.country)
        # is NULL by definition (no exchange_id).
        resp = (
            supabase.table("company")
            .select("company_id, company_name, gurufocus_ticker")
            .is_("exchange_id", "null")
            .order("company_name")
            .limit(5000)
            .execute()
        )
        rows = resp.data or []
        cids = [r["company_id"] for r in rows]
        # Pull the universe_ticker / sector from membership for context.
        # No country in universe_membership today, but the universe label
        # often hints at it (e.g. ACWI-Italy memberships → Italian).
        mem_by_cid: dict[int, list[dict]] = {}
        for m in fetch_in_chunks(
            cids,
            lambda chunk: supabase.table("universe_membership")
            .select("company_id, universe_ticker, sector, target_month, universe_id")
            .in_("company_id", chunk)
            .order("target_month", desc=True)
            .execute(),
        ):
            mem_by_cid.setdefault(m["company_id"], []).append(m)
        out = []
        for r in rows:
            cid = r["company_id"]
            mems = mem_by_cid.get(cid, [])
            latest = mems[0] if mems else None
            out.append({
                "company_id": cid,
                "company_name": r.get("company_name"),
                "gurufocus_ticker": r.get("gurufocus_ticker"),
                "latest_universe_ticker": latest.get("universe_ticker") if latest else None,
                "latest_sector": latest.get("sector") if latest else None,
                "universe_count": len(mems),
            })
        return {"count": len(out), "companies": out}

    return await asyncio.to_thread(_query)


@router.post("/api/admin/companies/resolve-missing-exchanges")
async def resolve_missing_exchanges(
    authorization: str = Header(None),
    dry_run: bool = True,
):
    """For every `company.exchange_id IS NULL` row, run an OpenFIGI
    lookup by `gurufocus_ticker` and update `exchange_id` to the
    resolved exchange's id. Returns a per-company outcome so the
    caller can audit:

      `{count_total, count_resolved, count_unresolved, count_unmapped,
        resolved: [...], unresolved: [...], unmapped: [...]}`

    - `resolved`: OpenFIGI returned an exchange + we found it in
      `gurufocus_exchange`. With `dry_run=true` (default), the row is
      NOT updated — the response just shows what WOULD change. Pass
      `dry_run=false` to commit.
    - `unresolved`: OpenFIGI returned no match for the ticker (silent
      skip; the row stays NULL and the user has to fix manually via
      /companies).
    - `unmapped`: OpenFIGI returned a match, but the exchange code
      isn't in our `gurufocus_exchange` table — add the row first,
      then re-run.

    Ambiguous tickers (e.g. one ticker listed on multiple exchanges)
    use OpenFIGI's first match. Cross-check `resolved[].openfigi_exchange`
    against your expectation before committing."""
    _require_admin(authorization)

    def _resolve() -> dict:
        # Fetch NULL-exchange rows.
        rows_resp = (
            supabase.table("company")
            .select("company_id, gurufocus_ticker, company_name")
            .is_("exchange_id", "null")
            .limit(5000)
            .execute()
        )
        rows = rows_resp.data or []
        if not rows:
            return {
                "count_total": 0,
                "count_resolved": 0,
                "count_unresolved": 0,
                "count_unmapped": 0,
                "resolved": [],
                "unresolved": [],
                "unmapped": [],
                "dry_run": dry_run,
            }
        # Load exchange_code → exchange_id map.
        exch_resp = (
            supabase.table("gurufocus_exchange")
            .select("exchange_id, exchange_code")
            .execute()
        )
        code_to_id = {
            (r.get("exchange_code") or "").upper(): r["exchange_id"]
            for r in (exch_resp.data or [])
            if r.get("exchange_code")
        }
        # Build OpenFIGI input. We have no country signal on a NULL-
        # exchange row, so OpenFIGI runs without exchCode hint (its
        # exchange-disambiguation lookup uses the global ticker space
        # and returns its best guess).
        from ingest.resolve_tickers import resolve_via_openfigi  # noqa: PLC0415
        unknowns = [
            {"ticker": (r.get("gurufocus_ticker") or "").strip(), "country": "", "exchange": ""}
            for r in rows
            if (r.get("gurufocus_ticker") or "").strip()
        ]
        try:
            openfigi_results = resolve_via_openfigi(unknowns)
        except Exception as e:
            raise HTTPException(
                502,
                f"OpenFIGI lookup failed: {type(e).__name__}: {e}",
            )
        # Index OpenFIGI's results by ticker for the row-level loop.
        resolved_by_ticker = {r["ticker"].upper(): r for r in openfigi_results}

        resolved: list[dict] = []
        unresolved: list[dict] = []
        unmapped: list[dict] = []
        for r in rows:
            cid = r["company_id"]
            ticker = (r.get("gurufocus_ticker") or "").strip().upper()
            name = r.get("company_name") or ""
            if not ticker:
                unresolved.append({
                    "company_id": cid,
                    "company_name": name,
                    "gurufocus_ticker": None,
                    "reason": "Empty ticker; manual fix required.",
                })
                continue
            hit = resolved_by_ticker.get(ticker)
            if not hit:
                unresolved.append({
                    "company_id": cid,
                    "company_name": name,
                    "gurufocus_ticker": ticker,
                    "reason": "OpenFIGI returned no match.",
                })
                continue
            new_exchange_code = (hit.get("gurufocus_exchange") or "").upper()
            new_exchange_id = code_to_id.get(new_exchange_code)
            if new_exchange_id is None:
                unmapped.append({
                    "company_id": cid,
                    "company_name": name,
                    "gurufocus_ticker": ticker,
                    "openfigi_exchange": new_exchange_code,
                    "reason": (
                        f"OpenFIGI returned exchange {new_exchange_code!r} "
                        f"but it's not in our gurufocus_exchange table. "
                        f"Add the row first, then re-run."
                    ),
                })
                continue
            # We have a resolution. Commit (or stage) the update.
            if not dry_run:
                try:
                    supabase.table("company").update({
                        "exchange_id": new_exchange_id,
                    }).eq("company_id", cid).execute()
                except Exception as e:
                    unresolved.append({
                        "company_id": cid,
                        "company_name": name,
                        "gurufocus_ticker": ticker,
                        "openfigi_exchange": new_exchange_code,
                        "reason": f"DB update failed: {type(e).__name__}: {e}",
                    })
                    continue
            resolved.append({
                "company_id": cid,
                "company_name": name,
                "gurufocus_ticker": ticker,
                "openfigi_exchange": new_exchange_code,
                "exchange_id": new_exchange_id,
                "openfigi_ticker": hit.get("gurufocus_ticker"),
            })

        return {
            "count_total": len(rows),
            "count_resolved": len(resolved),
            "count_unresolved": len(unresolved),
            "count_unmapped": len(unmapped),
            "resolved": resolved,
            "unresolved": unresolved,
            "unmapped": unmapped,
            "dry_run": dry_run,
        }

    return await asyncio.to_thread(_resolve)


@router.get("/api/admin/companies/flagged")
async def list_flagged_companies(
    window_days: int = 10,
    authorization: str = Header(None),
):
    """Ad-hoc audit for manual review: companies that look suspicious
    based on two heuristics. Pure triage — nothing is auto-modified.

      * `adr_in_name`: company_name contains 'ADR' (case-insensitive).
        Often surfaces wrong-variant mappings — an ADR depositary
        listing got linked instead of the primary local security,
        which then poisons everything downstream (sector, returns).
      * `flat_prices`: latest `window_days` close_price observations
        are all the exact same value. Strong signal for a stale /
        dead listing OR a wrong (primary→ADR or similar) mapping
        whose ticker continues to ship a stub. Companies already
        stamped `delisted_at` are excluded — flat prices on those
        are expected.

    Backed by the `company_flat_price_run` RPC (single SQL query,
    way faster than paginating metric_data from Python) plus a
    direct `ILIKE` on company.company_name for the ADR check.
    """
    _require_admin(authorization)

    def _query() -> dict:
        # ADR-in-name — direct query.
        adr_resp = (
            supabase.table("company")
            .select(
                "company_id, company_name, gurufocus_ticker, "
                "delisted_at, out_of_scope_at, out_of_scope_reason, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            )
            .ilike("company_name", "%ADR%")
            .order("company_name")
            .limit(5000)
            .execute()
        )
        adr_rows = []
        for r in (adr_resp.data or []):
            adr_rows.append({
                "company_id": r["company_id"],
                "company_name": r.get("company_name"),
                "gurufocus_ticker": r.get("gurufocus_ticker"),
                "gurufocus_exchange": (r.get("gurufocus_exchange") or {}).get("exchange_code"),
                "delisted_at": r.get("delisted_at"),
                "out_of_scope_at": r.get("out_of_scope_at"),
                "out_of_scope_reason": r.get("out_of_scope_reason"),
            })

        # Flat-prices — via RPC.
        try:
            flat_resp = supabase.rpc(
                "company_flat_price_run",
                {"window_days": window_days},
            ).execute()
            flat_raw = flat_resp.data or []
        except APIError as e:
            # Most common cause: migration not applied yet. Return an
            # empty list rather than 500ing the whole endpoint so the
            # ADR-name half still works.
            flat_raw = []
            adr_rows.insert(0, {
                "_warning": (
                    f"company_flat_price_run RPC unavailable "
                    f"({e.message if hasattr(e, 'message') else e}). "
                    f"Apply migration 20260530000000_company_flag_rpcs.sql."
                ),
            })

        # Hydrate flat-prices rows with name/ticker/exchange + drop
        # already-known-delisted companies (flat prices there are
        # expected, not suspicious).
        flat_info_by_cid = {int(r["company_id"]): r for r in flat_raw}
        flat_cids = list(flat_info_by_cid.keys())
        flat_rows = []
        for r in fetch_in_chunks(
            flat_cids,
            lambda chunk: supabase.table("company")
            .select(
                "company_id, company_name, gurufocus_ticker, "
                "delisted_at, out_of_scope_at, out_of_scope_reason, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            )
            .in_("company_id", chunk)
            .execute(),
        ):
            cid = int(r["company_id"])
            if r.get("delisted_at") is not None:
                continue  # flat prices expected on delisted listings
            info = flat_info_by_cid.get(cid, {})
            flat_rows.append({
                "company_id": cid,
                "company_name": r.get("company_name"),
                "gurufocus_ticker": r.get("gurufocus_ticker"),
                "gurufocus_exchange": (r.get("gurufocus_exchange") or {}).get("exchange_code"),
                "delisted_at": r.get("delisted_at"),
                "out_of_scope_at": r.get("out_of_scope_at"),
                "out_of_scope_reason": r.get("out_of_scope_reason"),
                "flat_value": info.get("flat_value"),
                "window_start": info.get("window_start"),
                "window_end": info.get("window_end"),
                "row_count": info.get("row_count"),
            })
        flat_rows.sort(key=lambda x: (x.get("company_name") or "").lower())

        return {
            "window_days": window_days,
            "adr_in_name": {"count": len(adr_rows), "companies": adr_rows},
            "flat_prices": {"count": len(flat_rows), "companies": flat_rows},
        }

    return await asyncio.to_thread(_query)
