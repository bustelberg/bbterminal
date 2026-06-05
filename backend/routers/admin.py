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
         "https://<backend>/api/admin/portfolio/latest"

Endpoints:
    GET /api/admin/portfolio/latest      — target portfolio with IBKR-relevant fields
    GET /api/admin/portfolio/{id}        — same shape, specific snapshot_id
    GET /api/admin/schedules             — every scheduled strategy + full latest portfolio
                                            (one-shot for external buyer scripts)
    GET /api/admin/schedules/{id}        — one scheduled strategy + its latest portfolio
    GET /api/admin/runs/latest           — most recent pipeline run
    GET /api/admin/pipeline-runs         — recent runs list (monitoring)
    GET /api/admin/health                — composite freshness check
    GET /api/admin/data-freshness        — per-source freshness breakdown
    GET /api/admin/sanity-check          — pass/fail bundle of common checks
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
    _fetch_latest_snapshots_for,
    _summarize_run,
    _summarize_schedule,
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
        "TSE", "HKEX", "KSE", "BSE", "NSE", "SGX", "TPE",
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


@router.get("/api/admin/portfolio/latest")
async def get_latest_portfolio(authorization: str = Header(...)):
    """Return the most recent current-picks snapshot in IBKR-friendly
    shape. 404 when no snapshot has been produced yet."""
    _require_admin(authorization)

    def _query() -> dict:
        try:
            resp = (
                supabase.table("current_picks_snapshot")
                .select("*")
                .order("created_at", desc=True)
                .limit(1)
                .execute()
            )
        except APIError as e:
            raise HTTPException(500, f"DB read failed: {e}")
        if not resp.data:
            raise HTTPException(404, "No current_picks_snapshot rows exist yet")
        return _build_portfolio_payload(resp.data[0])

    return await asyncio.to_thread(_query)


@router.get("/api/admin/portfolio/{snapshot_id}")
async def get_portfolio_by_id(snapshot_id: int, authorization: str = Header(...)):
    """Return a specific snapshot by id, same shape as /latest."""
    _require_admin(authorization)

    def _query() -> dict:
        resp = (
            supabase.table("current_picks_snapshot")
            .select("*")
            .eq("snapshot_id", snapshot_id)
            .limit(1)
            .execute()
        )
        if not resp.data:
            raise HTTPException(404, f"Snapshot #{snapshot_id} not found")
        return _build_portfolio_payload(resp.data[0])

    return await asyncio.to_thread(_query)


# ─── Schedules ─────────────────────────────────────────────────────


@router.get("/api/admin/schedules")
async def list_schedules(
    enabled_only: bool = True,
    authorization: str = Header(...),
):
    """Every scheduled strategy on the system with its latest portfolio
    attached. Returns a list (newest-created last) so an external buyer
    script can iterate strategies, see when each is due to rebalance
    (`next_due_at`), and pull the holdings they should currently be
    holding (`latest_portfolio.holdings`).

    Query: `enabled_only=true` (default) hides paused strategies; pass
    `false` to see everything."""
    _require_admin(authorization)

    def _query() -> list[dict]:
        q = supabase.table("scheduled_strategy").select("*").order("created_at")
        if enabled_only:
            q = q.eq("enabled", True)
        try:
            resp = q.execute()
        except APIError as e:
            raise HTTPException(500, f"DB read failed: {e}")
        rows = resp.data or []
        latest = _fetch_latest_snapshots_for([r["id"] for r in rows])
        return [_summarize_schedule(r, latest.get(r["id"])) for r in rows]

    return await asyncio.to_thread(_query)


@router.get("/api/admin/schedules/{strategy_id}")
async def get_schedule(strategy_id: int, authorization: str = Header(...)):
    """One scheduled strategy + its full latest portfolio. Same shape as
    one entry of `/api/admin/schedules`. 404 when the strategy doesn't
    exist."""
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
        latest = _fetch_latest_snapshots_for([strategy_id]).get(strategy_id)
        return _summarize_schedule(strat, latest)

    return await asyncio.to_thread(_query)


# ─── Pipeline runs ─────────────────────────────────────────────────


@router.get("/api/admin/runs/latest")
async def get_latest_run(authorization: str = Header(...)):
    """Return a compact summary of the most recent pipeline run plus
    the most recent SUCCESSFUL run (when different). Use this as the
    one-line "is anything working?" probe — if `latest.status` is `ok`
    and `latest.finished_at` is within the last week, the system is
    healthy."""
    _require_admin(authorization)

    def _query() -> dict:
        latest_resp = (
            supabase.table("ingest_run")
            .select("*")
            .order("started_at", desc=True)
            .limit(1)
            .execute()
        )
        latest = latest_resp.data[0] if latest_resp.data else None
        last_ok_resp = (
            supabase.table("ingest_run")
            .select("*")
            .eq("status", "ok")
            .order("started_at", desc=True)
            .limit(1)
            .execute()
        )
        last_ok = last_ok_resp.data[0] if last_ok_resp.data else None
        return {
            "latest": _summarize_run(latest) if latest else None,
            "latest_successful":
                _summarize_run(last_ok)
                if last_ok and (not latest or last_ok["run_id"] != latest["run_id"])
                else None,
        }

    return await asyncio.to_thread(_query)


@router.get("/api/admin/pipeline-runs")
async def list_pipeline_runs(
    limit: int = 20,
    authorization: str = Header(...),
):
    """Recent pipeline runs (newest first), compact summary per row.
    Caps `limit` at 100."""
    _require_admin(authorization)
    limit = max(1, min(100, limit))

    def _query() -> list[dict]:
        resp = (
            supabase.table("ingest_run")
            .select("*")
            .order("started_at", desc=True)
            .limit(limit)
            .execute()
        )
        return [_summarize_run(r) for r in (resp.data or [])]

    return await asyncio.to_thread(_query)


# ─── Health + freshness ────────────────────────────────────────────


@router.get("/api/admin/data-freshness")
async def get_data_freshness(authorization: str = Header(...)):
    """How current each data source is. The IBKR script can use the
    `close_price_age_trading_days` field as a gate: bail out before
    placing orders if it's > some threshold."""
    _require_admin(authorization)

    def _query() -> dict:
        latest_close = _max_target_date("close_price")
        latest_volume = _max_target_date("volume")
        latest_snap = (
            supabase.table("current_picks_snapshot")
            .select("snapshot_id, as_of_date, latest_price_date, created_at")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        latest_run = (
            supabase.table("ingest_run")
            .select("run_id, started_at, finished_at, status")
            .order("started_at", desc=True)
            .limit(1)
            .execute()
        )
        return {
            "now": _now_utc().isoformat(),
            "close_price": {
                "latest_target_date": latest_close.isoformat() if latest_close else None,
                "age_trading_days": _trading_day_age(latest_close),
            },
            "volume": {
                "latest_target_date": latest_volume.isoformat() if latest_volume else None,
                "age_trading_days": _trading_day_age(latest_volume),
            },
            "latest_snapshot":
                latest_snap.data[0] if latest_snap.data else None,
            "latest_pipeline_run":
                latest_run.data[0] if latest_run.data else None,
        }

    return await asyncio.to_thread(_query)


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


@router.get("/api/admin/sanity-check")
async def sanity_check(authorization: str = Header(...)):
    """Sanity diagnostics for verifying everything still hangs together.
    Independent from `health` — these are coarse counts and shapes the
    user (or a CI smoke test) can eyeball to confirm the system isn't
    quietly broken."""
    _require_admin(authorization)

    def _query() -> dict:
        out: dict = {"now": _now_utc().isoformat()}
        # Counts per major table — quick "is anything totally missing?"
        for table in [
            "company",
            "gurufocus_exchange",
            "exchange_fee",
            "backtest_run",
            "ingest_run",
            "current_picks_snapshot",
            "scheduled_strategy",
        ]:
            try:
                r = supabase.table(table).select("*", count="exact").limit(0).execute()
                out[f"{table}_count"] = getattr(r, "count", None)
            except Exception as e:
                out[f"{table}_count"] = f"ERR: {type(e).__name__}: {e}"

        # Schedule strategies — how many enabled / total?
        sc = (
            supabase.table("scheduled_strategy")
            .select("id, enabled, backtest_run_id")
            .execute()
        )
        rows = sc.data or []
        out["scheduled_strategies"] = {
            "total": len(rows),
            "enabled": sum(1 for r in rows if r.get("enabled")),
            "backtest_run_ids": [r["backtest_run_id"] for r in rows if r.get("enabled")],
        }

        # Template-managed universes presence: every registered
        # template's canonical row (or None if not yet refreshed).
        tmpl = (
            supabase.table("universe")
            .select("universe_id, template_key, label")
            .not_.is_("template_key", "null")
            .execute()
        )
        out["template_universes"] = {
            r["template_key"]: r["universe_id"] for r in (tmpl.data or [])
        }

        # Recent run status distribution (last 20)
        runs = (
            supabase.table("ingest_run")
            .select("status")
            .order("started_at", desc=True)
            .limit(20)
            .execute()
        )
        status_counts: dict[str, int] = {}
        for r in (runs.data or []):
            s = r.get("status") or "unknown"
            status_counts[s] = status_counts.get(s, 0) + 1
        out["recent_runs_status"] = status_counts

        # Latest snapshot summary
        snap = (
            supabase.table("current_picks_snapshot")
            .select("snapshot_id, as_of_date, latest_price_date, holdings, created_at")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if snap.data:
            row = snap.data[0]
            out["latest_snapshot"] = {
                "snapshot_id": row.get("snapshot_id"),
                "as_of_date": row.get("as_of_date"),
                "latest_price_date": row.get("latest_price_date"),
                "created_at": row.get("created_at"),
                "holdings_count": len(row.get("holdings") or []),
            }
        else:
            out["latest_snapshot"] = None
        return out

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
