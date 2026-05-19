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
    GET /api/admin/portfolio/latest    — target portfolio with IBKR-relevant fields
    GET /api/admin/portfolio/{id}      — same shape, specific snapshot_id
    GET /api/admin/runs/latest         — most recent pipeline run
    GET /api/admin/pipeline-runs       — recent runs list (monitoring)
    GET /api/admin/health              — composite freshness check
    GET /api/admin/data-freshness      — per-source freshness breakdown
    GET /api/admin/sanity-check        — pass/fail bundle of common checks
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, Header, HTTPException
from postgrest.exceptions import APIError

from deps import supabase
from routers.auth import _require_admin

router = APIRouter(tags=["admin"])


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ─── Portfolio ─────────────────────────────────────────────────────


def _build_portfolio_payload(snapshot_row: dict) -> dict:
    """Convert a `current_picks_snapshot` row into the IBKR-friendly
    response shape — every field a rebalancing script would need:

        ticker            symbol on the home exchange (GuruFocus form)
        exchange          GuruFocus exchange code (NYSE, NASDAQ, OHEL, …)
        currency          ISO 4217 currency code
        side              "long" or "short"
        target_weight     fractional weight in the portfolio (sum ≈ 1.0)
        company_id        DB id, useful for cross-referencing
        company_name      display name
        sector            GICS sector (for verification)
        entry_price_local most recent close in the listing currency
        entry_price_eur   …same converted to EUR
        score             the momentum score at selection time

    The IBKR symbol/exchange mapping isn't done here — callers know
    their own broker conventions and we don't want to lock in any
    particular translation. We just hand back the canonical GuruFocus
    fields and let the script adapt.
    """
    raw_holdings = snapshot_row.get("holdings") or []
    cfg = snapshot_row.get("config") or {}

    # Resolve company → exchange. The snapshot's holdings don't carry
    # exchange directly (only currency); we look it up via the company
    # table joined to gurufocus_exchange.
    cids = [int(h["company_id"]) for h in raw_holdings if h.get("company_id") is not None]
    exchange_by_cid: dict[int, str] = {}
    if cids:
        for chunk_start in range(0, len(cids), 50):
            chunk = cids[chunk_start : chunk_start + 50]
            resp = (
                supabase.table("company")
                .select(
                    "company_id, "
                    "gurufocus_exchange:gurufocus_exchange(exchange_code)"
                )
                .in_("company_id", chunk)
                .execute()
            )
            for row in (resp.data or []):
                exch = (row.get("gurufocus_exchange") or {}).get("exchange_code") or ""
                exchange_by_cid[int(row["company_id"])] = exch

    total_weight = 0.0
    holdings_out: list[dict] = []
    for h in raw_holdings:
        cid = int(h.get("company_id")) if h.get("company_id") is not None else None
        weight = float(h.get("weight") or 0.0)
        total_weight += weight
        holdings_out.append({
            "company_id": cid,
            "ticker": h.get("ticker"),
            "exchange": exchange_by_cid.get(cid, "") if cid is not None else "",
            "currency": h.get("currency"),
            "side": h.get("side") or "long",
            "target_weight": round(weight, 6),
            "company_name": h.get("company_name"),
            "sector": h.get("sector"),
            "entry_price_local": h.get("entry_price_local"),
            "entry_price_eur": h.get("entry_price_eur"),
            "entry_date": h.get("entry_date"),
            "score": h.get("score"),
        })

    return {
        "snapshot_id": snapshot_row.get("snapshot_id"),
        "as_of_date": snapshot_row.get("as_of_date"),
        "latest_price_date": snapshot_row.get("latest_price_date"),
        "triggered_by": snapshot_row.get("triggered_by"),
        "created_at": snapshot_row.get("created_at"),
        "strategy": {
            "name": snapshot_row.get("name"),
            "selection_mode": cfg.get("selection_mode"),
            "strategy_type": cfg.get("strategy_type", "long_only"),
            "index_universe": cfg.get("index_universe"),
            "top_n_sectors": cfg.get("top_n_sectors"),
            "top_n_per_sector": cfg.get("top_n_per_sector"),
            "rebalance_frequency": cfg.get("rebalance_frequency"),
        },
        "holdings": holdings_out,
        "holdings_count": len(holdings_out),
        "total_weight": round(total_weight, 6),
    }


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


# ─── Pipeline runs ─────────────────────────────────────────────────


def _summarize_run(row: dict) -> dict:
    """Compact summary of an ingest_run row, dropping the verbose
    additions/removals lists from acwi_summary (counts only). Useful
    for monitoring endpoints where the caller wants a quick health
    snapshot, not the full diff."""
    acwi = row.get("acwi_summary") or {}
    # `momentum_summary` is a list — one entry per scheduled strategy
    # that ran. Older pre-rebuild rows held a single dict; coerce them
    # to a list so consumers can iterate uniformly.
    mom_raw = row.get("momentum_summary")
    if isinstance(mom_raw, list):
        mom_list = mom_raw
    elif isinstance(mom_raw, dict):
        mom_list = [mom_raw]
    else:
        mom_list = []
    return {
        "run_id": row.get("run_id"),
        "job_name": row.get("job_name"),
        "triggered_by": row.get("triggered_by"),
        "started_at": row.get("started_at"),
        "finished_at": row.get("finished_at"),
        "status": row.get("status"),
        "current_phase": row.get("current_phase"),
        "acwi": {
            "target_month": row.get("acwi_target_month"),
            "additions": acwi.get("additions_count"),
            "removals": acwi.get("removals_count"),
            "renames": acwi.get("renames_count"),
        } if acwi else None,
        "prices": {
            "companies_processed": row.get("companies_processed") or 0,
            "prices_refreshed": row.get("prices_refreshed") or 0,
            "volumes_refreshed": row.get("volumes_refreshed") or 0,
            "forbidden": row.get("forbidden_count") or 0,
            "delisted": row.get("delisted_count") or 0,
            "errors": row.get("error_count") or 0,
        },
        "momentum": [
            {
                "strategy_id": m.get("strategy_id"),
                "strategy_name": m.get("strategy_name"),
                "snapshot_id": m.get("snapshot_id"),
                "holdings_count": m.get("holdings_count"),
                "latest_price_date": m.get("latest_price_date"),
                "status": m.get("status"),
                "error_message": m.get("error_message"),
            }
            for m in mom_list
        ],
        "error_summary": row.get("error_summary"),
    }


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


def _max_target_date(metric_code: str) -> date | None:
    """Latest `target_date` in `metric_data` for one metric_code."""
    try:
        resp = (
            supabase.table("metric_data")
            .select("target_date")
            .eq("metric_code", metric_code)
            .order("target_date", desc=True)
            .limit(1)
            .execute()
        )
    except Exception:
        return None
    if not resp.data:
        return None
    raw = resp.data[0].get("target_date")
    try:
        return date.fromisoformat(str(raw)[:10]) if raw else None
    except ValueError:
        return None


def _trading_day_age(latest: date | None) -> int | None:
    """Approximate age of `latest` in trading days (Mon-Fri only).
    Returns None when latest is missing. Used as a coarse signal — a
    Sunday call where `latest` is Friday should read 0, not 2."""
    if latest is None:
        return None
    today = date.today()
    if latest >= today:
        return 0
    days = 0
    cursor = today
    while cursor > latest:
        cursor = cursor - timedelta(days=1)
        if cursor.weekday() < 5:  # 0..4 = Mon..Fri
            days += 1
    return days


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

        # ACWI universe presence
        acwi = (
            supabase.table("universe")
            .select("universe_id, label")
            .eq("label", "ACWI")
            .limit(1)
            .execute()
        )
        out["acwi_universe_id"] = (
            acwi.data[0]["universe_id"] if acwi.data else None
        )

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
