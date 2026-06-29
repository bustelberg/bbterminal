"""CRUD + run-history endpoints for the schedule (HTTP layer).

Each `scheduled_strategy` row is self-contained: it carries its own
`config` (BacktestRequest shape) + `frequency`. The smart pipeline's
momentum phase iterates every enabled row on each daily tick and produces
one snapshot per strategy: a `rebalance` (fresh picks) when the strategy is
due, or a `price_update` (last rebalance's holdings re-priced) otherwise.

The non-HTTP logic lives in sibling modules so this file stays a thin
router:

  momentum.schedule            pure due-date math (compute_next_due_at,
                               _initial_next_due_at + the anchored grid)
  routers._schedule_snapshots  current_picks_snapshot writers
                               (compute_and_save_price_update, backtest seed)
  routers._schedule_backfill   the background backfill worker + startup reset
  routers._schedule_hydration  run-history hydration (_hydrate + MTD/YTD walk)

`reset_stale_backfills` is re-exported here so `main.py`'s startup hook
(`scheduled_strategies.reset_stale_backfills`) keeps its import path.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from deps import fetch_in_chunks, supabase
from momentum.schedule import _expected_latest_trading_day, _initial_next_due_at

from ._schedule_backfill import reset_stale_backfills  # noqa: F401 — re-exported for main.py
from ._schedule_hydration import _hydrate, build_live_curve
from ._schedule_snapshots import _seed_snapshot_from_backtest

_log = logging.getLogger(__name__)

router = APIRouter(tags=["schedule"])

FREQUENCIES = ("daily", "weekly", "monthly", "bimonthly", "quarterly")


def _is_admin(request: Request) -> bool:
    """True when the verified caller is an admin (the auth middleware stamps
    `request.state.auth`). Non-admins get the read-only, `user_visible`-only
    view of the schedule."""
    info = getattr(request.state, "auth", None) or {}
    return info.get("role") == "admin"


# ─── Pydantic shapes ──────────────────────────────────────────────


class ScheduledStrategyCreate(BaseModel):
    """Body for POST. `config` is the full BacktestRequest payload (we
    don't re-validate it here; the pipeline drives it through
    `BacktestRequest(**config)` and surfaces any failure as a per-
    strategy error in the run's templates_summary).

    `backtest_run_id` is REQUIRED. Every scheduled strategy must
    originate from a backtested variant — that gives /schedule a
    persistent equity-curve / monthly-history record to anchor the
    live snapshots against. The manual-add flow has been retired."""
    name: str
    frequency: str
    config: dict
    backtest_run_id: int
    # Optional go-live date. NULL/omitted → the strategy's created_at is
    # used as the equity-curve marker + live cutoff.
    start_date: date | None = None


class ScheduledStrategyPatch(BaseModel):
    enabled: bool | None = None
    # Rename the strategy. Whitespace-trimmed; empty/blank is rejected.
    name: str | None = None
    # Admin-only: expose this strategy on the read-only /schedule view for
    # non-admin users. Off by default (schedule starts empty for users).
    user_visible: bool | None = None
    # Configurable go-live date (red dashed marker + live cutoff). A
    # present `start_date` sets it; `clear_start_date=True` resets it to
    # NULL (fall back to created_at). They're mutually exclusive — a
    # non-null start_date wins if both are sent.
    start_date: date | None = None
    clear_start_date: bool | None = None
    # NOTE: `rebalance_weekday` is intentionally NOT patchable. It's baked
    # into the strategy's `config` at schedule time (from the source
    # backtest variant) and defines the rebalance grid the smart pipeline
    # keys off — changing it in place would desync `next_due_at` from the
    # snapshots already produced. Re-create the strategy to change it.


# ─── Shared creation helper ───────────────────────────────────────


def create_scheduled_strategy_row(
    name: str,
    frequency: str,
    config: dict,
    backtest_run_id: int,
    start_date: date | None = None,
) -> dict:
    """Insert a new `scheduled_strategy` row + seed its first snapshot from the
    source backtest's last period, and return the raw inserted row.

    Shared by the HTTP `add_scheduled_strategy` endpoint and the Diversifier's
    `schedule-as-strategy` flow (which schedules a blended momentum+ETF
    backtest). Sets `next_due_at` to the next date on the rebalance grid
    (`_initial_next_due_at`, stamped 02:00 UTC) so the entry runs on the next
    eligible daily tick. The seed is best-effort: a failure leaves the strategy
    with backtest-only history until its first live rebalance."""
    weekday = int((config or {}).get("rebalance_weekday", 0) or 0)
    next_due = _initial_next_due_at(frequency, weekday).isoformat()
    insert_row: dict = {
        "name": name.strip(),
        "frequency": frequency,
        "config": config,
        "enabled": True,
        "next_due_at": next_due,
        "backtest_run_id": backtest_run_id,
    }
    if start_date is not None:
        insert_row["start_date"] = start_date.isoformat()
    try:
        resp = supabase.table("scheduled_strategy").insert(insert_row).execute()
    except Exception as e:
        raise HTTPException(500, f"Insert failed: {type(e).__name__}: {e}")
    if not resp.data:
        raise HTTPException(500, "Insert returned no row")
    new_row = resp.data[0]
    try:
        _seed_snapshot_from_backtest(
            int(new_row["id"]), backtest_run_id, name.strip(), config,
        )
    except Exception as e:
        _log.warning(
            "[add] strategy=%s seed failed: %s: %s",
            new_row.get("id"), type(e).__name__, e,
        )
    return new_row


# ─── Endpoints ────────────────────────────────────────────────────


@router.get("/api/scheduled-strategies/held-companies")
async def list_held_companies(request: Request):
    """Pooled set of companies currently held across every enabled
    scheduled strategy. Drives the /schedule "Misc jobs → Currently held
    companies" panel — gives the user full transparency over which
    company is in which strategy's portfolio, when each position was
    opened, and where the next daily price refresh will be writing data.

    Aggregation: for each enabled strategy, take the most-recent
    `current_picks_snapshot` (any kind — rebalance or price_update;
    they share the same holdings shape). Pool the holdings, dedup by
    `company_id`, and attach one `held_by` entry per strategy that holds
    that company. Companies with no snapshot yet are skipped silently.

    Returns:
        {
          "total_companies": int,           # distinct companies pooled
          "total_strategies": int,          # strategies contributing
          "freshness_summary": {            # what date prices we actually have
            "latest_close_date": str|None,  # max(target_date) across held companies
            "fresh_count": int,             # companies at the latest_close_date
            "stale_count": int,             # companies with an older latest target_date
            "missing_count": int,           # companies with NO close_price data at all
          },
          "companies": [{
            "company_id", "ticker", "exchange",
            "company_name", "sector",
            "currency": str|None,                 # native trading currency (from the listing exchange)
            "gurufocus_url": str|None,            # canonical GuruFocus summary link
            "latest_close_price_date": str|None,  # max(target_date) in metric_data for this company
            "latest_close_price": float|None,     # close at that date, in `currency` (unconverted)
            "fx_rate_per_eur": float|None,        # latest {currency}/EUR rate (same source as /fx-rates; 1.0 for EUR)
            "latest_close_price_eur": float|None, # latest_close_price / fx_rate_per_eur
            "held_by": [{
              "strategy_id", "strategy_name",
              "snapshot_id", "snapshot_kind",  # "rebalance"|"price_update"
              "as_of_date",                    # when this position was opened
              "latest_price_date",             # most recent close seen for it
              "target_weight",                 # fractional, 0..1
              "score", "entry_price_local", "entry_date",
            }]
          }]
        }
    """
    admin = _is_admin(request)

    def _query() -> dict:
        # Step 1 — every enabled scheduled strategy (non-admins: only the
        # ones flagged user_visible, matching their read-only schedule view).
        strat_q = (
            supabase.table("scheduled_strategy")
            .select("id, name")
            .eq("enabled", True)
        )
        if not admin:
            strat_q = strat_q.eq("user_visible", True)
        strat_resp = strat_q.execute()
        strategies = strat_resp.data or []
        if not strategies:
            return {"total_companies": 0, "total_strategies": 0, "companies": []}
        strategy_name_by_id: dict[int, str] = {
            int(s["id"]): (s.get("name") or f"Strategy #{s['id']}")
            for s in strategies
        }
        sched_ids = list(strategy_name_by_id.keys())

        # Step 2 — latest snapshot per strategy (regardless of kind).
        snap_resp = (
            supabase.table("current_picks_snapshot")
            .select(
                "snapshot_id, scheduled_strategy_id, kind, as_of_date, "
                "latest_price_date, holdings, created_at"
            )
            .in_("scheduled_strategy_id", sched_ids)
            .order("created_at", desc=True)
            .execute()
        )
        latest_by_sched: dict[int, dict] = {}
        for s in (snap_resp.data or []):
            sid = s.get("scheduled_strategy_id")
            if sid is None or sid in latest_by_sched:
                continue
            latest_by_sched[int(sid)] = s

        # Step 3 — pool holdings, attaching attribution per strategy.
        # Keyed by company_id; each entry's held_by list grows as we
        # iterate. Strategies with no snapshot yet are silently
        # skipped (first-run before backfill or pipeline ever touched them).
        pooled: dict[int, dict] = {}
        for sched_id, snap in latest_by_sched.items():
            for h in (snap.get("holdings") or []):
                cid_raw = h.get("company_id")
                if cid_raw is None:
                    continue
                cid = int(cid_raw)
                bucket = pooled.setdefault(cid, {
                    "company_id": cid,
                    "ticker": h.get("ticker"),
                    "company_name": h.get("company_name"),
                    "sector": h.get("sector"),
                    "exchange": "",  # filled in step 4 below
                    "held_by": [],
                })
                # Holdings stored on the snapshot don't carry exchange;
                # but they do carry ticker + name + sector. We pick the
                # first non-null value across strategies for stability,
                # then overwrite from the company table below.
                if not bucket.get("ticker"):
                    bucket["ticker"] = h.get("ticker")
                if not bucket.get("company_name"):
                    bucket["company_name"] = h.get("company_name")
                if not bucket.get("sector"):
                    bucket["sector"] = h.get("sector")
                bucket["held_by"].append({
                    "strategy_id": sched_id,
                    "strategy_name": strategy_name_by_id[sched_id],
                    "snapshot_id": snap.get("snapshot_id"),
                    "snapshot_kind": snap.get("kind"),
                    "as_of_date": snap.get("as_of_date"),
                    "latest_price_date": snap.get("latest_price_date"),
                    "target_weight": float(h.get("weight") or 0.0),
                    "score": h.get("score"),
                    "entry_price_local": h.get("entry_price_local"),
                    "entry_date": h.get("entry_date") or snap.get("as_of_date"),
                })

        if not pooled:
            return {
                "total_companies": 0,
                "total_strategies": len(latest_by_sched),
                "companies": [],
            }

        # Step 4 — exchange lookup. Holdings JSONB doesn't include the
        # GuruFocus exchange code or trading currency; fetch them from
        # `company` joined to `gurufocus_exchange`. Batched by IN_CHUNK_SIZE
        # to stay under the PostgREST URL-length window.
        cids = list(pooled.keys())
        for r in fetch_in_chunks(
            cids,
            lambda chunk: supabase.table("company")
            .select(
                "company_id, company_name, gurufocus_ticker, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code, currency_code)"
            )
            .in_("company_id", chunk)
            .execute(),
        ):
            cid = int(r["company_id"])
            if cid not in pooled:
                continue
            gfx = r.get("gurufocus_exchange") or {}
            exch = gfx.get("exchange_code") or ""
            pooled[cid]["exchange"] = exch
            # Native trading currency (from the listing exchange) — the
            # latest close below is in this currency, unconverted.
            pooled[cid]["currency"] = gfx.get("currency_code")
            # Prefer the authoritative ticker/name from `company`
            # — the snapshot's holdings can carry slightly stale
            # values after a renamed-ticker override.
            if r.get("gurufocus_ticker"):
                pooled[cid]["ticker"] = r["gurufocus_ticker"]
            if r.get("company_name"):
                pooled[cid]["company_name"] = r["company_name"]

        # Canonical GuruFocus summary link per company, from the resolved
        # ticker + exchange (single-sourced via the shared helper so it
        # matches every other GF link in the app).
        from ingest.gurufocus_url import gurufocus_url  # noqa: PLC0415
        for cid, bucket in pooled.items():
            bucket["gurufocus_url"] = gurufocus_url(
                bucket.get("ticker"), bucket.get("exchange")
            )

        # Step 5 — freshness + latest price lookup. Latest `close_price`
        # target_date AND native-currency value per held company. The held
        # set is tiny (~24 names), so query just those ids via a fast indexed
        # DISTINCT ON (direct-Postgres COPY) instead of the full-table
        # `company_latest_close_price_dates` RPC, which aggregates ALL of
        # metric_data and times out.
        latest_close_by_cid: dict[int, str | None] = {}
        latest_price_by_cid: dict[int, float | None] = {}
        try:
            from momentum.data._pg import load_latest_close_prices_via_copy  # noqa: PLC0415
            fast = load_latest_close_prices_via_copy(cids)
            if fast is not None:
                for cid, row in fast.items():
                    latest_close_by_cid[cid] = row.get("date")
                    latest_price_by_cid[cid] = row.get("price")
            else:
                # Fallback (no SUPABASE_DB_URL): per-company latest close,
                # one cheap indexed query each (held set is small).
                for cid in cids:
                    r = (
                        supabase.table("metric_data")
                        .select("target_date, numeric_value")
                        .eq("metric_code", "close_price")
                        .eq("company_id", cid)
                        .order("target_date", desc=True)
                        .limit(1)
                        .execute()
                    )
                    if r.data:
                        latest_close_by_cid[cid] = r.data[0]["target_date"]
                        val = r.data[0].get("numeric_value")
                        latest_price_by_cid[cid] = float(val) if val is not None else None
        except Exception:
            # On any error the endpoint still returns the holdings — freshness
            # + price just render as "unknown" in the UI.
            latest_close_by_cid = {}
            latest_price_by_cid = {}

        for cid, bucket in pooled.items():
            bucket["latest_close_price_date"] = latest_close_by_cid.get(cid)
            bucket["latest_close_price"] = latest_price_by_cid.get(cid)

        # Step 5b — FX to EUR. Reuses `fetch_latest_from_db` — the SAME
        # latest-rate-per-currency lookup that backs the /fx-rates page's
        # `/api/fx/latest`, so any currency the FX page shows resolves here
        # too (units of currency per 1 EUR). A per-price-date window would
        # leave cells blank whenever ECB's latest published rate lags the
        # close date; the latest stored rate is the correct forward-filled
        # value for a marked-to-market holding anyway. EUR passes through at
        # 1.0; the EUR price is `local / rate`. Best-effort — null on error.
        latest_fx_by_ccy: dict[str, float] = {}
        try:
            from fx_rates import fetch_latest_from_db  # noqa: PLC0415
            for row in fetch_latest_from_db(supabase):
                code = row.get("currency")
                rate = row.get("rate")
                if code and rate:
                    latest_fx_by_ccy[code] = float(rate)
        except Exception:
            latest_fx_by_ccy = {}
        for bucket in pooled.values():
            cur = bucket.get("currency")
            price = bucket.get("latest_close_price")
            rate = 1.0 if cur == "EUR" else (latest_fx_by_ccy.get(cur) if cur else None)
            bucket["fx_rate_per_eur"] = rate
            bucket["latest_close_price_eur"] = (
                round(price / rate, 4)
                if rate and price is not None and rate > 0
                else None
            )

        # Compute the freshness summary against the EXPECTED latest trading
        # day — NOT the held set's own max (which would call everything
        # "fresh" the moment they all share the same stale date). The
        # expected day is the most recent weekday strictly before today (the
        # last settled close the daily pipeline could have fetched). A held
        # company is fresh when its latest close ≥ that day, stale when it's
        # behind (new closes to fetch), missing when it has no close at all.
        # `latest_close_date` reports the data we actually HAVE (held max).
        dates = [v for v in latest_close_by_cid.values() if v]
        latest_close_date = max(dates) if dates else None
        expected_iso = _expected_latest_trading_day(date.today()).isoformat()
        fresh_count = 0
        stale_count = 0
        missing_count = 0
        for cid in pooled.keys():
            d = pooled[cid].get("latest_close_price_date")
            if d is None:
                missing_count += 1
            elif str(d)[:10] >= expected_iso:
                fresh_count += 1
            else:
                stale_count += 1

        companies = list(pooled.values())
        # Sort by (sector, ticker) for stable rendering. Empty sector
        # bucket lands at the bottom.
        companies.sort(key=lambda c: (
            (c.get("sector") or "~"),  # ~ sorts after letters in ASCII
            (c.get("ticker") or "").upper(),
        ))

        return {
            "total_companies": len(companies),
            "total_strategies": len(latest_by_sched),
            "freshness_summary": {
                "latest_close_date": latest_close_date,
                # The reference the fresh/stale split is measured against.
                "expected_close_date": expected_iso,
                "fresh_count": fresh_count,
                "stale_count": stale_count,
                "missing_count": missing_count,
            },
            "companies": companies,
        }
    return await asyncio.to_thread(_query)


@router.get("/api/scheduled-strategies")
async def list_scheduled_strategies(request: Request):
    """Every scheduled strategy with its last-snapshot summary. Admins see all;
    non-admins (the read-only /schedule view) see only `user_visible` ones —
    so the page starts empty until an admin opts strategies in."""
    admin = _is_admin(request)

    def _query() -> list[dict]:
        q = supabase.table("scheduled_strategy").select("*").order("created_at")
        if not admin:
            q = q.eq("user_visible", True)
        return _hydrate(q.execute().data or [])
    return await asyncio.to_thread(_query)


@router.post("/api/scheduled-strategies")
async def add_scheduled_strategy(body: ScheduledStrategyCreate):
    """Create a new scheduled strategy. Sets `next_due_at` to the next date
    on its rebalance grid (stamped 02:00 UTC — the threshold the daily 05:00
    UTC tick picks up) so the entry runs on the next eligible tick."""
    if body.frequency not in FREQUENCIES:
        raise HTTPException(
            400,
            f"Unknown frequency {body.frequency!r}; expected one of {list(FREQUENCIES)}",
        )
    if not body.name.strip():
        raise HTTPException(400, "name must be non-empty")
    if not isinstance(body.config, dict) or not body.config:
        raise HTTPException(400, "config must be a non-empty object")

    def _insert() -> dict:
        # Seed the current holdings from the saved backtest's last period so
        # the daily price refresh can track them immediately — no off-cycle
        # rebalance needed. The next universe reprice + re-selection happens
        # at `next_due_at` (the next grid rebalance).
        new_row = create_scheduled_strategy_row(
            body.name, body.frequency, body.config, body.backtest_run_id, body.start_date,
        )
        return _hydrate([new_row])[0]
    return await asyncio.to_thread(_insert)


@router.patch("/api/scheduled-strategies/{strategy_id}")
async def patch_scheduled_strategy(strategy_id: int, body: ScheduledStrategyPatch):
    """Toggle `enabled` and/or set the configurable `start_date` (the
    go-live marker + live cutoff). Re-pointing at a different config isn't
    allowed in place — delete + re-add to keep per-snapshot attribution
    unambiguous."""
    update_dict: dict = {"updated_at": datetime.now(timezone.utc).isoformat()}
    if body.enabled is not None:
        update_dict["enabled"] = body.enabled
    if body.name is not None:
        trimmed = body.name.strip()
        if not trimmed:
            raise HTTPException(400, "name must be non-empty")
        update_dict["name"] = trimmed
    if body.clear_start_date:
        update_dict["start_date"] = None
    elif body.start_date is not None:
        update_dict["start_date"] = body.start_date.isoformat()
    if body.user_visible is not None:
        update_dict["user_visible"] = body.user_visible
    # `updated_at` is always present — require at least one real field so a
    # no-op PATCH is a clear 400 rather than a silent timestamp bump.
    if len(update_dict) == 1:
        raise HTTPException(
            400,
            "Nothing to update (pass `enabled`, `name`, `start_date`, "
            "`clear_start_date`, or `user_visible`).",
        )

    def _update() -> dict:
        resp = (
            supabase.table("scheduled_strategy")
            .update(update_dict)
            .eq("id", strategy_id)
            .execute()
        )
        if not resp.data:
            raise HTTPException(404, f"Scheduled strategy #{strategy_id} not found")
        return _hydrate(resp.data)[0]
    return await asyncio.to_thread(_update)


@router.delete("/api/scheduled-strategies")
async def delete_all_scheduled_strategies():
    """Wipe every scheduled strategy. Snapshots stay (their
    `scheduled_strategy_id` FK is set to NULL via cascade) so the
    historical run-history view remains inspectable. Mostly used to
    reset the /schedule page after experimenting with multiple
    permutations."""
    def _delete() -> dict:
        # Fetch the ids first so we can return a count — `delete()`
        # without a filter is rejected by Supabase by default, so use
        # `neq(id, 0)` to match all rows.
        resp = (
            supabase.table("scheduled_strategy")
            .delete()
            .neq("id", 0)
            .execute()
        )
        return {"deleted_count": len(resp.data or [])}
    return await asyncio.to_thread(_delete)


@router.delete("/api/scheduled-strategies/{strategy_id}")
async def delete_scheduled_strategy(strategy_id: int):
    """Remove from the schedule. Past snapshots are preserved (the
    snapshot's `scheduled_strategy_id` FK is set to NULL via the
    foreign-key cascade, so they're orphaned but visible for historical
    inspection)."""
    def _delete() -> dict:
        resp = (
            supabase.table("scheduled_strategy")
            .delete()
            .eq("id", strategy_id)
            .execute()
        )
        if not resp.data:
            raise HTTPException(404, f"Scheduled strategy #{strategy_id} not found")
        return {"deleted": strategy_id}
    return await asyncio.to_thread(_delete)


@router.get("/api/scheduled-strategies/{strategy_id}/runs")
async def list_strategy_runs(strategy_id: int, request: Request, limit: int = 50):
    """Run history for one scheduled strategy. Joins via the new
    `current_picks_snapshot.scheduled_strategy_id` FK so it stays clean
    even after schema-evolution churn on adjacent tables."""
    limit = max(1, min(200, limit))
    admin = _is_admin(request)

    def _query() -> dict:
        sched_resp = (
            supabase.table("scheduled_strategy")
            .select("*")
            .eq("id", strategy_id)
            .limit(1)
            .execute()
        )
        if not sched_resp.data:
            raise HTTPException(404, f"Scheduled strategy #{strategy_id} not found")
        sched = sched_resp.data[0]
        # Non-admins may only open a strategy an admin flagged user_visible.
        if not admin and not sched.get("user_visible"):
            raise HTTPException(403, "Not available")

        snap_resp = (
            supabase.table("current_picks_snapshot")
            .select(
                "snapshot_id, ingest_run_id, created_at, as_of_date, "
                "latest_price_date, holdings, kind, is_backfill, period_return_pct"
            )
            .eq("scheduled_strategy_id", strategy_id)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        snapshots = snap_resp.data or []

        # Suppress backfill rebalance rows whose `as_of_date` is also
        # covered by a NEWER non-backfill snapshot (a daily-refresh
        # price_update or a live pipeline rebalance). The backfill
        # row's data is point-in-time stale at that point — the user
        # already has the latest data via the newer snapshot, and
        # showing both creates a confusing "2026-05-04 backfill
        # +0.45% (data through 05-06)" alongside "2026-05-04 price
        # update +2.07% (data through 05-25)" pair for the same
        # open period. `_compute_period_returns` keeps the full
        # history above — this filter is purely cosmetic.
        non_backfill_asofs = {
            s["as_of_date"] for s in snapshots
            if not (s.get("kind") == "rebalance" and s.get("is_backfill"))
            and s.get("as_of_date")
        }
        snapshots = [
            s for s in snapshots
            if not (s.get("kind") == "rebalance" and s.get("is_backfill"))
            or s.get("as_of_date") not in non_backfill_asofs
        ]

        run_ids = list({s["ingest_run_id"] for s in snapshots if s.get("ingest_run_id")})
        runs_by_id: dict[int, dict] = {}
        if run_ids:
            runs_resp = (
                supabase.table("ingest_run")
                .select("*")
                .in_("run_id", run_ids)
                .execute()
            )
            runs_by_id = {r["run_id"]: r for r in (runs_resp.data or [])}

        def _sector_counts(holdings: list[dict] | None) -> dict[str, int]:
            """Group this snapshot's holdings by sector. Used by the
            UI's per-row sector grid (vertically aligned across rows so
            persistent sectors are easy to eyeball)."""
            out: dict[str, int] = {}
            for h in holdings or []:
                sec = (h.get("sector") or "").strip() or "—"
                out[sec] = out.get(sec, 0) + 1
            return out

        history = [
            {
                "snapshot_id": s["snapshot_id"],
                "created_at": s["created_at"],
                "as_of_date": s["as_of_date"],
                "latest_price_date": s.get("latest_price_date"),
                "holdings_count": len(s.get("holdings") or []),
                "kind": s.get("kind"),
                "is_backfill": bool(s.get("is_backfill")),
                "period_return_pct": s.get("period_return_pct"),
                "sector_counts": _sector_counts(s.get("holdings")),
                # `ingest_run` is null for backfill rows (they weren't
                # produced by any pipeline tick).
                "ingest_run": runs_by_id.get(s["ingest_run_id"]) if s.get("ingest_run_id") else None,
            }
            for s in snapshots
        ]

        # Live extension of the source-backtest daily curve — grafts the
        # held portfolio's forward performance (the price-update job marks
        # the snapshots to market through the latest priced day) onto the
        # frozen backtest curve so the detail view's monthly-returns +
        # equity curve track the latest priced day instead of ending where
        # the backtest was saved. Same snapshot source the run-history
        # rollups use. Best-effort: None when there's no source backtest or
        # no live data fresher than the curve's end.
        live_curve = None
        if sched.get("backtest_run_id"):
            curve_hist = (
                supabase.table("current_picks_snapshot")
                .select("kind, as_of_date, latest_price_date, period_return_pct, created_at")
                .eq("scheduled_strategy_id", strategy_id)
                .order("latest_price_date", desc=False)
                .order("created_at", desc=False)
                .execute()
            )
            live_curve = build_live_curve(
                int(sched["backtest_run_id"]), curve_hist.data or []
            )

        return {
            "id": sched["id"],
            "name": sched.get("name") or f"Strategy #{sched['id']}",
            "frequency": sched.get("frequency"),
            "config": sched.get("config") or {},
            "enabled": sched.get("enabled", True),
            "created_at": sched.get("created_at"),
            # Configurable go-live date (red dashed equity-curve marker +
            # live cutoff). NULL → frontend defaults to created_at.
            "start_date": sched.get("start_date"),
            "last_run_at": sched.get("last_run_at"),
            "next_due_at": sched.get("next_due_at"),
            # Variant-add flow stores the source backtest here. Frontend
            # fetches /api/momentum/backtests/{run_id} on expansion to
            # render the full equity curve + monthly history with the
            # red dashed go-live marker at `start_date` (or created_at).
            "backtest_run_id": sched.get("backtest_run_id"),
            "backfill": {
                "status": sched.get("backfill_status"),
                "progress_pct": sched.get("backfill_progress_pct"),
                "message": sched.get("backfill_message"),
                "error": sched.get("backfill_error"),
                "started_at": sched.get("backfill_started_at"),
                "finished_at": sched.get("backfill_finished_at"),
            },
            "runs": history,
            # {cutover_date, points:[{date,cumulative_return_pct}], as_of_date}
            # or None. Frontend keeps backtest daily points before
            # cutover_date and appends `points` (same cumulative scale).
            "live_curve": live_curve,
        }

    return await asyncio.to_thread(_query)
