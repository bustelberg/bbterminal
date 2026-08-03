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

from ._authz import is_admin_request
from ._schedule_backfill import reset_stale_backfills  # noqa: F401 — re-exported for main.py
from ._schedule_hydration import (
    _hydrate,
    basket_price_staleness,
    build_live_curve,
    live_period_records,
)
from ._schedule_snapshots import _seed_snapshot_from_backtest

_log = logging.getLogger(__name__)

router = APIRouter(tags=["schedule"])

FREQUENCIES = ("daily", "weekly", "monthly", "bimonthly", "quarterly")


def _is_admin(request: Request) -> bool:
    """True when the verified caller is an admin (the auth middleware stamps
    `request.state.auth`). Non-admins get the read-only, `user_visible`-only
    view of the schedule. Delegates to the shared `is_admin_request` so the
    "view as regular user" semantics stay identical across endpoints."""
    return is_admin_request(request)


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
                # The cash sleeve (company_id 0 / is_cash) isn't a priceable
                # security — exclude it from the held/price-update set entirely.
                if cid == 0 or h.get("is_cash"):
                    continue
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

        # Step 4 — identity + currency lookup. Holdings JSONB doesn't include
        # the exchange code or trading currency. Real companies (positive id)
        # resolve from `company`; ETF-overlay holdings (negative id =
        # -benchmark_id) resolve from `benchmark` instead — without this they'd
        # carry no currency and (step 5) no price, so they'd be miscounted as
        # "missing" in the freshness summary even though the price-update keeps
        # their `benchmark_price` current. Batched to stay under the PostgREST
        # URL-length window.
        cids = list(pooled.keys())
        company_cids = [c for c in cids if c >= 0]
        benchmark_ids = [-c for c in cids if c < 0]
        for r in fetch_in_chunks(
            company_cids,
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

        # ETF-overlay holdings → identity + currency from `benchmark`.
        for b in fetch_in_chunks(
            benchmark_ids,
            lambda chunk: supabase.table("benchmark")
            .select("benchmark_id, ticker, name, currency")
            .in_("benchmark_id", chunk)
            .execute(),
        ):
            cid = -int(b["benchmark_id"])
            if cid not in pooled:
                continue
            pooled[cid]["exchange"] = "ETF"
            pooled[cid]["is_etf"] = True
            pooled[cid]["currency"] = b.get("currency")
            if b.get("ticker"):
                pooled[cid]["ticker"] = b["ticker"]
            if b.get("name"):
                pooled[cid]["company_name"] = b["name"]

        # Canonical GuruFocus summary link per company, from the resolved
        # ticker + exchange. ETF overlays have no GuruFocus listing page → no link.
        from ingest.gurufocus_url import gurufocus_url  # noqa: PLC0415
        for cid, bucket in pooled.items():
            bucket["gurufocus_url"] = (
                None if bucket.get("is_etf")
                else gurufocus_url(bucket.get("ticker"), bucket.get("exchange"))
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
            fast = load_latest_close_prices_via_copy(company_cids)
            if fast is not None:
                for cid, row in fast.items():
                    latest_close_by_cid[cid] = row.get("date")
                    latest_price_by_cid[cid] = row.get("price")
            else:
                # Fallback (no SUPABASE_DB_URL): per-company latest close,
                # one cheap indexed query each (held set is small).
                for cid in company_cids:
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

        # ETF overlays: latest close from `benchmark_price` (their metric_data
        # equivalent), so they count toward freshness like any held instrument
        # instead of being flagged "missing".
        for bid in benchmark_ids:
            try:
                r = (
                    supabase.table("benchmark_price")
                    .select("target_date, price")
                    .eq("benchmark_id", bid)
                    .order("target_date", desc=True)
                    .limit(1)
                    .execute()
                )
            except Exception:
                continue
            if r.data:
                cid = -bid
                latest_close_by_cid[cid] = r.data[0]["target_date"]
                val = r.data[0].get("price")
                latest_price_by_cid[cid] = float(val) if val is not None else None

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
        # Manual drag-order first (nulls last), then creation order for rows the
        # admin hasn't reordered yet.
        q = (
            supabase.table("scheduled_strategy")
            .select("*")
            .order("sort_order", desc=False, nullsfirst=False)
            .order("created_at")
        )
        if not admin:
            q = q.eq("user_visible", True)
        return _hydrate(q.execute().data or [])
    return await asyncio.to_thread(_query)


class ReorderRequest(BaseModel):
    # The strategy ids in the desired display order (top → bottom).
    ordered_ids: list[int]


# NOTE: must be declared BEFORE the `/{strategy_id}` routes — otherwise FastAPI
# matches "reorder" against the int path param and 422s.
@router.patch("/api/scheduled-strategies/reorder")
async def reorder_scheduled_strategies(body: ReorderRequest):
    """Persist the drag-reordered display order: `sort_order = position` for each
    id in `ordered_ids` (top = 0). The list GET sorts by `sort_order` then
    `created_at`. Admin-only (the API gate blocks non-admin writes here)."""
    def _apply() -> dict:
        now = datetime.now(timezone.utc).isoformat()
        for pos, sid in enumerate(body.ordered_ids):
            supabase.table("scheduled_strategy").update(
                {"sort_order": pos, "updated_at": now}
            ).eq("id", int(sid)).execute()
        return {"ok": True, "count": len(body.ordered_ids)}
    return await asyncio.to_thread(_apply)


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


class SetCashRequest(BaseModel):
    # Cash allocation as a fraction 0..1 (e.g. 0.1 = 10% cash).
    cash_pct: float


class SleeveEtf(BaseModel):
    benchmark_id: int
    # ⚠ ABSOLUTE — this ETF's share of the WHOLE portfolio, in percent, the
    # number the user typed. Stored invested-relative (see `set_strategy_sleeves`).
    weight_pct: float
    band_pct: float = 0.0


class SetSleevesRequest(BaseModel):
    # Cash as a fraction 0..1; ETFs as absolute percentages of the whole book.
    cash_pct: float = 0.0
    etfs: list[SleeveEtf] = []


def _write_sleeves(strategy_id: int, cash: float, overlay: list[dict]) -> dict:
    """Store the sleeves on the strategy config, restate the OPEN period's book,
    and re-price — the shared body of the cash-only and full-sleeve endpoints.

    Order is load-bearing: the rebalance snapshot is restated FIRST, because
    `compute_and_save_price_update` derives the priced book from it. Re-pricing
    first would re-price the old sleeves and then be overwritten."""
    row = (
        supabase.table("scheduled_strategy")
        .select("id, config")
        .eq("id", strategy_id)
        .limit(1)
        .execute()
    ).data
    if not row:
        raise HTTPException(404, f"Scheduled strategy #{strategy_id} not found")
    cfg = dict(row[0].get("config") or {})
    cfg["cash_pct"] = cash
    cfg["etf_overlay"] = overlay
    supabase.table("scheduled_strategy").update(
        {"config": cfg, "updated_at": datetime.now(timezone.utc).isoformat()}
    ).eq("id", strategy_id).execute()

    # Restate the open period + re-price so the new weighting shows at once (no
    # wait for the daily tick). Best-effort: a strategy with no rebalance yet
    # just picks the sleeves up on its first one.
    try:
        from routers._schedule_snapshots import (  # noqa: PLC0415
            apply_sleeves_to_snapshot,
            compute_and_save_price_update,
        )
        rebal = (
            supabase.table("current_picks_snapshot")
            .select("snapshot_id")
            .eq("scheduled_strategy_id", strategy_id)
            .eq("kind", "rebalance")
            .order("as_of_date", desc=True)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        ).data
        if rebal:
            apply_sleeves_to_snapshot(
                int(rebal[0]["snapshot_id"]), etf_overlay=overlay, cash_pct=cash,
            )
        compute_and_save_price_update(strategy_id, ingest_run_id=None, cash_pct=cash)
    except Exception as e:
        logging.getLogger(__name__).warning(
            "[sleeves] re-price after sleeve change failed for strategy %s: %s: %s",
            strategy_id, type(e).__name__, e,
        )
    updated = (
        supabase.table("scheduled_strategy").select("*").eq("id", strategy_id).execute()
    )
    return _hydrate(updated.data)[0]


@router.patch("/api/scheduled-strategies/{strategy_id}/cash")
async def set_strategy_cash(strategy_id: int, body: SetCashRequest):
    """Set a strategy's CASH allocation (0..1), leaving its ETF sleeves alone.

    Cash scales every other holding's weight by (1-cash) and adds a flat
    0%-return cash sleeve, so the reported weights + the return pick up the cash
    drag. Re-prices immediately. See `PATCH …/sleeves` to set cash and the ETF
    overlay together.

    Admin-only: the API gate blocks all non-admin writes here, so read-only users
    can see the cash allocation but can't change it."""
    cash = min(max(float(body.cash_pct), 0.0), 1.0)

    def _apply() -> dict:
        row = (
            supabase.table("scheduled_strategy")
            .select("config").eq("id", strategy_id).limit(1).execute()
        ).data
        if not row:
            raise HTTPException(404, f"Scheduled strategy #{strategy_id} not found")
        overlay = list((row[0].get("config") or {}).get("etf_overlay") or [])
        return _write_sleeves(strategy_id, cash, overlay)
    return await asyncio.to_thread(_apply)


@router.patch("/api/scheduled-strategies/{strategy_id}/sleeves")
async def set_strategy_sleeves(strategy_id: int, body: SetSleevesRequest):
    """Set a strategy's CASH and ETF sleeves by hand; the stock picks take the rest.

    ⚠ THE INPUT IS ABSOLUTE, THE STORAGE IS INVESTED-RELATIVE, AND THE DIFFERENCE
    IS NOT COSMETIC. What you type is each sleeve's share of the whole portfolio
    (10% cash + 20% ETF ⇒ 70% stocks). What `config.etf_overlay[].weight_pct`
    means — set by the diversifier, consumed by the blended backtest — is a share
    of the INVESTED book, i.e. after cash is taken out. Storing 20 there with 10%
    cash would hold 18%, not the 20% you asked for. Converted here
    (`weight_pct = absolute / (1 − cash)`) so both readers stay correct and no
    stored convention changes.

    The stock weights are re-derived from the underlying strategy's selection —
    the stored sleeve-scaled weights are renormalized to sum-1 before the new
    sleeves are applied (`momentum.portfolio_math.apply_sleeves`), so repeated
    edits can't compound the shrink.

    ⚠ IT RESTATES THE OPEN PERIOD, it does not open a new one: the ETF sleeves are
    priced from the same entry bar the stock sleeve entered on, so the period's
    return stays measured over one window. The next rebalance re-selects normally.

    Admin-only (the API gate blocks non-admin writes)."""
    cash = min(max(float(body.cash_pct), 0.0), 1.0)
    etfs = [e for e in body.etfs if (e.weight_pct or 0) > 0]

    seen: set[int] = set()
    for e in etfs:
        if e.weight_pct < 0:
            raise HTTPException(422, f"Benchmark {e.benchmark_id}: weight can't be negative")
        if e.benchmark_id in seen:
            raise HTTPException(422, f"Benchmark {e.benchmark_id} is listed twice")
        seen.add(e.benchmark_id)

    etf_total = sum(float(e.weight_pct) for e in etfs) / 100.0
    # Round-trip float noise (0.1+0.2) must not reject a book the user typed as
    # exactly 100 — but a genuine over-allocation is refused, never silently
    # scaled down to fit, because scaling would hold weights nobody chose.
    if etf_total + cash > 1.0 + 1e-9:
        raise HTTPException(
            422,
            f"Cash ({cash * 100:.2f}%) + ETFs ({etf_total * 100:.2f}%) = "
            f"{(cash + etf_total) * 100:.2f}% of the portfolio — over 100%. "
            "The stock sleeve takes what's left, so these must sum to at most 100.",
        )
    invested = 1.0 - cash
    if etfs and invested <= 1e-9:
        raise HTTPException(422, "100% cash leaves nothing to hold an ETF with — remove the ETFs or lower cash.")

    def _apply() -> dict:
        if etfs:
            bids = sorted(seen)
            known = {
                b["benchmark_id"]
                for b in (
                    supabase.table("benchmark").select("benchmark_id")
                    .in_("benchmark_id", bids).execute()
                ).data or []
            }
            missing = [b for b in bids if b not in known]
            if missing:
                raise HTTPException(422, f"Unknown benchmark id(s): {missing}")
            # A benchmark with no price history would be held at its weight and
            # contribute NO return — the weighted aggregate would silently be
            # over the priced part only, which reads as a real number.
            unpriced = [
                b for b in bids
                if not (
                    supabase.table("benchmark_price").select("target_date")
                    .eq("benchmark_id", b).limit(1).execute()
                ).data
            ]
            if unpriced:
                raise HTTPException(
                    422,
                    f"Benchmark id(s) {unpriced} have no price history — they can't be "
                    "weighted into a portfolio whose return is priced daily.",
                )
        overlay = [
            {
                "benchmark_id": int(e.benchmark_id),
                # absolute % of the book → % of the invested book
                "weight_pct": round(float(e.weight_pct) / invested, 6),
                "band_pct": float(e.band_pct or 0.0),
            }
            for e in etfs
        ]
        return _write_sleeves(strategy_id, cash, overlay)
    return await asyncio.to_thread(_apply)


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
        live_records: list[dict] = []
        if sched.get("backtest_run_id"):
            # Full snapshot history WITH holdings — the live curve now follows the
            # actual live rebalances (each real basket re-priced daily), and the
            # holdings table lists every live rebalance period. Both need the
            # holdings + is_backfill flag, and ALL periods (not just the recent
            # `limit`), so this is a separate unbounded fetch from `snapshots`.
            curve_hist = (
                supabase.table("current_picks_snapshot")
                .select(
                    "kind, as_of_date, latest_price_date, period_return_pct, "
                    "created_at, holdings, is_backfill"
                )
                .eq("scheduled_strategy_id", strategy_id)
                .order("latest_price_date", desc=False)
                .order("created_at", desc=False)
                .execute()
            )
            cash = float((sched.get("config") or {}).get("cash_pct") or 0.0)
            live_curve = build_live_curve(
                int(sched["backtest_run_id"]), curve_hist.data or [], cash,
            )
            # Live rebalance baskets as PeriodRecord rows, so the detail view's
            # holdings table lists the newly-computed portfolios alongside the
            # frozen backtest periods.
            live_records = live_period_records(
                curve_hist.data or [], int(sched["backtest_run_id"]), cash,
            )

        # Stale-price guard for the live tail: when the latest plotted point
        # carries some holdings forward at an older close (GuruFocus publish
        # lag), the current-month heatmap cell isn't a clean mark-to-market.
        # Surfaced so the frontend replaces that cell with a warning listing
        # the lagging assets. Only meaningful once a live tail exists.
        stale_prices = (
            basket_price_staleness(
                int(sched["backtest_run_id"]),
                # Check the basket that's actually plotted: the latest LIVE
                # rebalance's holdings when the strategy has rebalanced, else the
                # backtest's last basket (None → the function loads it).
                live_records[-1]["holdings"] if live_records else None,
            )
            if live_curve and sched.get("backtest_run_id")
            else None
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
            # PeriodRecord[] — the strategy's live rebalance baskets, chained
            # onto the backtest's cumulative. The frontend appends these to
            # `monthly_records` so the holdings table + sector timeline include
            # the newly-computed portfolios. Empty when no live rebalances yet.
            "live_records": live_records,
            # {reference_date, month, missing:[{company_id,label,ticker,
            # last_close}]} or None. When present, the latest live point mixes
            # carried-forward prices for `missing`; the frontend warns on that
            # month's heatmap cell instead of showing a partial number.
            "stale_prices": stale_prices,
        }

    return await asyncio.to_thread(_query)


class RepricedHolding(BaseModel):
    """One holding's four price marks after a reload, plus what moved."""

    ticker: str | None = None
    company_name: str | None = None
    is_etf: bool = False
    entry_date: str | None = None
    entry_price_local: float | None = None
    entry_price_eur: float | None = None
    exit_date: str | None = None
    exit_price_local: float | None = None
    exit_price_eur: float | None = None
    forward_return_pct: float | None = None
    # ⚠ WHAT ACTUALLY CHANGED, per field. A reload that reports only the new numbers cannot be
    # told apart from one that did nothing — and "did it fix it?" is the entire reason the button
    # exists. Field names, e.g. ["entry_price_local", "forward_return_pct"]; empty = untouched.
    changed: list[str] = []


class RepriceResult(BaseModel):
    """The outcome of reloading one strategy's prices — see the endpoint's docstring."""

    strategy_id: int
    snapshot_id: int | None = None
    holdings: list[RepricedHolding] = []
    changed_holdings: int = 0
    note: str | None = None


@router.post("/api/scheduled-strategies/{strategy_id}/reprice",
             response_model=RepriceResult)
async def reprice_scheduled_strategy(strategy_id: int):
    """Reload one strategy's PRICES. It does not re-select, and that distinction is the point.

    ⚠ IT NEVER RE-DECIDES WHAT IS HELD. Re-running the selection for a past date is "Force
    re-rebalance", and it is not a repair: `metric_data` is NOT append-only in `target_date` —
    GuruFocus publishes late closes stamped with their true earlier date — so a past basket
    cannot be reproduced from the live database and re-selecting would silently rewrite what the
    strategy held. (That is the failure the golden-master test exists to catch.) This reloads the
    marks on the holdings that ARE there: start and end, local and converted.

    ⚠ IT IS THE SAME FUNCTION THE NIGHTLY TICK RUNS — `compute_and_save_price_update` — not a
    second implementation of it. A button that priced a book its own way would be a new source of
    truth that agrees with the pipeline right up until it doesn't. What the button buys is the
    timing: the fix lands now instead of at 05:00 UTC.

    Corrects, on every run:
      * `exit_price_local` + `exit_date` from the latest close, for every holding;
      * `exit_price_eur`, converted at that date's rate;
      * `entry_price_eur` — always for an ETF, and for anyone else whose EUR mark is missing;
      * `entry_price_local` for an ETF, re-derived from `benchmark_price` as of its own entry
        date. That last one is what repairs the truncated-read corruption (an SPMO entry of
        40.18, a 2019 close, sitting beside a correct 143.83 exit on the same day).

    A company's `entry_price_local` is deliberately NOT re-derived — see above; that is history,
    not a cache.
    """
    def _run() -> dict:
        from routers._schedule_snapshots import (  # noqa: PLC0415
            compute_and_save_price_update,
        )

        def _latest() -> dict | None:
            rows = (
                supabase.table("current_picks_snapshot")
                .select("snapshot_id, holdings")
                .eq("scheduled_strategy_id", strategy_id)
                .order("as_of_date", desc=True)
                .order("created_at", desc=True)
                .limit(1)
                .execute()
            ).data
            return rows[0] if rows else None

        # BEFORE, so the response can say what moved rather than only what it now is.
        before = {
            str(h.get("ticker") or h.get("company_id")): h
            for h in ((_latest() or {}).get("holdings") or [])
        }

        snapshot_id = compute_and_save_price_update(strategy_id, ingest_run_id=None)
        after = _latest()
        if not after:
            return {"strategy_id": strategy_id, "snapshot_id": snapshot_id, "holdings": [],
                    "changed_holdings": 0,
                    "note": ("Nothing to re-price — this strategy has no stored holdings yet. "
                             "It needs a rebalance first.")}

        watched = ("entry_price_local", "entry_price_eur", "exit_price_local",
                   "exit_price_eur", "entry_date", "exit_date", "forward_return_pct")
        out: list[dict] = []
        n_changed = 0
        for h in (after.get("holdings") or []):
            key = str(h.get("ticker") or h.get("company_id"))
            prev = before.get(key) or {}
            changed = [f for f in watched if prev.get(f) != h.get(f)] if prev else []
            if changed:
                n_changed += 1
            cid = h.get("company_id")
            out.append({
                "ticker": h.get("ticker"),
                "company_name": h.get("company_name"),
                "is_etf": cid is not None and cid < 0,
                "entry_date": h.get("entry_date"),
                "entry_price_local": h.get("entry_price_local"),
                "entry_price_eur": h.get("entry_price_eur"),
                "exit_date": h.get("exit_date"),
                "exit_price_local": h.get("exit_price_local"),
                "exit_price_eur": h.get("exit_price_eur"),
                "forward_return_pct": h.get("forward_return_pct"),
                "changed": changed,
            })
        # WARNING, not info: uvicorn hides `info` in production, and this line is the record that
        # somebody re-priced a book by hand and what it moved.
        logging.getLogger(__name__).warning(
            "[reprice] strategy %s -> snapshot %s: %d of %d holding(s) changed",
            strategy_id, snapshot_id, n_changed, len(out))
        return {"strategy_id": strategy_id, "snapshot_id": snapshot_id, "holdings": out,
                "changed_holdings": n_changed,
                "note": None if n_changed else "Every price was already current."}

    return await asyncio.to_thread(_run)
