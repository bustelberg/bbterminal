"""Phase 4 — momentum compute (current-picks snapshots).

Per-strategy isolation pattern: a single failing strategy never aborts the
phase; each result lands as an entry in `ingest_run.momentum_summary` with a
full traceback on failure.

  _run_momentum_phase        per enabled `scheduled_strategy`, either a fresh
                             rebalance (when due) or a price update on the
                             last rebalance's holdings.
  _run_smart_momentum_phase  thin wrapper that drives `_run_momentum_phase`
                             with the smart plan's per-strategy due decision
                             + same-period dedup on the price updates.

Heavy imports (pandas-pulling momentum stream, BacktestRequest) are kept
function-local so importing this module stays cheap at boot.
"""
from __future__ import annotations

import asyncio
import json as _json
import logging
from datetime import datetime, timezone

from deps import supabase

from .runlog import _Throttle, _now_utc_iso, _update_run


def _run_momentum_phase(
    run_id: int,
    *,
    due_override: dict[int, bool] | None = None,
    dedupe_price_updates: bool = False,
    include_rebalances: bool = True,
    include_price_updates: bool = True,
) -> None:
    """Phase 3 — compute current-portfolio holdings for every enabled row
    in `scheduled_strategy`. Each strategy gets its own
    `current_picks_snapshot` tagged with `ingest_run_id` +
    `scheduled_strategy_id`, so the /schedule per-strategy detail view
    can JOIN them back to this run.

    Each scheduled_strategy carries its own `config` (BacktestRequest
    payload) and `frequency`. The phase only computes strategies whose
    `next_due_at` is in the past (or NULL — fresh entries). After a
    successful compute the row's `last_run_at` is set to now and
    `next_due_at` is advanced per `frequency` + the baked
    `rebalance_weekday` (see
    `momentum.schedule.compute_next_due_at`).

    `due_override` (smart pipeline) supplies the per-strategy due decision
    from the derived plan instead of re-reading `next_due_at` — keeping the
    pipeline's behaviour identical to the plan the UI shows. When a
    strategy isn't in the map it falls back to the `next_due_at` check.

    `dedupe_price_updates` (smart pipeline) makes a non-due strategy's
    price-update behave like the daily MTD refresh: if an identical
    snapshot already exists for the same open period + latest-price-date,
    the freshly-inserted one is deleted so re-running the tick doesn't
    grow the history.

    Per-strategy isolation: a single failing strategy doesn't abort
    the phase. Each result lands as a `templates_summary`-style entry
    in `ingest_run.momentum_summary` (with full Python traceback on
    failure, for debugging from /schedule). If ANY strategy errored,
    the phase raises a summarized error so the outer pipeline marks
    the run `error` — but every successful snapshot is still
    persisted, and every strategy's `next_due_at` is still bumped on
    success."""
    log = logging.getLogger(__name__)
    now_iso = _now_utc_iso()

    # Pull EVERY enabled scheduled strategy. Each one will produce
    # exactly one snapshot per tick — a fresh rebalance if it's due
    # (`next_due_at` past), otherwise a `price_update` on the last
    # rebalance's holdings.
    sched_resp = (
        supabase.table("scheduled_strategy")
        .select("id, name, frequency, config, enabled, last_run_at, next_due_at")
        .eq("enabled", True)
        .order("created_at")
        .execute()
    )
    scheduled = sched_resp.data or []
    if not scheduled:
        log.info(
            "[pipeline.momentum] run_id=%s no scheduled strategies — skipping",
            run_id,
        )
        _update_run(run_id, momentum_summary=[])
        return

    # Imports are local so the module loads cheaply at boot — the momentum
    # stream pulls in pandas/numpy etc.
    import traceback as _traceback  # noqa: PLC0415
    from routers.momentum.backtest_stream.models import BacktestRequest  # noqa: PLC0415
    from routers.momentum.backtest_stream.stream import (  # noqa: PLC0415
        _momentum_backtest_stream,
    )
    from momentum.schedule import (  # noqa: PLC0415
        compute_next_due_at as _compute_next_due_at,
    )
    from routers._schedule_snapshots import (  # noqa: PLC0415
        compute_and_save_price_update as _compute_and_save_price_update,
    )
    from routers.momentum._helpers import strategy_hash as _sel_hash  # noqa: PLC0415

    summaries: list[dict] = []
    errors: list[str] = []
    total = len(scheduled)
    # Shared-selection memo for this run: the momentum stock pick is fully
    # determined by `strategy_hash` (signal/category weights, top-N, universe,
    # selection mode …) — NOT by the ETF overlay or cash sleeve. So multiple
    # scheduled strategies that run the SAME base strategy and only differ in
    # their ETF/cash produce the identical selection. We compute that heavy
    # selection ONCE per hash and clone it for the rest, applying each strategy's
    # own overlay/cash on top: one calc rebalances them all.
    base_by_hash: dict[str, dict] = {}

    for idx, sched in enumerate(scheduled, start=1):
        strategy_id = sched["id"]
        strategy_name = sched.get("name") or f"Strategy #{strategy_id}"
        frequency = sched.get("frequency")
        next_due_iso = sched.get("next_due_at")
        # "Due to rebalance" — the derived plan decides when supplied
        # (smart pipeline); otherwise first-run (next_due_at IS NULL) or
        # the next-due tick has arrived. Not due → price update on the last
        # rebalance's holdings.
        if due_override is not None and strategy_id in due_override:
            is_due_to_rebalance = due_override[strategy_id]
        else:
            is_due_to_rebalance = (next_due_iso is None) or (next_due_iso <= now_iso)
        # Apply the op gates. The split pipeline runs each concern alone:
        #   price-update op  (include_rebalances=False): a due strategy still
        #       just gets re-priced here — the separate rebalance op does its
        #       actual rebalance — so MTD stays fresh regardless of due-ness.
        #   rebalance op     (include_price_updates=False): non-due strategies
        #       are skipped entirely rather than re-priced (price-update op
        #       owns that), so this op only ever rebalances the due set.
        do_rebalance = is_due_to_rebalance and include_rebalances
        if not do_rebalance and not include_price_updates:
            continue
        kind = "rebalance" if do_rebalance else "price_update"
        _update_run(
            run_id,
            current_message=(
                f"Strategy {idx} of {total} · "
                f"{'rebalancing' if do_rebalance else 'price-updating'}: {strategy_name}…"
            ),
        )

        entry: dict = {
            "strategy_id": strategy_id,
            "strategy_name": strategy_name,
            "frequency": frequency,
            "kind": kind,
            "config": sched.get("config") or {},
            "snapshot_id": None,
            "holdings_count": 0,
            "latest_price_date": None,
            "status": "error",
            "error_message": None,
            "error_traceback": None,
        }

        # ── Branch A: not due → price update on last rebalance ────
        if not do_rebalance:
            try:
                snapshot_id = _compute_and_save_price_update(
                    strategy_id=strategy_id,
                    ingest_run_id=run_id,
                    is_backfill=False,
                    # Live cash % from the strategy config (not the rebalance
                    # snapshot's), so an admin's cash change applies on this tick.
                    cash_pct=float((sched.get("config") or {}).get("cash_pct") or 0.0),
                )
                if snapshot_id is None:
                    # No prior rebalance to price-update from. The very
                    # first tick after add should always be a rebalance,
                    # so this is a strange-but-non-fatal state.
                    entry["status"] = "ok"
                    entry["error_message"] = "No prior rebalance to price-update from"
                else:
                    # Hydrate the entry summary from the fresh snapshot.
                    pu_resp = supabase.table("current_picks_snapshot").select(
                        "as_of_date, holdings, latest_price_date"
                    ).eq("snapshot_id", snapshot_id).limit(1).execute()
                    pu = (pu_resp.data or [{}])[0]
                    entry["status"] = "ok"
                    # Same-period dedup (smart pipeline): if an identical
                    # snapshot already exists for this strategy's open
                    # period + latest-price-date, drop the redundant new
                    # one so re-running the tick doesn't grow history.
                    deduped_to = (
                        _dedupe_price_update(strategy_id, snapshot_id, pu)
                        if dedupe_price_updates else None
                    )
                    if deduped_to is not None:
                        entry["snapshot_id"] = deduped_to
                        entry["latest_price_date"] = pu.get("latest_price_date")
                        entry["error_message"] = (
                            f"no change since prior snapshot for period "
                            f"as_of={pu.get('as_of_date')} "
                            f"(lpd={pu.get('latest_price_date')}) — skipped"
                        )
                    else:
                        entry["snapshot_id"] = snapshot_id
                        entry["holdings_count"] = len(pu.get("holdings") or [])
                        entry["latest_price_date"] = pu.get("latest_price_date")
                # Bump last_run_at (but NOT next_due_at — the strategy
                # didn't actually rebalance, it just got re-priced).
                supabase.table("scheduled_strategy").update({
                    "last_run_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                    "updated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                }).eq("id", strategy_id).execute()
            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                tb = _traceback.format_exc()
                entry["error_message"] = msg
                entry["error_traceback"] = tb
                errors.append(f"[{strategy_name}] {msg}")
                log.warning(
                    "[pipeline.momentum] run_id=%s strategy=%s price_update failed: %s\n%s",
                    run_id, strategy_name, msg, tb,
                )

            summaries.append(entry)
            _update_run(run_id, momentum_summary=summaries)
            continue

        # ── Branch B: due → fresh rebalance ───────────────────────
        try:
            cfg = dict(sched.get("config") or {})
            if not cfg:
                raise RuntimeError(
                    f"Scheduled strategy #{strategy_id} has no config"
                )
            # Pipeline-only overrides — the saved config is the user's
            # intent; we only force the mode/cache flags so it computes
            # a fresh current-portfolio snapshot.
            #
            # db_only=True: the rebalance JUST RUNS THE CALCULATION against the
            # prices already in the DB — it never triggers the per-company
            # GuruFocus ensure-fetch loop (db_only=False). Fetching prices is the
            # price_update op's / month-end refresh's job; a rebalance must not
            # hang on 1,479 API calls. Companies with stale/missing DB prices are
            # dropped by signals.py's 30-day staleness guard (the rebalance op
            # already surfaces a non-blocking freshness warning for those).
            cfg["mode"] = "current_portfolio"
            cfg["force_recompute"] = True
            cfg["db_only"] = True
            cfg.pop("variants", None)
            cfg.pop("n_trials", None)
            try:
                req = BacktestRequest(**cfg)
            except Exception as e:
                raise RuntimeError(
                    f"Scheduled strategy config doesn't validate as BacktestRequest: {e}"
                )

            snapshot_id: int | None = None
            stream_err: str | None = None
            holdings_count = 0
            latest_price_date: str | None = None

            base_hash = _sel_hash(req)
            if base_hash in base_by_hash:
                # Another due strategy this run already computed this exact
                # selection (they differ only by ETF overlay / cash). Clone its
                # base stock snapshot instead of re-running the whole momentum
                # stream — the ETF overlay + cash below are applied per-strategy,
                # so each still gets its own correct snapshot.
                base = base_by_hash[base_hash]
                snapshot_id = _clone_rebalance_snapshot(
                    base, strategy_id, run_id, config=sched.get("config") or {},
                )
                holdings_count = len(base.get("holdings") or [])
                latest_price_date = base.get("latest_price_date")
                _update_run(run_id, current_message=(
                    f"Strategy {idx} of {total} · reusing shared selection for "
                    f"{strategy_name} (same picks as another strategy)…"))
            else:
                msg_throttle = _Throttle()

                async def _drain() -> None:
                    nonlocal snapshot_id, stream_err, holdings_count, latest_price_date
                    async for chunk in _momentum_backtest_stream(req):
                        if not isinstance(chunk, str) or not chunk.startswith("data: "):
                            continue
                        try:
                            evt = _json.loads(chunk[len("data: "):].strip())
                        except _json.JSONDecodeError:
                            continue
                        t = evt.get("type")
                        if t == "progress":
                            m = evt.get("message")
                            if m and msg_throttle.should_write():
                                await asyncio.to_thread(
                                    _update_run,
                                    run_id,
                                    current_message=f"[{idx}/{total} {strategy_name}] {m}",
                                )
                        elif t == "current_portfolio":
                            payload = evt.get("data") or {}
                            snapshot_id = payload.get("snapshot_id")
                            holdings_count = len(payload.get("holdings") or [])
                            latest_price_date = payload.get("latest_price_date")
                        elif t == "error":
                            stream_err = evt.get("message") or "unknown error"

                asyncio.run(_drain())

                if stream_err:
                    raise RuntimeError(stream_err)
                if snapshot_id is None:
                    raise RuntimeError("Momentum compute finished without persisting a snapshot")

                # Capture the PURE stock selection (before the ETF overlay/cash
                # below mutates this snapshot) so strategies later in this run
                # that share the selection can clone it instead of recomputing.
                base_by_hash[base_hash] = _read_base_snapshot(snapshot_id)

            # Tag the snapshot with the pipeline run + scheduled
            # strategy it came from, and re-tag as 'auto' (the SSE flow
            # inside the stream writes 'manual').
            try:
                supabase.table("current_picks_snapshot").update({
                    "triggered_by": "auto",
                    "ingest_run_id": run_id,
                    "scheduled_strategy_id": strategy_id,
                }).eq("snapshot_id", snapshot_id).execute()
            except Exception as e:
                log.warning(
                    "[pipeline.momentum] run_id=%s failed to tag snapshot=%s: %s: %s",
                    run_id, snapshot_id, type(e).__name__, e,
                )

            # Advance the schedule clock: mark this strategy as just
            # ran + compute its next due tick from the frequency. Best-
            # effort — a checkpoint write failure here doesn't roll back
            # the snapshot.
            try:
                ran_at = datetime.now(timezone.utc).replace(microsecond=0)
                weekday = int(cfg.get("rebalance_weekday", 0) or 0)
                # Advance the clock relative to the GRID date we rebalanced for
                # (the snapshot's as_of_date), NOT `now`. When the tick fires
                # early (Saturday, for the upcoming Monday) `now` is before the
                # grid date, so computing "next after now" would re-pick the same
                # grid date and re-rebalance every day until it. The grid date
                # advances to the next period.
                ref = ran_at
                try:
                    s = (
                        supabase.table("current_picks_snapshot")
                        .select("as_of_date").eq("snapshot_id", snapshot_id)
                        .limit(1).execute()
                    )
                    grid_iso = (s.data or [{}])[0].get("as_of_date")
                    if grid_iso:
                        ref = datetime.fromisoformat(str(grid_iso)[:10]).replace(tzinfo=timezone.utc)
                except Exception:
                    pass
                next_due = (
                    _compute_next_due_at(frequency, ref, weekday).isoformat()
                    if frequency else None
                )
                supabase.table("scheduled_strategy").update({
                    "last_run_at": ran_at.isoformat(),
                    "next_due_at": next_due,
                    "updated_at": ran_at.isoformat(),
                }).eq("id", strategy_id).execute()
            except Exception as e:
                log.warning(
                    "[pipeline.momentum] run_id=%s strategy=%s failed to bump schedule clock: %s: %s",
                    run_id, strategy_name, type(e).__name__, e,
                )

            # Blended variant: append the ETF overlay holdings (negative
            # company_id) to the freshly-rebalanced stock picks, scale the
            # stock weights to the strategy sleeve, and recompute the
            # blended period return. The daily price_update job re-prices
            # both companies and ETFs thereafter.
            if cfg.get("etf_overlay"):
                try:
                    new_count = _apply_etf_overlay_to_snapshot(
                        int(snapshot_id), cfg["etf_overlay"]
                    )
                    if new_count is not None:
                        holdings_count = new_count
                except Exception as e:
                    log.warning(
                        "[pipeline.momentum] run_id=%s strategy=%s ETF overlay failed: %s: %s",
                        run_id, strategy_name, type(e).__name__, e,
                    )

            # Cash sleeve: scale every holding (stocks + any ETF overlay) by
            # (1-cash) and append a flat 0%-return cash holding, so the reported
            # weights + return pick up the cash drag. Sits on top of the overlay.
            cash_pct = float(cfg.get("cash_pct") or 0.0)
            if cash_pct > 0:
                try:
                    n_cash = _apply_cash_to_snapshot(int(snapshot_id), cash_pct)
                    if n_cash is not None:
                        holdings_count = n_cash
                except Exception as e:
                    log.warning(
                        "[pipeline.momentum] run_id=%s strategy=%s cash sleeve failed: %s: %s",
                        run_id, strategy_name, type(e).__name__, e,
                    )

            entry["snapshot_id"] = snapshot_id
            entry["holdings_count"] = holdings_count
            entry["latest_price_date"] = latest_price_date
            entry["status"] = "ok"
            log.info(
                "[pipeline.momentum] run_id=%s strategy=%s snapshot=%s holdings=%s",
                run_id, strategy_name, snapshot_id, holdings_count,
            )
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            tb = _traceback.format_exc()
            entry["error_message"] = msg
            entry["error_traceback"] = tb
            errors.append(f"[{strategy_name}] {msg}")
            log.warning(
                "[pipeline.momentum] run_id=%s strategy=%s failed: %s\n%s",
                run_id, strategy_name, msg, tb,
            )

        summaries.append(entry)
        # Persist incremental progress so the UI sees each strategy
        # land as it completes, not only after the whole phase is done.
        _update_run(run_id, momentum_summary=summaries)

    if errors:
        raise RuntimeError(
            f"{len(errors)} of {total} strategies failed: " + " | ".join(errors[:3])
        )


def _read_base_snapshot(snapshot_id: int) -> dict:
    """Read the fields needed to CLONE a freshly-computed rebalance snapshot for
    another strategy sharing its selection. Called right after the momentum
    stream persists it, so `holdings` are the PURE stock picks (no ETF overlay /
    cash applied yet)."""
    r = (
        supabase.table("current_picks_snapshot")
        .select("holdings, as_of_date, latest_price_date, daily_picks, strategy_hash, name")
        .eq("snapshot_id", snapshot_id)
        .limit(1)
        .execute()
    )
    return (r.data or [{}])[0]


def _clone_rebalance_snapshot(
    base: dict, strategy_id: int, run_id: int, *, config: dict,
) -> int:
    """Insert a new `rebalance` snapshot for `strategy_id` from a shared base
    selection (same stock picks as another strategy this run). Copies the base's
    holdings + as_of/latest dates + daily_picks + strategy_hash, tagged to this
    strategy + run. The caller then applies THIS strategy's ETF overlay + cash on
    top, so the clone becomes its own correct blended snapshot. `current_picks_day`
    rows aren't duplicated — they're keyed by `strategy_hash`, shared across the
    group, and the base compute already wrote them.

    Note `config` is THIS strategy's own saved config (its ETF overlay / cash),
    not the base's — so the price-update re-pricer's `cash_pct` fallback reads the
    right value for this strategy."""
    row = {
        "triggered_by": "auto",
        "as_of_date": base.get("as_of_date"),
        "latest_price_date": base.get("latest_price_date"),
        "config": config,
        "holdings": base.get("holdings") or [],
        "daily_picks": base.get("daily_picks") or [],
        "strategy_hash": base.get("strategy_hash"),
        "name": base.get("name"),
        "kind": "rebalance",
        "is_backfill": False,
        "ingest_run_id": run_id,
        "scheduled_strategy_id": strategy_id,
    }
    ins = supabase.table("current_picks_snapshot").insert(row).execute()
    return int(ins.data[0]["snapshot_id"])


def _apply_cash_to_snapshot(snapshot_id: int, cash_pct: float) -> int | None:
    """Apply a cash sleeve to a stored snapshot's holdings IN PLACE: strip any
    existing cash, scale the rest by (1-cash), append the flat 0%-return cash
    holding, and recompute `period_return_pct` off the single source of truth.
    Returns the new holdings count (None if the snapshot is gone). Idempotent."""
    from momentum.portfolio_math import (  # noqa: PLC0415
        apply_cash_allocation,
        portfolio_eur_return_pct,
    )
    snap = (
        supabase.table("current_picks_snapshot")
        .select("holdings")
        .eq("snapshot_id", snapshot_id)
        .limit(1)
        .execute()
    ).data
    if not snap:
        return None
    holdings = apply_cash_allocation(snap[0].get("holdings") or [], cash_pct)
    supabase.table("current_picks_snapshot").update({
        "holdings": holdings,
        "period_return_pct": portfolio_eur_return_pct(holdings),
    }).eq("snapshot_id", snapshot_id).execute()
    return len(holdings)


def _apply_etf_overlay_to_snapshot(snapshot_id: int, etf_overlay: list[dict]) -> int | None:
    """Rewrite a freshly-rebalanced snapshot's holdings to the BLEND: scale the
    momentum stock picks to the strategy sleeve and append one ETF holding
    (negative company_id) per overlay entry, priced from `benchmark_price`
    (entry at the rebalance `as_of_date`, exit at `latest_price_date`). Recompute
    `period_return_pct` as the weighted blend. Returns the new holdings count
    (None if there's nothing to do)."""
    import bisect  # noqa: PLC0415

    from momentum.blend_backtest import make_etf_holding, scale_stock_weights  # noqa: PLC0415

    overlay = [o for o in (etf_overlay or []) if o.get("benchmark_id")]
    if not overlay:
        return None
    snap_resp = (
        supabase.table("current_picks_snapshot")
        .select("holdings, as_of_date, latest_price_date")
        .eq("snapshot_id", snapshot_id)
        .limit(1)
        .execute()
    )
    if not snap_resp.data:
        return None
    snap = snap_resp.data[0]
    # Already blended (idempotent) — bail if any ETF holding is present.
    stock_holdings = [h for h in (snap.get("holdings") or []) if (h.get("company_id") or 0) >= 0]
    as_of = str(snap.get("as_of_date") or "")[:10]
    latest = str(snap.get("latest_price_date") or as_of)[:10]
    # ETF entry must anchor to the SAME bar the stock sleeve entered on — the
    # prior trading day's close the picks are anchored to (`run_current_portfolio`
    # enters stocks at `_price_on_or_before(rebalance_date − 1)`), NOT the raw
    # `as_of`. `as_of` is the nominal rebalance grid date, which can be a FUTURE
    # Monday when the tick fires early (Saturday, for the upcoming Monday). Using
    # it stamped a future ETF entry_date and priced entry against a not-yet-real
    # (or corrupt future) benchmark bar — the SPMO +277% incident. Take the stock
    # sleeve's actual entry_date (they all share the prior-trading-day anchor);
    # fall back to `latest` (the freshest real close, never the future) for a
    # pure-ETF strategy with no stock holdings.
    stock_entry_dates = [
        str(h.get("entry_date"))[:10] for h in stock_holdings if h.get("entry_date")
    ]
    entry_ref = min(stock_entry_dates) if stock_entry_dates else latest
    if latest and entry_ref > latest:
        entry_ref = latest  # never price entry past the latest real close

    bids = [int(o["benchmark_id"]) for o in overlay]
    meta_resp = (
        supabase.table("benchmark")
        .select("benchmark_id, ticker, name, sector, currency")
        .in_("benchmark_id", bids)
        .execute()
    )
    meta = {m["benchmark_id"]: m for m in (meta_resp.data or [])}

    # Daily benchmark closes per id, for as-of (last-on-or-before) lookups.
    px: dict[int, tuple[list[str], list[float]]] = {}
    for bid in bids:
        rows = (
            supabase.table("benchmark_price")
            .select("target_date, price")
            .eq("benchmark_id", bid)
            .lte("target_date", latest or as_of)
            .order("target_date")
            .execute()
        ).data or []
        if rows:
            px[bid] = ([str(r["target_date"])[:10] for r in rows], [float(r["price"]) for r in rows])

    def _asof(bid: int, day: str) -> float | None:
        pair = px.get(bid)
        if not pair or not day:
            return None
        ds, ps = pair
        i = bisect.bisect_right(ds, day) - 1
        return ps[i] if i >= 0 else None

    total_etf_pct = sum(float(o.get("weight_pct") or 0.0) for o in overlay)
    strat_w = max(0.0, 1.0 - total_etf_pct / 100.0)

    etf_holdings: list[dict] = []
    for o in overlay:
        bid = int(o["benchmark_id"])
        m = meta.get(bid) or {}
        etf_holdings.append(make_etf_holding(
            benchmark_id=bid,
            ticker=m.get("ticker") or f"BM{bid}",
            name=m.get("name") or m.get("ticker") or f"Benchmark {bid}",
            sector=m.get("sector"),
            weight=float(o.get("weight_pct") or 0.0) / 100.0,
            entry_price=_asof(bid, entry_ref),
            exit_price=_asof(bid, latest),
            entry_date=entry_ref or None,
            exit_date=latest or None,
            currency=m.get("currency"),
        ))

    merged = scale_stock_weights(stock_holdings, strat_w) + etf_holdings

    # Blended period return via the SINGLE source of truth (weighted per-holding
    # `forward_return_pct`) — identical basis to compute_and_save_price_update.
    from momentum.portfolio_math import portfolio_eur_return_pct  # noqa: PLC0415
    period_return = portfolio_eur_return_pct(merged)

    supabase.table("current_picks_snapshot").update({
        "holdings": merged,
        "period_return_pct": period_return,
    }).eq("snapshot_id", snapshot_id).execute()
    return len(merged)


def _price_update_marks_sig(holdings: list[dict] | None) -> tuple:
    """Order-independent signature of the per-holding marks that DEFINE a
    price_update snapshot: (company_id, side, exit_date, exit_price_local,
    forward_return_pct). Two snapshots with the same signature are genuine
    duplicates; a different signature means a holding's mark moved."""
    out: list[tuple] = []
    for h in holdings or []:
        exit_local = h.get("exit_price_local")
        fwd = h.get("forward_return_pct")
        out.append((
            h.get("company_id"),
            h.get("side") or "long",
            str(h.get("exit_date") or ""),
            round(float(exit_local), 6) if exit_local is not None else None,
            round(float(fwd), 4) if fwd is not None else None,
        ))
    out.sort(key=lambda t: (t[0] if t[0] is not None else 0, t[1]))
    return tuple(out)


def _dedupe_price_update(strategy_id: int, new_snapshot_id: int, new_row: dict) -> int | None:
    """Same-period dedup, shared by the smart momentum phase + daily MTD.

    Deletes the just-inserted snapshot and returns the surviving one ONLY when an
    existing snapshot for the SAME strategy + open period (`as_of_date`) +
    `latest_price_date` ALSO has identical per-holding marks. The holdings check
    is essential, not cosmetic: `latest_price_date` is the MAX exit date across
    holdings, so a lagging ETF catching up to a company's already-max date leaves
    the max unchanged while its OWN price/return moved — the date-only key wrongly
    treated that as a no-op and DELETED the fresh snapshot, freezing the ETF's
    mark on every affected strategy. Returns None when there's no true duplicate
    (the new row stands)."""
    new_as_of = new_row.get("as_of_date")
    new_lpd = new_row.get("latest_price_date")
    if not new_as_of or not new_lpd:
        return None
    dup = (
        supabase.table("current_picks_snapshot")
        .select("snapshot_id, holdings")
        .eq("scheduled_strategy_id", strategy_id)
        .eq("as_of_date", new_as_of)
        .eq("latest_price_date", new_lpd)
        .neq("snapshot_id", new_snapshot_id)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    if not dup.data:
        return None
    prior = dup.data[0]
    if _price_update_marks_sig(new_row.get("holdings")) != _price_update_marks_sig(prior.get("holdings")):
        # A mark actually changed (e.g. a lagging ETF caught up) — the snapshots
        # only SHARE a max date; they are NOT duplicates. Keep the fresh one.
        return None
    surviving = int(prior["snapshot_id"])
    supabase.table("current_picks_snapshot").delete().eq(
        "snapshot_id", new_snapshot_id
    ).execute()
    return surviving


def _run_smart_momentum_phase(run_id: int, plan) -> None:
    """Phase 4 for the smart pipeline. Drives `_run_momentum_phase` with the
    derived plan's per-strategy due decision (so the pipeline rebalances
    exactly the strategies the plan marked due) and the daily-MTD dedup on
    the non-due price updates (so re-running the tick doesn't grow history).

    `plan` is a `planner.SmartPlan`."""
    due = {sp.strategy_id: sp.is_due for sp in plan.strategies}
    _run_momentum_phase(run_id, due_override=due, dedupe_price_updates=True)
