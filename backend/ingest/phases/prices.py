"""Refresh company prices and volumes through Yahoo and build job company lists."""
from __future__ import annotations

import logging
from datetime import date

from deps import fetch_in_chunks, supabase

from ..staleness import trading_days_between
from .runlog import _update_run, log_step


def _load_all_companies() -> list[dict]:
    """Paginate the `company` table, returning rows usable by ensure_*. Rows
    without a ticker or an exchange code are dropped (nothing to fetch).

    Result is sorted "most-stale first": companies with NO close_price data
    come first, then companies whose latest close_price target_date is
    oldest. This guarantees that on every run the genuinely-missing data
    (the rows that drive the /backtest "N companies have NO price data"
    warning) gets fetched in the first few minutes rather than after the
    full universe has been re-checked. Already-fresh companies still get
    touched at the end of the run via the fast db_max freshness short-circuit
    in `ensure_*_for_company`, so this ordering doesn't drop any work."""
    out: list[dict] = []
    offset = 0
    page = 1000
    while True:
        resp = (
            supabase.table("company")
            .select(
                "company_id, gurufocus_ticker, delisted_at, out_of_scope_at, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            )
            .is_("delisted_at", "null")
            .is_("out_of_scope_at", "null")
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        for r in batch:
            exch = (r.get("gurufocus_exchange") or {}).get("exchange_code") or ""
            ticker = r.get("gurufocus_ticker") or ""
            if not ticker or not exch:
                continue
            out.append({
                "cid": int(r["company_id"]),
                "ticker": ticker,
                "exchange": exch,
            })
        if len(batch) < page:
            break
        offset += page

    # Bound database work per statement and reuse the already-loaded IDs.
    try:
        latest_by_cid = _latest_close_dates_for([c["cid"] for c in out])
        out.sort(key=lambda c: latest_by_cid.get(c["cid"], ""))
    except Exception as e:
        logging.getLogger(__name__).warning(
            "[_load_all_companies] could not fetch latest close_price dates, "
            "falling back to insertion order: %s: %s",
            type(e).__name__, e,
        )
    return out


def _run_prices_phase(
    run_id: int,
    accumulated_errors: list[str],
    companies_override: list[dict] | None = None,
    budget_by_region: dict[str, int] | None = None,
) -> None:
    """Refresh company market data from Yahoo through the asset pipeline.

    This replaced the former GuruFocus price/volume phase.  Existing mappings
    are gap-refreshed; unresolved ISINs are sent through the single throttled
    resolver, which writes both close and volume into ``asset_price``.
    ``budget_by_region`` is intentionally ignored: Yahoo has no subscription
    regions or per-region quota.
    """
    from asset_pipeline import price_refresh, queue  # noqa: PLC0415

    def _step(message: str, *, level: str = "info") -> None:
        log_step(run_id, message, level=level, phase="prices")

    companies = companies_override if companies_override is not None else _load_all_companies()
    cids = sorted({int(c["cid"]) for c in companies if c.get("cid") is not None})
    if not cids:
        _update_run(run_id, current_message="No companies to refresh.")
        return
    isins: list[str] = []
    for chunk in (cids[i:i + 800] for i in range(0, len(cids), 800)):
        rows = (supabase.table("company").select("isin").in_("company_id", chunk).execute().data or [])
        isins.extend(str(r["isin"]).upper() for r in rows if r.get("isin"))
    _update_run(run_id, companies_total=len(cids),
                current_message=f"Refreshing Yahoo prices for {len(isins)} company ISINs…")
    _step("Yahoo market-data refresh started. No GuruFocus quota exhausted or 403 unsubscribed "
          "states apply; unmapped/delisted instruments are reported by the resolver.")
    queue.enqueue(isins, skip_existing=True)
    resolved = queue.process_slice(limit=len(isins), isins=isins)
    scoped = set(isins)
    backfilled = price_refresh.backfill_missing(scoped)
    refreshed = price_refresh.refresh_stale(held_only=False, isins=scoped, sleep_s=0)
    errors = int(resolved.get("failed", 0)) + int(backfilled.get("failed", 0)) + int(refreshed.get("failed", 0))
    _update_run(run_id, companies_processed=len(cids), prices_refreshed=(
        int(resolved.get("ok", 0)) + int(backfilled.get("backfilled", 0)) + int(refreshed.get("moved", 0))),
        volumes_refreshed=(int(resolved.get("ok", 0)) + int(backfilled.get("backfilled", 0)) + int(refreshed.get("moved", 0))),
        forbidden_count=0, delisted_count=int(resolved.get("unmapped", 0)), error_count=errors,
        current_message=(f"Yahoo refresh complete: {refreshed.get('moved', 0)} updated, "
                         f"{resolved.get('ok', 0)} resolved, {errors} errors."))
    if errors:
        failures = [
            *resolved.get("failures", []),
            *backfilled.get("failures", []),
            *(row for row in refreshed.get("results", []) if row.get("status") == "error"),
        ]
        samples = []
        for row in failures:
            instrument = " / ".join(filter(None, (row.get("symbol"), row.get("isin")))) or "Unknown instrument"
            message = f"{instrument}: {row.get('error') or 'Unknown Yahoo error'}"
            _step(message, level="error")
            if len(samples) < 3:
                samples.append(message[:240])
        summary = f"Yahoo price refresh had {errors} failed instrument(s)."
        if samples:
            summary += "\n" + "\n".join(samples)
        accumulated_errors.append(summary)
    _step(f"Yahoo refresh complete: {refreshed.get('moved', 0)} updated, "
          f"{refreshed.get('unchanged', 0)} unchanged (already current or no newer stored close), "
          f"{resolved.get('unmapped', 0)} delisted or unmapped, {errors} errors.",
          level="error" if errors else "info")


def _refresh_price_coverage(step=None) -> None:
    """Re-aggregate `company_price_coverage` — the GuruFocus price/volume summary `asset_grid`
    reads to show its `gf_price_*` columns.

    Refreshed HERE because this phase is the only thing that writes `metric_data` price/volume
    rows, so the summary is stale for exactly as long as it takes this call to run and never
    longer. A scheduled refresh on its own timer would drift from the writer and serve a
    confident, out-of-date "last close" — the failure that looks like data rather than absence.

     BEST-EFFORT ON PURPOSE. The prices themselves are already committed by the time we get
    here; a failure to refresh a derived summary must not fail the run or mask the price
    counters above it. Logged at WARNING (uvicorn leaves the root logger at WARNING, so `info`
    is invisible in production) and reported as a step, so a silently stale grid is impossible.

    `step` is the phase's own reporter, passed in — it is a closure over the run, not a module
    global, so the alternative would be a second reporting path that could drift from it.
    """
    logger = logging.getLogger(__name__)
    try:
        supabase.rpc("refresh_company_price_coverage").execute()
        logger.info("[pipeline.prices] company_price_coverage refreshed")
    except Exception as e:  # noqa: BLE001
        logger.warning("[pipeline.prices] company_price_coverage refresh FAILED: %s: %s",
                       type(e).__name__, e)
        if step is not None:
            step(f"GuruFocus coverage summary not refreshed ({type(e).__name__}) — "
                 "the /asset-pipeline gf_price_* columns may lag until the next run.",
                 level="error")


def _checkpoint(run_id: int, snap: dict, total: int | None = None) -> None:
    """Periodic STRUCTURED-counter write, once per company. Best-effort — a
    transient blip is harmless; the next one (or the final summary) catches up.
    The human-readable `current_message` is owned by the phase's heartbeat
    thread (rate / ETA / in-flight companies / stall warning), so this only
    keeps the numeric counters the UI reads current between beats — writing a
    message here too would just flicker over the richer heartbeat line."""
    _update_run(
        run_id,
        companies_processed=snap["processed"],
        prices_refreshed=snap["prices"],
        volumes_refreshed=snap["volumes"],
        forbidden_count=snap["forbidden"],
        delisted_count=snap["delisted"],
        error_count=snap["errors"],
    )


def _pool_held_holding_ids() -> set[int]:
    """All `company_id`s (BOTH signs) in the latest snapshot of every ENABLED
    scheduled strategy. Positive = real companies (priced from `metric_data`);
    negative = `-benchmark_id` (synthetic ETF-overlay holdings, priced from
    `benchmark_price`). The shared base for the held-company + held-benchmark
    collectors."""
    strat_resp = (
        supabase.table("scheduled_strategy")
        .select("id")
        .eq("enabled", True)
        .execute()
    )
    sched_ids = [r["id"] for r in (strat_resp.data or [])]
    if not sched_ids:
        return set()

    snap_resp = (
        supabase.table("current_picks_snapshot")
        .select("scheduled_strategy_id, holdings, created_at")
        .in_("scheduled_strategy_id", sched_ids)
        .order("created_at", desc=True)
        .execute()
    )
    ids: set[int] = set()
    seen: set[int] = set()
    for s in (snap_resp.data or []):
        sid = s.get("scheduled_strategy_id")
        if sid is None or sid in seen:
            continue
        seen.add(sid)
        for h in (s.get("holdings") or []):
            cid = h.get("company_id")
            if cid is not None:
                ids.add(int(cid))
    return ids


def _collect_held_company_ids() -> set[int]:
    """The real companies (positive ids) held across enabled strategies —
    priced from `metric_data`. Shared by the held-price refresh + the
    stale-price retry probe."""
    return {c for c in _pool_held_holding_ids() if c >= 0}


def _collect_held_benchmark_ids() -> set[int]:
    """The benchmark ids behind the ETF-overlay holdings (negative company_id =
    `-benchmark_id`) held across enabled strategies — priced from
    `benchmark_price`."""
    return {-c for c in _pool_held_holding_ids() if c < 0}


def refresh_held_benchmarks(run_id: int) -> int:
    """Re-fetch the latest prices for every ETF-overlay benchmark held across
    enabled strategies, so a held ETF's `benchmark_price` stays as fresh as the
    held companies' `metric_data`. Without this the price-update kept only the
    real companies current (it drops negative/benchmark ids), leaving the ETF
    rows stale — a stale `exit_date`/return in the current-portfolio card.

    Best-effort per benchmark (a fetch/upsert failure is logged + skipped).
    Returns the count refreshed."""
    from ingest.constants import DATA_CUTOFF  # noqa: PLC0415
    from asset_pipeline.yahoo import YahooThrottled, closed_daily_bars  # noqa: PLC0415

    bids = _collect_held_benchmark_ids()
    if not bids:
        return 0
    log = logging.getLogger(__name__)
    refreshed = 0
    for bid in bids:
        try:
            bm = (
                supabase.table("benchmark")
                .select("ticker")
                .eq("benchmark_id", bid)
                .limit(1)
                .execute()
            )
            if not bm.data:
                continue
            ticker = bm.data[0]["ticker"]
            bars = closed_daily_bars(ticker)
            if not bars:
                log.warning("[price_update] benchmark %s (%s) Yahoo returned no closed bars", bid, ticker)
                continue
            rows = [
                {"benchmark_id": bid, "target_date": day.isoformat(), "price": close}
                for day, close, _volume in bars if day >= DATA_CUTOFF
            ]
            for i in range(0, len(rows), 500):
                supabase.table("benchmark_price").upsert(
                    rows[i:i + 500], on_conflict="benchmark_id,target_date"
                ).execute()
            refreshed += 1
        except YahooThrottled as e:
            log.warning("[price_update] held benchmark refresh paused: %s", e)
            break
        except Exception as e:
            log.warning("[price_update] benchmark %s refresh failed: %s: %s", bid, type(e).__name__, e)
    return refreshed


def _collect_held_companies(run_id: int) -> list[dict]:
    """Pool company_ids across the latest snapshot of every enabled
    scheduled strategy. Returns the list shape `_run_prices_phase`
    expects: `[{"cid", "ticker", "exchange"}]`. Duplicates across
    strategies collapse into a single entry."""
    company_ids = _collect_held_company_ids()
    if not company_ids:
        return []

    # Batch the IN(...) lookup to stay under the Cloudflare-502 URL-length
    # window (see deps.fetch_in_chunks / IN_CHUNK_SIZE).
    out: list[dict] = []
    for r in fetch_in_chunks(
        list(company_ids),
        lambda chunk: supabase.table("company")
        .select(
            "company_id, gurufocus_ticker, "
            "gurufocus_exchange:gurufocus_exchange(exchange_code)"
        )
        .in_("company_id", chunk)
        .execute(),
    ):
        exch = (r.get("gurufocus_exchange") or {}).get("exchange_code") or ""
        ticker = r.get("gurufocus_ticker") or ""
        if not ticker or not exch:
            continue
        out.append({
            "cid": int(r["company_id"]),
            "ticker": ticker,
            "exchange": exch,
        })
    return out


def _latest_close_dates_for(company_ids: list[int]) -> dict[int, str]:
    """Read latest GuruFocus dates with bounded, indexed company seeks.

    Paging the global RPC limits response size but may still compute the full
    universe on each page. The scoped RPC bounds database work as well.
    """
    latest: dict[int, str] = {}
    for start in range(0, len(company_ids), 100):
        rows = supabase.rpc("company_latest_metric_dates_for", {
            "p_company_ids": company_ids[start:start + 100],
            "p_metric_code": "close_price",
        }).execute().data or []
        for row in rows:
            cid, day = row.get("company_id"), row.get("latest_target_date")
            if cid is not None and day:
                latest[int(cid)] = str(day)[:10]
    return latest


def _latest_close_dates_all() -> dict[int, str]:
    """Latest dates for all companies, in bounded batches. Empty on error."""
    latest_by_cid: dict[int, str] = {}
    page, offset = 1000, 0
    for _ in range(20):
        try:
            resp = (
                supabase.table("company").select("company_id").order("company_id")
                .range(offset, offset + page - 1)
                .execute()
            )
        except Exception as e:
            logging.getLogger(__name__).warning(
                "[prices] latest-close RPC failed: %s: %s", type(e).__name__, e,
            )
            return {}
        batch = resp.data or []
        if not batch:
            break
        try:
            latest_by_cid.update(_latest_close_dates_for([int(r["company_id"]) for r in batch]))
        except Exception as e:
            logging.getLogger(__name__).warning(
                "[prices] latest-close batch failed: %s: %s", type(e).__name__, e)
            return {}
        if len(batch) < page:
            break
        offset += page
    return latest_by_cid


def _price_status_excluded_ids(cids: set[int]) -> set[int]:
    """Of `cids`, those marked delisted / out-of-scope / illiquid — companies
    whose latest close lags BY DESIGN (they're not actively priced), so they
    must not count as "stale held prices" worth retrying."""
    if not cids:
        return set()
    excluded: set[int] = set()
    for r in fetch_in_chunks(
        list(cids),
        lambda chunk: supabase.table("company")
        .select("company_id, delisted_at, out_of_scope_at, illiquid_at")
        .in_("company_id", chunk)
        .execute(),
    ):
        if r.get("delisted_at") or r.get("out_of_scope_at") or r.get("illiquid_at"):
            excluded.add(int(r["company_id"]))
    return excluded


def held_prices_lagging() -> bool:
    """True when ≥1 ACTIVELY-priced held instrument's latest close is behind the
    freshest close in the DB — i.e. GuruFocus has published the latest session
    for the pack but not yet for this name (publish lag), so a retry can pick it
    up.

    Staleness is anchored to the GLOBAL latest close, NOT the calendar day. That
    matters: at the 05:00 UTC tick today's close isn't published yet, so EVERY
    name — including the freshest — sits one session back; a calendar check would
    call that "stale" every day. Against the global latest, the normal state is
    "0 behind the pack" (not lagging); only a name that fell behind the
    still-advancing pack is flagged. A total GuruFocus outage freezes the whole
    pack together (everyone 0 behind) → no false retries, same property the
    delisting sweep relies on. Held names marked delisted/out-of-scope/illiquid
    are skipped (they lag by design). Best-effort — any probe failure returns
    False (no retry) rather than raising."""
    try:
        held = _collect_held_company_ids()
        if not held:
            return False
        latest_by_cid = _latest_close_dates_all()
        if not latest_by_cid:
            return False
        try:
            global_latest = date.fromisoformat(max(latest_by_cid.values()))
        except ValueError:
            return False
        excluded = _price_status_excluded_ids(held)
        for cid in held:
            if cid in excluded:
                continue
            d = latest_by_cid.get(cid)
            if not d:
                # Held + active + no close data at all → definitely refetch.
                return True
            try:
                latest = date.fromisoformat(d)
            except ValueError:
                continue
            if trading_days_between(latest, global_latest) >= 1:
                return True
        return False
    except Exception as e:
        logging.getLogger(__name__).warning(
            "[prices] held_prices_lagging probe failed: %s: %s", type(e).__name__, e,
        )
        return False


def universe_freshness(universe_cids):
    """Per-exchange freshness of a specific universe (the DB-backed gatherer for
    `ingest.freshness.classify_universe_freshness`).

    Cheap by design — no GuruFocus calls: reads each company's latest close date
    (one paginated RPC, shared with the prices phase's most-stale ordering) + its
    listing exchange, marks delisted/out-of-scope/illiquid AND
    GuruFocus-unsubscribed names as excluded (they lag by design), and hands the
    lot to the pure classifier. Use it to decide readiness / pick the laggards to
    re-fetch WITHOUT churning the whole universe through the API."""
    from ingest.freshness import FreshnessReport, classify_universe_freshness  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    cids = sorted({int(c) for c in universe_cids})
    if not cids:
        return FreshnessReport(global_latest=None, exchange_latest={})

    # Listing exchange per company (the peer-group key).
    exchange_by_cid: dict[int, str | None] = {}
    for r in fetch_in_chunks(
        cids,
        lambda chunk: supabase.table("company")
        .select("company_id, gurufocus_exchange:gurufocus_exchange(exchange_code)")
        .in_("company_id", chunk)
        .execute(),
    ):
        exch = (r.get("gurufocus_exchange") or {}).get("exchange_code")
        exchange_by_cid[int(r["company_id"])] = exch

    # Latest close date per company (reuse the paginated all-companies RPC, then
    # narrow to this universe — one cheap round-trip set, no per-cid queries).
    all_latest = _latest_close_dates_all()
    latest_by_cid = {c: all_latest.get(c) for c in cids}

    # Exclude names that lag by design: status markers + out-of-coverage venues.
    excluded = _price_status_excluded_ids(set(cids))
    for c, exch in exchange_by_cid.items():
        if not is_gf_subscribed_exchange(exch):
            excluded.add(c)

    return classify_universe_freshness(
        latest_by_cid, exchange_by_cid, excluded_ids=excluded,
    )
