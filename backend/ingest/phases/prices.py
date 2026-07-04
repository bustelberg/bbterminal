"""Phase 3 — price + volume refresh.

The original heart of the job: walk every surviving `company` and pump
each through `ensure_prices_for_company` + `ensure_volume_for_company`
(GuruFocus), tallying per-class counters (prices / volumes / forbidden /
delisted / errors) onto the run row. This module also owns the two
company-list loaders the phase drives over — `_load_all_companies`
(full universe, most-stale-first) and `_collect_held_companies` (the
pooled held set used by the daily MTD refresh) — plus the `_checkpoint`
helper that flushes counters to `ingest_run` mid-run.

`ensure_*_for_company` short-circuits on fresh DB rows, so the
concurrency cap only bites when we're actually pulling new data.
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from deps import fetch_in_chunks, supabase

from ..staleness import trading_days_between
from .runlog import _now_utc_iso, _update_run

# Concurrency cap — same as self_heal. GuruFocus is rate-limit-sensitive
# and the ensure_* helpers short-circuit on fresh DB rows, so the bound
# only matters when we're actually pulling fresh data. 12 keeps a typical
# weekly run roughly in line with the ~10-minute target now that the universe
# is ~2.8k companies (instead of ~1.8k when 4 was chosen). curl_cffi's
# Cloudflare ladder handles 12-wide comfortably; bump further only if 429s
# stay absent across multiple runs.
_MAX_WORKERS = 12
# Checkpoint frequency — write progress to the row every N companies.
# Previously 25, which produced visibly "chunky" progress jumps
# (0 → 25 → 50 → …). 1 = write on every company so the UI reflects
# real-time progress; the wall-clock throttle on `current_message`
# (_MESSAGE_THROTTLE_SECONDS) bounds total DB write volume regardless,
# and the counter-only update is cheap enough to do per-row.
_CHECKPOINT_EVERY = 1
# Live-progress heartbeat cadence + the idle threshold past which the UI/logs
# explicitly flag the phase as STALLED (the processed count hasn't advanced in
# this many seconds — a hung fetch, not just a slow one).
_HEARTBEAT_SECONDS = 3.0
_STALL_WARN_SECONDS = 45.0
# A single in-flight fetch older than this is almost certainly wedged on the
# Cloudflare/impersonation-ladder retry path (the throttled tail of a big run),
# not merely slow — surfaced explicitly so a hung name reads as such.
_SLOW_FETCH_WARN_SECONDS = 25.0


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

    # Most-stale-first ordering. One RPC fetches the latest close_price
    # target_date per company; companies with no row come back with NULL
    # which we map to the empty string so they sort lexicographically
    # before any real date. Failure here just falls back to insertion
    # order — the phase still works, just without prioritization.
    #
    # Pagination: PostgREST caps responses at `db-max-rows` (1000 in the
    # cloud project, 10000 in local Docker Supabase via config.toml). The
    # RPC returns one row per company (~2800) so without paging in prod
    # only the first 1000 would have stale-priority — see the 2G Energy
    # incident on /companies that exposed the same trap on the
    # company_universe_labels RPC.
    try:
        latest_by_cid: dict[int, str] = {}
        page = 1000
        offset = 0
        for _attempt in range(20):
            latest_resp = (
                supabase.rpc("company_latest_close_price_dates", {})
                .range(offset, offset + page - 1)
                .execute()
            )
            batch = latest_resp.data or []
            if not batch:
                break
            added = 0
            for row in batch:
                cid = int(row["company_id"])
                if cid in latest_by_cid:
                    continue
                latest_by_cid[cid] = row.get("latest_target_date") or ""
                added += 1
            if added == 0 or len(batch) < page:
                break
            offset += page
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
    """Phase 3 — the price/volume refresh that used to be the whole job.
    Walks every row in `company`, parallel-pumps each through
    `ensure_prices_for_company` + `ensure_volume_for_company`, and
    updates `ingest_run` with the per-class counters every
    `_CHECKPOINT_EVERY` companies. Forbidden / delisted are tallied
    silently; the first 5 unexpected errors land in `error_summary`.

    `companies_override` short-circuits `_load_all_companies()`; pass the
    pooled held-company list for the daily MTD refresh so we don't churn
    through the full ~2000-company universe.

    `budget_by_region` caps GuruFocus calls per region (`{usa, europe, asia}` →
    requests still available this month). When set, `region_remaining` is
    decremented by each fetch's actual `api_calls`; once a region hits 0 its
    remaining companies are skipped (tallied in `budget_skipped`) instead of
    hammering an exhausted quota. Used by the month-end full-price refresh."""
    from ingest.api_usage import _region_for_exchange  # noqa: PLC0415
    from ingest.prices import (  # noqa: PLC0415
        clear_db_max_date_cache,
        ensure_prices_for_company,
        ensure_volume_for_company,
        prime_db_max_dates,
    )

    log = logging.getLogger(__name__)
    counters = {
        "processed": 0,
        "prices": 0,
        "volumes": 0,
        "forbidden": 0,
        "delisted": 0,
        "errors": 0,
        "budget_skipped": 0,
    }
    forbidden_exchanges: set[str] = set()
    error_examples: list[str] = []
    lock = threading.Lock()
    # Per-region remaining monthly quota (only when budgeting). Decremented by
    # each fetch's real `api_calls`; a region at 0 skips its remaining names.
    region_remaining: dict[str, int] = dict(budget_by_region) if budget_by_region else {}
    # Each worker records the company it's CURRENTLY on, keyed by thread name —
    # at most _MAX_WORKERS entries; the next company on that thread overwrites
    # the previous, so no cleanup is needed. The heartbeat reads these to show
    # exactly what's in flight.
    current_by_thread: dict[str, str] = {}

    companies = companies_override if companies_override is not None else _load_all_companies()

    if not companies:
        # Empty universe — still considered a successful prices phase.
        _update_run(run_id, current_message="No companies to refresh.")
        return

    total = len(companies)
    # Surface the denominator immediately so the UI shows "0 of N"
    # instead of "starting…" while the first 25 companies process.
    _update_run(
        run_id,
        companies_total=total,
        current_message=f"Refreshing 0 of {total} companies (concurrency {_MAX_WORKERS})…",
    )

    def _refresh_one(c: dict) -> None:
        cid = c["cid"]
        ticker = c["ticker"]
        exch = c["exchange"]
        region = _region_for_exchange(exch)
        checkpoint: dict | None = None
        # Publish what this worker is about to fetch + WHEN it started, so the
        # heartbeat can show the live in-flight set with per-company elapsed time
        # (this overwrites the thread's previous company).
        with lock:
            current_by_thread[threading.current_thread().name] = (
                f"{exch}:{ticker}", time.monotonic(),
            )

        # Budget gate — when month-end budgeting, skip companies whose region
        # has no monthly GuruFocus quota left (counts toward budget_skipped, no
        # API call).
        if budget_by_region is not None:
            skip = False
            with lock:
                if region_remaining.get(region, 0) <= 0:
                    counters["processed"] += 1
                    counters["budget_skipped"] += 1
                    skip = True
                    if counters["processed"] % _CHECKPOINT_EVERY == 0:
                        checkpoint = dict(counters)
            if checkpoint:
                _checkpoint(run_id, checkpoint, total)
                checkpoint = None
            if skip:
                return

        # Short-circuit on known-forbidden exchanges. Same pattern as
        # `momentum.data.self_heal`: a single 403 marks the exchange so
        # the next ~80 companies on it skip the API call entirely.
        with lock:
            if exch in forbidden_exchanges:
                counters["processed"] += 1
                counters["forbidden"] += 1
                if counters["processed"] % _CHECKPOINT_EVERY == 0:
                    checkpoint = dict(counters)
            if checkpoint:
                _checkpoint(run_id, checkpoint, total)
                checkpoint = None
        if exch in forbidden_exchanges:
            return

        try:
            r_p = ensure_prices_for_company(supabase, cid, ticker, exch)
        except Exception as e:
            with lock:
                counters["processed"] += 1
                counters["errors"] += 1
                if len(error_examples) < 5:
                    error_examples.append(
                        f"cid={cid} ({exch}:{ticker}) price: {type(e).__name__}: {e}"
                    )
                if counters["processed"] % _CHECKPOINT_EVERY == 0:
                    checkpoint = dict(counters)
            if checkpoint:
                _checkpoint(run_id, checkpoint, total)
            return

        if r_p.is_forbidden:
            with lock:
                forbidden_exchanges.add(exch)
                counters["processed"] += 1
                counters["forbidden"] += 1
                if budget_by_region is not None:
                    region_remaining[region] = region_remaining.get(region, 0) - r_p.api_calls
                if counters["processed"] % _CHECKPOINT_EVERY == 0:
                    checkpoint = dict(counters)
            if checkpoint:
                _checkpoint(run_id, checkpoint, total)
            return
        if r_p.is_delisted:
            with lock:
                counters["processed"] += 1
                counters["delisted"] += 1
                if budget_by_region is not None:
                    region_remaining[region] = region_remaining.get(region, 0) - r_p.api_calls
                if counters["processed"] % _CHECKPOINT_EVERY == 0:
                    checkpoint = dict(counters)
            if checkpoint:
                _checkpoint(run_id, checkpoint, total)
            # Persist the delisted-at marker so the next run + the audit
            # path can short-circuit instead of re-probing. Best-effort —
            # a transient blip here just means we re-probe next run.
            try:
                supabase.table("company").update(
                    {"delisted_at": _now_utc_iso()}
                ).eq("company_id", cid).is_("delisted_at", "null").execute()
            except Exception as e:
                logging.getLogger(__name__).warning(
                    "[prices_phase] failed to mark cid=%s delisted: %s: %s",
                    cid, type(e).__name__, e,
                )
            return

        # If the price fetch had to fall through to a different exchange
        # (e.g. XTER:D7C 404 → STU:D7C 200), use the resolved one for
        # the volume call AND repoint the company row so future runs
        # skip the dead primary. This is the place we trust the
        # iShares-derived `XTER` claim was wrong; the actual home is
        # whatever GuruFocus served.
        effective_exch = r_p.resolved_exchange or exch
        if r_p.resolved_exchange and r_p.resolved_exchange != exch:
            try:
                exch_row = (
                    supabase.table("gurufocus_exchange")
                    .select("exchange_id")
                    .eq("exchange_code", r_p.resolved_exchange)
                    .limit(1)
                    .execute()
                )
                if exch_row.data:
                    new_eid = exch_row.data[0]["exchange_id"]
                    supabase.table("company").update(
                        {"exchange_id": new_eid}
                    ).eq("company_id", cid).execute()
            except Exception as e:
                logging.getLogger(__name__).warning(
                    "[prices_phase] failed to repoint cid=%s exchange %s -> %s: %s: %s",
                    cid, exch, r_p.resolved_exchange, type(e).__name__, e,
                )

        try:
            r_v = ensure_volume_for_company(supabase, cid, ticker, effective_exch)
        except Exception as e:
            with lock:
                counters["processed"] += 1
                counters["errors"] += 1
                if r_p.rows_loaded > 0:
                    counters["prices"] += 1
                if budget_by_region is not None:
                    region_remaining[region] = region_remaining.get(region, 0) - r_p.api_calls
                if len(error_examples) < 5:
                    error_examples.append(
                        f"cid={cid} ({exch}:{ticker}) volume: {type(e).__name__}: {e}"
                    )
                if counters["processed"] % _CHECKPOINT_EVERY == 0:
                    checkpoint = dict(counters)
            if checkpoint:
                _checkpoint(run_id, checkpoint, total)
            return

        with lock:
            counters["processed"] += 1
            if r_p.rows_loaded > 0:
                counters["prices"] += 1
            if r_v.rows_loaded > 0:
                counters["volumes"] += 1
            if budget_by_region is not None:
                region_remaining[region] = region_remaining.get(region, 0) - (r_p.api_calls + r_v.api_calls)
            if counters["processed"] % _CHECKPOINT_EVERY == 0:
                checkpoint = dict(counters)
        if checkpoint:
            _checkpoint(run_id, checkpoint, total)

    # Collapse the workers' per-company max-date reads (close_price + volume)
    # into ONE grouped query per metric up front — thousands of concurrent
    # PostgREST round-trips otherwise (which tripped Cloudflare's HTTP/2 GOAWAY).
    # Cleared in finally so it never leaks stale entries into the next run.
    prime_db_max_dates(supabase, [c["cid"] for c in companies])

    # Live-progress heartbeat: every few seconds publish rate + ETA + the
    # companies each worker is currently on + how long since the processed count
    # last advanced — so a genuine STALL is unmistakable (the idle seconds keep
    # climbing and we flag it), not a static "Running…". Own thread; the
    # per-company `_checkpoint` keeps the structured counters live between beats.
    hb_start = time.monotonic()
    hb_stop = threading.Event()

    def _heartbeat() -> None:
        last_done, last_change = -1, time.monotonic()
        while not hb_stop.wait(_HEARTBEAT_SECONDS):
            now = time.monotonic()
            with lock:
                done = counters["processed"]
                errs, forb = counters["errors"], counters["forbidden"]
                # (label, seconds-in-flight) per worker, oldest first.
                inflight = sorted(
                    ((lbl, now - st) for lbl, st in current_by_thread.values()),
                    key=lambda x: -x[1],
                )
            if done != last_done:
                last_done, last_change = done, now
            idle = now - last_change
            elapsed = max(1e-6, now - hb_start)
            rate = done / elapsed * 60.0            # companies/min
            eta_min = (total - done) / rate if rate > 0 else 0.0
            # Annotate each in-flight company with how long it's been fetching —
            # a wedged name (Cloudflare/ladder churn on the throttled tail) shows
            # an ever-growing age, so "stuck" is unmistakable from "slow".
            sample = ", ".join(f"{lbl} ({age:.0f}s)" for lbl, age in inflight[:8]) + (
                f" +{len(inflight) - 8} more" if len(inflight) > 8 else "")
            oldest = inflight[0][1] if inflight else 0.0
            # The tail is being throttled when every worker has been wedged on the
            # SAME fetch for a while but the processed count is barely moving.
            throttled = oldest >= _SLOW_FETCH_WARN_SECONDS
            stalled = idle >= _STALL_WARN_SECONDS
            warn = ""
            if throttled:
                warn = (
                    f" · ⚠ tail throttled — oldest fetch {oldest:.0f}s "
                    "(GuruFocus/Cloudflare rate-limiting; each wedged name is "
                    "retried across the impersonation ladder, then skipped)"
                )
            elif stalled:
                warn = f" · ⚠ STALLED {idle:.0f}s with no completion"
            msg = (
                f"Refreshing {done}/{total} · {rate:.0f}/min · ETA {eta_min:.0f}m · "
                f"{forb} forbidden, {errs} err · now fetching: {sample or '—'}{warn}"
            )
            # Write the structured counter alongside the message so the card's
            # header count (companies_processed) can't drift ahead of the
            # heartbeat line during a wedged tail (no completions → no checkpoint).
            _update_run(run_id, current_message=msg, companies_processed=done)
            (log.warning if (throttled or stalled) else log.info)(
                "[pipeline.prices] run_id=%s %s", run_id, msg)

    hb_thread = threading.Thread(target=_heartbeat, daemon=True, name=f"prices-hb-{run_id}")
    hb_thread.start()
    try:
        with ThreadPoolExecutor(
            max_workers=_MAX_WORKERS, thread_name_prefix=f"ingest-{run_id}"
        ) as executor:
            list(executor.map(_refresh_one, companies))
    finally:
        hb_stop.set()
        hb_thread.join(timeout=2)
        clear_db_max_date_cache()

    # Final counter write — orchestrator handles status/finished_at.
    _update_run(
        run_id,
        companies_processed=counters["processed"],
        prices_refreshed=counters["prices"],
        volumes_refreshed=counters["volumes"],
        forbidden_count=counters["forbidden"],
        delisted_count=counters["delisted"],
        error_count=counters["errors"],
        current_message=(
            f"Prices phase done: {counters['processed']} of {total} processed · "
            f"{counters['prices']} prices / {counters['volumes']} volumes refreshed · "
            f"{counters['forbidden']} forbidden, {counters['errors']} errors"
            + (f" · {counters['budget_skipped']} skipped (monthly budget reached)"
               if counters['budget_skipped'] else "")
        ),
    )
    if error_examples:
        accumulated_errors.append(
            "Prices phase per-company errors:\n" + "\n".join(error_examples[:5])
        )
    log.info(
        "[pipeline.prices] run_id=%s done: %s processed, %s prices, %s volumes, "
        "%s forbidden, %s delisted, %s errors",
        run_id, counters["processed"], counters["prices"], counters["volumes"],
        counters["forbidden"], counters["delisted"], counters["errors"],
    )


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
    from ingest.api_usage import track_api_call  # noqa: PLC0415
    from ingest.constants import DATA_CUTOFF  # noqa: PLC0415
    from ingest.prices import _fetch_price_from_api, _parse_price_series  # noqa: PLC0415

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
            # ETFs are US-listed — same fetch the /api/benchmarks refresh uses.
            data, fetch_log, _status = _fetch_price_from_api(ticker, "NYSE")
            track_api_call(supabase, "NYSE")
            if not data:
                log.warning("[price_update] benchmark %s (%s) fetch failed: %s", bid, ticker, fetch_log)
                continue
            parsed = _parse_price_series(data)
            if not parsed:
                continue
            rows = [
                {"benchmark_id": bid, "target_date": d.isoformat(), "price": p}
                for d, p in parsed if d >= DATA_CUTOFF
            ]
            for i in range(0, len(rows), 500):
                supabase.table("benchmark_price").upsert(
                    rows[i:i + 500], on_conflict="benchmark_id,target_date"
                ).execute()
            refreshed += 1
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


def _latest_close_dates_all() -> dict[int, str]:
    """`company_id → latest close-price date (YYYY-MM-DD)` for every company,
    via the `company_latest_close_price_dates` RPC (same source the prices
    phase sorts on + `/api/data/price-coverage`). Paginated past the
    PostgREST 1000-row cap. Empty dict on error / no data."""
    latest_by_cid: dict[int, str] = {}
    page, offset = 1000, 0
    for _ in range(20):
        try:
            resp = (
                supabase.rpc("company_latest_close_price_dates", {})
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
        for row in batch:
            cid = row.get("company_id")
            d = row.get("latest_target_date")
            if cid is not None and d:
                latest_by_cid[int(cid)] = str(d)[:10]
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
