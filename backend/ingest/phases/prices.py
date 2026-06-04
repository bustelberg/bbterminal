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
from concurrent.futures import ThreadPoolExecutor

from deps import fetch_in_chunks, supabase

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
) -> None:
    """Phase 3 — the price/volume refresh that used to be the whole job.
    Walks every row in `company`, parallel-pumps each through
    `ensure_prices_for_company` + `ensure_volume_for_company`, and
    updates `ingest_run` with the per-class counters every
    `_CHECKPOINT_EVERY` companies. Forbidden / delisted are tallied
    silently; the first 5 unexpected errors land in `error_summary`.

    `companies_override` short-circuits `_load_all_companies()`; pass the
    pooled held-company list for the daily MTD refresh so we don't churn
    through the full ~2000-company universe."""
    from ingest.prices import (  # noqa: PLC0415
        ensure_prices_for_company,
        ensure_volume_for_company,
    )

    log = logging.getLogger(__name__)
    counters = {
        "processed": 0,
        "prices": 0,
        "volumes": 0,
        "forbidden": 0,
        "delisted": 0,
        "errors": 0,
    }
    forbidden_exchanges: set[str] = set()
    error_examples: list[str] = []
    lock = threading.Lock()

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
        checkpoint: dict | None = None

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
                if counters["processed"] % _CHECKPOINT_EVERY == 0:
                    checkpoint = dict(counters)
            if checkpoint:
                _checkpoint(run_id, checkpoint, total)
            return
        if r_p.is_delisted:
            with lock:
                counters["processed"] += 1
                counters["delisted"] += 1
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
            if counters["processed"] % _CHECKPOINT_EVERY == 0:
                checkpoint = dict(counters)
        if checkpoint:
            _checkpoint(run_id, checkpoint, total)

    with ThreadPoolExecutor(
        max_workers=_MAX_WORKERS, thread_name_prefix=f"ingest-{run_id}"
    ) as executor:
        list(executor.map(_refresh_one, companies))

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
    """Periodic progress write. Best-effort — a transient blip on the
    checkpoint is harmless; the next one (or the final summary) will
    catch up. Includes a `current_message` summarizing per-class
    counters so /schedule renders an actionable status line between
    structured-counter updates."""
    if total is not None:
        msg = (
            f"Refreshing {snap['processed']} of {total} companies · "
            f"{snap['prices']}p / {snap['volumes']}v refreshed · "
            f"{snap['forbidden']} forbidden, {snap['errors']} errors"
        )
    else:
        msg = (
            f"{snap['processed']} processed · "
            f"{snap['prices']}p / {snap['volumes']}v refreshed · "
            f"{snap['forbidden']} forbidden, {snap['errors']} errors"
        )
    _update_run(
        run_id,
        companies_processed=snap["processed"],
        prices_refreshed=snap["prices"],
        volumes_refreshed=snap["volumes"],
        forbidden_count=snap["forbidden"],
        delisted_count=snap["delisted"],
        error_count=snap["errors"],
        current_message=msg,
    )


def _collect_held_companies(run_id: int) -> list[dict]:
    """Pool company_ids across the latest snapshot of every enabled
    scheduled strategy. Returns the list shape `_run_prices_phase`
    expects: `[{"cid", "ticker", "exchange"}]`. Duplicates across
    strategies collapse into a single entry."""
    strat_resp = (
        supabase.table("scheduled_strategy")
        .select("id")
        .eq("enabled", True)
        .execute()
    )
    sched_ids = [r["id"] for r in (strat_resp.data or [])]
    if not sched_ids:
        return []

    snap_resp = (
        supabase.table("current_picks_snapshot")
        .select("scheduled_strategy_id, holdings, created_at")
        .in_("scheduled_strategy_id", sched_ids)
        .order("created_at", desc=True)
        .execute()
    )
    company_ids: set[int] = set()
    seen: set[int] = set()
    for s in (snap_resp.data or []):
        sid = s.get("scheduled_strategy_id")
        if sid is None or sid in seen:
            continue
        seen.add(sid)
        for h in (s.get("holdings") or []):
            cid = h.get("company_id")
            if cid is not None:
                company_ids.add(int(cid))

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
