"""Stale-price delisting sweep.

A company whose latest GuruFocus close stays many trading days behind the
market has stopped trading — delisted, acquired, or long-halted. GuruFocus
keeps serving the (frozen) price history, so the per-company "delisted"
message detection in `ingest.prices` never fires for these; this sweep
catches them purely from the DB (the latest `close_price` date we already
store), no GuruFocus calls, so it runs cheaply over the whole `company`
table even in the held-only daily pipeline.

Only companies that HAD price data and have gone stale are marked — a
company with NO close data is a wrong-exchange / out-of-scope listing
(already flagged `gurufocus_lookup_failed_at` / `out_of_scope_at`), not a
delisting, so it's left alone.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from deps import paginate, supabase as _default_supabase

from .staleness import trading_days_between

log = logging.getLogger(__name__)

# Default: a listing whose last close is more than this many trading days
# behind the most-recent expected close is treated as delisted (~3 weeks).
DEFAULT_DELISTING_THRESHOLD_TRADING_DAYS = 15


@dataclass
class DelistingSweepResult:
    checked: int = 0          # active (non-delisted, in-scope) companies examined
    with_data: int = 0        # of those, how many have any close-price data
    newly_delisted: int = 0   # how many we marked delisted this pass
    skipped_no_pg: bool = False  # direct-Postgres unavailable → sweep skipped
    examples: list[str] = field(default_factory=list)  # "EXCH:TICKER (last YYYY-MM-DD)"


def sweep_delisted_companies(
    supabase=None,
    *,
    threshold_trading_days: int = DEFAULT_DELISTING_THRESHOLD_TRADING_DAYS,
) -> DelistingSweepResult:
    """Mark companies whose latest close is > `threshold_trading_days` behind
    the FRESHEST close in the whole DB as delisted (`delisted_at`). DB-only,
    idempotent (already-delisted / out-of-scope rows are skipped, and
    `delisted_at` is only stamped where it's still NULL). Best-effort: a
    per-row write failure is logged and skipped.

    Staleness is measured against the global freshest close, NOT the calendar
    day, so a GuruFocus outage (e.g. a Cloudflare IP-block on the host) can't
    trigger false mass-delistings: when nothing gets fresh prices every
    company's latest close stalls together and stays ~0 trading days behind
    the global latest. Only a name that falls behind the still-advancing pack
    is a genuine delisting."""
    sb = supabase or _default_supabase
    result = DelistingSweepResult()

    # Latest close date per company — one indexed GROUP BY via direct Postgres.
    from momentum.data._pg import load_all_latest_close_dates_via_copy  # noqa: PLC0415
    latest_by_cid = load_all_latest_close_dates_via_copy()
    if latest_by_cid is None:
        # No SUPABASE_DB_URL: the PostgREST RPC over the full table times out,
        # so skip rather than risk a slow/failed sweep.
        result.skipped_no_pg = True
        log.info("[delisting] SUPABASE_DB_URL not set — skipping stale-price sweep")
        return result

    # Anchor staleness to the freshest close anywhere in the DB (the market's
    # last known good day), not `today`.
    global_latest_str = max(latest_by_cid.values(), default=None)
    if not global_latest_str:
        return result  # no price data at all → nothing to judge
    try:
        global_latest = date.fromisoformat(global_latest_str[:10])
    except ValueError:
        return result

    now_iso = datetime.now(timezone.utc).isoformat()
    for r in paginate(
        lambda lo, hi: sb.table("company")
        .select(
            "company_id, gurufocus_ticker, "
            "gurufocus_exchange:gurufocus_exchange(exchange_code)"
        )
        .is_("delisted_at", "null")
        .is_("out_of_scope_at", "null")
        .range(lo, hi)
        .execute()
    ):
        result.checked += 1
        cid = int(r["company_id"])
        latest_str = latest_by_cid.get(cid)
        if not latest_str:
            continue  # no close data → wrong-exchange / out-of-scope, not delisted
        result.with_data += 1
        try:
            latest = date.fromisoformat(latest_str[:10])
        except ValueError:
            continue
        if trading_days_between(latest, global_latest) <= threshold_trading_days:
            continue
        try:
            upd = (
                sb.table("company")
                .update({"delisted_at": now_iso})
                .eq("company_id", cid)
                .is_("delisted_at", "null")
                .execute()
            )
            if upd.data:
                result.newly_delisted += 1
                if len(result.examples) < 20:
                    exch = (r.get("gurufocus_exchange") or {}).get("exchange_code") or ""
                    result.examples.append(
                        f"{exch}:{r.get('gurufocus_ticker')} (last {latest_str})"
                    )
        except Exception as e:
            log.warning(
                "[delisting] failed to mark cid=%s delisted: %s: %s",
                cid, type(e).__name__, e,
            )

    log.info(
        "[delisting] sweep done: checked=%s with_data=%s newly_delisted=%s",
        result.checked, result.with_data, result.newly_delisted,
    )
    return result
