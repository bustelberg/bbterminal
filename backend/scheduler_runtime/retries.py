"""Bounded held-price retry policy. Timer registration is supplied by dispatch."""
from __future__ import annotations
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from apscheduler.triggers.date import DateTrigger

_log = logging.getLogger('scheduler')

_PRICE_RETRY_DELAY_HOURS = float(os.environ.get("PRICE_RETRY_DELAY_HOURS", "3"))
_PRICE_RETRY_MAX_PER_DAY = int(os.environ.get("PRICE_RETRY_MAX_PER_DAY", "3"))
_price_retry_lock = threading.Lock()
# UTC-date ISO string → retries scheduled so far that day. Pruned to a single
# key (today's) on each schedule so it never grows.
_price_retry_counts: dict[str, int] = {}

def schedule_price_retry(sched, fire, *, reason: str = "") -> None:
    """Schedule a one-shot held-price retry `_PRICE_RETRY_DELAY_HOURS` out when
    the enabled strategies' held prices are STILL stale after a price-update.

    Called at the end of every `price_update` op (daily tick, startup catch-up,
    AND manual Run-now) so any path that leaves held prices behind gets the
    auto-retry. No-op when prices are fresh, the scheduler is disabled
    (CI / DISABLE_SCHEDULER), or today's retry budget (`_PRICE_RETRY_MAX_PER_DAY`)
    is spent — at which point the next 05:00 UTC tick takes over. The retry
    re-runs the whole price-update, but the per-company freshness short-circuit
    in `_run_prices_phase` means only the actually-stale names hit GuruFocus.

    Best-effort — never raises into the caller."""
    try:
        if sched is None or _PRICE_RETRY_MAX_PER_DAY <= 0:
            return
        # Publish-lag check (held name behind the GLOBAL latest close), NOT the
        # calendar-day `_held_prices_stale` the kickstart uses — otherwise the
        # normal "today's close isn't out yet" state would trigger a retry every
        # single day. See `held_prices_lagging`.
        from ingest.phases.prices import held_prices_lagging  # noqa: PLC0415
        if not held_prices_lagging():
            return
        today = datetime.now(timezone.utc).date().isoformat()
        with _price_retry_lock:
            used = _price_retry_counts.get(today, 0)
            if used >= _PRICE_RETRY_MAX_PER_DAY:
                _log.info(
                    "[scheduler] held prices still stale but today's retry budget "
                    "(%s) is spent — waiting for the next daily tick",
                    _PRICE_RETRY_MAX_PER_DAY,
                )
                return
            # Keep only today's key so the dict can't grow unbounded.
            _price_retry_counts.clear()
            attempt = used + 1
            _price_retry_counts[today] = attempt
        run_at = datetime.now(timezone.utc) + timedelta(hours=_PRICE_RETRY_DELAY_HOURS)
        sched.add_job(
            fire,
            DateTrigger(run_date=run_at),
            id="price_update_retry",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=3600,
        )
        _log.info(
            "[scheduler] held prices stale%s — retry %s/%s scheduled at %s",
            f" ({reason})" if reason else "", attempt, _PRICE_RETRY_MAX_PER_DAY,
            run_at.isoformat(),
        )
    except Exception as e:
        _log.warning(
            "[scheduler] maybe_schedule_price_retry failed: %s: %s",
            type(e).__name__, e,
        )
