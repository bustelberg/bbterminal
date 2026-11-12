"""Run recording, daily leases, and cancellation semantics shared by all dispatchers."""
from __future__ import annotations


class Cancelled(Exception):
    """A safe stopping boundary reached by a scheduled job body."""


def cancel_requested(ctx) -> bool:
    value = getattr(ctx, 'cancelled', False) if ctx is not None else False
    return bool(value() if callable(value) else value)


def require_complete(message: str, summary: dict) -> None:
    from dashboard_daily import IncompleteRefresh
    raise IncompleteRefresh(message, summary)


def run_job(job_id, ctx, triggered_by, *, bodies, record):
    from dashboard_daily import daily_lease
    if job_id not in {'dashboard_market', 'airs_vermogen_refresh', 'dashboard_post_airs'}:
        return run_recorded(job_id, ctx, triggered_by, bodies=bodies, record=record)
    acquired = False
    try:
        with daily_lease(job_id, triggered_by) as run:
            acquired = True
            if not run:
                return 'Automatic AIRS attempt already recorded today; no second scan started'
            return run_recorded(job_id, ctx, triggered_by, bodies=bodies, record=record)
    except Exception as exc:
        if not acquired:
            with record(job_id, triggered_by=triggered_by) as rec:
                rec.status = 'error'
                rec.detail = f'Daily refresh could not acquire its lease: {exc}'
        raise


def run_recorded(job_id, ctx, triggered_by, *, bodies, record):
    import jobs
    from ingest.api_usage import api_usage_job
    body = bodies[job_id]
    # Asset refresh owns its skipped/error receipts; never record it twice.
    if job_id == 'asset_price_refresh':
        body(ctx)
        return 'done'
    stopped = None
    with api_usage_job(job_id), record(job_id, triggered_by=triggered_by) as rec:
        try:
            detail, summary = body(ctx)
        except Cancelled as exc:
            # Set status inside the recorder so cancellation is never persisted as error.
            rec.status, rec.detail = 'cancelled', str(exc)
            stopped = str(exc)
        except Exception as exc:
            if isinstance(getattr(exc, 'summary', None), dict):
                rec.summary = exc.summary
            raise
        else:
            rec.done(detail, **{k: v for k, v in (summary or {}).items() if v is not None})
    if stopped is not None:
        raise jobs.JobCancelled(stopped)
    return detail
