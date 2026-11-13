"""Preparation job implementations; called through scheduler dispatch adapters."""
from __future__ import annotations
import logging

from scheduler_runtime.execution import Cancelled, cancel_requested, require_complete
from scheduler_runtime.context import LogContext
from scheduler_jobs.runtime import JobRuntime

_log = logging.getLogger('scheduler')

def _body_history_drift(ctx=None, *, runtime: JobRuntime) -> tuple[str, dict]:
    """ CANCEL LANDS AT THE NEXT PROBE. `on_step` fires per company, so this is one of the bodies
    that CAN stop quickly — it walks a fifth of the universe and each probe is one request."""
    from ingest.history_drift import daily_drift_check  # noqa: PLC0415

    seen = {"n": 0}
    report = runtime.reporter(ctx)

    def _step(m, lvl) -> None:
        (_log.warning if lvl in ("warn", "error") else _log.info)("[drift] %s", m)
        #  No total — the walk's size is decided inside `daily_drift_check` and never handed out,
        # so the bar stays indeterminate and the LINE carries the progress. A fabricated total
        # would give a bar that reaches 100% and keeps going.
        seen["n"] += 1
        report(seen["n"], 0, m)
        #  Raised, not returned. `daily_drift_check` has no stop hook, so the only way out is to
        # unwind it from the callback it does have — at a per-company boundary, where the last
        # company is either fully refetched or untouched.
        if ctx is not None and getattr(ctx, "cancelled", False):
            raise Cancelled(
                f"stopped after {seen['n']} probe(s); everything refetched so far is kept")

    res = daily_drift_check(on_step=_step)
    drifted = res.get("drifted") or []
    if drifted:
        _log.warning("[scheduler] history drift: %s of %s probed companies had rewritten "
                     "history and were refetched", len(drifted), res.get("probed"))
    #  Finding nothing is the job working, not the job skipping. Most days no vendor rewrote
    # anything, and that is `ok` with a count of zero — the number is the point, because a probe
    # count that quietly falls to 0 means the WALK stopped, which looks identical to a clean week
    # if only the drift count is recorded.
    return (f"{len(drifted)} drifted of {res.get('probed')} probed",
            {"probed": res.get("probed"), "drifted": len(drifted),
             "names": [str(d) for d in drifted][:20]})


def _body_analyse_prepare(ctx=None, *, runtime: JobRuntime) -> tuple[str, dict]:
    from routers._prepared_analyse import run_controlled  # noqa: PLC0415
    result = run_controlled(busy=runtime.sources_busy,
                            should_stop=lambda: cancel_requested(ctx), on_step=runtime.reporter(ctx))
    if result.get('unavailable') or result.get('errors'):
        require_complete('Analyse preparation could not publish all requested views', result)
    return (result.get('skipped') or f"Prepared {result.get('holdings', 0)} holdings views and {result.get('benchmarks', 0)} benchmark(s)", result)


def _body_dashboard_market(ctx=None, *, runtime: JobRuntime) -> tuple[str, dict]:
    runtime.wait_for_yahoo_queue(ctx)
    failures = {}
    for stage in ("fx_sync", "asset_price_refresh", "benchmark_price_slice", "benchmark_fundamentals_fill"):
        if cancel_requested(ctx):
            raise Cancelled("daily dashboard refresh cancelled")
        try:
            runtime.run_job(stage, ctx)
        except Exception as exc:
            # Finish independent sources, but retain every stage's own error record.
            failures[stage] = str(exc)
    if failures:
        require_complete("Daily dashboard sources are incomplete", {"errors": failures})
    return "Market data and due fundamentals checked", {"stages": 4}


def _body_dashboard_freshness(ctx=None, *, runtime: JobRuntime) -> tuple[str, dict]:
    from collections import Counter

    from dashboard_daily import freshness
    result = freshness(dashboard_only=True)
    # If source coverage is complete and only publication lags, catch up now
    # rather than failing repeatedly while two-model background batches run.
    publication_issues = [issue for account in result['accounts'] for issue in account['issues']]
    if (result['incomplete'] and not result['empty_roster'] and not result['global_issues']
            and publication_issues and all(issue.startswith('Analyse:') for issue in publication_issues)
            and not any(account.get('unmapped_isins') for account in result['accounts'])):
        from routers._prepared_analyse import run_controlled
        try:
            repair = run_controlled(busy=runtime.sources_busy, should_stop=lambda: cancel_requested(ctx),
                                    on_step=runtime.reporter(ctx), max_portfolios=None, max_seconds=900)
        except Exception as exc:
            repair = {'errors': {'repair': f'{type(exc).__name__}: {exc}'}}
        result = {**freshness(dashboard_only=True), 'analyse_repair': repair}
    if result["incomplete"] or result["empty_roster"] or result["global_issues"]:
        issues = Counter(issue for account in result["accounts"] for issue in set(account["issues"]))
        reasons = list(result["global_issues"][:2])
        if result["empty_roster"]:
            reasons.insert(0, "No dashboard portfolios found")
        reasons.extend(f"{issue} ({count} portfolios)" for issue, count in issues.most_common(2))
        if len(issues) > 2:
            reasons.append(f"{len(issues) - 2} additional issue(s); see accounts and issue_groups in the dashboard_freshness receipt")
        detail = f"Dashboard incomplete: {result['incomplete']}/{result['checked']} portfolios"
        if reasons:
            detail += " · " + " · ".join(reasons)
        require_complete(detail, result)
    return f"All {result['checked']} dashboard portfolios current", result


def _body_dashboard_post_airs(ctx=None, *, runtime: JobRuntime) -> tuple[str, dict]:
    from dashboard_daily import prepare_month
    from routers._airs_monthly_report import collect_report
    from airs_history import finalise_previous_year
    from routers._airs_accounts import list_accounts
    from routers._airs_price_check import refresh_price_checks
    from routers._prepared_analyse import run_controlled
    from management_portfolios import is_dashboard_account, dashboard_refresh_key
    summary, failures = {}, {}
    def stop():
        return cancel_requested(ctx)
    accounts = sorted((row["portefeuille"] for row in list_accounts()
                       if row.get("holdings") and is_dashboard_account(row.get('portefeuille'))),
                      key=dashboard_refresh_key)
    from routers._monthly_benchmark import ensure_daily
    step = runtime.reporter(ctx)
    step(0, 0, "Preparing monthly benchmarks")
    summary['monthly_benchmarks'] = ensure_daily(accounts, ctx)
    if summary['monthly_benchmarks']['errors']:
        failures['monthly_benchmarks'] = summary['monthly_benchmarks']['errors']
    try:
        step(0, 0, "Finalising previous-year archives")
        summary["year_end"] = finalise_previous_year(ctx, dashboard_only=True)
        if any(row["errors"] for row in summary["year_end"]):
            failures["year_end"] = summary["year_end"]
    except Exception as exc:
        failures["year_end"] = str(exc)
    try:
        step(0, 0, "Refreshing saved price checks")
        summary["checks"] = refresh_price_checks(accounts, should_stop=stop, freshen=False,
                                               only_if_changed=True, on_step=runtime.reporter(ctx))
        if summary["checks"].get("errors"):
            failures["checks"] = summary["checks"]["errors"]
    except Exception as exc:
        failures["checks"] = str(exc)
    if stop():
        raise Cancelled('post-AIRS price checks cancelled; completed comparisons were saved')
    summary["monthly"] = []
    monthly_ctx = ctx if ctx is not None else LogContext(cancelled=stop, step=runtime.reporter(ctx), label="Monthly AIRS reports")
    for index, account in enumerate(accounts):
        if stop():
            raise Cancelled("post-AIRS preparation cancelled")
        runtime.reporter(ctx)(index, len(accounts), f"Preparing stored monthly report: {account}")
        try:
            result = prepare_month(account)
            summary["monthly"].append(result)
            if result["errors"]:
                # Stored daily snapshots can lack the month-opening valuation.
                # Fetch the dated reports after the regular AIRS scan has finished.
                result["airs_download"] = collect_report(account, result["end"][:7], monthly_ctx)
                result["errors"] = {}
        except Cancelled:
            raise
        except Exception as exc:
            failures[account] = str(exc)
    try:
        from scheduler_jobs.weekly_returns import run as prepare_weekly
        _, summary['weekly_returns'] = prepare_weekly(ctx, collect_history=False, report=runtime.reporter(ctx))
    except Cancelled:
        raise
    except Exception as exc:
        failures['weekly_returns'] = f'{type(exc).__name__}: {exc}'
    try:
        step(0, 0, "Preparing Analyse views")
        summary["analyse"] = run_controlled(busy=lambda: runtime.sources_busy(ignore_airs=True),
                                            should_stop=stop, on_step=runtime.reporter(ctx),
                                            max_portfolios=None, max_seconds=900)
        if any(summary["analyse"].get(key) for key in ("errors", "unavailable", "skipped", "batch_exhausted", "budget_exhausted", "changed_during_run", "pending_keys", "cancelled")):
            failures["analyse"] = summary["analyse"]
    except Exception as exc:
        failures["analyse"] = str(exc)
    # Audit actual sources even if a preceding phase failed. Both receipts remain durable.
    try:
        runtime.run_job("dashboard_freshness", ctx)
    except Exception as exc:
        failures["audit"] = {"message": str(exc), "receipt": getattr(exc, "summary", None)}
    summary["errors"] = failures
    if failures:
        for stage, reason in failures.items():
            _log.error("[scheduler] Post-AIRS %s failed: %s", stage, reason)
        causes = []
        for stage, reason in failures.items():
            message = reason.get('message', reason) if isinstance(reason, dict) else reason
            causes.append(f'{stage}: {str(message)[:600]}')
        require_complete('Post-AIRS preparation incomplete: ' + ' · '.join(causes)
                         + '. Completed work is saved. Next: resolve the listed source errors, '
                         'then retry dashboard_post_airs; full evidence is in the job receipt.', summary)
    return "Price checks, monthly reports and Analyse prepared; freshness verified", summary


def _body_table_size_sample(ctx=None, *, runtime: JobRuntime) -> tuple[str, dict]:
    """ NOT CANCELLABLE, AND NOT WORTH MAKING SO — one catalog read and ~50 small inserts, over in
    milliseconds. A cancel would land after it finished."""
    from db_growth import sample_table_sizes  # noqa: PLC0415

    res = sample_table_sizes()
    _log.info("[scheduler] db size snapshot: %s tables, %s MB total (biggest %s)",
              res.get("tables"), res.get("total_mb"), res.get("biggest"))
    return f"{res.get('tables')} tables, {res.get('total_mb')} MB total", res


