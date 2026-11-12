"""AIRS refresh workflows shared by HTTP job adapters and scheduled workers.

No HTTP handlers or scheduler imports belong here. Scanners retain ownership of
session locks and persistence; adapters own job registration and run recording.
"""
from __future__ import annotations

import logging
import re

log = logging.getLogger(__name__)


class RefreshCancelled(Exception):
    """Service cancellation translated by the caller's job transport."""


def scan_models(ctx, *, force: bool = False) -> str:
    from jobs import JobCancelled

    #  The bar's position is carried, not re-derived per event. `ctx.progress` writes
    # `done`/`total` onto the job every time, so a narration line emitted mid-count with a
    # freshly-zeroed pair would blank a bar that is genuinely at 40/58 — and the next `count`
    # would snap it back. The list phase legitimately has no denominator; 0 renders as
    # indeterminate (`JobToaster`: `total > 0 ? pct : null`), which is what it is.
    at = {"done": 0, "total": 0}

    def on_event(kind: str, **kw) -> None:
        msg = str(kw.get("message") or "")
        #  `n` ARRIVES ONE LINE BEFORE THE FIRST `i`. The count phase announces its
        # denominator ("counting holdings for 58 fixed portfolios") before it downloads
        # anything, so adopting it from ANY event that carries it puts a real 0/58 on the bar
        # for the ~10s of the first XLS instead of an indeterminate one.
        if kw.get("n"):
            at["total"] = int(kw["n"])
        if kind == "count":
            at["done"] = int(kw.get("i") or 0)
        elif kind == "portfolios":
            # It carries the whole roster as a payload; the job wants the one number.
            msg = f"{kw.get('count') or 0} portfolios listed — counting holdings…"
        #  NO `error` BRANCH, AND THAT IS NOT AN OVERSIGHT. Neither of these two scanners
        # reports a fault as an EVENT — they raise, which the job runner already turns into a
        # `failed` card. (The Front-Office scraper elsewhere in `airs_scanner` does emit one;
        # this path does not, and handling a kind that never arrives reads as coverage.)
        if msg:
            ctx.progress(at["done"], at["total"], msg)

    ctx.progress(0, 0, "signing in to AirSPMS and reading the model-portfolio list…")
    result = collect_model_compositions(
        on_event=on_event, force=force, should_stop=lambda: ctx.cancelled,
        on_selection=lambda count: ctx.progress(
            0, count, f"{count} model compositions missing — repairing those only…"))
    if result is None:
        return "Another AIRS scan is already running — nothing was re-read"
    rows, to_count = result['rows'], result['selected']

    counted = sum(1 for p in to_count if p.get("holdings") is not None)
    if ctx.cancelled:
        #  Raised, not returned — a returned string is a `done` job, and `done` is a word other
        # code ACTS ON. See the same fix on the account job: returning here would paint a green
        # card over a run that stopped early. `JobCancelled`'s message becomes the summary, so
        # nothing is lost by raising it.
        raise JobCancelled(f"Cancelled — {at['done']}/{at['total']} counted, "
                           f"{len(rows)} portfolios stored")
    return (f"{len(rows)} portfolios listed; {counted}/{len(to_count)} "
            f"model compositions {'re-read' if force else 'repaired'}")


def refresh_models(ctx) -> str:
    from jobs import JobCancelled
    from services.job_progress import reporter

    try:
        message, _summary = price_models(
            stored_composition=False, on_step=reporter(ctx),
            should_stop=lambda: ctx.cancelled)
    except RefreshCancelled as exc:
        raise JobCancelled(str(exc)) from exc
    if ctx.cancelled:
        raise JobCancelled(f"Cancelled — {message}")
    return message


def refresh_accounts(ctx, *, force: bool = False) -> str:
    from jobs import JobCancelled

    from management_portfolios import dashboard_progress_label

    def progress(done, total, message):
        # Keep scraper receipts in the server log; show one clear activity per portfolio.
        if total and (match := re.match(r"^\d+/\d+: (.+)$", message)):
            ctx.progress(max(0, done - 1), total, f"{match[1]} — Reading from AIRS…")
        elif not total:
            ctx.progress(0, 0, "Connecting to AIRS…")

    res = scan_accounts(
        triggered_by="manual-force" if force else "manual",
        force=force,
        dashboard_only=True,
        include_price_checks=False,
        on_step=progress,
        on_account_done=lambda done, total, account: ctx.progress(
            done, total, f"{dashboard_progress_label(account)} — Updated from AIRS",
            account=account, account_done=True),
        #  `ctx.check()` RAISES to unwind a job; here we only need the FLAG, because the scan
        # has to run its own finalisation (write the status, release `_LOCK`) before returning.
        # `cancelled` is the registry's own view of whether Cancel was pressed.
        should_stop=lambda: ctx.cancelled,
    )
    if res.get("status") == "busy":
        return "Another AIRS refresh is already running — nothing was re-read"
    stopped = res.get("cancelled_at")
    errs = res.get("errors") or []
    #  The data's own date rides along. The counts are about what WE fetched; the row
    # badges measure AIRS's valuation date, and without this the two read as contradictory
    # ("44 accounts" against "3 trading days old"). See `format_run_message`.
    summary = (f"{res.get('complete_accounts', 0)}/{res.get('portfolios_found', 0)} accounts, "
               f"{res.get('holdings_rows', 0)} holdings"
               + (f" · newest AIRS valuation {res['newest_as_of']}"
                  if res.get('newest_as_of') else ''))
    if stopped:
        #  Raised, not returned, and that is a bug fix rather than a tidy-up. A worker that
        # RETURNS is `done` — and `done` is not decoration here, the button ACTS on it: the
        # frontend runs phase two (the minutes-long model scan) only `if (job.status ===
        # 'done')`, precisely so a Cancel does not buy the reader minutes more of exactly what
        # they asked to stop. Returning a string beginning "Cancelled" satisfied the reader and
        # not the gate, so Cancel painted a green card and then started the slow half anyway —
        # which from the outside is a Cancel button that does nothing.
        #
        # `JobCancelled`'s message becomes the summary, so the sentence below is not lost.
        raise JobCancelled(f"Cancelled before {stopped} — {summary} stored before stopping")
    if res.get("status") == "error":
        raise RuntimeError(res.get("message") or "AIRS scan stored nothing")
    return summary + (f" — {len(errs)} report(s) failed, see the console" if errs else "")


def refresh_account(ctx, *, portefeuille: str, cascade: bool = True) -> str:
    from routers._airs_full_refresh import job_verdict, refresh_portfolio_fully

    from management_portfolios import dashboard_progress_label
    label = dashboard_progress_label(portefeuille)
    ctx.progress(0, 1, f"{label} — Reading from AIRS…")
    full = refresh_portfolio_fully(
        portefeuille=portefeuille, cascade=cascade,
        halves=("book",), include_price_checks=False,
        should_stop=lambda: ctx.cancelled)
    #  The wording lives in `job_verdict`, NOT HERE. It was a closure, so nothing could test
    # it — and it read the BOOK's cancel flag alone while `refresh_portfolio_fully` also
    # cancels at two boundaries where there is no book result to carry one, which turned a
    # pressed Cancel into a red "AIRS scan failed — no reports returned". See its docstring.
    return job_verdict(portefeuille, full)


def price_models(*, stored_composition: bool = True, on_step=None, should_stop=None) -> tuple[str, dict]:
    """Price paired models, reusing today's scan only for the scheduled pipeline.

    Manual refreshes read compositions live: the preceding repair-only scan may have
    retained yesterday's valid composition. Attempt every model and report failures together.
    """
    from dashboard_daily import IncompleteRefresh
    from routers._airs_account_links import list_account_links  # noqa: PLC0415
    from routers._airs_portfolio_refresh import refresh_portfolio  # noqa: PLC0415
    from management_portfolios import is_dashboard_account, dashboard_refresh_key, dashboard_progress_label

    step = on_step or (lambda *_: None)
    stop = should_stop

    #  Only the paired ones. A model with no account running it has no valuation to keep current,
    # and an account with no model has no composition to price — `refresh_portfolio_fully` would
    # report `absent` for every one of them and spend a request finding out.
    paired = {a["model_portfolio_id"]: a["portefeuille"] for a in list_account_links()["accounts"]
              if a.get("model_portfolio_id") is not None and is_dashboard_account(a.get("portefeuille"))}
    if not paired:
        return "no paired model portfolios to price", {"portfolios": 0}

    done = {"n": 0}

    def _on_result(name: str, res: dict) -> None:
        done["n"] += 1
        message = f"{dashboard_progress_label(name)} — {res.get('model_status') or res.get('status')}"
        if res.get("error"):
            message += f": {res['error']}"
        step(done["n"], len(paired), message)

    step(0, len(paired), f"pricing {len(paired)} model portfolio(s)")
    results = []
    for pid, account in sorted(paired.items(), key=lambda item: dashboard_refresh_key(item[1])):
        if stop and stop():
            raise RefreshCancelled("stopped before model pricing")
        try:
            result = refresh_portfolio(pid, lambda kind, **kw: step(done["n"], len(paired), kw.get("message", "")),
                                       stored_composition=stored_composition)
            results.append({"portefeuille": account, "model_portfolio_id": pid,
                            "model_status": "skipped" if result.get("status") == "skipped" else "ok",
                            "model": result})
        except Exception as exc:
            results.append({"portefeuille": account, "model_portfolio_id": pid,
                            "model_status": "error", "error": str(exc)})
            log.exception("[scheduler] AIRS model pricing failed for %s (model %s): %s", account, pid, exc)
        _on_result(account, results[-1])
    ok = sum(1 for r in results if r.get("model_status") == "ok")
    bad = [r.get("portefeuille") for r in results if r.get("model_status") not in ("ok", "skipped")]
    skipped = sum(r.get("model_status") == "skipped" for r in results)
    summary = {"portfolios": len(paired), "priced": ok, "skipped": skipped, "failed": len(bad),
               "failed_names": bad, "results": results}
    msg = f"{ok}/{len(paired)} model portfolio(s) repriced"
    if skipped:
        msg += f"; {skipped} without a Fixed composition (not applicable)"
    if bad:
        msg += f" — {len(bad)} FAILED: {', '.join(str(b) for b in bad[:5])}"
        raise IncompleteRefresh(msg, summary)
    return msg, summary


def scan_accounts(**options) -> dict:
    """Shared scanner entry; callers choose force/progress policy, scanner owns its lock."""
    from airs_vermogen import run_airs_vermogen_refresh_sync

    return run_airs_vermogen_refresh_sync(**options)


def collect_model_compositions(*, on_event, force: bool, should_stop=None,
                               on_selection=None, wait_seconds: int = 0,
                               require_nonempty: bool = False) -> dict | None:
    """Discover, persist and scan dashboard models under the shared AIRS session lock.

    A nonblocking manual caller gets None when busy; the daily caller can wait.
    Partial composition failures are saved and returned separately from discovery.
    """
    from airs_vermogen import _LOCK, _acquire_session
    from airs_scanner import fetch_model_portfolios_sync, count_model_portfolio_holdings_sync
    from management_portfolios import dashboard_model_rows
    from routers import _airs_portfolio_store as store

    acquired = _acquire_session(wait_seconds) if wait_seconds else _LOCK.acquire(blocking=False)
    if not acquired:
        return None
    errors = []
    try:
        rows = fetch_model_portfolios_sync(on_event)
        if require_nonempty and not rows:
            raise RuntimeError("AIRS model discovery returned no portfolios")
        store.save_portfolios(rows)
        scoped = dashboard_model_rows(rows)
        missing = set() if force else store.unscanned_position_ids([row['id'] for row in scoped])
        selected = scoped if force else [row for row in scoped if row['id'] in missing]
        if not force and on_selection:
            on_selection(len(selected))
        def failed(pid, error):
            errors.append({'id': pid, 'error': str(error)})
            store.save_positions_error(pid, error)
        kwargs = dict(on_positions=store.save_positions, on_error=failed)
        if should_stop is not None:
            kwargs['should_stop'] = should_stop
        count_model_portfolio_holdings_sync(selected, on_event, **kwargs)
        return {'rows': rows, 'selected': selected, 'errors': errors}
    finally:
        _LOCK.release()
