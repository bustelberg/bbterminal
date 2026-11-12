"""Airs job implementations; called through scheduler dispatch adapters."""
from __future__ import annotations
import logging

from scheduler_runtime.execution import Cancelled, cancel_requested, require_complete
from scheduler_jobs.runtime import JobRuntime

_log = logging.getLogger('scheduler')

def _body_airs_model_prices(ctx=None, *, stored_composition: bool = True, runtime: JobRuntime) -> tuple[str, dict]:
    """Scheduled adapter for the shared paired-model pricing workflow."""
    from services.airs_refresh import RefreshCancelled, price_models

    try:
        return price_models(stored_composition=stored_composition,
                            on_step=runtime.reporter(ctx),
                            should_stop=lambda: cancel_requested(ctx))
    except RefreshCancelled as exc:
        raise Cancelled(str(exc)) from exc




def _body_airs_vermogen(ctx=None, *, runtime: JobRuntime) -> tuple[str, dict]:
      """ CANCEL LANDS BETWEEN ACCOUNTS, NEVER INSIDE ONE — `run_airs_vermogen_refresh_sync`
      already takes the `should_stop` hook and checks it at exactly that boundary, because an
      account's four reports are downloaded and stored as a unit. Everything already stored is kept.

       ONE RESULT FOR BOTH HALVES, because it is one JOB — but each half keeps its own try/except,
      so a failed composition scan still cannot cost the daily valuation. It reports `error` if
      EITHER failed and names which: "not fatal to the accounts refresh" is a statement about
      control flow, not a reason to report a failure as ok."""
      summary: dict = {}
      failures: list[str] = []
      stop = (lambda: bool(getattr(ctx, "cancelled", False))) if ctx is not None else None
      step = runtime.reporter(ctx)
      if True:
        try:
            from services.airs_refresh import scan_accounts as run_airs_vermogen_refresh_sync
            #  Two phases, two counters, and the message says which. The accounts pass counts
            # accounts and the model pass counts models; the totals are only known when each starts,
            # so the bar restarts between them. A restarting bar reads as a failure unless the line
            # under it names the phase — hence the prefixes below.
            res = run_airs_vermogen_refresh_sync(
                triggered_by="auto", force=True, should_stop=stop, dashboard_only=True,
                on_step=lambda d, t, m: step(d, t, f"Accounts · {m}")) or {}
            if res.get("status") == "busy":
                summary["session_busy"] = True
                raise RuntimeError("AIRS browser session is busy; daily account scan did not run")
            summary.update(accounts=res.get("complete_accounts"),
                           portfolios_found=res.get("portfolios_found"),
                           holdings_rows=res.get("holdings_rows"),
                           cancelled_at=res.get("cancelled_at"))
            if res.get("status") != "ok" or res.get("errors") or res.get("accounts_failed"):
                failures.append(f"accounts: {res.get('detail') or res.get('message') or res.get('status')}")
                summary["account_errors"] = res.get("errors") or []
        except Exception as e:
            failures.append(f"accounts: {type(e).__name__}: {e}")
            _log.exception(
                "[scheduler] airs_vermogen refresh failed: %s: %s", type(e).__name__, e,
            )
        if summary.get("session_busy"):
            require_complete("Daily AIRS scan could not acquire the browser session", {**summary, "errors": failures})
        #  The model portfolios too — nothing scheduled has ever scanned them, and the pairing
        # Silently depends on them. This tick refreshes the ACCOUNTS (Rendement,
        # Vermogensoverzicht); the model COMPOSITIONS were only ever populated by pressing "Scan
        # AIRS" on the portfolios page by hand. So a deployment where nobody pressed it has an
        # empty `airs_model_portfolio_position` — and `_airs_account_links._models()` keeps only
        # models that HAVE a composition, so every account then matches nothing, loses its
        # pairing, and Analyse falls back to an unpaired basket for books that are perfectly
        # fine. That is the production symptom this exists to end: "No valued positions to show",
        # on a portfolio whose rows expand normally one panel away.
        #
        #  AFTER the accounts, in the SAME thread, never beside it. Both drive one authenticated
        # AirSPMS session through Playwright; two scrapers at once is a contended login, and the
        # failure mode there is a half-finished scan rather than an error.
        #  Skipped entirely on a cancel. Running minutes more of scraping after the reader asked
        # to stop is the same mistake the fleet refresh already refuses to make.
        try:
            if stop is not None and stop():
                raise Cancelled("stopped between accounts; everything stored so far is kept")
            #  The scanner already emits per-item events — it was being handed a no-op. Both
            # `fetch_model_portfolios_sync` and `count_model_portfolio_holdings_sync` send a
            # `message` per portfolio; forwarding them is the difference between a toast that says
            # "starting…" for four minutes and one that names the book it is on.
            #
            #  The pair is read as data now (2026-08-17). It used to say the scanner did not expose
            # `i`/`n`, so this counted `count` events itself and reported a total of 0 — an
            # indeterminate bar for the four minutes this phase runs. `count_model_portfolio_
            # holdings_sync` carries them as fields since the manual button became a job, and both
            # callers read the same pair rather than each keeping a private tally. Still never
            # parsed out of the message: the prose is for the reader, the numbers are for the bar.
            models_at = {"done": 0, "total": 0}
            model_errors = []

            def _relay(kind: str, **kw) -> None:
                msg = kw.get("message")
                if not msg:
                    return
                if kind == "count":
                    models_at["done"] = int(kw.get("i") or 0)
                if kw.get("n"):
                    models_at["total"] = int(kw["n"])
                step(models_at["done"], models_at["total"], f"Models · {msg}")
                #  Checked here too. The model scan is the long half (one edit page + one XLS per
                # portfolio, minutes); without this a Cancel pressed during it would be honoured
                # only after every remaining book had been downloaded.
                #
                #  Two mechanisms reach the same boundary, and that is worth knowing. This raises
                # out of the event hook; the manual job passes `should_stop=` and the scanner
                # returns. Both stop BETWEEN portfolios — this one because `count` is emitted after
                # the row is downloaded, counted and persisted — so neither can leave a row half
                # written. Named rather than unified: rewriting the scheduler's cancellation is a
                # separate change from making the button's own scan stoppable.
                if stop is not None and stop():
                    raise Cancelled(
                        f"stopped after {models_at['done']} model(s); everything stored is kept")

            from services.airs_refresh import collect_model_compositions
            model_result = collect_model_compositions(
                on_event=_relay, force=True, wait_seconds=60, require_nonempty=True)
            if model_result is None:
                raise RuntimeError("Daily model scan could not acquire the AIRS session")
            rows = model_result['rows']
            scoped_models = model_result['selected']
            model_errors = model_result['errors']
            _log.warning("[scheduler] airs model-portfolio scan: %d portfolio(s) stored", len(rows))
            summary["models"] = len(scoped_models)
            summary['models_discovered'] = len(rows)
            if model_errors:
                summary["model_errors"] = model_errors
                failures.append(f"models: {len(model_errors)} compositions failed")
        except Cancelled:
            raise
        except Exception as e:
            # Best effort, and NEVER fatal to the accounts refresh above — that one is the daily
            # valuation and must not be lost to a failure in the composition scan.
            failures.append(f"models: {type(e).__name__}: {e}")
            _log.exception(
                "[scheduler] airs model-portfolio scan failed: %s: %s", type(e).__name__, e,
            )
        # Refresh shared prices after AIRS has published its account valuation dates.
        # Do not give the market stage an independent clock: preparation must follow both.
        try:
            if stop is not None and stop():
                raise Cancelled("stopped before market data; stored work is kept")
            runtime.run_job("dashboard_market", ctx)
        except Cancelled:
            raise
        except Exception as exc:
            summary["market"] = getattr(exc, "summary", {"error": str(exc)})
            failures.append(f"market: {exc}")
        # The last AIRS phase deliberately starts only after the account and composition browser
        # work is finished. A separate job would race the same AirSPMS session and either
        # job could leave a partial refresh behind.
        try:
            if stop is not None and stop():
                raise Cancelled("stopped before paired-model pricing; stored work is kept")
            pricing_message, pricing = runtime.model_prices(ctx)
            summary["pricing"] = pricing
            _log.info("[scheduler] AIRS paired-model pricing: %s", pricing_message)
            # Report now reads AIRS's own monthly observations and dated exports.
            # No Yahoo/Brinson attribution warming is needed after this scan.
        except Cancelled:
            raise
        except Exception as e:
            summary["pricing"] = getattr(e, "summary", {"error": str(e)})
            failures.append(f"pricing: {type(e).__name__}: {e}")
            _log.exception(
                "[scheduler] AIRS paired-model pricing failed: %s: %s", type(e).__name__, e,
            )
      try:
          runtime.run_job("dashboard_post_airs", ctx)
      except Exception as exc:
          summary["post_airs"] = getattr(exc, "summary", {"error": str(exc)})
          failures.append(f"post-AIRS: {exc}")
      if failures:
          require_complete(" · ".join(failures)[:400], {**summary, "errors": failures})
      return (f"{summary.get('accounts')} account(s), {summary.get('models')} model(s), "
              f"{(summary.get('pricing') or {}).get('priced', 0)} model(s) priced", summary)


