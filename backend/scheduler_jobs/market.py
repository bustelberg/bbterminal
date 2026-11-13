"""Market job implementations; called through scheduler dispatch adapters."""
from __future__ import annotations
import logging

from scheduler_runtime.execution import Cancelled, cancel_requested, require_complete
from scheduler_runtime.context import LogContext
from scheduler_jobs.runtime import JobRuntime

_log = logging.getLogger('scheduler')

_RANKED_UNIVERSES = ("ACWI", "SP500", "AEX")
_RANK_COVERAGE_WARN = 70.0
_FUNDAMENTAL_INDICES = ("ACWI", "SP500", "AEX")
_FUNDAMENTALS_REGION_FLOOR = 2000

def _body_asset_price_refresh(ctx=None, *, runtime: JobRuntime) -> tuple[str, dict]:
    """ THE EXISTING BODY, UNCHANGED — it already owns its own `record_run` (it has three distinct
    `skipped` outcomes to report) and its own try/except. Wrapping it again would open a second run
    row for one run, so `_run_body` skips its own record for this one."""
    #  One line before delegating, because everything after it is inside `_run_asset_price_refresh`
    # and that function narrates into its OWN run record, not into this job's progress stream. Its
    # three early exits are quick, but the working path fetches the gap for every held instrument —
    # so without this its toast reads "starting…" from press to finish. Making the inner function
    # take a reporter is the fuller fix; it owns its own `record_run` and three `skipped` outcomes,
    # so it is not a change to make in passing.
    runtime.reporter(ctx)(0, 0, "refreshing held instruments from Yahoo — detail in the run record…")
    runtime.asset_refresh("daily dashboard stage", raise_on_failure=True)
    return "see the run record", {}


def _run_asset_price_refresh(trigger: str, *, raise_on_failure: bool = False, runtime: JobRuntime) -> None:
    """The body, shared by the 06:00 tick and the startup catch-up. Never raises.

     THE THREE EARLY EXITS ARE `skipped`, NOT `ok` AND NOT FAILURES. Standing down for the queue
    worker, finding everything current, and waiting on a database that has not finished booting are
    all correct behaviour — but recording them as `ok` would make a job that stood down every single
    day for a fortnight (because a stalled worker looked live) indistinguishable from one doing its
    work. The overview can then show the reason.
    """
    #  `startup` IS ITS OWN TRIGGER, so the overview does not read a restart storm under
    # `uvicorn --reload` as the 06:00 tick having fired forty times.
    triggered = "startup" if "startup" in trigger else "auto"
    with runtime.record_run("asset_price_refresh", triggered_by=triggered) as rec:
      try:
        from asset_pipeline import price_refresh, queue as _q  # noqa: PLC0415

        #  First, wait for the database — this fires on startup, in parallel with the Supabase
        # stack coming up, and every query below (the queue heartbeat is simply the first) fails
        # with PGRST002 until PostgREST has loaded its schema cache. Losing the catch-up to a
        # boot race costs a whole day of stale held prices; see `_await_db_ready`.
        if not runtime.await_db_ready(f"asset price refresh ({trigger})"):
            if raise_on_failure:
                raise RuntimeError("Database was not ready for the daily market refresh")
            rec.skip("the database was not ready in time")
            return

        if _q.is_worker_active():
            if raise_on_failure:
                raise RuntimeError("Yahoo ingest queue is active; daily market refresh could not run")
            _log.info(
                "[scheduler] asset price refresh (%s) SKIPPED — the ingest queue worker is live "
                "(last activity %s); adding Yahoo load now risks corrupting the listings it is "
                "resolving.", trigger, _q.last_activity(),
            )
            rec.skip(f"the ingest queue worker is live (last activity {_q.last_activity()})")
            return

        # Detect BEFORE fetching. A restart must not cost ~220 Yahoo calls just to discover
        # there was nothing to do — and with `--reload` in dev, restarts are constant. This is
        # a handful of queries (one grouped COPY), so the common case is a near-free no-op.
        watched = price_refresh.watched_isins()
        stale, latest, considered = price_refresh.find_stale(isins=watched, stale_days=1)
        # A resolved sold name with no rows at all is not "stale" (there is no
        # last date to compare), so repair that distinct case after the cheap
        # detection pass. This uses its existing symbol and cannot remap it.
        backfill = price_refresh.backfill_missing(watched)
        # A handful of reviewed funds publish their official NAV outside Yahoo. Keep those exact
        # sources on the same daily path: otherwise they are only repaired after a user opens a
        # risk view that has already failed. This is a bounded, reviewed list (currently one
        # source), never a search or a guessed proxy.
        nav_fallback = price_refresh.backfill_nav_fallbacks(watched)

        # Analyse also shows benchmark returns derived from a small set of Yahoo ETF proxies.
        # These used to be refreshed only from the smart daily pipeline, which is conditional on
        # enabled strategies. That left a quiet deployment with current holdings but an ageing
        # benchmark until somebody opened Analyse and its lazy repair ran. Keep all Yahoo-backed
        # Analyse inputs on this unconditional daily path instead. The helper protects against a
        # duplicate same-day request when the smart pipeline did run.
        try:
            from routers._benchmark_etf import refresh_index_proxies  # noqa: PLC0415
            proxies_refreshed = refresh_index_proxies(strict=True) if raise_on_failure else refresh_index_proxies()
        except Exception as proxy_error:  # noqa: BLE001
            # A proxy failure should not discard a successful holdings refresh. Individual proxy
            # failures are also logged in refresh_index_proxies, so preserve the main job's result.
            proxies_refreshed = None
            _log.warning(
                "[scheduler] asset price refresh (%s): Analyse benchmark proxy refresh failed: "
                "%s: %s", trigger, type(proxy_error).__name__, proxy_error,
            )

        if raise_on_failure and (backfill.get("failed") or nav_fallback.get("failed") or not proxies_refreshed):
            nav_errors = '; '.join(f"{row['isin']} / {row['provider']} issue {row['issue_id']}: {row['error']}"
                                  for row in nav_fallback.get('results', []) if row.get('error'))
            require_complete(f"Price source preparation incomplete: missing-series failures={backfill.get('failed', 0)}, "
                             f"NAV failures={nav_fallback.get('failed', 0)}, benchmark proxies={proxies_refreshed}. "
                             f"{nav_errors} Completed prices are saved; retry asset_price_refresh after resolving the source error.",
                             {'backfill': backfill, 'nav_fallback': nav_fallback,
                              'analyse_benchmark_proxies_refreshed': proxies_refreshed})

        if not stale:
            _log.info(
                "[scheduler] asset price refresh (%s): all %s watched instrument(s) current as of "
                "%s — nothing to do", trigger, considered, latest,
            )
            detail = f"all {considered} watched instrument(s) current as of {latest}"
            if backfill["backfilled"]:
                detail += f"; {backfill['backfilled']} absent series backfilled"
            if nav_fallback["stored"]:
                detail += f"; {nav_fallback['stored']} published NAV fallback refreshed"
            if proxies_refreshed is not None:
                detail += f"; {proxies_refreshed} Analyse benchmark proxy series refreshed"
            else:
                rec.skip(detail)
            rec.done(detail, considered=considered, latest=str(latest), stale=0,
                     price_backfilled=backfill["backfilled"],
                     nav_fallback_refreshed=nav_fallback["stored"],
                     nav_fallback=nav_fallback,
                     analyse_benchmark_proxies_refreshed=proxies_refreshed)
            return

        # WARNING, not info: uvicorn leaves the ROOT logger at WARNING, so an `info` here is
        # invisible in production — and "we found the held prices stale and are refetching them"
        # is exactly the line you want in the deploy log. `_maybe_kickstart_smart` logs its own
        # firing at warning for the same reason. The no-op case above stays at info: a healthy
        # restart should be quiet.
        _log.warning(
            "[scheduler] asset price refresh (%s): %s of %s watched instrument(s) stale vs %s "
            "(oldest %s) — fetching the gap",
            trigger, len(stale), considered, latest, stale[0]["last_close"],
        )
        r = price_refresh.refresh_stale(isins=watched, stale_days=1)
        _log.warning(
            "[scheduler] asset price refresh (%s) done — %s moved, %s unchanged, %s failed",
            trigger, r["moved"], r["unchanged"], r["failed"],
        )
        detail = f"{r['moved']} moved, {r['unchanged']} unchanged, {r['failed']} failed"
        remaining = r.get("remaining_stale", 0)
        if remaining:
            detail += f"; {remaining} still stale"
            for result in r.get("results", []):
                if result.get("still_stale"):
                    _log.warning("[scheduler] asset price refresh (%s): %s (%s) still dated %s; expected %s — %s",
                                 trigger, result["symbol"], result.get("isin"), result["latest_close"],
                                 result["expected_close"], result.get("error") or result.get("reason") or "price advanced but remains behind")
        # A request completing is not proof that prices became current. Keep the unresolved
        # instruments in the durable receipt, including on startup, without crashing startup.
        unavailable = sum(bool(row.get('still_stale')) and row.get('status') == 'unavailable' for row in r.get('results', []))
        r['vendor_unavailable'] = unavailable
        if r["failed"] or remaining > unavailable or r.get("skipped"):
            require_complete(f"Watched instrument price refresh incomplete: {detail}", r)
        if proxies_refreshed is not None:
            detail += f"; {proxies_refreshed} Analyse benchmark proxy series refreshed"
        if nav_fallback["stored"]:
            detail += f"; {nav_fallback['stored']} published NAV fallback refreshed"
        rec.done(detail,
                 considered=considered, stale=len(stale), moved=r["moved"],
                 unchanged=r["unchanged"], failed=r["failed"],
                 remaining_stale=remaining, results=r.get("results", []),
                 price_backfilled=backfill["backfilled"],
                 nav_fallback_refreshed=nav_fallback["stored"],
                 nav_fallback=nav_fallback,
                 analyse_benchmark_proxies_refreshed=proxies_refreshed)
      except Exception as e:  # noqa: BLE001
        #  Caught here, so the record must be set by hand. `record_run` marks a run failed off the
        # exception PROPAGATING; swallowing it (which this must, to keep the scheduler thread
        # alive) would otherwise close the row as `ok`.
        rec.status = "error"
        if isinstance(getattr(e, "summary", None), dict):
            rec.summary = e.summary
        rec.detail = f"{type(e).__name__}: {str(e)[:400]}"
        _log.exception("[scheduler] asset price refresh (%s) failed", trigger)
        if raise_on_failure:
            raise


def _body_benchmark_price_slice(ctx=None, *, runtime: JobRuntime) -> tuple[str, dict]:
    """Refresh benchmark prices and caps, then rank the refreshed universes."""
    from routers._benchmark_refresh import refresh_benchmark  # noqa: PLC0415

    step = runtime.reporter(ctx)
    summaries: dict[str, dict] = {}
    # ONE union for the whole automatic run. ACWI contains most of SP500 and part of AEX; these
    # sets ensure the later passes reuse those results instead of asking Yahoo for them again.
    # Manual benchmark Refresh does not pass the sets and still fetches every constituent.
    price_symbols: set[str] = set()
    cap_symbols: set[str] = set()
    for i, label in enumerate(_RANKED_UNIVERSES, start=1):
        try:
            step(i - 1, len(_RANKED_UNIVERSES), f"{label}: starting")

            def emit(_kind: str, **fields) -> None:
                message = str(fields.get("message") or "").strip()
                if message:
                    step(i - 1, len(_RANKED_UNIVERSES), f"{label}: {message}")

            summaries[label] = refresh_benchmark(
                label, emit,
                run_price_symbols=price_symbols,
                run_cap_symbols=cap_symbols,
                skip_current_prices=True,
            )
            step(i, len(_RANKED_UNIVERSES), f"{label}: complete")
        except Exception as exc:                                    # noqa: BLE001
            _log.warning("[benchmark-prices] %s failed (%s: %s)",
                         label, type(exc).__name__, exc)
            summaries[label] = {"failed": True, "error": str(exc)}
            step(i, len(_RANKED_UNIVERSES), f"{label}: failed")

    fetched = sum(int(s.get("prices_fetched") or 0) for s in summaries.values())
    moved = sum(int(s.get("prices_moved") or 0) for s in summaries.values())
    failed = sum(int(s.get("prices_failed") or 0) for s in summaries.values())
    reused = sum(int(s.get("prices_skipped_overlap") or 0) for s in summaries.values())
    current = sum(int(s.get("prices_skipped_current") or 0) for s in summaries.values())
    _log.info("[benchmark-prices] %s; %d price series fetched, %d updated, %d failed",
              {label: s.get("priceable", 0) for label, s in summaries.items()}, fetched, moved, failed)
    rank_detail, rank_summary = runtime.relative_momentum(ctx)
    if failed or any(s.get("failed") or s.get("universe_members") == 0 for s in summaries.values()) or rank_summary.get("failed"):
        require_complete("Dashboard benchmark prices or momentum ranks are incomplete",
                          {"benchmarks": summaries, "relative_momentum": rank_summary, "prices_failed": failed})
    return (f"{fetched} unique price series checked; {reused} overlapping constituent(s) reused; "
            f"{current} already current; {moved} updated; {rank_detail}",
            {"benchmarks": summaries, "prices_fetched": fetched, "prices_moved": moved,
             "prices_failed": failed, "prices_reused": reused,
             "prices_skipped_current": current,
             "relative_momentum": rank_summary})


def _body_relative_momentum_refresh(ctx=None, *, runtime: JobRuntime) -> tuple[str, dict]:
    """Compute and persist ranks after benchmark prices have refreshed.

     ONE UNIVERSE'S FAILURE IS NOT THE JOB'S, the same rule as the index refresh beside it: a
    universe with no members or a bad price load must not cost the other two their ranks.

     IT RANKS AS OF THE NEWEST CLOSE WE HOLD, NEVER `today`. Today is routinely a date we have no
    prices for — a weekend, a holiday, a pipeline that has not run — and asking for it would rank
    an empty set or drop every name on the staleness rule. This is the same date /backtest uses for
    its default end.
    """
    from momentum import relative  # noqa: PLC0415
    from routers.momentum._helpers import latest_db_price_date  # noqa: PLC0415

    step = runtime.reporter(ctx)
    as_of = latest_db_price_date()
    if as_of is None:
        #  Not an error. No closes at all is a statement about the price pipeline, not about this
        # job, and failing here would point an operator at the wrong thing.
        return "no close prices held — nothing to rank", {"ranked": 0, "skipped": True}

    done: list[str] = []
    failed: list[str] = []
    thin: list[str] = []
    n = len(_RANKED_UNIVERSES)
    #  THE `on_step` BELOW ALREADY EXISTED AND WENT ONLY TO THE LOG, which is invisible to whoever
    # pressed the button — ~12s and 2,270 rows of silence. It now goes to both: the log keeps the
    # per-universe detail for Railway, the toast gets the same line.
    step(0, n, f"ranking {n} universe(s) as of {as_of}…")
    for i, label in enumerate(_RANKED_UNIVERSES, start=1):
        if cancel_requested(ctx):
            break
        try:
            result = relative.compute(
                label, as_of,
                on_step=lambda m, _l=label, _i=i: (
                    _log.info("[relative_momentum] %s: %s", _l, m),
                    step(_i - 1, n, f"{_l} · {m}"),
                )[0],
            )
            relative.persist(result)
            cov = result.coverage_pct
            done.append(f"{label} {result.universe_n}/{result.members_total}")
            step(i, n, f"{label} — {result.universe_n}/{result.members_total} ranked "
                       f"({cov:.0f}% coverage)")
            if cov < _RANK_COVERAGE_WARN:
                #  WARNING, because uvicorn leaves the root logger at WARNING in production and an
                #   `info` here would be invisible exactly when it matters.
                thin.append(f"{label} {cov:.0f}%")
                _log.warning("[relative_momentum] %s coverage %.1f%% (%d of %d) — constituent "
                             "closes may have stopped refreshing", label, cov,
                             result.universe_n, result.members_total)
        except Exception as e:  # noqa: BLE001 — one universe must not take the others down
            _log.warning("[relative_momentum] %s failed: %s: %s", label, type(e).__name__, e)
            failed.append(f"{label}: {type(e).__name__}")
            #  A failed universe still advances the counter, or a job that fails on its first
            # label sits at 0/3 looking hung for the other two — the exact reading this whole
            # change exists to fix.
            step(i, n, f"{label} — FAILED: {type(e).__name__}")

    msg = f"ranked as of {as_of} — {', '.join(done) or 'nothing'}"
    if thin:
        msg += f" ·  thin coverage: {', '.join(thin)}"
    if failed:
        msg += f" · failed {', '.join(failed)}"
    return msg, {"as_of": as_of.isoformat(), "ranked": len(done),
                 "failed": len(failed), "thin": len(thin)}


def _body_acwi_weekly_coverage(ctx=None, *, runtime: JobRuntime) -> tuple[str, dict]:
    from routers._acwi_weekly import run
    from index_universe.acwi.weekly_holdings import publication_connection, refresh
    class WeeklyContext(LogContext):
        def emit(self, kind, message='', **data):
            super().emit(kind, message, **data)
            if ctx is not None and kind == 'diagnostic':
                ctx.emit(kind, message, **data)
        def spent(self, calls):
            super().spent(calls)
            if ctx is not None:
                ctx.spent(calls)
    with publication_connection() as conn:
        if conn is None:
            return 'Weekly ACWI maintenance is already running.', {'skipped': 'another replica holds the lease'}
        weekly_ctx = WeeklyContext(lambda: cancel_requested(ctx), step=runtime.reporter(ctx), label='ACWI')
        weekly_ctx.progress(0, 0, 'Importing the official iShares ACWI holdings…')
        try:
            holdings = refresh(conn, weekly_ctx)
        except Cancelled:
            raise
        except Exception as exc:
            holdings = {'status': 'failed', 'error': str(exc)}
            _log.exception('[scheduler] ACWI holdings import failed; retaining the previous snapshot')
        weekly_ctx.emit('diagnostic', 'iShares ACWI holdings import', details=holdings)
        message, result = run(weekly_ctx, reserve=_FUNDAMENTALS_REGION_FLOOR)
        result['holdings_import'] = holdings
        if holdings['status'] == 'failed':
            require_complete('ACWI holdings import failed; previous snapshot retained', result)
        return message, result


def _body_acwi_monthly_fundamentals(ctx=None, *, runtime: JobRuntime) -> tuple[str, dict]:
    from index_universe.acwi.weekly_holdings import publication_connection
    from routers._acwi_weekly import run

    with publication_connection() as conn:
        if conn is None:
            return 'ACWI maintenance is already running.', {'skipped': 'another replica holds the lease'}
        return run(ctx if ctx is not None else LogContext(step=runtime.reporter(ctx), label='ACWI monthly'),
                   reserve=_FUNDAMENTALS_REGION_FLOOR, statements_only=True)


def _fundamental_company_ids(label: str) -> list[tuple[int, str]]:
    """`[(company_id, gurufocus exchange_code)]` for one index — the GuruFocus side of it.

     NOT `_asset_benchmark.members()`. That returns the ASSET world, whose `company_id` slot
    actually carries an `analysis_id` (its own docstring says so) — handing those to a fundamentals
    fill would look up entirely unrelated companies and quietly fill the wrong ones. Fundamentals
    are keyed on the GuruFocus company.

     THE EXCHANGE RIDES ALONG because the quota is PER REGION, and the region is a property of the
    listing. Without it the budget gate could only be all-or-nothing.
    """
    from deps import IN_CHUNK_SIZE, supabase  # noqa: PLC0415

    uni = (supabase.table("universe").select("universe_id")
           .eq("label", label).limit(1).execute().data or [])
    if not uni:
        return []
    ids: set[int] = set()
    off = 0
    while True:  #  PAGED — 1,998 members for ACWI against PostgREST's 1,000-row cloud cap.
        rows = (supabase.table("universe_membership").select("company_id")
                .eq("universe_id", uni[0]["universe_id"]).order("company_id")
                .range(off, off + 999).execute().data or [])
        if not rows:
            break
        ids.update(r["company_id"] for r in rows)
        off += len(rows)
    out: list[tuple[int, str]] = []
    ordered = sorted(ids)
    for i in range(0, len(ordered), IN_CHUNK_SIZE):
        for c in (supabase.table("company")
                  .select("company_id,gurufocus_exchange:gurufocus_exchange(exchange_code)")
                  .in_("company_id", ordered[i:i + IN_CHUNK_SIZE]).execute().data or []):
            out.append((c["company_id"],
                        ((c.get("gurufocus_exchange") or {}) or {}).get("exchange_code") or ""))
    return out


def _body_benchmark_fundamentals(ctx=None, *, runtime: JobRuntime) -> tuple[str, dict]:
    """Refresh due fundamentals, estimates, and indicators for benchmark constituents.

     THE BUDGET GATE IS THE POINT, and it is the shape `full_price_refresh` already uses: read
    the per-region remaining and DROP the companies whose region is at the floor, rather than
    calling and failing. An exhausted region does not refuse politely — it returns errors that read
    as data problems, and a job that discovers its quota by exhausting it takes the month-end price
    refresh down with it.

     DECIDED BEFORE ANY CALL, PER REGION. A pass that started and stopped halfway would leave an
    index part-filled with no record of where it got to; budgeting up front lets the summary say
    what was deferred, and `only_due=True` means next quarter picks up exactly those.

     ONE INDEX'S FAILURE IS NOT THE JOB'S — they are independent, and the summary names whichever
    fell over.
    """
    from deps import supabase  # noqa: PLC0415
    from ingest.api_usage import _region_for_exchange, remaining_budget  # noqa: PLC0415, PLC2701
    from routers._fundamental_fill import fill_company_ids  # noqa: PLC0415
    from jobs import JobCancelled  # noqa: PLC0415

    budget = remaining_budget(supabase)
    room = {r: max(0, n - _FUNDAMENTALS_REGION_FLOOR) for r, n in budget.items()}
    _log.warning("[benchmark_fundamentals] quota left %s — usable above the %s floor: %s",
                 budget, _FUNDAMENTALS_REGION_FLOOR, room)

    step = runtime.reporter(ctx)
    filled = 0
    deferred = 0
    failed: list[str] = []
    n = len(_FUNDAMENTAL_INDICES)
    seen: set[int] = set()
    receipts: dict[str, dict] = {}
    step(0, n, f"reading quota — usable above the {_FUNDAMENTALS_REGION_FLOOR}-call floor: {room}")
    from dashboard_daily import held_company_ids
    from routers._fundamental_backfill import smart_flags_bulk
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange
    labels = ("Dashboard holdings", *_FUNDAMENTAL_INDICES)
    n = len(labels)
    for i, label in enumerate(labels, start=1):
        if cancel_requested(ctx):
            raise Cancelled("daily fundamentals cancelled")
        #  A fresh ctx per label so its lines carry the index they belong to. This is a pass over
        # ~2,500 companies across three indices; an unlabelled line cannot say which one it is in.
        log_ctx = LogContext(lambda: cancel_requested(ctx), step=step, label=label)
        try:
            pairs = held_company_ids() if label == "Dashboard holdings" else runtime.fundamental_ids(label)
            pairs = [(cid, exch) for cid, exch in pairs if cid not in seen]
            seen.update(cid for cid, _ in pairs)
            flags = smart_flags_bulk([cid for cid, _ in pairs])
        except Exception as e:  # noqa: BLE001
            _log.warning("[benchmark_fundamentals] %s: could not read membership: %s: %s",
                         label, type(e).__name__, e)
            failed.append(f"{label}: {type(e).__name__}: {e}")
            continue
        keep: list[int] = []
        quota_deferred = 0
        for cid, exch in pairs:
            needed = sum(bool(value) for value in flags.get(cid, {}).values())
            if not needed:
                continue
            # The shared fill records subscription exclusions without vendor requests. They
            # must not consume the regional reserve and defer companies we can actually fetch.
            if not exch or not is_gf_subscribed_exchange(exch):
                keep.append(cid)
                continue
            region = _region_for_exchange(exch)
            if room.get(region, 0) < needed + 1:
                deferred += 1
                quota_deferred += 1
                continue
            room[region] -= needed + 1
            keep.append(cid)
        if not keep:
            receipts[label] = {"loaded": 0, "quota_deferred": quota_deferred}
            step(i, n, f"{label} — {quota_deferred} deferred by quota" if quota_deferred
                 else f"{label} — no feeds due")
            continue
        try:
            step(i - 1, n, f"{label} — filling {len(keep)} constituent(s)…")
            # `smart` makes the weekly job evaluate each feed on its own cadence: filings wait for
            # a due period, while analyst estimates (which drive EGM growth) refresh when their
            # source check becomes stale. `only_due` would suppress that second path.
            outcome: dict = {"quota_deferred": quota_deferred}
            receipts[label] = outcome
            message = fill_company_ids(log_ctx, label, keep, feeds="smart", receipt=outcome, best_effort=True)
            from routers._fundamental_fill import VENDOR_EMPTY_MARKER
            reasons = outcome.get('failure_reasons') or {}
            expected_gaps = bool(reasons) and all(VENDOR_EMPTY_MARKER in reason or 'unsubscribed region' in reason.lower()
                                                 for reason in reasons)
            if outcome.get("failed") and not expected_gaps:
                failed.append(f"{label}: {message}")
            filled += outcome.get("loaded", 0)
            deferred += outcome.get("deferred", 0)
            step(i, n, message)
        except Cancelled:
            # Cancellation is control flow, not an index failure.  Let
            # `_run_body` translate it to the registry's `JobCancelled` so
            # the Run-now card cannot claim a partial fill completed.
            raise
        except JobCancelled as e:
            raise Cancelled(str(e)) from e
        except Exception as e:  # noqa: BLE001
            _log.warning("[benchmark_fundamentals] %s failed: %s: %s", label, type(e).__name__, e)
            outcome = receipts.setdefault(label, {})
            outcome["error"] = str(e)
            filled += outcome.get("loaded", 0)
            deferred += outcome.get("deferred", 0)
            failed.append(f"{label}: {type(e).__name__}: {e}")
            step(i, n, f"{label} — FAILED: {e}")

    msg = f"{filled} constituents filled"
    if deferred:
        msg += f" · {deferred} deferred to a later daily pass (quota reserve or vendor interruption)"
    if failed:
        msg += f" · failed {', '.join(failed)}"
    summary = {"filled": filled, "deferred": deferred, "failed": len(failed), "errors": failed,
               "unavailable": sum(r.get("unavailable", 0) for r in receipts.values()),
               "results": receipts}
    if failed:
        require_complete(msg, summary)
    return msg, summary


def _body_fx_sync(ctx=None, *, runtime: JobRuntime) -> tuple[str, dict]:
    """ NOT CANCELLABLE MID-RUN, and `ctx` is accepted only so every body has one shape. There is
    no boundary worth checking, and a partial FX table is worse than a complete one (a missing rate
    silently drops a holding from its portfolio — see `_fx`'s paging note).

     IT IS NOT "A HANDFUL OF REQUESTS OVER SECONDS", WHICH IS WHAT THIS DOCSTRING USED TO CLAIM
    AND WHY IT EMITTED NOTHING. It asks for ~40 currencies from 2000-01-01 (the window that repairs
    every currency's HEAD — see `sync_fx_rates_to_db`'s both-ends widening), so it runs for as long
    as the ECB takes to answer for all of them. Emitting nothing left its toast reading
    "starting…" for the whole run, next to a job that WAS narrating — which reads as blocked rather
    than busy, and was reported as exactly that. The house rule is that a long job emits a first
    line BEFORE any work; a single blocking call is precisely the shape that forgets to."""
    from datetime import date as _date  # noqa: PLC0415
    from airs_vermogen import latest_completed_market_date  # noqa: PLC0415

    from deps import supabase  # noqa: PLC0415
    from fx_rates import ECB_CURRENCIES, _USD_PEGS  # noqa: PLC0415
    from momentum.data import sync_fx_rates_to_db  # noqa: PLC0415

    step = runtime.reporter(ctx)
    currencies = list(ECB_CURRENCIES) + list(_USD_PEGS.keys()) + ["TWD"]
    #  Before the call, not after it. The whole run is inside `sync_fx_rates_to_db`, so a line
    # emitted afterwards would arrive with the summary and tell the reader nothing they had not
    # already stopped waiting for.
    step(0, len(currencies), f"asking the ECB for {len(currencies)} currencies since 2000-01-01…")
    completed = 0

    def _progress(code: str, result: dict) -> None:
        """Relay the sync's per-currency completion callback to the job card.

        `sync_fx_rates_to_db` keeps its four-request worker pool; its callback is
        invoked by the coordinator as each result is collected, so this only
        improves observability and cannot increase ECB or database concurrency.
        """
        nonlocal completed
        completed += 1
        outcome = str(result.get("status") or "unknown")
        rows = int(result.get("rows") or 0)
        message = f"{code}: {outcome}"
        if rows:
            message += f" — {rows:,} rate(s) stored"
        elif result.get("error"):
            message += f" — {result['error']}"
        step(completed, len(currencies), message)

    status = sync_fx_rates_to_db(
        supabase, currencies, _date(2000, 1, 1), _date.fromisoformat(latest_completed_market_date()), on_progress=_progress,
    )
    synced = sum(1 for s in status.values() if s.get("status") == "synced")
    errors = sum(1 for s in status.values() if s.get("status") == "error")
    if errors:
        require_complete(f"FX refresh incomplete: {errors} currencies failed", {"currencies": status, "errors": errors})
    _log.info("[scheduler] fx sync done: %s/%s currencies updated, %s errors",
              synced, len(status), errors)
    #  A sync that updated nothing is the normal case, not a skip. It is idempotent and fetches
    # only the gap, so on a day the ECB has already been read every currency is correctly up to
    # date — `ok` with a count of 0, which is what makes a run of zeros over a WEEK legible as the
    # ECB feed having died.
    return (f"{synced}/{len(status)} currencies updated, {errors} error(s)",
            {"currencies": len(status), "synced": synced, "errors": errors})




def process_asset_queue(*, runtime: JobRuntime) -> None:
    """Drain one slice of the asset-pipeline ingest queue. THE single Yahoo/
    OpenFIGI consumer for uploaded ISINs — runs on a short interval with
    max_instances=1, so slices process back-to-back without ever overlapping (no
    competing Yahoo traffic → no throttle-corrupted resolutions). A no-op when the
    queue is empty. Never raises into the scheduler."""
    if runtime.active_jobs() & {"dashboard_market", "airs_vermogen_refresh", "dashboard_post_airs", "airs_price_checks"}:
        return  # Give the daily refresh exclusive priority over new queue slices.
    try:
        from dashboard_daily import daily_refresh_active  # noqa: PLC0415
        if daily_refresh_active():
            return
        from asset_pipeline import queue as _q  # noqa: PLC0415
        from routers._asset_financials import _BENCHMARK_RISK_ETF  # noqa: PLC0415
        # One tracker is the shared benchmark leg of every beta calculation.  A
        # regular FIFO backlog can otherwise leave a newly queued tracker behind
        # thousands of unrelated uploads and blank the entire beta column.
        r = _q.process_slice(priority_isins=list(_BENCHMARK_RISK_ETF.values()))
        if r.get("processed"):
            _log.info("[scheduler] asset ingest queue: %s processed (%s ok, %s failed, %s remaining)",
                      r.get("processed"), r.get("ok"), r.get("failed"), r.get("remaining"))
    except Exception:  # noqa: BLE001
        _log.exception("[scheduler] asset ingest queue worker failed")


def bootstrap_acwi_holdings() -> None:
    """Catch up the holdings snapshot without running the weekly fundamentals job."""
    from index_universe.acwi.weekly_holdings import needs_bootstrap, publication_connection, refresh

    try:
        with publication_connection() as conn:
            if conn is not None and needs_bootstrap(conn):
                receipt = refresh(conn, LogContext(label='ACWI startup'))
                _log.warning('[scheduler] ACWI startup holdings import: %s', receipt)
    except Exception:
        _log.exception('[scheduler] ACWI startup holdings import failed; retaining the previous snapshot')


