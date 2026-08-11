"""ONE bulk fundamentals fill, over whatever set of companies the caller names.

⚠⚠ THIS EXISTS SO "INGEST" CANNOT COME TO MEAN TWO THINGS. The body below was inline in
`benchmarks.ingest_index_fundamentals_job`, selecting its companies from a benchmark's membership.
A portfolio needs the identical work over a different id list — and the tempting shape (copy the
job, swap the selector) is how one copy quietly grows a retry the other lacks, or stops bypassing
the Storage cache, and the two buttons drift into meaning different things. Selection is the
caller's business; the FILL is here, once.

    who to fetch      the caller decides -- an index's constituents, a portfolio's holdings
    what to fetch     `feeds` (statements = one API call, everything the grids draw)
    whether to force  `force` -- see `ingest_company`'s TWO caches
    who is worth it   `only_due` -- the detector, see `ingest.earnings.due`

⚠ IT REPORTS THE QUOTA BEFORE IT STARTS AND REFUSALS AS IT GOES. A region at zero means every
further call is wasted, and a company on an unsubscribed exchange is an answer with a reason —
never a failure.
"""
from __future__ import annotations

import itertools
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from deps import supabase

# How many companies are fetched at once.
#
# ⚠ EIGHT, NOT TWELVE, DELIBERATELY. Measured against the live API: 6 calls take 15.42s serially
# and 4.56s on six threads (3.4x, zero refusals); at twelve, throughput doubled again, so GuruFocus's
# ceiling was never found. But the 12-thread run returned one empty response in twelve — not a 403,
# so not a quota refusal, and one sample is not proof of a limit. The marginal gain past eight is
# small and the downside of probing for the edge is a fill that silently does less than it says.
#
# ⚠ AND THAT MEASUREMENT WAS API-ONLY. Each company also uploads to Storage and upserts tens of
# thousands of `metric_data` rows, which lands on OUR database — the real speed-up is smaller.
#
# ⚠⚠ EIGHT WAS MEASURED AGAINST THE WRONG BOTTLENECK, AND IN PRODUCTION IT BROKE THE RUN
# (2026-08-11). An SP500 refresh reported:
#
#     [backfill] NYSE:MDT  failed — APIError 57014: canceling statement due to statement timeout
#     [backfill] NASDAQ:NDAQ failed — APIError 57014
#     [26/501] Cadence Design Systems — failed — ReadTimeout
#
# The API was never the constraint; OUR DATABASE was. Eight concurrent writers each upserting tens
# of thousands of rows into `metric_data` — whose indexes are **15.2 GB, four times their
# reindexed size** — pushed individual statements past prod's 2-minute `statement_timeout`. The
# writes then produced **1,065,898 dead tuples**, autovacuum started on the same table, and the
# vacuum and the writers fought over the same disk: measured mid-run, query latency spread from
# 344ms to 5,340ms and connections went 10 → 28.
#
# ⚠ AND THE PARALLELISM WAS NEVER BUYING WHAT IT LOOKED LIKE. `_api_request` gates every GuruFocus
# call behind a GLOBAL 1.5s minimum interval, so the API half of these threads serialises anyway —
# eight workers cannot go faster than one call per 1.5s no matter how many there are. What the
# extra threads did buy was eight-way write contention on the slowest table we own. Worst of both.
#
# THREE, then. Enough to overlap a Storage upload with another company's DB write, few enough that
# no single statement queues behind seven others. ⚠ Raising it again is not a tuning knob until
# `metric_data` has been REINDEXED (15.2 GB → ~3.7 GB) — the bloat is what makes each upsert slow
# enough to time out.
FILL_WORKERS = 3


def due_company_ids(ids: list[int], today: date | None = None) -> tuple[list[int], str | None]:
    """Of these companies, the ones that plausibly have a fiscal period we have not fetched.

    Returns `(ids, note)`. `note` is non-None when the filter could not run and the caller is
    getting the FULL list back — never a silently narrowed one.

    ⚠ THE `LIKE` PATTERN ESCAPES ITS UNDERSCORES, AND THE UNESCAPED VERSION IS WRONG IN A WAY THAT
    LOOKS RIGHT. `_` is a single-character wildcard in SQL LIKE, so `'quarterly__%'` also matches
    `quarterly_revenue_estimate` — the ANALYST FORECAST rows, which carry period dates years in the
    future (ASML had 2028-03-31). Feeding those to the detector makes every company look
    comfortably up to date, so the button would go quiet exactly when there is work to do.

    ⚠ NO COPY PATH MEANS NO FILTER, NOT A GUESS. Without a direct connection this cannot read the
    period axis cheaply, so it hands back everything and says so. Degrading the OPTIMISATION is
    fine; degrading the ANSWER is not.
    """
    from collections import defaultdict  # noqa: PLC0415

    from common.pg import _run_copy  # noqa: PLC0415

    from ingest.earnings.due import period_due  # noqa: PLC0415

    if not ids:
        return [], None
    buf = _run_copy(
        r"COPY (SELECT company_id, target_date::text FROM metric_data "
        r"WHERE company_id = ANY(%s::int[]) AND metric_code LIKE 'quarterly\_\_%%' "
        r"GROUP BY 1, 2) TO STDOUT WITH CSV", (list(ids),))
    if buf is None:
        return list(ids), "no direct-Postgres connection — could not check what is due, so all are offered"

    periods: dict[int, list[str]] = defaultdict(list)
    for line in buf.getvalue().decode().splitlines():
        cid, d = line.split(",")
        periods[int(cid)].append(d)

    today = today or date.today()
    out = []
    for cid in ids:
        # ⚠ A COMPANY WITH NO QUARTERLY HISTORY AT ALL IS DUE BY DEFINITION — the detector returns
        # None for it (no spacing to infer a cadence from), and reading that None as "nothing to do"
        # would permanently exclude precisely the companies that have never been fetched.
        if not periods.get(cid) or period_due(periods[cid], today) is not None:
            out.append(cid)
    return out, None


def fill_company_ids(ctx, label: str, ids: list[int], *, feeds: str = "statements",
                     force: bool = False, limit: int = 0, only_due: bool = False,
                     today: date | None = None) -> str:
    """Fetch the GuruFocus feeds for `ids`, reporting through the job `ctx`. Returns the summary.

    ⚠⚠ `feeds="statements"` NARROWS **TWO** THINGS, AND NARROWING ONLY ONE IS A BUG. A fill makes
    two independent decisions: WHO is in the work list (`needs`, which returns anyone missing any of
    three sentinels) and WHICH feeds run for each. Narrowing only the second leaves companies
    selected because they lack estimates or indicators — for whom the narrowed action runs nothing
    at all. Measured on SP500: 216 companies need a feed, 206 need statements, so 10 would have been
    iterated, spent zero calls, and reported "nothing to do", which reads as a broken button.

    ⚠ `force` IS EXPRESSED AS THE `need_*` FLAGS, NEVER AS `ingest_company(force=True)`. That
    argument runs ALL THREE feeds regardless of the flags, so under `statements` it would quietly
    triple the spend on data no page draws.
    """
    from ingest.api_usage import remaining_budget  # noqa: PLC0415
    from routers import _blend_cache  # noqa: PLC0415
    from routers._fundamental_backfill import (  # noqa: PLC0415
        company_rows, eligible, ingest_company, needs,
    )

    ids = sorted(set(ids))
    offered = len(ids)
    due_note = None
    if only_due:
        ids, due_note = due_company_ids(ids, today)

    comps = company_rows(ids)
    if force:
        # ⚠ EVERY COMPANY IS WORK, SO NOTHING IS PROBED. `needs` answers "who is missing this
        # feed", and under force that answer changes nothing — it would just be the expensive part
        # of the setup thrown away (one `metric_data` read per sentinel, and the indicator sentinel
        # is ~526 rows per company). The flags are set to what a forced run means: fetch it,
        # whatever we hold.
        todo = [{**c, "need_fin": True, "need_est": True, "need_ind": True}
                for c in comps.values()]
    else:
        todo = needs(comps, feeds=("fin",) if feeds == "statements" else None)
    # ⚠ SELECTION AND ACTION NARROW TOGETHER, and this is also where `force` is applied — which is
    # why force cannot widen the feeds. A forced run arrives with all three flags true; this clears
    # two of them under `statements`, exactly as for an un-forced one. The `need_fin` filter is a
    # no-op under force rather than a second selection rule.
    #
    # ⚠ THE TWO FLAGS ARE SET EXPLICITLY, WHICH IS NOT REDUNDANT NOW THEY ARE UNPROBED.
    # `ingest_company` reads `c.get(flag, True)` — an ABSENT flag means "fetch it".
    if feeds == "statements":
        todo = [{**c, "need_est": False, "need_ind": False}
                for c in todo if c.get("need_fin")]
    skipped = [(c, eligible(c)) for c in todo]
    work = [c for c, why in skipped if why is None]
    refused = [(c, why) for c, why in skipped if why]
    if limit:
        work = work[:limit]

    scope = ("refetching every one" if force
             else "missing statements" if feeds == "statements"
             else "missing a feed")
    budget = remaining_budget(supabase)
    left = " · ".join(f"{k.upper() if k == 'usa' else k.title()} {v:,}"
                      for k, v in sorted(budget.items()))
    head = f"{offered} companies"
    if only_due and not due_note:
        # ⚠ SAY WHAT THE DETECTOR REMOVED. "19 companies · 19 to fetch" and "19 companies, 12 due"
        # are very different presses, and the second is the one that explains why the other seven
        # were left alone.
        head += f" · {len(ids)} may have new data"
    ctx.emit(
        "start",
        f"{head} · {len(todo)} {scope} · {len(work)} to fetch"
        + (f" · {len(refused)} can’t be fetched" if refused else "")
        + f" · quota left: {left}",
        done=0, total=len(work))
    if due_note:
        ctx.emit("info", due_note)
    # ⚠ REFUSALS ARE EVENTS, NOT FAILURES — an unsubscribed exchange is an answer. They go into the
    # log the toast carries rather than onto the bar, which counts work done.
    for w, why in refused:
        ctx.emit("skip", f"{w.get('company_name') or w['company_id']}: {why}")

    counter = itertools.count(1)
    tally_lock = threading.Lock()
    ok = failed = rows = calls = 0

    def _one(c: dict) -> None:
        nonlocal ok, failed, rows, calls
        # ⚠ THE CANCEL BOUNDARY, AND IT IS FIRST. Everything still queued raises here the moment
        # Cancel is pressed; the eight already inside `ingest_company` finish the company they are
        # on, because that is where the database is left consistent.
        ctx.check()
        # ⚠ `refresh_cache=force` — THE SECOND CACHE. `force` alone only ignores what `metric_data`
        # holds; the GuruFocus blob in Storage would still be replayed, so a forced press over an
        # already-loaded set would rewrite identical rows, spend zero calls and change nothing.
        r = ingest_company(c, refresh_cache=force)
        # ⚠ RETRY ONCE ON AN EMPTY ANSWER. This company was selected because it is missing the feed
        # (or the run is forced), so zero rows with no error means the fetch came back with nothing.
        # It costs one call to correct and, left alone, looks identical to a company that genuinely
        # has no data.
        if not r["error"] and r["rows"] == 0:
            r = ingest_company(c, refresh_cache=force)
        n = next(counter)
        with tally_lock:
            rows += r["rows"]
            calls += r.get("calls", 0)
            if r["error"]:
                failed += 1
            else:
                ok += 1
        ctx.spent(r.get("calls", 0))
        # ⚠ THE COUNTER, NOT THE ARRIVAL ORDER, IS THE POSITION. Eight threads report concurrently,
        # so `[7/206]` can reach the toast before `[6/206]`; `n` comes from an atomic counter so the
        # bar only ever moves forward.
        who = c.get("company_name") or c.get("gurufocus_ticker") or c["company_id"]
        outcome = ("failed — " + r["error"] if r["error"]
                   # ⚠ AN ANSWER, NOT A NON-EVENT: every feed was already loaded.
                   else "already up to date" if not r["done"]
                   else "loaded")
        ctx.progress(n, len(work), f"[{n}/{len(work)}] {who} — {outcome}",
                     company_id=c["company_id"], failed=bool(r["error"]))

    if work:
        with ThreadPoolExecutor(max_workers=FILL_WORKERS, thread_name_prefix="fill") as pool:
            # `list(...)` so exceptions surface here rather than being swallowed by the executor's
            # lazy iterator.
            list(pool.map(_one, work))
    # Drop the cached blends only if something was actually written. A fill with no work spends zero
    # API calls and leaves every cached line correct; clearing them would buy nothing but a rebuild.
    if ok:
        _blend_cache.invalidate()
    return (f"{label} — {ok} companies {'refetched' if force else 'loaded'}"
            + (f", {failed} failed" if failed else "")
            + f", {rows:,} data points"
            + (f", {calls:,} API calls" if calls else ""))
