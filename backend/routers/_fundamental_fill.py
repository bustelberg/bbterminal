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
import logging
import random
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from common.pg import copy_connection_scope
from deps import supabase

_log = logging.getLogger(__name__)

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
# call behind a GLOBAL minimum interval, so the API half of these threads serialises anyway — eight
# workers cannot go faster than one call per interval no matter how many there are. What the extra
# threads did buy was eight-way write contention on the slowest table we own. Worst of both.
#
# ⚠⚠ THAT CALCULUS MOVES WITH THE INTERVAL, AND IT HAS MOVED. At the measured 0.75s, three workers
# against a ~1.1s vendor latency cap out around 2.7 calls/s while the gate allows 1.33 — the gate is
# still the constraint, but only by 2x rather than 4x. Below ~0.4s the CONCURRENCY becomes the
# binding one and this number is what would have to rise. Do not raise it on that reasoning alone:
# the write-contention argument above is what actually broke production, and it is only dormant
# because `changed_rows` removed the write volume, not because it was wrong.
#
# THREE, then. Enough to overlap a Storage upload with another company's DB write, few enough that
# no single statement queues behind seven others. ⚠ Raising it again is not a tuning knob until
# `metric_data` has been REINDEXED (15.2 GB → ~3.7 GB) — the bloat is what makes each upsert slow
# enough to time out.
#
# ⚠⚠ AND SINCE `ingest.metric_upsert.changed_rows` (2026-08-17) RAISING IT WOULD BUY ALMOST NOTHING
# ANYWAY — the reason to leave it alone has changed from dangerous to pointless, which is worth
# knowing before someone measures the DB again and concludes the fence can come down. The write
# volume this number was protecting against is gone: a company that has filed nothing new now writes
# ZERO rows instead of up to 36,494 (measured — Dassault's whole refresh went 17.48s of upserting to
# 1.58s end to end). What is left per company is one COPY and a handful of round trips, so the
# binding constraint is now the GLOBAL gate in `_api_request` (`_min_interval()`), which no worker count can
# go faster than. More threads would queue on that lock instead of on the database.
FILL_WORKERS = 3


#: The ONE quarterly line the due detector reads the fiscal-period axis off.
#:
#: ⚠⚠ IT REPLACED A `LIKE 'quarterly\_\_%'` PREFIX SCAN, AND THAT SCAN WAS THE SINGLE SLOWEST THING
#: IN A SMART PRESS. A prefix cannot use any index we have — the database collates `en_US.UTF-8`, so
#: a btree on `metric_code` gives no range for it, and the leading column of the covering index is
#: `metric_code` anyway. Measured on ACWI (2026-08-17, 69,003,374 rows in `metric_data`): a **Seq
#: Scan of the whole table**, 31,424,726 rows out, HashAggregated down to 127,001 distinct pairs
#: with **479 MB spilled to disk** — 26-57s depending on cache, and every second of it BEFORE the
#: job's first progress line, so the button reads as hung. The same answer off one exact code is an
#: index scan: **1.8s**.
#:
#: ⚠ IT IS THE SAME ANSWER, NOT A CHEAPER APPROXIMATION OF ONE. Measured over all 1,949 ACWI
#: constituents, this code alone reproduces the prefix scan EXACTLY — 1,712 companies, 127,001
#: distinct (company, period) pairs, an identical `due` set, and the same newest period for every
#: single company. The whole quarterly period axis is carried by every line in the block, so one
#: line is the axis.
#:
#: ⚠ AND IT IS NOT THE QUARTERLY TWIN OF THE `fin` SENTINEL, WHICH IS THE OBVIOUS PICK. That would
#: be `quarterly__Cashflow Statement__Free Cash Flow`, and **11 constituents do not have it** (a
#: bank's template omits the line, exactly as it omits gross profit) — measured, it returns 126,063
#: pairs and moves one company in and out of `due`. Coverage here ties out at 1,712, which is
#: precisely the number carrying the annual `fin` sentinel: present whenever the statements feed has
#: run, absent otherwise.
#:
#: ⚠ AND A COMPANY MISSING THIS LINE IS DUE, NOT FRESH — see the ⚠ in the loop below. The failure
#: direction of a bad pick here is an extra fetch, never a company that silently stops being offered.
DUE_PERIOD_CODE = "quarterly__Per Share Data__Revenue per Share"


def order_work(work: list[dict], rng: random.Random | None = None) -> list[dict]:
    """The order companies are fetched in: LEAST RECENTLY CHECKED FIRST, ties broken at random.

    ⚠⚠ THE PROBLEM IS REAL AND IT IS ABOUT RUNS THAT DO NOT FINISH. The work list used to come out
    in `company_id` order, so a press that is cancelled — or that dies, or that is capped by
    `limit` — always chewed through the same front of the list. Press it three times for five
    minutes each and you have fetched the same opening slice three times and never reached the tail.
    Everything the stamps saved on repeat presses was being spent again on re-treading known ground.

    ⚠ RANDOM WAS THE OBVIOUS FIX AND THIS IS STRICTLY BETTER AT THE SAME JOB. Shuffling makes each
    press cover a random slice, so coverage after k partial runs is `N(1 - (1 - m/N)^k)` — it
    approaches everything and never gets there, and two consecutive presses still overlap by chance.
    Ordering by "when did we last look" makes the frontier ADVANCE: a company just fetched is
    stamped, sorts to the back, and the next press starts where this one stopped. Full coverage in
    `ceil(N/m)` presses with no overlap at all, and it is the same rule `ingest/phases/prices.py`
    already uses for the price refresh ("most-stale-first").

    ⚠ THE RANDOM TIE-BREAK IS NOT DECORATION. Every never-asked company has the same key (no stamp),
    and a company that FAILS is never stamped — so without it the failures, and any company the
    vendor has no answer for, would sit at the identical front position press after press. The
    jitter is what stops a deterministic order from becoming a deterministic rut.

    ⚠ IT ORDERS ON THE FEEDS THIS RUN WILL ACTUALLY FETCH. A company whose statements are due but
    whose estimates were checked an hour ago should be ranked on the statements stamp — the other
    one is not what this press is about. An absent flag means "fetch it" (`ingest_company` reads
    `c.get(flag, True)`), so an unprobed feed counts.

    ⚠ AND A MISSING STAMP SORTS FIRST, BY BEING THE EMPTY STRING. These are ISO timestamps, so
    lexical order IS chronological and `""` precedes every real one — never asked is exactly what
    should go first, and it needs no special case.
    """
    # ⚠ LAZY, like every other cross-import here — `_fundamental_backfill` imports
    # `due_company_ids` back out of this module, so a module-level pair would be a cycle.
    from routers._fundamental_backfill import FEED_FETCHED_AT  # noqa: PLC0415

    rng = rng or random.Random()

    def _key(c: dict) -> tuple[str, float]:
        stamps = [c.get(col) or "" for tag, col in FEED_FETCHED_AT.items()
                  if c.get(f"need_{tag}", True)]
        return (min(stamps) if stamps else "", rng.random())

    return sorted(work, key=_key)


def due_company_ids(ids: list[int], today: date | None = None) -> tuple[list[int], str | None]:
    """Of these companies, the ones that plausibly have a fiscal period we have not fetched.

    Returns `(ids, note)`. `note` is non-None when the filter could not run and the caller is
    getting the FULL list back — never a silently narrowed one.

    ⚠ IT ASKS FOR ONE EXACT `metric_code`, NEVER A `quarterly__%` PREFIX — see `DUE_PERIOD_CODE` for
    what that cost and why one line is the whole period axis. The prefix also had a trap that is now
    gone by construction: `_` is a single-character wildcard in SQL LIKE, so an UNESCAPED
    `'quarterly__%'` matches `quarterly_revenue_estimate` too — the ANALYST FORECAST rows, whose
    period dates are years in the future (ASML had 2028-03-31). Feeding those to the detector makes
    every company look comfortably up to date, so the button goes quiet exactly when there is work
    to do. An equality match cannot express that mistake.

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
        "COPY (SELECT company_id, target_date::text FROM metric_data "
        "WHERE company_id = ANY(%s::int[]) AND metric_code = %s "
        "GROUP BY 1, 2) TO STDOUT WITH CSV", (list(ids), DUE_PERIOD_CODE))
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
        #
        # ⚠ THIS IS ALSO THE SAFETY NET UNDER `DUE_PERIOD_CODE`. A company that has quarterly data
        # but not that particular line reads as "no periods" and is offered, costing one call. The
        # opposite fallback — absent means fresh — would quietly retire it from every future press.
        if not periods.get(cid) or period_due(periods[cid], today) is not None:
            out.append(cid)
    return out, None


def _refresh_prices(ctx, label: str, comps: list[dict]) -> str:
    """Re-fetch the daily closes for these companies, bypassing the price cache. One line summary.

    ⚠⚠ PRICES ARE NOT ONE OF THE THREE FEEDS, AND THAT IS WHY "Refresh fundamentals" NEVER MOVED
    A SHARE PRICE. `ingest_company` runs `fin`/`est`/`ind` — statements, consensus, indicators — and
    `metric_data.close_price` comes from an entirely separate ingest. So Quick Valuation's "Current
    share price" tile, and the closes its multiple chart is priced off, were untouched by the one
    button on that screen that says it refreshes the data.

    ⚠ `force_refresh=True`, DELIBERATELY, AND IT IS THE SAME ARGUMENT `ingest_company` MAKES ABOUT
    ITS OWN CACHE: pressing Refresh is a request to LOOK. The staleness window that is right for a
    nightly pass is wrong for a human who has just pressed a button and is watching the number.

    ⚠ IT NEVER RAISES. A price fetch failing must not lose the fundamentals that already landed —
    the feeds ran first and are already written.
    """
    from ingest.constants import DATA_CUTOFF  # noqa: PLC0415
    from ingest.prices import ensure_prices_for_company  # noqa: PLC0415

    ok = failed = skipped = 0
    rows = 0
    ctx.emit("info", f"{label}: refreshing prices for {len(comps):,} compan"
                     f"{'y' if len(comps) == 1 else 'ies'}…")
    for n, c in enumerate(comps, 1):
        ctx.check()
        exch = ((c.get("gurufocus_exchange") or {}) or {}).get("exchange_code")
        # ⚠ A ROW WITH NO LISTING IS SKIPPED BUT STILL COUNTED. `continue`-ing past the progress
        # emit below stalled the bar whenever the LAST company was one of these — the run finished
        # reporting "prices 18/20" and looked hung at exactly the moment it was done.
        if c.get("gurufocus_ticker") and exch:
            try:
                res = ensure_prices_for_company(
                    supabase, c["company_id"], c["gurufocus_ticker"], exch,
                    force_refresh=True, data_cutoff=DATA_CUTOFF)
                ok += 1
                rows += getattr(res, "rows_loaded", 0) or 0
            except Exception as e:  # noqa: BLE001 — one price must not lose the run
                failed += 1
                _log.warning("[fill] price refresh failed for %s: %s: %s",
                             c.get("gurufocus_ticker"), type(e).__name__, e)
        else:
            skipped += 1
        if n % 5 == 0 or n == len(comps):
            ctx.emit("info", f"{label}: prices {n}/{len(comps)}")
    # ⚠ THE SKIPPED COUNT IS NAMED, never folded into the total. A holding with no GuruFocus
    # listing (an ETF, a certificate, cash) has no price for us to fetch — which is a different
    # answer from a fetch that failed, and the two send an operator to different places.
    return (f"prices: {ok} refreshed ({rows:,} row(s))"
            + (f", {failed} failed" if failed else "")
            + (f", {skipped} with no GuruFocus listing" if skipped else ""))


def fill_company_ids(ctx, label: str, ids: list[int], *, feeds: str = "statements",
                     prices: bool = False,
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
    from jobs import JobCancelled  # noqa: PLC0415 — same lazy-import shape as every other caller

    from ingest.api_usage import remaining_budget  # noqa: PLC0415
    from routers import _blend_cache  # noqa: PLC0415
    from routers._fundamental_backfill import (  # noqa: PLC0415
        company_rows, eligible, ingest_company, needs, smart_flags_bulk,
    )

    ids = sorted(set(ids))
    offered = len(ids)
    due_note = None
    # ⚠⚠ THE SETUP NARRATES ITSELF, AND IT HAS TO. Everything from here to the `start` line below is
    # database work with no output — and on an index it is not a moment. Measured on ACWI before the
    # `DUE_PERIOD_CODE` fix, the deciding alone took **31 seconds** with the toast reading
    # "starting…" the whole way, and the first per-company line lands only once the first of three
    # GuruFocus feeds has been fetched AND written on top of that. A card that says nothing for a
    # minute is indistinguishable from a hung one — which is exactly how this was reported.
    #
    # ⚠ NO `done`/`total` ON THESE. The bar stays indeterminate until there is a work list to count
    # against; putting a number on it here would show a progress percentage of a thing not yet
    # decided, and it would then jump backwards when the real total arrives.
    ctx.emit("info", f"{label}: reading what we hold for {offered:,} companies…")
    # ⚠ `smart` CARRIES ITS OWN DUE TEST, PER FEED. Running the company-level filter as well
    # would drop a constituent whose statements are not due before its stale consensus was ever
    # looked at — the two would compound into "skip unless a filing is due", which is the one
    # thing smart mode exists not to be.
    if only_due and feeds != "smart":
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
    elif feeds == "smart":
        # ⚠⚠ THE SAME RULE THE ROW BUTTON USES, over the whole list in five reads rather than
        # five per company — see `smart_flags_bulk`. This is what makes the bulk press genuinely
        # "N smart presses" instead of a second, cheaper-looking policy that quietly differs.
        ctx.emit("info", f"{label}: deciding which of {len(comps):,} have anything new…")
        flags = smart_flags_bulk(list(comps))
        todo = [{**c, **flags.get(c["company_id"], {})} for c in comps.values()]
        todo = [c for c in todo if c["need_fin"] or c["need_est"] or c["need_ind"]]
    else:
        # ⚠ PROBE ONLY THE SENTINEL THIS RUN CAN ACT ON. Each one is its own `metric_data`
        # read, so asking for three when the fill will only run one is two thirds of the work
        # thrown away — and `ind`'s sentinel is the expensive one (~535 rows per company).
        probe = {"statements": ("fin",), "estimates": ("est",)}.get(feeds)
        todo = needs(comps, feeds=probe)
    # ⚠ SELECTION AND ACTION NARROW TOGETHER, and this is also where `force` is applied — which is
    # why force cannot widen the feeds. A forced run arrives with all three flags true; this clears
    # two of them under `statements`, exactly as for an un-forced one. The `need_fin` filter is a
    # no-op under force rather than a second selection rule.
    #
    # ⚠ THE TWO FLAGS ARE SET EXPLICITLY, WHICH IS NOT REDUNDANT NOW THEY ARE UNPROBED.
    # `ingest_company` reads `c.get(flag, True)` — an ABSENT flag means "fetch it".
    # ⚠⚠ `all` NARROWS NOTHING, AND IT IS SPELLED OUT RATHER THAN LEFT TO FALL THROUGH. Every
    # other value here clears two of the three flags; `all` deliberately clears none, so a forced
    # press runs statements, estimates AND indicators. It exists because the Fundamental modal asks
    # for it: `indicator_q_forward_pe_ratio` is the ONLY line left on Quick Valuation's multiple
    # chart, and `annual_eps_nri_estimate` is the dotted forecast leg on Long Equity's EPS card —
    # neither is in `statements`, so pressing Refresh could never move either, however often.
    # ⚠ IT WOULD HAVE WORKED BY ACCIDENT (an unknown value falls past both branches with its flags
    # intact) and that is precisely why it is written down: behaviour nothing names is behaviour
    # the next `elif` deletes.
    if feeds == "all":
        pass
    elif feeds == "statements":
        todo = [{**c, "need_est": False, "need_ind": False}
                for c in todo if c.get("need_fin")]
    # ⚠⚠ THE ESTIMATES FILL, AND IT SELECTS ON ITS OWN SENTINEL. Under `statements` a company
    # that already holds financials is skipped — correct there, and fatal here: nearly every
    # constituent holds financials and almost none holds a consensus, so selecting on `need_fin`
    # would find nothing to do and the button would report "0 loaded" on an index whose forecast
    # line is missing for 1,364 of 1,715 names. Same shape of bug as the `require_market_cap`
    # one the index job carries its own ⚠⚠ about: selecting on the wrong fact removes exactly
    # the companies the run exists to load.
    elif feeds == "estimates":
        todo = [{**c, "need_fin": False, "need_ind": False}
                for c in todo if c.get("need_est")]
    skipped = [(c, eligible(c)) for c in todo]
    work = [c for c, why in skipped if why is None]
    refused = [(c, why) for c, why in skipped if why]
    # ⚠⚠ ORDER BEFORE `limit`, NOT AFTER, or the whole point is lost — a capped run would take the
    # same `company_id`-ordered prefix and the reordering would only shuffle within it. See
    # `order_work`: least recently checked first, so a press that is cancelled or capped picks up
    # where the last one stopped instead of re-fetching the same opening slice.
    work = order_work(work)
    if limit:
        work = work[:limit]

    # ⚠ A CANCEL PRESSED DURING THE SETUP LANDS HERE, BEFORE THE FIRST API CALL IS SPENT. Deciding
    # what to fetch is seconds of database work with nothing on the bar yet, and a press in that
    # window used to sit unacknowledged until the pool started and the first worker reached its own
    # check — long enough to look ignored, and on a forced run it is the difference between spending
    # nothing and spending the first three companies' quota.
    ctx.check()

    scope = ("refetching every one" if force
             else "missing statements" if feeds == "statements"
             else "missing a feed")
    if prices:
        scope += " (+ prices)"
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
        # ⚠ SAY THE ORDER, because a reader watching names go past will otherwise assume it is
        # alphabetical or by weight and read the sequence as meaningless. It is the answer to "why
        # am I not seeing the same companies as last time" — which is the whole point of it.
        + " · least-recently-checked first"
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
    # Rows the vendor returned that were already stored, so nothing was written for them. Reported
    # separately in the summary — see `ingest.metric_upsert.changed_rows` for why it is usually the
    # larger of the two by orders of magnitude.
    unchanged = 0
    # ⚠⚠ NARRATE THE FEEDS OF THE FIRST COMPANY ONLY, AND THE "ONLY" IS THE WHOLE DESIGN. A
    # per-company line is emitted when that company FINISHES, so on a fill whose first unit of work
    # is three GuruFocus feeds — each gated behind the global 1.5s minimum interval and each
    # followed by tens of thousands of `metric_data` upserts — the bar sits at 0 for the better part
    # of a minute with nothing moving. `ingest_company` already offers `on_step`, which fires BEFORE
    # each feed precisely so that gap is visible.
    #
    # ⚠ AND IT STOPS AFTER THE FIRST, FOR TWO REASONS. Three workers narrating every feed would put
    # three companies' names through one line at random, which reads as thrashing rather than as
    # progress — and `job.events` is append-only and re-scanned on every 0.15s stream tick, so
    # tripling 1,700 events into 5,100 makes the watcher's own cost grow with the run. Once the
    # first company lands, `[n/total]` is moving and the reader has what they need.
    first_landed = threading.Event()

    def _one(c: dict) -> None:
        """One company, inside ONE direct-Postgres connection.

        ⚠ THE SCOPE IS WHY THIS IS A WRAPPER. `changed_rows` runs a `COPY` per feed, and outside a
        scope every one of them opens a fresh connection — measured in `common/pg.py` at **220ms in
        production** (TCP, then TLS, then Supavisor auth) against 24ms locally, so a profile taken on
        a laptop cannot see it. Three feeds is 660ms per company of pure handshake, ~19 minutes over
        an index. The job runs on a plain `threading.Thread`, and a ContextVar starts empty in a new
        thread, so nothing upstream had opened one for us.

        ⚠ PER COMPANY, NOT PER RUN — the scope keys its connections by thread id (psycopg is not
        thread-safe), and holding one open across a whole 2-hour fill would keep three server-side
        sessions parked for the duration. A company is the unit of work; it is also the right unit
        of connection.
        """
        with copy_connection_scope():
            _one_inner(c)

    def _one_inner(c: dict) -> None:
        nonlocal ok, failed, rows, unchanged, calls
        # ⚠ THE FIRST CANCEL BOUNDARY. Everything still queued raises here the moment Cancel is
        # pressed — but on its own this only ever bounded the wait by a WHOLE COMPANY; see
        # `should_stop` below for the one that makes the press feel immediate.
        ctx.check()
        who = c.get("company_name") or c.get("gurufocus_ticker") or c["company_id"]

        def _step(tag: str, i: int, total: int) -> None:
            if not first_landed.is_set():
                ctx.emit("info", f"{who}: fetching {tag} ({i} of {total})…")

        # ⚠ `refresh_cache=force` — THE SECOND CACHE. `force` alone only ignores what `metric_data`
        # holds; the GuruFocus blob in Storage would still be replayed, so a forced press over an
        # already-loaded set would rewrite identical rows, spend zero calls and change nothing.
        #
        # ⚠⚠ `should_stop` IS THE FINE CANCEL BOUNDARY AND IT WAS SIMPLY NOT PASSED — which is why
        # Cancel felt broken here while the per-row Refresh stopped promptly. `ingest_company` has
        # taken this hook all along and checks it BETWEEN feeds (`benchmarks.py`'s single-company
        # job passes it); without it, a company already inside the call runs all THREE of its
        # remaining GuruFocus fetches to completion after the press — each one a wait on the global
        # rate gate, an HTTP round trip, a Storage upload and a write. Three workers deep, that is
        # the tens of seconds of apparently-nothing-happening.
        #
        # Between feeds is the right boundary and not merely a convenient one: a feed either
        # completes and is written or does not, and a company left with statements but no estimates
        # is a state a half-run backfill has always produced — `needs()` picks it up next time.
        r = ingest_company(c, refresh_cache=(force or feeds == "smart"), on_step=_step,
                           should_stop=lambda: ctx.cancelled)
        # ⚠ RETRY ONCE ON AN EMPTY ANSWER. This company was selected because it is missing the feed
        # (or the run is forced), so nothing at all coming back means the fetch returned nothing.
        # It costs one call to correct and, left alone, looks identical to a company that genuinely
        # has no data.
        #
        # ⚠⚠ "EMPTY" IS `rows + unchanged == 0`, NOT `rows == 0`, AND THE DIFFERENCE IS AN ENTIRE
        # SECOND PASS OVER THE INDEX. Since `ingest.metric_upsert.changed_rows`, `rows` counts what
        # was WRITTEN — so a company that is perfectly up to date now writes zero, which is the
        # normal outcome and used to be unreachable. Testing `rows == 0` alone would re-fetch every
        # healthy constituent: on ACWI that is ~1,700 companies x up to 3 feeds of pure waste, and
        # it would look like the optimisation had made the run twice as expensive.
        #
        # ⚠ AND NOT AFTER A STOP. A company that halted between feeds legitimately wrote nothing;
        # retrying it would spend fresh API calls on the far side of a Cancel — the one moment the
        # reader has explicitly asked us not to.
        if not r["error"] and not r.get("stopped") and r["rows"] == 0 and not r.get("unchanged"):
            r = ingest_company(c, refresh_cache=(force or feeds == "smart"))
        first_landed.set()
        with tally_lock:
            rows += r["rows"]
            unchanged += r.get("unchanged", 0)
            calls += r.get("calls", 0)
            if r["error"]:
                failed += 1
            elif not r.get("stopped"):
                # ⚠ A STOPPED COMPANY IS NOT A LOADED ONE. It ran some of its feeds and is counted
                # in neither column — the summary reports where the run stopped instead, so nothing
                # claims this company was finished.
                ok += 1
        ctx.spent(r.get("calls", 0))
        if r.get("stopped"):
            # ⚠ THE SPEND IS BANKED BEFORE THE RAISE. Those calls came out of the monthly quota
            # whether or not the run was cancelled, and a cancelled card reporting zero is the one
            # that gets pressed again. `check()` then raises, because cancellation is why we are
            # here.
            ctx.check()
        n = next(counter)
        # ⚠ THE COUNTER, NOT THE ARRIVAL ORDER, IS THE POSITION. Eight threads report concurrently,
        # so `[7/206]` can reach the toast before `[6/206]`; `n` comes from an atomic counter so the
        # bar only ever moves forward.
        outcome = ("failed — " + r["error"] if r["error"]
                   # ⚠ AN ANSWER, NOT A NON-EVENT: no feed was selected, so no call was spent.
                   else "already up to date" if not r["done"]
                   # ⚠⚠ A THIRD OUTCOME, AND SINCE `changed_rows` IT IS THE COMMON ONE ON AN INDEX:
                   # the feeds RAN, we paid for them, and the vendor's answer matched what we hold
                   # row for row. Reporting that as "loaded" would credit a fetch with work it did
                   # not do; reporting it as the line above would claim no call was spent.
                   else f"no change ({r['unchanged']:,} rows already stored)"
                        if r["rows"] == 0 and r.get("unchanged")
                   else "loaded")
        ctx.progress(n, len(work), f"[{n}/{len(work)}] {who} — {outcome}",
                     company_id=c["company_id"], failed=bool(r["error"]))

    stopped = False
    if work:
        with ThreadPoolExecutor(max_workers=FILL_WORKERS, thread_name_prefix="fill") as pool:
            # ⚠⚠ `submit` + explicit futures, NOT `pool.map`, AND THE REASON IS THE `finally`.
            # `map` submits every item up front too, so a Cancel used to leave ~1,600 queued work
            # items that the executor still had to START, one per thread hand-off, purely so each
            # could raise at its own `ctx.check()`. Worse, the only thing dropping them was the
            # `finally` inside `map`'s result generator — which runs when that generator is
            # COLLECTED, i.e. cancel latency resting on refcounting. `future.cancel()` on a queued
            # future is explicit and immediate.
            futures = [pool.submit(_one, c) for c in work]
            try:
                # In submission order, so `f.result()` re-raises the first real failure exactly as
                # `list(map(...))` did.
                for f in futures:
                    f.result()
            except JobCancelled:
                # ⚠ CAUGHT, NOT PROPAGATED, SO THE RUN CAN SAY WHERE IT STOPPED. `jobs.py` turns a
                # `JobCancelled` message into the card's summary, and "cancelled — stopped at a safe
                # point" answers none of the questions a reader has after pressing it. Re-raised
                # below with the tally.
                stopped = True
            finally:
                # ⚠ EVERY PATH, NOT JUST THE CANCEL. A genuine failure mid-run left the same queue
                # behind. `cancel()` is a no-op on a future already running or done, so the ≤3
                # companies in flight still finish their current feed and are still written.
                for f in futures:
                    f.cancel()
    # ⚠⚠ ALWAYS, NOT `if ok`. This used to clear the caches only when the fill had written
    # something, reasoning that a no-work press leaves every cached line correct. It does not:
    # "correct" there means *consistent with what THIS process last read*, and the rows can have
    # moved underneath it — a per-row Fetch in the same modal, the scheduler, a script, another
    # replica. The cached metric reads live 30 minutes, so a press could legitimately return a
    # byte-identical stale table, which from the reader's seat is indistinguishable from a broken
    # button. That is exactly how "I pressed Refresh benchmark and the row is still empty" happens
    # for a company whose data is sitting in `metric_data`.
    #
    # The saving it bought was a lazy rebuild of a ≤24-entry cache, on a button pressed by hand. A
    # refresh control that can hand back a stale view is not worth seconds of rebuild.
    #
    # ⚠⚠ AND A CANCELLED RUN REACHES IT TOO, WHICH IT DID NOT BEFORE. `JobCancelled` used to
    # propagate straight out of the pool, past this line — so the press that stopped a fill part-way
    # left the blend cache holding pre-fill rows for whatever the run HAD already written. That is
    # precisely the "I pressed refresh and the row is still empty" failure the paragraph above is
    # about, arriving through the one door it was not guarded on.
    _blend_cache.invalidate()

    # ⚠⚠ AFTER THE FEEDS, NOT BESIDE THEM. A price fetch is a different ingest with a different
    # cache, and the feeds are the expensive, cancellable half — running prices first would spend
    # the cheap calls and then risk the run being stopped before the data anybody pressed the button
    # for arrived. It is also why a failure here cannot lose what the feeds already wrote.
    #
    # ⚠ OVER `work`, THE COMPANIES THIS RUN ACTUALLY TOUCHED — not `comps`. A press that was capped
    # by `limit`, or narrowed by the due filter, must not quietly re-price the whole book: the two
    # halves of one button should describe the same set.
    price_note = _refresh_prices(ctx, label, work) if prices and work else ""

    summary = (f"{label} — {ok} companies {'refetched' if force else 'loaded'}"
               + (f", {failed} failed" if failed else "")
               + f", {rows:,} data points"
               # ⚠ SAID OUT LOUD, BECAUSE A SMALL `data points` FIGURE NOW MEANS SOMETHING GOOD.
               # Before `changed_rows` this run wrote every row the vendor returned, so the number
               # was in the millions and measured effort rather than effect. "412 data points ·
               # 31,204,880 already stored" is the run reporting what it changed and what it did not
               # have to touch; the first number alone would read as a fill that barely worked.
               + (f", {unchanged:,} already stored" if unchanged else "")
               + (f", {calls:,} API calls" if calls else "")
               + (f" · {price_note}" if price_note else ""))
    if stopped:
        # ⚠⚠ THE CANCELLED CARD REPORTS WHAT IT GOT THROUGH, AND THAT IS THE WHOLE ANSWER TO "did
        # it even work?". `jobs.py` promotes this message to the job's summary precisely so a worker
        # that stopped part-way can say what the registry cannot — bare, it read "cancelled — stopped
        # at a safe point", which is indistinguishable from a Cancel that did nothing at all.
        #
        # ⚠ AND IT SAYS THE WORK IS KEPT. Every company counted here is fully written; a reader who
        # believes a cancel rolled something back presses the expensive button again.
        raise JobCancelled(
            f"CANCELLED after {ok + failed} of {len(work)} — {summary}. "
            "Everything fetched before the stop is stored; press again to continue from there.")
    return summary
