"""Fetching the THREE GuruFocus feeds a company needs before the Long Equity charts can be drawn.

⚠ THREE INGESTS, AND RUNNING ONLY SOME IS WHAT MAKES A CHART LOOK BROKEN. Financials (the
    statements), analyst estimates (forward EPS) and indicators (forward P/E) are THREE separate
    GuruFocus calls, and a company can have any one without the others. Measured 2026-07-23:
    fetching financials alone for 156 companies left "Share price vs Owner Earnings" empty on every
    one of them, and adding the estimates still left Forward P/E empty — because Forward P/E is not
    an estimate at all, it is `indicator_q_forward_pe_ratio`. The suite fills in AROUND the panels
    that cannot, which reads as a bug in the charts rather than as data nobody fetched.

    ⚠⚠ `routers._fundamental_ingest.ingest_fundamentals_for_isin` RUNS ONLY THE FIRST. That is
    right for what it does — closing a coverage-table gap, where the statements are the whole
    question — but it means "ingest" means two different things in this codebase. Anything that
    wants a company chartable end to end must come through here.

⚠ THIS MODULE EXISTS SO THE SCRIPT AND THE ENDPOINTS CANNOT DIVERGE. `scripts/ingest_held_
    financials.py` had the sentinels, the subscription gate and the three-call sequence inline;
    the /benchmarks table now needs the identical behaviour from a button. A copied second version
    is how one of them quietly starts fetching two feeds again.
"""
from __future__ import annotations

import logging
from typing import Callable

from deps import supabase
from index_universe.acwi.exchange_map import is_gf_subscribed_exchange
from routers._earnings_pg import company_ids_with_metric_via_copy

_log = logging.getLogger(__name__)

# PostgREST truncates a single response at 1,000 rows on cloud (10,000 locally). Named so the
# paging rule below reads as deliberate rather than as an arbitrary number.
_PAGE = 1000

# One sentinel per feed — the row whose PRESENCE means that feed has run. Probed with ONE code,
# never `LIKE 'annuals__%'`: a wildcard over 20 companies is ~40k rows against PostgREST's silent
# 1,000-row cap, and every company past the cut-off would look like it had nothing.
SENTINELS: dict[str, str] = {
    "fin": "annuals__Cashflow Statement__Free Cash Flow",
    "est": "annual_pettm_estimate",
    # ⚠ NOT AN ESTIMATE. Forward P/E is the INDICATORS feed, a third call again — and
    # `annual_pettm_estimate` is also a forward P/E and is also present, which is what makes this
    # easy to get wrong. No chart reads that one.
    "ind": "indicator_q_forward_pe_ratio",
}

COMPANY_SELECT = ("company_id,company_name,gurufocus_ticker,isin,"
                  "gurufocus_exchange:gurufocus_exchange(exchange_code)")


def _has(cids: list[int], metric_code: str) -> set[int]:
    """Which of these companies carry `metric_code` at all.

    ⚠ PAGED, AND THE UNPAGED VERSION WAS WRONG IN EXACTLY THE WAY THAT DOES NOT LOOK WRONG. It
    batched 20 ids under a single `.limit(1000)`, reasoning that 20 companies could not exceed the
    cap. That holds for an ANNUAL line — Free Cash Flow is ~28 rows per company, so 560 — and fails
    completely for a quarterly INDICATOR series: `indicator_q_forward_pe_ratio` is **526 rows per
    company**, so a chunk of 20 asks for 10,520 rows, PostgREST silently returns the first 1,000,
    and only the first ~2 companies of every 20 are ever seen.

    Measured on SP500 mid-backfill: this returned **38** where the truth was **214**. Nothing
    errored. The damage was not the wrong number on screen but that `needs()` reads this — so ~90%
    of the companies that ALREADY had indicators were marked as missing them, and the backfill
    re-fetched data it held, spending a GuruFocus call each time.

    ⚠ ADVANCE BY WHAT CAME BACK, BREAK ON AN EMPTY PAGE. `len(page) < _PAGE` is only correct while
    the server's cap is >= the page size — which is precisely the assumption that failed above.

    ⚠ ORDERED ON A UNIQUE KEY. `metric_data` is unique on (company_id, metric_code, target_date),
    and the code is fixed here, so (company_id, target_date) is unique — without it Postgres makes
    no promise about tied rows across separate LIMIT/OFFSET queries and a page boundary inside a
    tie serves a row twice or never.

    The cost is real and accepted: proving 214 booleans reads ~112k rows, about 110 requests. It
    runs on a panel load and before a bulk run, not per company, and a cheap wrong answer here
    spends API quota — which is the more expensive mistake.

    ⚠ ONE `DISTINCT` COPY FIRST, THIS PAGER AS THE FALLBACK — same contract as `_rows_by_company`.
    The paragraph above is what the fallback still costs, and on ACWI (~1,900 constituents) it is
    four times the SP500 figure: **95 round trips per sentinel at the very least**, before the
    pages an indicator series adds. That was the single largest component of the fundamentals
    grid's load time. `company_ids_with_metric_via_copy` asks the database for the distinct ids
    instead of reading every row to infer them, and returns `None` — never an empty set — when it
    cannot run, so a fall-back is a slow answer and never a wrong one.
    """
    fast = company_ids_with_metric_via_copy(cids, metric_code)
    if fast is not None:
        return fast

    out: set[int] = set()
    for i in range(0, len(cids), 20):
        chunk = cids[i:i + 20]
        off = 0
        while True:
            page = (supabase.table("metric_data").select("company_id")
                    .in_("company_id", chunk).eq("metric_code", metric_code)
                    .order("company_id").order("target_date")
                    .range(off, off + _PAGE - 1).execute().data or [])
            if not page:
                break
            out.update(m["company_id"] for m in page)
            off += len(page)
    return out


def company_rows(cids: list[int]) -> dict[int, dict]:
    out: dict[int, dict] = {}
    for i in range(0, len(cids), 100):
        for c in (supabase.table("company").select(COMPANY_SELECT)
                  .in_("company_id", cids[i:i + 100]).execute().data or []):
            out[c["company_id"]] = c
    return out


def needs(comps: dict[int, dict], feeds: tuple[str, ...] | None = None) -> list[dict]:
    """Each company annotated with which feeds it is missing; complete ones dropped.

    `feeds` names the SENTINELS to probe — `None` (the default) means all three, which is what a
    caller wanting a company chartable end to end needs.

    ⚠ EACH SENTINEL IS ITS OWN READ OF `metric_data`, SO ASKING FOR ONE COSTS A THIRD OF ASKING FOR
    THREE. Two callers only ever look at `need_fin`: the fundamentals grid, which counts the
    fillable constituents, and the bulk fill under its default `feeds="statements"`, which
    immediately clears `need_est`/`need_ind` again. Probing all three for them was a third of the
    work used and two thirds thrown away — and `ind`'s sentinel is the expensive one
    (`indicator_q_forward_pe_ratio` is ~526 rows per company against Free Cash Flow's ~28).

    ⚠ AN UNPROBED FEED IS ABSENT FROM THE ROW, NEVER PRESENT AS `False`. `ingest_company` reads
    `c.get(flag, True)` — a missing flag means "fetch it", which is the safe default for a caller
    that did not ask. A `False` we never verified would be a claim the feed is already loaded, and
    would silently stop it ever being fetched. Every caller that hands these rows to
    `ingest_company` either probes all three or sets the rest explicitly.
    """
    keys = tuple(feeds) if feeds else tuple(SENTINELS)
    cids = sorted(comps)
    have = {k: _has(cids, SENTINELS[k]) for k in keys}
    out = []
    for cid, c in comps.items():
        flags = {f"need_{k}": cid not in have[k] for k in keys}
        if any(flags.values()):
            out.append({**c, **flags})
    return out


def eligible(c: dict) -> str | None:
    """None if this company can be fetched; otherwise the reason it cannot.

    ⚠ AN UNSUBSCRIBED EXCHANGE IS REFUSED, NOT ATTEMPTED. LSE, ASX and the rest return
    "unsubscribed" — the call is spent and nothing comes back.
    """
    if not c.get("gurufocus_ticker"):
        return "no GuruFocus ticker"
    exch = ((c.get("gurufocus_exchange") or {}) or {}).get("exchange_code")
    if not exch:
        return "no exchange"
    if not is_gf_subscribed_exchange(exch):
        return f"{exch} is outside the GuruFocus subscription"
    return None


def feed_flags(force: bool, feeds: str, missing: dict | None = None) -> dict:
    """The `need_fin`/`need_est`/`need_ind` flags for one company — the ONE place that decides
    which GuruFocus feeds a press pays for.

    ⚠⚠ `force` MEANS "IGNORE WHAT WE HOLD", NEVER "RUN EVERYTHING". Expressed as flags it can be
    narrowed afterwards; passed to `ingest_company(force=True)` it CANNOT, because that argument
    short-circuits the flags (`if force or c.get(flag, True)`) and runs all three feeds. The
    /benchmarks index fill always knew this — the per-company job did not, so the drill-down's
    per-row Refresh (`?force=true&feeds=statements`) spent **3 API calls per company instead of 1**
    on estimates and indicators no table on that screen draws. Measured on DSM-Firmenich, 2026-08-12.

    `missing` is the `needs()` row when we probed; absent under `force`, where every feed is work by
    definition and probing would only be an expensive way to reach the same answer.
    """
    flags = ({"need_fin": True, "need_est": True, "need_ind": True} if force
             else {k: bool((missing or {}).get(k)) for k in ("need_fin", "need_est", "need_ind")})
    # ⚠ NARROWS, NEVER WIDENS — and it is applied last for exactly that reason. `statements` is one
    # call and fills every line the fundamentals grid and the Long Equity tab draw, market cap
    # included; the other two feed the forward-EPS and indicator charts and nothing else.
    if feeds == "statements":
        flags = {**flags, "need_est": False, "need_ind": False}
    # ⚠⚠ `estimates` IS THE TARGETED FILL, AND IT EXISTS BECAUSE NEITHER EXISTING VALUE REACHES THE
    # FORECAST LINE. `statements` never asks for a consensus, so an index's analyst-expectation leg
    # can never appear however often it is pressed; `all` reaches it at THREE calls per constituent
    # — ~5,145 on ACWI — of which two fill a grid that already has them. Measured 2026-08-14: 351 of
    # ACWI's 1,715 charted names carry a consensus, so this fills the other 1,364 at one call each
    # and spends nothing on the rest.
    elif feeds == "estimates":
        flags = {**flags, "need_fin": False, "need_ind": False}
    return flags


def ingest_company(c: dict, *, force: bool = False, refresh_cache: bool = False,
                   on_step: Callable[[str, int, int], None] | None = None,
                   should_stop: Callable[[], bool] | None = None) -> dict:
    """Run the feeds this company is missing. Returns {done: [...], rows: n, error: str|None}.

    ⚠ IT NEVER RAISES. One company's failure must not end a 400-company run, and a caller streaming
    progress needs the reason on the row rather than as a dead connection.

    `on_step(tag, index, total)` fires BEFORE each feed, so a progress bar moves when the work
    starts rather than when it finishes — three feeds that each take seconds otherwise look frozen
    and then jump.

    ⚠ `should_stop` IS THE ONLY REASON CANCEL CAN WORK, and it is checked BETWEEN feeds, never
    during one. A GuruFocus fetch either completes and is written or does not; stopping between
    them leaves the database in a state it could have reached on its own (a company with
    statements and no estimates is exactly what a half-run backfill has always produced, and
    `needs()` will pick it up next time). Interrupting mid-write would not be. The stop is
    reported as `stopped` on the result rather than as an error — it is an outcome the caller
    asked for.

    ⚠⚠ `force` AND `refresh_cache` ARE TWO DIFFERENT CACHES, ONE LAYER APART, AND ONLY DOING ONE OF
    THEM IS A REFRESH THAT REFRESHES NOTHING.

        force          ignore what `metric_data` already holds — run the feed even though the
                       sentinel row says it has run before. WITHOUT this a company loaded a year
                       ago is never selected again.
        refresh_cache  ignore the GuruFocus blob in Storage — re-ask the API. WITHOUT this the
                       selected company is re-loaded from the SAME bytes we already had, so the
                       run writes identical rows, spends zero calls, and changes nothing on screen.

    `is_cache_fresh` derives its window from the data's own cadence, which for a quarterly filer is
    ~91 days plus a 50% buffer — so a blob keeps counting as fresh for weeks after the next quarter
    has actually been published. That is the right economy for a background backfill and the wrong
    one for a human pressing Refresh: pressing it is a request to LOOK, and the price half of that
    same button settled this identically (`_benchmark_refresh`: *a press always fetches, every
    constituent, no staleness tolerance*).

    They are separate parameters because the callers genuinely differ — a triage pass wants `force`
    without the calls, and nothing wants the calls without `force`.
    """
    from ingest.earnings import (  # noqa: PLC0415
        fetch_analyst_estimates, fetch_financials, fetch_indicators,
    )

    exch = ((c.get("gurufocus_exchange") or {}) or {}).get("exchange_code")
    cid, tic = c["company_id"], c["gurufocus_ticker"]
    done: list[str] = []
    rows = 0
    # ⚠ WHAT WAS SPENT, NOT WHAT WAS ASKED FOR. A feed that hits a fresh cache loads rows and
    # costs NOTHING, so counting the feeds we ran would overstate the bill — `EarningsResult`
    # carries the real number and this passes it up. The quota is monthly and finite; a caller
    # showing a user "3 API calls" for a run that spent zero teaches them to distrust the figure.
    calls = 0
    feeds = [(flag, fn, tag) for flag, fn, tag in
             (("need_fin", fetch_financials, "fin"),
              ("need_est", fetch_analyst_estimates, "est"),
              ("need_ind", fetch_indicators, "ind"))
             if force or c.get(flag, True)]
    try:
        for i, (_flag, fn, tag) in enumerate(feeds, 1):
            if should_stop is not None and should_stop():
                return {"done": done, "rows": rows, "calls": calls, "error": None, "stopped": True}
            if on_step is not None:
                on_step(tag, i, len(feeds))
            r = fn(supabase, cid, tic, exch, force_refresh=refresh_cache)
            n = getattr(r, "rows_loaded", 0) or 0
            rows += n
            calls += getattr(r, "api_calls", 0) or 0
            done.append(f"{tag} {n}")
        return {"done": done, "rows": rows, "calls": calls, "error": None, "stopped": False}
    except Exception as e:  # noqa: BLE001
        _log.warning("[backfill] %s:%s failed — %s: %s", exch, tic, type(e).__name__, e)
        # ⚠ THE COUNT SURVIVES THE FAILURE. A run that spent two calls and then threw still spent
        # them; reporting 0 because it ended badly would hide exactly the quota you most want to
        # know about.
        return {"done": done, "rows": rows, "calls": calls,
                "error": f"{type(e).__name__}: {str(e)[:120]}", "stopped": False}


#: How old our copy of a CONTINUOUSLY-REVISED feed may be before a smart press re-asks for it.
#:
#: â â  THE ONE NUMBER HERE THAT IS NOT MEASURED, AND IT IS ONLY HALF A GUESS. Statements need no such
#: rule â a company files on a schedule and `period_due` answers exactly when a new one can exist.
#: Estimates and indicators have no fiscal boundary: analysts revise a consensus whenever they like,
#: so "is it stale" can only be a question about elapsed time. Seven days is taken from the one
#: cadence that IS observable â the forward-P/E series arrives WEEKLY (measured on company 11: 102
#: of 107 gaps are exactly 7 days) â so a shorter window cannot find new points and a longer one
#: leaves them unfetched. The estimates feed reuses it for want of anything better to derive from;
#: if it proves too eager, this is the constant to move.
SMART_REFRESH_AFTER_DAYS = 7


def _is_stale(last: "date | None", today: "date") -> bool:
    """Is our copy of a continuously-revised feed old enough to be worth re-asking for?

    â  NEVER-WRITTEN COUNTS AS STALE. `None` here means we hold nothing, which is the strongest
    reason to fetch there is — reading it as "not stale" would make a feed we have never
    fetched look permanently up to date.

    â  ONE DEFINITION, TWO CALLERS. The row button and the bulk button must not come to disagree
    about what "smart" means, or the big one stops being N presses of the small one.
    """
    return last is None or (today - last).days >= SMART_REFRESH_AFTER_DAYS


def _last_written(company_id: int, metric_code: str) -> "date | None":
    """When we last WROTE a row of this feed for this company, or None if we never have.

    â  `recorded_at`, NOT `target_date` â "when did we ask" against "what period is it about". A
    forecast's `target_date` is years in the future and says nothing about how old our copy is.

    â  ONE ROW, ORDERED â NOT A SCAN AND A MAX. `indicator_q_forward_pe_ratio` alone is ~535 rows per
    company, so reading them all to take the newest would make the cheap check the expensive part of
    the press.
    """
    from datetime import date as _d  # noqa: PLC0415
    try:
        r = (supabase.table("metric_data").select("recorded_at")
             .eq("company_id", company_id).eq("metric_code", metric_code)
             .order("recorded_at", desc=True).limit(1).execute().data or [])
    except Exception:                    # unreadable â treat as never written, i.e. fetch it
        return None
    if not r or not r[0].get("recorded_at"):
        return None
    try:
        return _d.fromisoformat(str(r[0]["recorded_at"])[:10])
    except ValueError:
        return None


def smart_flags(company_id: int) -> dict:
    """The `need_*` flags for ONE company under `feeds="smart"`: fetch a feed we are MISSING, or one
    that could plausibly have changed since we last asked. Nothing else.

    â â  "SKIP IT IF WE ALREADY HAVE SOME" IS THE OBVIOUS RULE AND IT IS THE WRONG ONE. That is what
    `needs()` answers â is the sentinel row PRESENT â and it makes a Refresh a no-op on exactly the
    companies a reader presses it for. KLA holds financials, so a presence test skips it, and the
    FY2026 figures it has just filed are never fetched. This component already carries that as a
    known defect ("pressing it for ASML today fetches nothing"). Presence is the wrong question; the
    right one is whether anything NEW can exist.

    â  AND THAT QUESTION HAS A DIFFERENT ANSWER PER FEED, which is why this is per feed rather than
    per company. A company files statements on a cadence, so `period_due` says precisely when a new
    period is available â no elapsed-time guess needed. A consensus and a weekly indicator series
    have no such boundary, so for those it is `SMART_REFRESH_AFTER_DAYS`. Gating all three on the
    fiscal detector would freeze a consensus for months; gating all three on elapsed days would
    re-ask for statements that cannot have changed.

    â  THE UNSUBSCRIBED CASE IS NOT HERE, AND DOES NOT NEED TO BE â `eligible()` already refuses such
    a company before any of this runs, so no call is spent and none of these probes happen either.
    """
    from routers._fundamental_fill import due_company_ids  # noqa: PLC0415
    from datetime import date as _d  # noqa: PLC0415

    today = _d.today()
    missing = {k: company_id not in _has([company_id], code) for k, code in SENTINELS.items()}
    due, _note = due_company_ids([company_id], today)
    stale = {k: _is_stale(_last_written(company_id, SENTINELS[k]), today) for k in ("est", "ind")}
    return {
        "need_fin": missing["fin"] or bool(due),
        "need_est": missing["est"] or stale["est"],
        "need_ind": missing["ind"] or stale["ind"],
    }


def smart_flags_bulk(cids: list[int]) -> dict[int, dict]:
    """`smart_flags` for MANY companies, in a fixed number of queries instead of a few per company.

    â â  THE PER-COMPANY VERSION IS CORRECT AND UNUSABLE IN BULK. It costs three sentinel probes plus
    two `recorded_at` lookups EACH â ~9,700 round trips for ACWI's ~1,949 constituents, to decide
    which of them are worth a GuruFocus call. The deciding would cost more than the fetching.

    Here it is five reads for the whole list: three sentinels (`_has`, already COPY-backed) and two
    grouped `max(recorded_at)` queries. Measured on ACWI: 0.21 s for the estimates feed and 2.07 s
    for the indicator one, against ~3,800 PostgREST round trips for the same two answers.

    â  SAME RULE, ONE IMPLEMENTATION OF IT. The per-feed staleness test lives in `_is_stale` so the
    row button and the bulk button cannot come to disagree about what "smart" means â which is the
    whole reason the two are meant to compose. A second copy of "missing OR due OR older than N"
    is how the big button quietly starts spending differently from N presses of the small one.

    â  A COMPANY ABSENT FROM `due` IS NOT DUE, and one absent from a `recorded_at` map was never
    written â both read as "fetch it" only when the feed is also missing. Falling back the other way
    (absent â fresh) would let a company that has never been fetched look up to date for ever.
    """
    from datetime import date as _d  # noqa: PLC0415

    from routers._earnings_pg import last_written_via_copy  # noqa: PLC0415
    from routers._fundamental_fill import due_company_ids  # noqa: PLC0415

    today = _d.today()
    cids = sorted(set(cids))
    have = {k: _has(cids, code) for k, code in SENTINELS.items()}
    due_ids, _note = due_company_ids(cids, today)
    due = set(due_ids)
    last = {}
    for key in ("est", "ind"):
        got = last_written_via_copy(cids, SENTINELS[key])
        # â  `None` IS THE FALLBACK SIGNAL, NOT AN EMPTY ANSWER â see the COPY helper. Without a
        # direct connection there is no cheap way to age these, and the honest default is to treat
        # them as stale: a smart press then behaves like the old `all`, which is expensive but never
        # wrong. Silently treating them as fresh would stop the feed being fetched at all.
        last[key] = got if got is not None else {}
    return {
        cid: {
            "need_fin": cid not in have["fin"] or cid in due,
            "need_est": cid not in have["est"] or _is_stale(last["est"].get(cid), today),
            "need_ind": cid not in have["ind"] or _is_stale(last["ind"].get(cid), today),
        }
        for cid in cids
    }
