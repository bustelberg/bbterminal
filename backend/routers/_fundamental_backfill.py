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
