"""Daily AIRS refresh — both reports, both tables.

Re-discovers the CURRENT live portfolio list from AirSPMS each run (the list
changes day-to-day), then for EVERY portfolio downloads + parses + stores both:
  - Rendement (ATT)             → `airs_performance`  (upsert per periode)
  - Vermogensoverzicht (VOLK)   → `airs_holding`      (replace per as-of date)
Both are deduped, so re-running adds no duplicate rows. Another site can read
these two tables straight from Supabase; this job keeps them fresh each day.

Runs as an in-process scheduled job (working days 11:00 Amsterdam — see
scheduler.py) and on-demand from the /airs-portfolio "Refresh now" button.
Reuses the existing scraper + parsers (`scan_portfolios_sync`,
`download_portfolio_sync`/`download_vermogensoverzicht_sync`, `parse_airs_excel`,
and `routers.airs._parse_att_excel`/`_save_performance_to_db`).
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Callable
import time as _time
from datetime import date, datetime, timedelta, timezone

from deps import supabase

_log = logging.getLogger(__name__)
_LOCK = threading.Lock()


def _acquire_session(wait: float | None = None) -> bool:
    """Take the ONE authenticated AirSPMS session, refusing (`None`) or queueing (`wait` seconds).

    ⚠⚠ THERE IS EXACTLY ONE SESSION AND IT CANNOT BE DRIVEN BY TWO THREADS. That is not a
    throughput choice to be tuned away: `airs_scanner._session` is a single logged-in cookie jar,
    and two threads issuing report downloads through it interleave into each other's responses.
    Everything that scrapes AirSPMS passes through here.

    ⚠ WHICH IS THE HONEST ANSWER TO "REFRESH ALL COULD JUST RUN THESE CONCURRENTLY". It can, and
    it is worth doing — but only the parts that do not touch AIRS. A full portfolio refresh is one
    AIRS leg (the reports, the composition) and four that talk to Yahoo, OpenFIGI, the ECB and our
    own database; the second group is where the minutes are and it parallelises freely. So this
    lock is deliberately held across the AIRS legs ONLY, and released before the rest, which is
    what makes N concurrent `refresh_portfolio_fully` calls safe AND faster than N sequential ones
    rather than merely safe.

    `wait=None` refuses at once — the right answer for a button, which must respond. A number
    queues for up to that long, for a caller that is already mid-job and would otherwise leave a
    portfolio half-refreshed. `False` means it was not taken and the caller must NOT release it.
    """
    if wait is None:
        return _LOCK.acquire(blocking=False)
    return _LOCK.acquire(timeout=wait)

# Latest in-process run status. The persistent "last successful refresh" is the
# freshest snapshot date in airs_holding (surfaced by get_status()), so the
# status survives a restart even though this dict doesn't.
_STATUS: dict = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "status": None,            # None | running | ok | error
    # ⚠ ONE SHORT LINE, and it is the WHOLE user-facing report — see `format_run_message`. The
    # per-report breakdown that used to live here is in `detail`, for the log and the console.
    "message": None,
    "detail": None,
    "triggered_by": None,
    "portfolios_found": 0,
    # What the run did to our copy of the fleet, in the terms the page states it in.
    "accounts_added": 0,
    "accounts_updated": 0,
    "accounts_up_to_date": 0,
    "accounts_failed": 0,
    "rendement_stored": 0,     # portfolios whose Rendement (ATT) was stored
    "vermogen_stored": 0,      # portfolios whose Vermogensoverzicht (VOLK) was stored
    "holdings_rows": 0,        # total holding rows stored
    "errors": [],
    # Failures grouped by CAUSE, commonest first — see `summarise_errors`. A bare count is
    # not a diagnosis, and 27 individual lines are not one either.
    "error_summary": [],
}


# Below this, a discovery is treated as FAILED rather than as "AIRS has very few accounts".
# Measured: the live filtered Front-Office list is 44. A login failure or a changed selector
# returns a handful of rows and no exception, and writing that roster would retire the whole
# table in one pass.
_MIN_ROSTER = 10

# What the three Front-Office filters (Actieve / Interne / Zonder consolidatie) are known to
# produce. Not enforced — AIRS's roster is allowed to change — but any other number is called out,
# because the same symptom has two opposite causes and only the page's own count separates them.
_EXPECTED_ROSTER = 44

# Dates that FAILED during the current run, and for WHICH accounts — see `_vermogen_most_recent`.
# Cleared at the start of every run: "unvalued" is true until AirSPMS's next end-of-day batch, not
# for ever.
#
# ⚠⚠ A SET OF DATES ONCE, AND THAT SHAPE WAS THE BUG (2026-08-21). One failure was taken as proof
# the date had no valuation FLEET-WIDE, so it was skipped for every later account. But a failure is
# only ever proof about the book that made it: books are valued on different cadences — this file
# says so itself two paragraphs down — so a book last valued a week ago walks back through six
# perfectly good dates, marks them all dead, and every account scanned after it skips them.
#
# Measured in production 2026-08-21, one run, 46 accounts: **29 badged "Vermogensoverzicht not
# retrieved", and nothing else ever missing**. The 16 complete books all reached 2026-08-20; not one
# badged book did. Their stored holdings are dated 08-13 to 08-19 — their real valuation dates — so
# the dates they needed existed and had been ruled out by a book scanned before them.
_UNVALUED_DATES: dict[str, set[str]] = {}

# The newest date ANY account has successfully fetched this run, or None. See `_vermogen_most_recent`
# — this is the half that makes the memo sound, and a quorum on its own is NOT enough.
_NEWEST_VALUED: str | None = None

# When the memo above was last started, on the monotonic clock — and how long it may live.
#
# ⚠⚠ THE MEMO EXPIRES ON A CLOCK RATHER THAN ON SOMEBODY REMEMBERING TO CLEAR IT (2026-08-22).
# `run_airs_vermogen_refresh_sync` cleared it at the top of a fleet run and documented why: "PER
# RUN, NOT PER PROCESS — caching it beyond one run would make a scan an hour later skip the very
# date that has since been valued." That was correct and it covered exactly ONE of the three entry
# points. `refresh_one_portfolio` — the per-row Refresh button, the Analyse modal's Refresh, and
# `refresh_many`/`refresh_portfolio_fully` under the 05:00 model-prices job — never cleared it, and
# the backend is a long-lived process.
#
# So pressing Refresh on a row inherited the previous run's ruled-out dates. The dates a fleet run
# rules out are, by construction, the NEWEST ones — today, and the weekend behind it — which are
# precisely the dates that have since been valued by the time anyone presses the button. The walk
# skipped them, landed on something older or exhausted its horizon, and the row kept the badge.
# Reported as "I still see ⚠ Vermogensoverzicht behind most portfolios" after the cascade fix, and
# it is the same failure one level up: a memo outliving the fact it records.
#
# ⚠ A TTL RATHER THAN A CLEAR IN EACH ENTRY POINT, because this is the second time this memo has
# poisoned a later caller and adding a fourth entry point would be the third. "This date has no
# valuation" is true only until AirSPMS's next end-of-day batch, so an expiry IS the fact's real
# shape; nothing has to remember anything.
#
# ⚠ EXPIRY ONLY EVER COSTS REQUESTS, NEVER CORRECTNESS. A fleet run longer than the TTL re-pays the
# discovery for its remaining accounts — a handful of round trips against a run of ~113, and the
# alternative is the run holding a memo that has outlived the batch it describes.
_MEMO_STARTED_AT: float | None = None
_MEMO_TTL_S = 900

# How many DIFFERENT accounts must fail on a date before it counts as fleet-wide unvalued.
#
# ⚠ A QUORUM, BECAUSE THE TWO CASES LOOK IDENTICAL FROM ONE ACCOUNT. A weekend or a batch that has
# not run fails for EVERY book; a book valued weekly fails alone. Nothing in the response
# distinguishes them, so the only available signal is agreement between books.
#
# ⚠⚠ AND A QUORUM ALONE STILL DOES NOT FIX IT — measured on the real fleet (29 books, valuation
# dates spread over 8 days). Books share cadences, so THREE books that are all a week behind rule
# out the very date a fourth book needs, and the failure comes straight back:
#
#     one failure rules a date out, 7-day walk    refreshed 22   badged 7   downloads  29
#     quorum 3 alone                              refreshed 22   badged 7   downloads  64
#     no memo at all                              refreshed 29   badged 0   downloads 139
#     quorum 3, and only ABOVE the newest success refreshed 29   badged 0   downloads 113
#
# The last rule is the one below, and the second condition is what makes it SOUND rather than
# merely better: a book cannot be valued AHEAD of the newest batch AirSPMS has run, so once any
# account has fetched date D, a newer date that several accounts failed on genuinely has no
# valuation for anybody. A date OLDER than a proven success is never ruled out — which is exactly
# the case that broke, and the reason the cheap version cannot be recovered by raising the quorum.
#
# ⚠ THE COST IS REAL AND IS THE RIGHT TRADE: ~113 downloads a run against ~29. It buys back the
# documented win (today, and Sat/Sun on a Monday, are ruled out after three books discover it) while
# leaving no book unrefreshed. The old number was cheap because 7 of 29 books silently did not run.
_UNVALUED_QUORUM = 3

# How far back to look for a book's own last valuation.
#
# ⚠ 7 WAS TOO SHORT AND THAT IS A SECOND, INDEPENDENT DEFECT. Of the 29 books badged on 2026-08-21,
# two were last valued 08-13 and 08-14 — 8 and 7 days back — so even with an empty memo the walk
# could never have reached them. The horizon has to cover the slowest cadence we actually see, not
# the fastest.
_WALK_BACK_DAYS = 14


def _reset_valuation_memo() -> None:
    """Start a fresh memo. See `_MEMO_STARTED_AT`.

    ⚠ BOTH HALVES TOGETHER, ALWAYS. `_NEWEST_VALUED` is the licence to rule a date out and
    `_UNVALUED_DATES` is what it licences; keeping either without the other licences one run's
    answer against another run's evidence.
    """
    _UNVALUED_DATES.clear()
    globals()["_NEWEST_VALUED"] = None
    globals()["_MEMO_STARTED_AT"] = _time.monotonic()


def _expire_valuation_memo() -> None:
    """Drop the memo once it is older than `_MEMO_TTL_S` — called before every walk.

    ⚠ THE CHECK IS ON THE READ PATH, NOT ON THE ENTRY POINTS. That is the whole point: any caller
    that reaches `_vermogen_most_recent` gets a memo no older than the TTL, whether it remembered
    to start a run or not. A caller added tomorrow inherits the protection by existing.
    """
    started = _MEMO_STARTED_AT
    if started is None or (_time.monotonic() - started) > _MEMO_TTL_S:
        _reset_valuation_memo()


def _discover_portfolios() -> list[str]:
    """Current live AirSPMS portfolio names, scraped fresh (Playwright)."""
    from airs_scanner import scan_portfolios_sync  # noqa: PLC0415

    captured: list[dict] = []

    def _sink(msg_type: str, **kw):
        if msg_type == "portfolios":
            captured.extend(kw.get("data") or [])
        elif msg_type == "error":
            raise RuntimeError(kw.get("message") or "scan error")
        elif msg_type == "progress" and kw.get("message"):
            # ⚠ DISCOVERY USED TO NARRATE TO NOBODY. The scraper already emitted every step — which
            # filters it sent, AIRS's own "N Items in selectie", the per-page row counts — and this
            # sink threw all of it away, so the roster arrived as a bare number with no way to ask
            # how it was arrived at. It is the one phase where the answer is a COUNT, and a count
            # is exactly what cannot be checked after the fact.
            _emit("discovery", step=kw.get("step"), declared=kw.get("declared"),
                  message=f"  {kw['message']}")

    result = scan_portfolios_sync(_sink)
    rows = result if result else captured
    names: list[str] = []
    for r in rows:
        n = (r.get("portefeuille") or "").strip()
        if n:
            names.append(n)
    _record_roster(names)
    return names


def _record_roster(names: list[str]) -> None:
    """Persist WHICH accounts AIRS listed on this pass — the roster `list_accounts` reads.

    ⚠ WITHOUT THIS THE ANSWER IS THROWN AWAY. The discovery already knows the live set; it just
    used it to drive the scrape and forgot it. `airs_performance` cannot recover it: it says what
    a book made, which stays true long after AIRS stops listing the book.

    ⚠ ONE TIMESTAMP FOR THE WHOLE BATCH, so "the live set" is exactly `last_seen_at = max(...)`.
    Stamping each row with its own now() would make that comparison a race against the write.

    ⚠ AN EMPTY OR SUSPICIOUSLY SMALL DISCOVERY IS NOT WRITTEN. A login failure or a changed
    selector returns few rows, not an error, and recording that would retire the entire table on
    the strength of a failed scrape. Better to keep yesterday's roster than to publish a wrong one.
    """
    if len(names) < _MIN_ROSTER:
        _log.warning("[airs_vermogen] discovery returned %d portfolios (< %d) — roster NOT "
                     "updated; keeping the previous one rather than retiring accounts on a "
                     "possibly-failed scrape", len(names), _MIN_ROSTER)
        return
    stamp = datetime.now(timezone.utc).isoformat()
    rows = [{"portefeuille": n, "last_seen_at": stamp} for n in sorted(set(names))]
    try:
        for i in range(0, len(rows), 200):
            supabase.table("airs_account_roster").upsert(
                rows[i:i + 200], on_conflict="portefeuille").execute()
    except Exception as e:  # noqa: BLE001 — the scrape itself must not fail on bookkeeping
        _log.warning("[airs_vermogen] could not record the account roster: %s: %s",
                     type(e).__name__, e)


# The reports an account needs for every figure on the portfolios page to describe the same
# moment. Order is display order, not fetch order.
#
# ⚠ `trans` JOINED THIS LIST ON 2026-08-05 AND THAT IS A DELIBERATE RE-DEFINITION OF "COMPLETE".
# Transacties used to be fetched ONLY when someone opened the Transactions panel, so after months
# of daily fleet scans exactly TWO of 44 books had ever had theirs stored — and every figure that
# needs flows (invested capital, money-weighted return, the realised leg, the whole look-through
# into a certificate) was silently unavailable everywhere else. A report nothing routinely fetches
# is a report that does not exist in practice.
# Two consequences, both wanted: every account now reads INCOMPLETE until re-scanned (it genuinely
# is — it is missing a report we now require), and the incremental pass at `needed = set(REPORTS)`
# stops skipping those books, so the fleet scan backfills transactions on its own.
REPORTS = ("att", "volk", "mut", "trans", "model")


def _record_reports(outcomes: dict[str, list[str]], stamp: str) -> None:
    """Persist which reports each account yielded on this pass — what `list_accounts` gates on.

    ⚠ THE OUTCOME IS THE FETCH'S, NOT THE TABLE'S. A book with no transactions this year returns a
    valid EMPTY Mutaties report; counting rows afterwards would mark it incomplete and hide a
    perfectly healthy account. `outcomes` therefore carries what the `try` blocks observed.

    ⚠ ONE TIMESTAMP FOR THE WHOLE BATCH, exactly as `_record_roster` does and for the same reason:
    "this refresh's verdict" is then `reports_at = max(...)`, not a race against the write.

    Best-effort — bookkeeping must never fail the scrape that produced it.
    """
    # ⚠ IT MUST NOT TOUCH `last_seen_at` — THAT FIELD BELONGS TO DISCOVERY, AND WRITING IT HERE
    # MADE ROWS DISAPPEAR. `_live_accounts` is "the accounts AIRS listed on the most recent
    # discovery", computed as `last_seen_at == max(last_seen_at)`. This function runs per account
    # AS THE SCAN PROGRESSES, so stamping it here re-defined "the live set" to mean "the accounts
    # scanned so far": mid-run the portfolios table filled with all 44 and then collapsed to the
    # one book that had just been scanned (measured 2026-07-30 — `BUS_WTS_StMerken_Dyn`, alone).
    #
    # And it survived the run: the scan is INCREMENTAL, so a pass that scanned 14 and skipped 30
    # left only those 14 carrying the newest stamp — the 30 healthy books were filtered out of
    # their own page until the next discovery re-stamped them.
    #
    # "AIRS listed this account" and "we scanned this account" are different facts about different
    # sets. `reports_at` is this function's timestamp; `last_seen_at` is `_record_roster`'s.
    # ⚠ UPDATE ONLY — AN ACCOUNT DISCOVERY HAS NEVER SEEN CANNOT BE INSERTED HERE. `last_seen_at`
    # is NOT NULL with no default, by design (see the migration: the live set IS
    # `last_seen_at = max(...)`, so a per-row default would let this function redefine it).
    #
    # ⚠⚠ THE FIRST ATTEMPT AT THIS FIX WAS THE `known` READ BELOW AND IT DID NOT WORK — see the
    # note on the write itself. Omitting a NOT NULL column fails whether or not the row exists,
    # because Postgres validates the tuple before it arbitrates the conflict. The read survives
    # only to WARN about accounts discovery has not seen; the write is now a plain UPDATE.
    # Measured in production 2026-08-03, refreshing one account:
    #
    #   null value in column "last_seen_at" of relation "airs_account_roster" violates not-null
    #   constraint — Failing row contains (AITopSelectie OFF DYN, null, ...)
    #
    # And the damage is not the missing row. This is a BATCHED upsert, so one unseen account
    # fails the outcomes of every account in its batch — on a full scan, all 44. The outcome
    # table is exactly what marks a row "att did not arrive", so the failure silences the very
    # warning it should have raised: the page then shows a stale figure with nothing saying the
    # newest report never came. `AITopSelectie OFF DYN` read +55.20% (June's `cumulatief_rendement`)
    # while July's −11.96% sat unfetched, and the row looked healthy.
    #
    # Giving `last_seen_at` a default would "fix" the error and break the live set instead: a row
    # inserted here with now() becomes the maximum, and `_live_accounts` would collapse to this
    # one account. Existence is discovery's fact to state. So we update what discovery knows and
    # say plainly what we skipped.
    names = sorted(outcomes)
    try:
        known: set[str] = set()
        for i in range(0, len(names), 200):
            known.update(
                r["portefeuille"] for r in
                (supabase.table("airs_account_roster").select("portefeuille")
                 .in_("portefeuille", names[i:i + 200]).execute().data or [])
                if r.get("portefeuille"))
    except Exception as e:  # noqa: BLE001 — bookkeeping must never fail the scrape
        _log.warning("[airs_vermogen] could not read the roster to record outcomes: %s: %s",
                     type(e).__name__, e)
        return

    unknown = [n for n in names if n not in known]
    if unknown:
        _log.warning(
            "[airs_vermogen] %d account(s) are not in the roster, so their report outcomes are "
            "NOT recorded and they cannot be flagged as incomplete: %s. Run a full discovery "
            "(the portfolios scan) — only discovery may create a roster row, because "
            "`last_seen_at` defines the live set.", len(unknown), ", ".join(unknown[:10]))

    todo = [n for n in names if n in known]
    if not todo:
        return
    # ⚠⚠ UPDATE, NOT UPSERT — AND THE `known` GUARD ABOVE NEVER FIXED THIS (2026-08-13). PostgREST's
    # upsert is `INSERT ... ON CONFLICT DO UPDATE`, and Postgres forms and VALIDATES the candidate
    # tuple before it arbitrates the conflict: a payload omitting `last_seen_at` — NOT NULL with no
    # default, by design — fails 23502 even when the row exists and would have been updated.
    # Measured directly:
    #
    #   select count(*) filter (where portefeuille='BUS_WTS_Dividend_Dyn')  ->  1   (it exists)
    #   insert ... on conflict (portefeuille) do update ...                 ->  23502
    #
    # So the 2026-08-03 fix — "only write rows discovery has already seen" — addressed the wrong
    # cause and left every account failing. On this scan that was ALL 44, silently: the outcome
    # table is what marks a row "att did not arrive", so the failure suppressed exactly the warning
    # it should have raised, and a stale figure kept looking healthy.
    #
    # ⚠ GROUPED BY THE OUTCOME SET, so a 44-account fleet scan is ~3 requests rather than 44: almost
    # every book yields the same `{att,model,mut,trans,volk}`, and the ones that differ are the
    # interesting ones. An `update ... in (…)` cannot carry a per-row value, which is exactly why
    # the grouping is by the value.
    by_outcome: dict[tuple[str, ...], list[str]] = {}
    for n in todo:
        by_outcome.setdefault(tuple(sorted(outcomes[n])), []).append(n)
    try:
        for reports, group in by_outcome.items():
            for i in range(0, len(group), 200):
                (supabase.table("airs_account_roster")
                 .update({"reports_ok": list(reports), "reports_at": stamp})
                 .in_("portefeuille", group[i:i + 200]).execute())
    except Exception as e:  # noqa: BLE001
        _log.warning("[airs_vermogen] could not record report outcomes: %s: %s",
                     type(e).__name__, e)


# How long a COMPLETE scan of an account stays good. AIRS publishes at most one valuation a day,
# so re-downloading four reports for an account we fully scanned this morning buys nothing and
# costs ~44× that. Env-tunable; the daily job's interval is far longer, so it still scans the fleet
# once a day exactly as before — this only collapses the repeat presses in between.
# ⚠ 20, NOT 12 — AIRS VALUES ONCE A DAY. The window only has to be shorter than the gap between
# two valuations; at 12h a mid-afternoon press re-downloaded the whole fleet for a valuation that
# had not moved since the morning. 20h still guarantees the daily job (a fixed weekday-morning
# tick, ~24h apart — see `scheduled_jobs.SCHEDULED_JOBS`) never skips a real one, and collapses
# every repeat press in between. ⚠ The time is NOT restated here: it moved 10:00 → 09:30 and a
# second copy of it would now be wrong. What this window depends on is the ~24h SPACING, not the
# hour — and the daily job forces anyway, so it cannot be skipped by this at all.
AIRS_FRESH_HOURS = float(os.environ.get("AIRS_FRESH_HOURS", "20"))

# Fewer holdings than this and a book is not a portfolio: the AIRS benchmarks carry exactly 1 and
# the `_MV` / `WTS test` shells carry none, against 10-29 for every real book. Same threshold the
# model-portfolios table uses, so "too small to be real" means one thing across the app.
MIN_REAL_HOLDINGS = int(os.environ.get("AIRS_MIN_REAL_HOLDINGS", "5"))

# ⚠⚠ HOW LONG A SKIPPED BOOK MAY GO UNREAD BEFORE IT IS READ ANYWAY. The size skip below is a COST
# decision — 60-odd downloads a run on books nobody opens — and a cost decision must not become a
# permanent exemption: a book that is never re-read has an `as_of` that never moves, so its row
# wears the amber "N trading days old" badge for ever while the run reports the fleet up to date.
#
# Measured 2026-08-17: the four `BUS_BM_*` benchmark accounts (1 holding each) were last read
# 2026-07-30 — 12 trading days — and were the ONLY rows on the page whose lag was OURS rather than
# AIRS's, i.e. the only four a refresh could have fixed, and the only four every refresh refused to
# touch. 14 days keeps ~90% of the saving (a skipped book costs 4 downloads roughly twice a month)
# and bounds how wrong a skipped row can get.
AIRS_BOGUS_MAX_AGE_HOURS = float(os.environ.get("AIRS_BOGUS_MAX_AGE_HOURS", "336"))


def bogus_accounts(counts: dict[str, int], verdicts: dict[str, dict],
                   *, now: datetime | None = None,
                   max_stale_hours: float | None = AIRS_BOGUS_MAX_AGE_HOURS,
                   visible: set[str] | None = None) -> set[str]:
    """Accounts too small to be portfolios, lower-cased. Pure.

    `counts` is the last known holdings per account (`_holding_counts`); `verdicts` is the roster's
    record of which reports each account last yielded (`_roster_verdicts`).

    ⚠ IT IS DECIDED ON THE PREVIOUS SCAN, WHICH IS THE ONLY THING AVAILABLE. Holdings are what the
    Vermogensoverzicht returns, so a book's size cannot be known before fetching it. Using the last
    known count means the first scan of an account always happens; from then on a shell costs
    nothing.

    ⚠ ZERO AND UNKNOWN LOOK IDENTICAL IN `counts`, AND CONFLATING THEM BREAKS IT IN ONE DIRECTION
    OR THE OTHER. A book that stores no holdings has no rows in `airs_holding`, so it is simply
    ABSENT — indistinguishable from one that has never been scanned. Treating absence as bogus would
    skip a brand-new account for ever (it could never acquire the holdings that would rescue it);
    treating it as unknown misses the emptiest books, which are exactly the `_MV` shells worth
    skipping. The roster settles it: `volk` in `reports_ok` means we DID fetch the
    Vermogensoverzicht, so absent-and-fetched is a measured zero.

    Measured 2026-07-30: 15 of 46 books qualify — 5 benchmarks at 1 holding, 10 shells at 0 — which
    is 60 downloads a run spent on books nobody looks at. `force` re-checks everything regardless.

    ⚠⚠ BUT A SKIP IS NOT AN EXEMPTION, AND FOR FOUR BOOKS IT HAD BECOME ONE. `max_stale_hours`
    re-admits a book we have not read in that long (`AIRS_BOGUS_MAX_AGE_HOURS`, 14 days), because a
    permanently skipped account is one whose `as_of` can never move: its row wears "13 trading days
    old" for ever, `lagOwner` correctly reports the lag as OURS — the one verdict that tells the
    reader a Refresh will fix it — and every Refresh then skips it again. Measured 2026-08-17: the
    four `BUS_BM_*` benchmarks, last read 2026-07-30, were exactly those rows. Pass
    `max_stale_hours=None` for the pure size question with no clock in it.

    ⚠ A BOOK WITH NO `reports_at` IS NOT RE-ADMITTED BY THE CLOCK. It cannot be stale-by-time if we
    have no time for it — and it is only in `verdicts` at all because a previous run wrote its
    `reports_ok`, so `volk` above has already established that we fetched it.

    ⚠⚠ AND ONLY A **VISIBLE** BOOK IS RE-ADMITTED, WHICH IS THE POINT OF THE WHOLE EXERCISE. The
    justification for reading a one-holding book at all is that its ROW carries a badge a reader
    cannot clear; a hidden account (`airs_account_hidden`) has no row, so a stale one is invisible by
    construction and re-reading it buys nothing. Measured 2026-08-17: 16 books are too small, and
    without this 14 of them would be re-admitted at once — including `wts test 1-4 fx` and the four
    `_MV` shells, which nobody can see. With it, only the books on the page come back. `visible=None`
    means "no list available", and then the age rule applies to all of them: failing toward doing the
    work matches `_roster_verdicts`, which scans everything when it cannot read the roster.
    """
    cutoff = None
    if max_stale_hours is not None:
        cutoff = (now or datetime.now(timezone.utc)) - timedelta(hours=max_stale_hours)
    out: set[str] = set()
    for name, v in verdicts.items():
        key = (name or "").strip().lower()
        if "volk" not in set(v.get("reports_ok") or ()):
            continue                      # never fetched its holdings — unknown, not empty
        if counts.get(name, counts.get(key, 0)) >= MIN_REAL_HOLDINGS:
            continue
        if (cutoff is not None
                and (visible is None or key in visible)
                and _older_than(v.get("reports_at"), cutoff)):
            continue                      # small, ON THE PAGE, and unread for a fortnight
        out.add(key)
    return out


def _older_than(stamp: str | None, cutoff: datetime) -> bool:
    """Is `stamp` (an ISO timestamp, possibly naive) strictly before `cutoff`?

    ⚠ AN UNPARSEABLE OR ABSENT STAMP IS "NOT OLD", so a bad value cannot silently re-admit every
    skipped book on every run — that would quietly undo the saving the skip exists for, and nothing
    on screen would say why the scan got slower.
    """
    if not stamp:
        return False
    try:
        ts = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except ValueError:
        return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts < cutoff


# ⚠⚠ WHERE THE RUN'S NARRATION GOES **NOW**. `_emit` writes to `_STATUS["log"]`, which the
# portfolios panel used to POLL — and stopped, when "Refresh all" became a job reporting into the
# shared toast. Nothing re-pointed the log at the new surface, so every phase that narrates through
# `_emit` was narrating to nobody, and `on_step` was wired only to the account loop.
#
# The visible symptom, reported 2026-08-17: the toast reads **"starting…" for a long time** before
# the first portfolio appears. Nothing is wrong — discovery is a headless-browser login, three menu
# navigations, three filters and a paged scrape of the Front-Office list, and it takes what it
# takes. It just says nothing while it does, which is the one thing a minutes-long job must not do:
# a silent scrape is indistinguishable from a hung one, and this file's own `_emit` docstring says
# so about the phase it then left silent.
#
# ⚠ MODULE-LEVEL IS SAFE HERE FOR THE SAME REASON `_STATUS` IS: `_LOCK` serialises every writer
# (the fleet run, a single-row refresh and the scheduler all take it), so there is exactly one run
# at a time. It is set inside the lock hold and cleared in the same `finally` that releases it.
_PROGRESS: Callable[[int, int, str], None] | None = None
#: The bar's position, carried so a narration line mid-loop does not blank a real 12/44.
_PROGRESS_AT: dict[str, int] = {"done": 0, "total": 0}


def _say(done: int, total: int, message: str) -> None:
    """Move the bar AND the line. The one place `_PROGRESS` is called with a new position."""
    _PROGRESS_AT["done"], _PROGRESS_AT["total"] = done, total
    if _PROGRESS is None:
        return
    try:
        _PROGRESS(done, total, message)
    except Exception:  # noqa: BLE001 — a reporter must never be the reason a scan fails
        _log.debug("[airs_vermogen] progress listener raised", exc_info=True)


def _emit(kind: str, **fields) -> None:
    """Append one step to the run's live log — what the scan is doing, as it does it.

    ⚠ AND FORWARD IT TO THE JOB'S PROGRESS LINE — see `_PROGRESS`. Every phase before the account
    loop (discovery, the roster check, the plan) reports ONLY through here, so without this the
    toast sits on "starting…" through all of it. The bar's position is unchanged: these lines
    narrate work that has no denominator yet, and re-reporting `0/0` mid-loop would blank a bar
    that is genuinely at 12/44.

    ⚠ A MINUTES-LONG SCRAPE WITH NO NARRATION IS INDISTINGUISHABLE FROM A HUNG ONE. The fleet pass
    is 44 accounts x 4 downloads behind a headless browser; before this the only thing anyone could
    see was `i/n: name…` and, at the very end, a summary. Which portfolios AIRS listed, which were
    skipped and why, and which of the four reports arrived for whom were all invisible while it
    mattered — and every one of them turned out to be where the bugs were.

    The list is polled with the status, so the UI can print each new entry exactly once. It is a
    log, so it is APPEND-ONLY and never rewritten: a step that already happened cannot become
    untrue later, and the operator's console must not silently disagree with what they read a
    minute ago.
    """
    entry = {"seq": len(_STATUS.get("log") or []), "kind": kind,
             "at": datetime.now(timezone.utc).isoformat(), **fields}
    _STATUS.setdefault("log", []).append(entry)
    msg = fields.get("message")
    if _PROGRESS is not None and msg:
        try:
            _PROGRESS(_PROGRESS_AT["done"], _PROGRESS_AT["total"], str(msg).strip())
        except Exception:  # noqa: BLE001 — telemetry must never break the scan
            _log.debug("[airs_vermogen] progress listener raised", exc_info=True)


def _parse_stamp(raw: str | None) -> datetime | None:
    """A Postgres timestamptz string → an aware datetime, or None if it isn't one.

    ⚠ UNPARSEABLE MUST MEAN "SCAN IT". Every caller treats None as stale, so a format we don't
    recognise costs one scan; the opposite default would silently skip an account for ever on the
    strength of a string we couldn't read.
    """
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    # A naive stamp is ours (we write `datetime.now(timezone.utc).isoformat()`), so read it as UTC
    # rather than as local time — an hours-wide error in exactly the comparison below.
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def accounts_to_scan(
    names: list[str],
    verdicts: dict[str, dict],
    now: datetime,
    max_age_hours: float = AIRS_FRESH_HOURS,
    force: bool = False,
) -> tuple[list[str], list[str]]:
    """Split the discovered accounts into (to_scan, already current). Pure — no I/O, no clock.

    An account is skipped ONLY when the last pass got **all four** reports and did so recently.
    Anything else is scanned:

      - never scanned, or its roster row was deleted  → scan (this is what makes the delete/refill
        test work: a deleted account has no verdict, so Refresh all refills exactly that gap)
      - the last pass was short a report               → scan (a retry is the whole point; skipping
        a partial account would make a transient failure permanent)
      - the verdict is older than `max_age_hours`      → scan

    ⚠ IT GATES ON WHAT WE FETCHED, NOT ON WHAT THE DATA SAYS. The tempting rule — "skip an account
    whose newest snapshot equals the fleet's newest snapshot" — punishes exactly the accounts that
    are fine: `_vermogen_most_recent` walks back to each book's own last VALUED date, and a book
    valued monthly legitimately sits weeks behind a daily-valued one. Under that rule it would
    never match the fleet maximum and would be re-downloaded on every single press, for ever.

    ⚠ A FUTURE STAMP IS STALE, NOT ETERNALLY FRESH. Clock skew or one bad row would otherwise pin
    an account in the skip list permanently, and the failure is invisible — it looks like the
    refresh working quickly.
    """
    if force:
        return list(names), []
    cutoff = now - timedelta(hours=max_age_hours)
    needed = set(REPORTS)
    to_scan: list[str] = []
    skipped: list[str] = []
    for name in names:
        v = verdicts.get(name) or {}
        at = _parse_stamp(v.get("reports_at"))
        got = set(v.get("reports_ok") or ())
        fresh = at is not None and cutoff <= at <= now and needed.issubset(got)
        (skipped if fresh else to_scan).append(name)
    return to_scan, skipped


def _roster_names() -> list[str]:
    """The accounts the LAST successful discovery found — the fallback when this one cannot run.

    ⚠ THIS IS A DEGRADED ANSWER AND ONLY THE CALLER CAN DECIDE THAT IT IS GOOD ENOUGH. It is the
    previous scrape's output, so it cannot contain a portfolio opened since; the caller says so
    on the run rather than letting a short list pass for a complete one. See the discovery
    fallback in `run_airs_vermogen_refresh_sync`.

    ⚠ EMPTY ON FAILURE, never a partial guess — the caller's `_MIN_ROSTER` floor then declines the
    fallback and reports the original discovery error, which is the honest outcome when we know
    neither the live population nor the stored one.
    """
    try:
        resp = (supabase.table("airs_account_roster")
                .select("portefeuille").limit(2000).execute())
    except Exception as e:  # noqa: BLE001 — the fallback must not raise over the error it handles
        _log.warning("[airs_vermogen] could not read the stored roster: %s: %s",
                     type(e).__name__, e)
        return []
    return sorted({(r.get("portefeuille") or "").strip()
                   for r in (resp.data or []) if (r.get("portefeuille") or "").strip()})


def _roster_verdicts() -> dict[str, dict]:
    """`portefeuille` → its last recorded `{reports_ok, reports_at}`. One row per account (~44).

    ⚠ ON FAILURE IT RETURNS EMPTY, WHICH MEANS "SCAN EVERYTHING". Failing toward doing the work
    costs a slow refresh; failing the other way would skip the fleet on the strength of a dropped
    connection and report it as up to date.
    """
    try:
        resp = (supabase.table("airs_account_roster")
                .select("portefeuille,reports_ok,reports_at").execute())
    except Exception as e:  # noqa: BLE001 — bookkeeping must never fail the scrape
        _log.warning("[airs_vermogen] could not read roster verdicts (scanning all): %s: %s",
                     type(e).__name__, e)
        return {}
    return {r["portefeuille"]: r for r in (resp.data or []) if r.get("portefeuille")}


# The tables a "delete this account" clears, and the ONLY ones it touches.
#
# ⚠ `airs_account_hidden` IS DELIBERATELY ABSENT. That row is a human DECISION to keep an account off the list;
# clearing it would resurrect an account somebody deliberately hid, as a side effect of a refresh
# test.
_DELETABLE_TABLES = (
    "airs_performance",       # the returns
    "airs_holding",           # the snapshots
    "airs_mutatie",           # dividends / flows
    "airs_model_weight",      # the book's own strategy weights
    "airs_account_roster",    # existence + which reports last arrived
    "airs_account_model_link",  # the fixed↔dynamic pairing (re-guessed on the next read)
)


def delete_account(portefeuille: str) -> dict:
    """Remove ONE account's scraped rows so a refresh can be watched rebuilding them.

    ⚠ THIS IS A REAL DELETE, AND `airs_account_hidden` EXISTS BECAUSE THAT IS USUALLY WRONG. To
    take an unwanted account off the list, hide it — the next scrape puts deleted rows straight
    back, so deleting achieves nothing and costs history. This is for the other case: proving the
    refresh actually refills a gap.

    ⚠ AND IT LOSES HISTORY THE REFRESH CANNOT RESTORE. A scan fetches `1 Jan → today`, so any
    `airs_performance` month before January is gone for good — the caller must say so before
    asking. The counts returned are what was actually removed, per table, so the damage is stated
    rather than assumed.
    """
    removed: dict[str, int] = {}
    for table in _DELETABLE_TABLES:
        try:
            # `count="exact"` so the answer is what the database did, not what we asked for.
            resp = (supabase.table(table).delete(count="exact")
                    .eq("portefeuille", portefeuille).execute())
            removed[table] = resp.count if resp.count is not None else len(resp.data or [])
        except Exception as e:  # noqa: BLE001 — report the tables that failed, delete the rest
            _log.warning("[airs_vermogen] deleting %s from %s failed: %s: %s",
                         portefeuille, table, type(e).__name__, e)
            removed[table] = -1
    total = sum(v for v in removed.values() if v > 0)
    _log.warning("[airs_vermogen] deleted account %s — %d rows across %d tables: %s",
                 portefeuille, total, len(_DELETABLE_TABLES), removed)
    return {"portefeuille": portefeuille, "deleted": removed, "total_rows": total}


def summarise_errors(errors: list[dict]) -> list[dict]:
    """Group failures by CAUSE, commonest first — the difference between an actionable message and
    a number.

    ⚠ "27 report(s) failed" IS NOT A DIAGNOSIS. It says something is wrong, gives no handle on
    what, and 27 individual lines are no better — nobody reads 27 stack summaries looking for the
    pattern. Grouped, the same data says "13 × Vermogensoverzicht: no valued snapshot in the last
    7 days" and the fix is obvious (those books have not been valued yet), versus "14 × Model:
    login expired", which is a different fix entirely.

    ⚠ THE MESSAGE IS TRUNCATED FOR THE KEY, NOT FOR DISPLAY. Two failures of the same kind often
    differ in a trailing detail (a date, an account code), and keying on the whole string would
    scatter one cause across a dozen groups of one — which is exactly the un-summarised list this
    exists to replace.
    """
    groups: dict[tuple[str, str, str], dict] = {}
    for e in errors:
        msg = (e.get("message") or "").strip()
        key = (e.get("report") or "?", e.get("error_type") or "?", msg[:80])
        g = groups.setdefault(key, {
            "report": key[0], "error_type": key[1], "message": msg[:200],
            "count": 0, "accounts": [],
        })
        g["count"] += 1
        # A few names, not all 13 — enough to go and look at one, not enough to be a second list.
        if len(g["accounts"]) < 4 and e.get("account"):
            g["accounts"].append(e["account"])
    return sorted(groups.values(), key=lambda g: -g["count"])


def count_outcomes(
    skipped: list[str], known: set[str], outcomes: dict[str, list[str]],
    small: list[str] | None = None,
) -> dict[str, int]:
    """What the run DID, in the four words an operator actually asks in: added, updated, already up
    to date, failed. Pure — `outcomes` is `{scanned account: the reports that arrived}`.

    ⚠ "NEW" IS DECIDED AGAINST THE ROSTER, NOT AGAINST AIRS. `known` is the accounts we already had
    a roster row for BEFORE this run started (`_roster_verdicts`, read once, before the loop). An
    account AIRS has always had but we have never scanned is genuinely *added* here — the sentence
    is about our database, which is the thing the button changed.

    ⚠ AND AN ACCOUNT THAT STORED NOTHING IS `failed`, NOT `updated`. Every report can fail while
    the account is still visited; counting the visit as an update would report work that did not
    happen, in the one number somebody reads to decide whether to press the button again. So the
    test is `outcomes[name]` being non-empty — at least one report arrived and was written.

    ⚠⚠ `small` IS THE FIFTH COUNT AND IT EXISTS BECAUSE THE PARTITION HAD QUIETLY STOPPED BEING ONE.
    The books `bogus_accounts` drops are removed from `todo` AFTER `accounts_to_scan` has split the
    fleet, so they were in neither `skipped` nor `outcomes` — they were in nothing. Measured
    2026-08-17: 45 accounts on the page, 16 of them dropped as too small, and the summary described
    29. A reader saw "0 added, 28 re-read, 1 skipped" and reasonably concluded the fleet was
    covered, while four of the missing sixteen had not been read in twelve trading days and wore an
    amber badge saying so. THAT is the contradiction between this line and the row icons; the
    valuation-date clause fixed the other half of it.

    The five counts partition the discovered fleet exactly (`skipped + small + len(outcomes)`), so
    they can be read as a whole without wondering where the missing accounts went.
    """
    added = updated = failed = 0
    for name, ok in outcomes.items():
        if not ok:
            failed += 1
        elif name in known:
            updated += 1
        else:
            added += 1
    return {"added": added, "updated": updated, "up_to_date": len(skipped), "failed": failed,
            "too_small": len(small or ())}


def format_run_message(counts: dict[str, int], newest_as_of: str | None = None) -> str:
    """The ONE line the page shows. Pure.

    ⚠ THIS IS THE WHOLE USER-FACING REPORT, AND THAT IS THE POINT. It used to read "30/44 accounts
    complete — 0 already current, 44 scanned: Rendement 44/44, Vermogensoverzicht 44/44 (710
    holdings), Mutaties 972 rows, Model 699 rows; 14 report(s) failed" — five report names, six
    ratios and two row counts, none of which answers "did it work". Per-report totals, per-cause
    failure groups and the raw error list all still exist; they go to the log and to `detail`.

    ⚠ `failed` IS THE ONE EXCEPTION AND IT STAYS. The banner turns amber when reports failed, and a
    colour with no reason beside it is worse than no colour at all — the reader knows only that
    something is wrong. One word ("2 failed") is not a report; it is what the amber means.

    All three good counts show even at zero: a fixed shape is read at a glance, whereas a line that
    drops its clauses has to be parsed before it can be understood.

    ⚠⚠ IT SAID "ALREADY UP TO DATE" AND THAT DIRECTLY CONTRADICTED THE ROW BADGES. Every count here
    is about OUR COPY — what we fetched, and when — while the ⓘ on each row measures **AIRS's
    valuation date**. Both were true at once and they read as opposites: the run reported
    "44 already up to date" while `DealmakersTopSelectie Offensief` wore "3 trading days old"
    (measured 2026-08-17: fetched 13:15 today, all five reports, `as_of` 2026-08-12).

    Two changes, and the second is the one that actually reconciles them:
      * the phrase now says what it means — these accounts were skipped because we READ them
        recently, not because the data behind them is current;
      * `newest_as_of` puts **the freshest valuation the fleet actually holds** in the same
        sentence. A reader who sees "newest AIRS valuation 2026-08-15" cannot then read the counts
        as a claim that every book is current to today, which is the whole misunderstanding.

    `newest_as_of` is optional so the pure-count callers and the tests keep working unchanged.
    """
    a = counts["added"]
    line = (f"{a} portfolio{'' if a == 1 else 's'} added, "
            f"{counts['updated']} re-read, "
            f"{counts['up_to_date']} skipped (we read them within {AIRS_FRESH_HOURS:g}h)")
    # ⚠ THE BOOKS THE RUN NEVER LOOKED AT, NAMED — see `count_outcomes`. Without this clause they
    # are in no count at all, and the line reads as a statement about the whole fleet while
    # describing two thirds of it. Only shown when there are some: a fixed shape is worth having
    # for the three counts a run always produces, and this one is a property of the fleet.
    if counts.get("too_small"):
        line += (f", {counts['too_small']} not re-read (under {MIN_REAL_HOLDINGS} holdings — "
                 f"re-read anyway after {AIRS_BOGUS_MAX_AGE_HOURS / 24:g} days)")
    if counts.get("failed"):
        line += f", {counts['failed']} failed"
    # ⚠ THE DATA'S OWN DATE, NOT OURS — see above. Named "AIRS valuation" rather than "as of" so it
    # cannot be read as the time of the scan.
    if newest_as_of:
        line += f" · newest AIRS valuation {newest_as_of}"
    return line


def scan_one(name: str, van: str, tot: str, on_report=None) -> dict:
    """Fetch + store ONE account's four AIRS reports. THE only place that work is written.

    ⚠ IT DOES NOT TAKE `_LOCK`. Both callers hold it already — `refresh_one_portfolio` for a single
    row, `run_airs_vermogen_refresh_sync` for the whole fleet — and taking it here would deadlock
    the fleet run against itself on its very first account.

    ⚠ THIS EXISTS BECAUSE THERE WERE TWO COPIES. "Refresh all" ran a bespoke loop and the per-row
    "Refresh" ran its own near-identical block: four try/excepts each, duplicated error strings,
    duplicated outcome bookkeeping. Two implementations of "scan an account" is one more than the
    number of ways an account can be scanned, and they had already drifted — only one of them
    recorded which reports arrived. Refresh-all is now literally refresh-one, N times.

    Returns `{reports_ok, holdings, mutaties, model_weights, as_of, errors}`. Each report is
    independent: one failing must not lose the other three — a book's dividends are worth having
    even when its valuation is unavailable.
    """
    from airs_scanner import download_portfolio_sync  # noqa: PLC0415
    from portfolio import parse_airs_excel  # noqa: PLC0415
    from routers.airs import _parse_att_excel, _save_performance_to_db  # noqa: PLC0415

    ok: list[str] = []
    # ⚠ STRUCTURED, NOT PRE-FORMATTED STRINGS. The fleet run groups 27 failures by CAUSE so the
    # operator sees "13 × Vermogensoverzicht: no valued snapshot in the last 7 days" instead of a
    # bare count — and regex-ing that back out of "BUS_X (Vermogensoverzicht: RuntimeError: …)"
    # would be parsing a message we formatted ourselves one line earlier.
    errors: list[dict] = []
    holdings = mutaties = transacties = model_weights = 0
    as_of = tot

    from airs_scanner import AirsNoData  # noqa: PLC0415

    def _step_detail(code: str) -> str:
        """What the successful download actually yielded — the number that makes "ok" verifiable."""
        return {"att": "stored", "volk": f"{holdings} holdings as of {as_of}",
                "mut": f"{mutaties} mutations", "trans": f"{transacties} transactions",
                "model": f"{model_weights} model weights",
                }.get(code, "")

    def _say(report: str, status: str, detail: str = "") -> None:
        """Report ONE download's outcome the moment it is known.

        ⚠ AS IT HAPPENS, NOT AT THE END. A fleet pass is 44 accounts x 4 downloads and runs for
        minutes; reporting only on completion means the operator watches a spinner and cannot tell
        a slow scan from a hung one, or see which account it is stuck on. Three outcomes, kept
        apart on purpose: `ok` stored something, `no_data` is AIRS answering that this book has no
        such report (see `AirsNoData`), `failed` is a fault.
        """
        if on_report:
            try:
                on_report(name, report, status, detail)
            except Exception:  # noqa: BLE001 — telemetry must never break the scan
                pass

    def _step(code: str, label: str, fn) -> None:
        # ⚠ EVERY REPORT TIMED. A scan is 4 downloads x N books behind a headless browser and it is
        # not obvious which of the four is slow — Vermogensoverzicht walks back over unvalued dates,
        # the others are one request. Naming the seconds is what turns "can we speed it up" into a
        # question with an answer.
        t0 = _time.perf_counter()

        def _ms() -> str:
            return f"{_time.perf_counter() - t0:.1f}s"
        try:
            fn()
            ok.append(code)
            _say(label, "ok", f"{_step_detail(code)} ({_ms()})")
        except AirsNoData as e:
            # ⚠ RETRIEVED, AND EMPTY. AIRS answered; this book simply has no such report — 14 of
            # 44 have no fixed MODEL because they are benchmarks, `meervoudig` books or test
            # shells. Counting it as `ok` is what makes the account COMPLETE, which is what stops
            # it wearing a permanent ⚠ and being re-scanned on every run for ever. See `AirsNoData`
            # for why this is safe to distinguish from a dead session.
            ok.append(code)
            _say(label, "no_data", f"AIRS has no such report for this book ({_ms()})")
            _log.info("[airs_vermogen] %s %s: no data — %s", name, label, e)
        except Exception as e:  # noqa: BLE001 — one report failing must not lose the others
            errors.append({"account": name, "report": label,
                           "error_type": type(e).__name__, "message": str(e)})
            _say(label, "failed", f"{type(e).__name__}: {e} ({_ms()})")
            _log.warning("[airs_vermogen] %s %s failed: %s: %s", name, label, type(e).__name__, e)

    def _att() -> None:
        _save_performance_to_db(name, _parse_att_excel(download_portfolio_sync(name, van, tot)))

    def _volk() -> None:
        nonlocal holdings, as_of
        # Most recent VALUED date, not today (which AirSPMS has not valued yet).
        as_of, vmo = _vermogen_most_recent(name, van)
        holdings = _save_holdings(name, as_of, parse_airs_excel(vmo))

    def _mut() -> None:
        nonlocal mutaties
        mutaties = _save_mutaties(name, van, tot)

    def _trans() -> None:
        nonlocal transacties
        from airs_scanner import AirsNoData  # noqa: PLC0415
        from routers._airs_transacties import _fetch_live, _store, ytd_window  # noqa: PLC0415

        # ⚠⚠ `ytd_window()`, NOT THIS SCAN'S `van`/`tot`. The Transactions panel treats a snapshot
        # of a DIFFERENT window as not-this-answer and re-fetches — so storing under any other
        # window writes a row the panel will never accept, and every open would go back out to
        # AIRS as if nothing had been cached. The two happen to be equal today; relying on that is
        # how they drift apart the first time a caller passes a custom range.
        tvan, ttot = ytd_window()
        try:
            sheet = _fetch_live(name, tvan, ttot)
        except AirsNoData:
            # ⚠ STORE THE EMPTY SNAPSHOT, THEN RE-RAISE. `_step` turns `AirsNoData` into `no_data`
            # and counts the account complete; without the write the book would be marked complete
            # while holding nothing, so the panel would re-download on every single open. Same
            # bargain `account_transactions` already strikes — one behaviour, two entry points.
            from airs_transacties import ParsedSheet  # noqa: PLC0415
            _store(name, tvan, ttot, ParsedSheet())
            raise
        _store(name, tvan, ttot, sheet)
        transacties = len(sheet.rows)

    def _model() -> None:
        nonlocal model_weights
        model_weights = _save_model_weights(name, van, tot)

    _step("att", "Rendement", _att)
    _step("volk", "Vermogensoverzicht", _volk)
    _step("mut", "Mutaties", _mut)
    _step("trans", "Transacties", _trans)
    _step("model", "Model", _model)

    return {"reports_ok": ok, "holdings": holdings, "mutaties": mutaties,
            "transacties": transacties, "model_weights": model_weights,
            "as_of": as_of, "errors": errors}


def _save_holdings(portefeuille: str, as_of: str, holdings) -> int:
    """Replace this portfolio's snapshot for `as_of` with `holdings`. Delete-then-
    insert so a position that dropped out doesn't linger from an earlier run."""
    rows = [
        {
            "portefeuille": portefeuille,
            "as_of_date": as_of,
            "holding_name": h.holding_name,
            # AIRS's own ISIN (`ISIN-code`, switched on 2026-07-23). None on the cash line and on
            # every snapshot older than that — `_airs_holding_isin` falls back to the name route.
            "isin": h.isin,
            "quantity": h.quantity,
            "currency": h.currency,
            "weight": h.weight,
            "start_value_eur": h.start_value_eur,
            "current_value_eur": h.current_value_eur,
            "ytd_return_eur": h.ytd_return_eur,
            "ytd_return_pct": h.ytd_return_pct,
            "ytd_return_local_pct": h.ytd_return_local_pct,
            # AIRS's own columns, as reported. Stored beside ours rather than instead of
            # them: two statements of the same quantity are the cross-check.
            "cost_basis_local": h.cost_basis_local,
            "current_price_local": h.current_price_local,
            "airs_weight": h.airs_weight,
            "fund_result_eur": h.fund_result_eur,
            "fx_result_eur": h.fx_result_eur,
            "airs_result_pct": h.airs_result_pct,
        }
        for h in holdings
        if h.holding_name
    ]
    if not rows:
        return 0
    (
        supabase.table("airs_holding")
        .delete().eq("portefeuille", portefeuille).eq("as_of_date", as_of).execute()
    )
    for i in range(0, len(rows), 200):
        supabase.table("airs_holding").insert(rows[i:i + 200]).execute()
    return len(rows)


def _save_mutaties(portefeuille: str, van: str, tot: str) -> int:
    """Download and store this account's Mutaties journal for [van, tot] — its dividend income.

    Delete-then-insert over the WHOLE account, not the window: the window is always "this year so
    far", so a narrower re-scan that only deleted its own range would leave last run's rows for the
    days it no longer covers and double-count them. One account, one current journal.

    ⚠ A book with no dividends yet is an EMPTY journal, which is an answer, not a failure. The
    caller treats a raised error as a failure, so a legitimately empty download must return 0.
    """
    from airs_mutaties import parse_mutaties  # noqa: PLC0415
    from airs_scanner import download_mutaties_sync  # noqa: PLC0415

    try:
        raw = download_mutaties_sync(portefeuille, van, tot)
    except RuntimeError as e:
        # `_download_report_sync` raises "Response too small" for BOTH an unvalued/empty report
        # and a dead session. The fleet loop logs it; we do not invent an empty journal, because
        # "no dividends" and "we could not ask" must not look alike.
        raise RuntimeError(f"Mutaties: {e}") from e
    rows = [{
        "portefeuille": portefeuille,
        "boekdatum": m.boekdatum.isoformat() if m.boekdatum else None,
        "grootboek": m.grootboek,
        "fonds": m.fonds,
        "omschrijving": m.omschrijving or None,
        "amount_eur": m.amount_eur,
        "amount_local": m.amount_local,
        "currency": m.currency,
        "fx_rate": m.fx_rate,
    } for m in parse_mutaties(raw) if m.fonds and m.grootboek]
    supabase.table("airs_mutatie").delete().eq("portefeuille", portefeuille).execute()
    for i in range(0, len(rows), 200):
        supabase.table("airs_mutatie").insert(rows[i:i + 200]).execute()
    return len(rows)


def _save_model_weights(portefeuille: str, van: str, tot: str) -> int:
    """Download and store this book's OWN model weights (`rapport_types=MODEL`).

    ⚠ THIS IS WHAT REPLACES THE FIXED↔DYNAMIC PAIRING. The weights are scoped to the dynamic
    portfolio, so there is no second AirSPMS portfolio to guess a partner for — and no
    mis-pairing that files a book's money under another strategy's name.

    Delete-then-insert per account, so a position dropped from the model disappears instead of
    lingering as a weight nothing holds.
    """
    from airs_model import model_total_pct, parse_model  # noqa: PLC0415
    from airs_scanner import download_model_sync  # noqa: PLC0415

    weights = parse_model(download_model_sync(portefeuille, van, tot))
    if not weights:
        return 0
    total = model_total_pct(weights)
    # ⚠ Measured at EXACTLY 100.000 on every book. A partial sheet understates every weight and
    # looks entirely normal, so it is refused rather than stored.
    if not (95.0 <= total <= 105.0):
        raise RuntimeError(
            f"MODEL percentages sum to {total}, not ~100 — refusing to store a partial model")
    rows = [{
        "portefeuille": portefeuille, "fonds": w.fonds, "model_pct": w.model_pct,
        "actual_pct": w.actual_pct, "drift_pct": w.drift_pct, "drift_eur": w.drift_eur,
        "buy": w.buy, "sell": w.sell, "model_value_eur": w.model_value_eur,
    } for w in weights]
    supabase.table("airs_model_weight").delete().eq("portefeuille", portefeuille).execute()
    for i in range(0, len(rows), 200):
        supabase.table("airs_model_weight").insert(rows[i:i + 200]).execute()
    return len(rows)


def run_airs_vermogen_refresh_sync(triggered_by: str = "manual", force: bool = False,
                                   on_step=None, should_stop=None) -> dict:
    """Discover → download → parse → store, for every live portfolio that NEEDS it. Serialized
    via `_LOCK` (a second trigger while one runs returns busy). Returns the final
    status dict. Call from a thread — it does blocking Playwright + DB work.

    ⚠ INCREMENTAL BY DEFAULT. Discovery always runs against AIRS — the live list is the point, and
    it is what refills an account somebody deleted — but an account whose last pass got all four
    reports within `AIRS_FRESH_HOURS` is skipped rather than re-downloaded (`accounts_to_scan`).
    `force=True` scans every discovered account regardless.

    `on_step(done, total, message)` and `should_stop()` are the JOB hooks — see
    `/api/airs/vermogen/refresh/job`. Both optional and both no-ops when absent, so the scheduler
    and the plain POST keep exactly today's behaviour.

    ⚠ CANCELLATION IS CHECKED BETWEEN ACCOUNTS, NEVER INSIDE ONE. An account's four reports are
    downloaded and stored as a unit; stopping midway would leave it holding two fresh reports and
    two stale ones, with nothing on the row to say which. Between accounts the state is always
    consistent — every book is either fully re-read or untouched — so that is the only safe
    boundary. The cost is that Cancel waits out the account in flight (seconds), which is stated
    in the UI rather than hidden.
    """
    if not _LOCK.acquire(blocking=False):
        return {"status": "busy", "message": "An AIRS refresh is already running"}
    global _PROGRESS  # noqa: PLW0603 — see `_PROGRESS`: one run at a time, guarded by `_LOCK`
    _PROGRESS = on_step
    _PROGRESS_AT.update(done=0, total=0)
    try:
        # ⚠ THE FIRST LINE GOES OUT BEFORE ANY WORK, so the toast leaves "starting…" within a
        # second of the press rather than at the first account — and it names the SLOW thing, so a
        # reader who presses this at 09:00 knows the wait is a browser login and not a stuck job.
        _say(0, 0, "signing in to AirSPMS and discovering portfolios…")

        today = date.today()
        van, tot = f"{today.year}-01-01", today.isoformat()
        _STATUS.update({
            "running": True,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "status": "running",
            "message": "Discovering active portfolios…",
            "detail": None,
            "triggered_by": triggered_by,
            "portfolios_found": 0,
            "accounts_added": 0,
            "accounts_updated": 0,
            "accounts_up_to_date": 0,
            "accounts_failed": 0,
            "rendement_stored": 0,
            "vermogen_stored": 0,
            "holdings_rows": 0,
            "errors": [],
            "error_summary": [],
            "log": [],
        })
        # ⚠ PER RUN, NOT PER PROCESS. "This date has no valuation" is true until AirSPMS's next
        # end-of-day batch; caching it beyond one run would make a scan an hour later skip the very
        # date that has since been valued.
        #
        # ⚠ THE TTL IN `_vermogen_most_recent` NOW GUARANTEES THIS ANYWAY, and the explicit reset
        # stays because a fleet run is the one caller that genuinely wants a clean slate at a known
        # moment rather than "some time in the last quarter of an hour". It is no longer the thing
        # holding the invariant up — which is the point, since for months it was, and it only ever
        # covered this one entry point.
        _reset_valuation_memo()
        _STATUS.update({
        })

        degraded: str | None = None
        try:
            names = _discover_portfolios()
        except Exception as e:
            # ⚠⚠ DISCOVERY IS THE RISKIEST STEP AND WAS THE ONLY FATAL ONE (2026-08-22). It is the
            # single place that drives somebody else's UI with a browser, and a menu item that
            # became unclickable ended a 46-account run at step one:
            #
            #   [airs_vermogen] discovery failed: TimeoutError: ElementHandle.click: Timeout 30000ms
            #   [job] Refresh all portfolios (airs.vermogen.refresh) failed
            #
            # Nothing was scanned, and the refresh is exactly what clears a ⚠ Vermogensoverzicht
            # badge — so a broken menu presented as forty-six stale books.
            #
            # ⚠ BUT WE ALREADY KNOW THE ACCOUNTS. `airs_account_roster` is the previous discovery's
            # own output, and the population changes a few times a year. Refusing to scan a roster
            # we are holding, because we could not re-derive the identical list, throws away the
            # entire run to protect against a difference that is usually empty.
            names = _roster_names()
            if len(names) < _MIN_ROSTER:
                _STATUS.update({
                    "status": "error",
                    "message": f"Portfolio discovery failed: {type(e).__name__}: {e}",
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                })
                _log.warning("[airs_vermogen] discovery failed: %s: %s", type(e).__name__, e)
                return dict(_STATUS)
            # ⚠ LOUD, AND CARRIED TO THE END. A degraded run must not look like a clean one: a book
            # added since the last discovery is NOT in this list and will not be scanned, which is
            # invisible from the result. `_MIN_ROSTER` is the same floor `_record_roster` uses to
            # decide a discovery is untrustworthy — one definition of "too few to believe".
            degraded = (f"⚠ Portfolio discovery failed ({type(e).__name__}) — scanning the "
                        f"{len(names)} accounts from the last successful discovery instead. "
                        "A portfolio added since then is NOT in this run.")
            _log.warning("[airs_vermogen] discovery failed: %s: %s — falling back to the stored "
                         "roster (%d accounts)", type(e).__name__, e, len(names))
            _emit("discovery", step="fallback", message=f"  {degraded}")
            _STATUS["message"] = degraded

        _STATUS["portfolios_found"] = len(names)
        # ⚠ THE ROSTER ITSELF, NAMED. "44 found" is a number you cannot check; the 44 names are the
        # thing to compare against AIRS's own "44 Items in selectie", and the only way to see that
        # discovery picked the Interne/actief/no-consolidation population and not some other one.
        _emit("discovered", count=len(names), names=names,
              message=f"AIRS lists {len(names)} portfolios")
        # ⚠ THE EXPECTED COUNT IS 44, AND ANYTHING ELSE IS WORTH SAYING OUT LOUD. The three filters
        # (Actieve / Interne / Zonder consolidatie) define exactly that population; a different
        # number means either a filter stopped applying or AIRS's own roster changed, and those need
        # opposite responses. The scraper's own "N Items in selectie" comparison, emitted just
        # above, says which.
        if len(names) != _EXPECTED_ROSTER:
            _emit("roster_unexpected", count=len(names), expected=_EXPECTED_ROSTER,
                  message=(f"⚠ EXPECTED {_EXPECTED_ROSTER} portfolios, got {len(names)}. If AIRS's "
                           f"own 'Items in selectie' above also says {len(names)}, the roster "
                           f"genuinely changed; if it says {_EXPECTED_ROSTER}, a filter or the "
                           f"pager is wrong."))
        # ⚠ THE SKIP IS DECIDED ONCE, BEFORE THE LOOP, AGAINST THE VERDICTS AS THEY WERE AT THE
        # START. Re-reading per account would let this run's own writes shorten its own worklist.
        # ⚠ READ ONCE, HERE. `known` is which accounts we already had a roster row for BEFORE this
        # run wrote any — that is what makes "added" mean anything. Re-reading it after the loop
        # would find every account known (this run just recorded them all) and report 0 added for
        # ever, including on the very first scan of a new book.
        # ⚠ THE DENOMINATOR ARRIVES HERE, AND THE BAR SHOULD TAKE IT IMMEDIATELY. Discovery has an
        # unknown length (indeterminate bar); from this point the run knows it is 44 accounts, and
        # the planning reads below are seconds of database work that would otherwise be one more
        # silent gap between "AIRS lists 44 portfolios" and the first account.
        _say(0, len(names), f"planning — reading which of {len(names)} accounts are current…")
        verdicts = _roster_verdicts()
        known = set(verdicts)
        todo, current = accounts_to_scan(
            names, verdicts, datetime.now(timezone.utc), force=force)
        # ⚠ THE SECOND SKIP, AND IT IS ABOUT THE BOOK RATHER THAN THE CLOCK. `accounts_to_scan`
        # answers "is this account's data fresh"; this answers "is this account worth fetching at
        # all". A benchmark with one holding and a `_MV` shell with none are re-downloaded four
        # times each, every run, for ever — and they are precisely the books that can never be
        # complete, so the freshness skip never catches them either. `force` re-checks everything.
        from routers._airs_accounts import (  # noqa: PLC0415
            _hidden_accounts, _holding_counts, _live_accounts,
        )

        bogus: set[str] = set()
        # ⚠ HELD FOR THE SUMMARY, NOT ONLY FOR THE LOG. These names used to exist solely inside the
        # `plan_bogus` event; the one line the page shows never mentioned them, so two thirds of the
        # fleet could go unread and the sentence still read as a report on all of it.
        skipped_bogus: list[str] = []
        if not force:
            try:
                counts, _, _isin = _holding_counts()
                # ⚠⚠ THE PAGE'S OWN TWO FILTERS, IN THE SAME ORDER — `list_accounts` drops an
                # account that is hidden OR that AIRS did not list on the last discovery, and BOTH
                # matter here. `airs_account_hidden` is currently empty; it is `_live_accounts` that
                # removes `wts test 1-4 fx` and the retired `_L` books, which is why filtering on
                # hidden alone re-admitted fourteen invisible shells instead of the five rows a
                # reader can actually see. A third definition of "visible" would drift from the list
                # it is supposed to mirror.
                live = _live_accounts()
                roster_keys = {n.strip().lower() for n in verdicts}
                visible = (live if live is not None else roster_keys) - _hidden_accounts()
                bogus = bogus_accounts(counts, verdicts, visible=visible)
            except Exception as e:  # noqa: BLE001 — a failed lookup must scan MORE, never less
                _log.warning("[airs_vermogen] could not read holdings counts (%s: %s) — scanning "
                             "every account", type(e).__name__, e)
            skipped_bogus = [n for n in todo if n.strip().lower() in bogus]
            if skipped_bogus:
                todo = [n for n in todo if n.strip().lower() not in bogus]
                _emit("plan_bogus", accounts=skipped_bogus, threshold=MIN_REAL_HOLDINGS,
                      message=(f"{len(skipped_bogus)} book(s) skipped as too small to be portfolios "
                               f"(< {MIN_REAL_HOLDINGS} holdings): {', '.join(skipped_bogus)}"))
        _STATUS["skipped"] = len(current)
        _emit("plan", to_scan=len(todo), skipped=len(current), todo=todo, current=current,
              forced=force,
              message=(f"{len(todo)} to scan, {len(current)} already current"
                       + (" (forced re-scan)" if force else "")))
        _log.info("[airs_vermogen] %d discovered — %d to scan, %d already current%s",
                  len(names), len(todo), len(current), " (forced)" if force else "")
        rendement_ok = vermogen_ok = holdings_total = mutaties_total = model_total = 0
        # ⚠ THE DATA'S OWN DATE, TRACKED SO THE SUMMARY CAN STATE IT — see
        # `format_run_message`. Only the accounts this run READ; a skipped one taught us
        # nothing new, and claiming its stored date as this run's finding would be a
        # sentence about work that did not happen.
        newest_as_of: str | None = None
        # ⚠ EVERY SCANNED ACCOUNT GETS AN ENTRY, INCLUDING ONE THAT YIELDS NOTHING. An account
        # missing from this dict would keep whatever verdict a previous run left behind, so a
        # report that started failing today would go on reading as complete. A SKIPPED account is
        # deliberately absent — keeping its verdict is exactly what skipping it means.
        outcomes: dict[str, list[str]] = {n: [] for n in todo}
        fleet_errors: list[dict] = []
        # ONE stamp for the whole run — see the note at the `_record_reports` call below.
        run_stamp = datetime.now(timezone.utc).isoformat()
        # ⚠ THE COUNTER RUNS OVER THE ROSTER, NOT THE WORKLIST. It used to read `i/len(todo)`, so a
        # pass that skipped 30 fresh accounts and scanned 14 counted "1/14…14/14" — and every one
        # of those 14 was a book with a failing report, which is precisely the population that can
        # never be skipped. The operator saw "3/14" against a list of 44 and had no way to tell
        # whether discovery had broken or the worklist was short on purpose. Walking all 44 and
        # SAYING which are skipped makes the two legible, and the number matches AIRS's own count.
        # ⚠ THE LOOP WALKS THE ROSTER, SO IT NEEDS EVERY SKIP REASON — not just the freshness one.
        # `todo` is filtered above, but iterating `names` is what makes the counter read n/44, so a
        # book removed from `todo` would otherwise be scanned here anyway.
        skipped_set = set(current)
        todo_set = set(todo)
        cancelled_at: str | None = None
        for i, name in enumerate(names, 1):
            # ⚠ BEFORE THE ACCOUNT, NOT INSIDE IT — see the docstring. `break`, not `return`, so the
            # run still falls through to `_finish` below and records what it DID store; abandoning
            # here would leave `_STATUS["running"]` true for ever and the next press would read
            # "busy" against a job nobody is running.
            if should_stop is not None and should_stop():
                cancelled_at = name
                _emit("cancelled", i=i, n=len(names), account=name,
                      message=f"[{i}/{len(names)}] cancelled before {name} — "
                              f"{i - 1} account(s) already stored")
                break
            # ⚠ THROUGH `_say`, NOT `on_step` DIRECTLY — it records the position so the per-report
            # lines `scan_one` emits underneath ("Vermogensoverzicht: ok — 31 holdings…") keep the
            # bar at 12/44 instead of resetting it to the 0/0 the pre-loop phases ran at.
            _say(i, len(names), f"{i}/{len(names)}: {name}")
            if name in skipped_set:
                _emit("account_skipped", i=i, n=len(names), account=name,
                      message=f"[{i}/{len(names)}] {name}: skipped — all reports current")
                continue
            if name not in todo_set:
                _emit("account_skipped", i=i, n=len(names), account=name, reason="too_small",
                      message=(f"[{i}/{len(names)}] {name}: skipped — under "
                               f"{MIN_REAL_HOLDINGS} holdings, not a portfolio"))
                continue
            _STATUS["message"] = f"{i}/{len(names)}: {name}…"
            _emit("account_start", i=i, n=len(names), account=name,
                  message=f"[{i}/{len(names)}] {name}")
            # ⚠ THE SAME `scan_one` THE PER-ROW REFRESH CALLS. Refresh-all IS refresh-one, N times
            # — see `scan_one` for why that had to stop being two implementations.
            res = scan_one(
                name, van, tot,
                # Each of the four downloads narrates itself the moment it lands, so a slow account
                # shows WHICH report it is waiting on rather than just a name and a spinner.
                on_report=lambda acct, report, status, detail: _emit(
                    "report", account=acct, report=report, status=status, detail=detail,
                    message=f"    {report}: {status}{f' — {detail}' if detail else ''}"),
            )
            outcomes[name] = res["reports_ok"]
            if res.get("as_of") and (newest_as_of is None or res["as_of"] > newest_as_of):
                newest_as_of = res["as_of"]
            rendement_ok += 1 if "att" in res["reports_ok"] else 0
            vermogen_ok += 1 if "volk" in res["reports_ok"] else 0
            holdings_total += res["holdings"]
            mutaties_total += res["mutaties"]
            model_total += res["model_weights"]
            # Structured, so the summary can group by cause rather than by wording.
            fleet_errors.extend(res["errors"])
            _STATUS["errors"] = [
                f"{e['account']} ({e['report']}: {e['error_type']}: {e['message']})"
                for e in fleet_errors]
            _STATUS["error_summary"] = summarise_errors(fleet_errors)
            # ⚠ RECORDED AFTER EVERY ACCOUNT, NOT ONCE AT THE END — the list is reloaded while the
            # scan runs, so a verdict written only on completion would leave every row already
            # scanned wearing a stale badge, and a run that died halfway would record nothing at
            # all about the accounts it did reach.
            #
            # ⚠ BUT WITH THE RUN'S STAMP, NOT `now()` PER ACCOUNT. `_missing_reports` and
            # `_complete_accounts` both read "this refresh's verdict" as `reports_at = max(...)`.
            # A fresh timestamp per account would make every account except the LAST look stale,
            # so 43 of 44 rows would silently drop out of the newest batch.
            _record_reports({name: res["reports_ok"]}, run_stamp)
            _emit("account_done", i=i, n=len(names), account=name,
                  got=sorted(res["reports_ok"]), failed=[e["report"] for e in res["errors"]],
                  complete=len(res["reports_ok"]) == len(REPORTS),
                  message=(f"  [{i}/{len(names)}] {name}: "
                           f"{len(res['reports_ok'])}/{len(REPORTS)} reports"
                           + (f", FAILED {[e['report'] for e in res['errors']]}"
                              if res["errors"] else "")))
            _STATUS["message"] = f"{i}/{len(names)} done: {name}"

        # ⚠ ONE JOB, ONE SUBJECT — and the lesson outlived the case that taught it. This used to
        # download the CRM "Alle relaties" export inline as well: a different report about
        # different objects (relations, not portfolios), so a CRM failure was appended to THIS
        # job'''s `errors` and counted in its "N report(s) failed" — a portfolio refresh reporting a
        # fault in a report nobody asked it to fetch. The CRM feature was retired entirely on
        # 2026-09-01; the rule against folding a second subject into this loop was not.
        total = len(todo)
        # ⚠ NOTHING TO DO IS A SUCCESS, NOT AN EMPTY FAILURE. `any_stored` alone would call a
        # fleet that is entirely up to date an "error" — the same vacuous-zero trap as a benchmark
        # reporting "0 of 0 constituents priced". A skip-everything run stored nothing precisely
        # because there was nothing to store.
        any_stored = bool(rendement_ok or vermogen_ok or not todo)
        # ⚠ RECORDED ONLY WHEN THE DISCOVERY ITSELF WAS TRUSTED, on the same threshold the roster
        # uses. A scrape that reached six accounts would otherwise mark the other thirty-eight
        # "no reports retrieved" and empty the portfolios page on the strength of a failed login.
        # Already written per account, under `run_stamp`, as each finished — nothing to flush here.
        # ⚠ A SKIPPED ACCOUNT COUNTS AS COMPLETE — being complete is WHY it was skipped. Counting
        # only the scanned ones would report "2/44 accounts complete" after a healthy no-op run,
        # which reads as catastrophic and is the exact opposite of what happened.
        complete = len(current) + sum(1 for ok in outcomes.values() if len(ok) == len(REPORTS))
        counts = count_outcomes(current, known, outcomes, skipped_bogus)
        # ⚠ EVERY REPORT THE JOB FETCHES IS NAMED — IN THE LOG. This string was the message, and it
        # said "Rendement 44/44, Vermogensoverzicht 31/44 … 27 report(s) failed", where 44−31=13
        # because `errors` also carried Mutaties and Model failures the message never mentioned:
        # two numbers nobody could reconcile, printed at everyone who pressed Refresh. It is a
        # developer's view of a scraper, so it goes where a developer looks. `detail` carries it to
        # the browser console; the page shows `format_run_message`.
        detail = (
            f"{complete}/{len(names)} accounts complete — {len(current)} already current, "
            f"{total} scanned"
            + (f": Rendement {rendement_ok}/{total}, "
               f"Vermogensoverzicht {vermogen_ok}/{total} ({holdings_total} holdings), "
               f"Mutaties {mutaties_total} rows, Model {model_total} rows" if total else "")
            + (f"; {len(_STATUS['errors'])} report(s) failed" if _STATUS["errors"] else "")
            # ⚠ A CANCELLED RUN MUST NOT READ AS A COMPLETE ONE. It stored real rows, so it is not
            # an error — but "38/44 accounts complete" with no other word implies the other six
            # failed, when in fact nobody ever asked for them.
            + (f"; CANCELLED before {cancelled_at} — the accounts after it were not read"
               if cancelled_at else "")
        )
        _STATUS.update({
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "cancelled_at": cancelled_at,
            "status": "cancelled" if cancelled_at else ("ok" if any_stored else "error"),
            "rendement_stored": rendement_ok,
            "vermogen_stored": vermogen_ok,
            "holdings_rows": holdings_total,
            "mutatie_rows": mutaties_total,
            "model_weight_rows": model_total,
            "complete_accounts": complete,
            "accounts_added": counts["added"],
            "accounts_updated": counts["updated"],
            "accounts_up_to_date": counts["up_to_date"],
            "accounts_failed": counts["failed"],
            # The freshest valuation this run actually READ — see `format_run_message`. Exposed
            # so the job summary can carry it too; a toast that ends on "44 accounts" and a row
            # that says "3 trading days old" otherwise look like they disagree.
            "newest_as_of": newest_as_of,
            # ⚠ THE DEGRADED NOTE LEADS, because it changes what every count below it MEANS. "46
            # accounts refreshed" off a stored roster is not the same claim as "46 accounts
            # refreshed" off a live discovery — the second says that is the whole population and
            # the first cannot. Appending it would put the caveat after the numbers it qualifies.
            "message": (f"{degraded} {format_run_message(counts, newest_as_of)}"
                        if degraded else format_run_message(counts, newest_as_of)),
            "detail": detail,
        })
        _log.info("[airs_vermogen] %s refresh — %s (%s)",
                  triggered_by, _STATUS["message"], detail)
        return dict(_STATUS)
    finally:
        _STATUS["running"] = False
        # ⚠ CLEARED WITH THE LOCK, IN THE SAME BLOCK. A sink left pointing at a finished job's
        # `ctx.progress` would have the NEXT run's `_emit` lines land on a card that is already
        # green — and the scheduler's ticks call this with no hook at all, which would leave the
        # last manual press's toast as the only thing they could reach.
        _PROGRESS = None
        _LOCK.release()


def _vermogen_most_recent(name: str, van: str) -> tuple[str, bytes]:
    """The Vermogensoverzicht for the most recent AVAILABLE valuation date, and that date.

    ⚠ AirSPMS VALUES END-OF-DAY. So `today` has no Vermogensoverzicht until its valuation runs, and
    a weekend or holiday never gets one — a request for an unvalued `datum_tot` returns an empty
    ~49-byte body (`Response too small`). The Rendement (ATT) report does NOT share this: it returns
    MONTHLY rows regardless of the exact date, which is why a same-day refresh fails on VOLK alone.

    So walk back from today and take the first date that returns a real file. That date IS the
    snapshot's as_of — the holdings are valued as of THEN, not today (matching what the AirSPMS UI
    shows, which also defaults to the last valued date, e.g. Friday's on a Monday).

    ⚠ THE WALK IS SHARED ACROSS THE RUN, because a day AirSPMS never valued is a fact about the
    day rather than about one book. Measured 2026-07-30: today's valuation had not run, so all ~25
    books with holdings paid one wasted request before landing on the 29th; on a Monday it is three
    (Mon, Sun, Sat) before Friday. `_UNVALUED_DATES` remembers the misses for the duration of a run,
    which removes 44-130 round trips from a full scan.

    ⚠⚠ BUT A SINGLE FAILURE DOES NOT PROVE IT, AND TREATING IT AS PROOF BROKE 29 OF 46 BOOKS
    (2026-08-21). The paragraph above used to end "so a date that has no valuation has none for ANY
    book" — true of a day the batch did not run, and NOT true of the case immediately below it: a
    book valued weekly fails on six good dates on its way back to its own, and every one of them was
    then skipped for every account scanned afterwards. The result is the worst kind of failure —
    silent, systematic, and self-inflating: the more books that walked back, the fewer dates were
    left for the rest, until the remainder skipped their entire horizon without making one request
    and raised. Every badge on the Overview page was this, and only ever on this report.

    So a date is ruled out only when BOTH hold: `_UNVALUED_QUORUM` different accounts have failed on
    it, AND it is newer than a date some account has already fetched successfully. A book cannot be
    valued ahead of the newest batch AirSPMS has run, so that pair is sound where the quorum alone
    is not — see the constant for the measurement showing a quorum by itself changes nothing.

    ⚠ ONLY MISSES ARE CACHED, NEVER HITS. A book valued monthly legitimately sits weeks behind a
    daily-valued one, so "this date worked for account A" says nothing about account B and caching
    it would hand B a stale snapshot.

    ⚠ AND THE WALK IS ALWAYS NEWEST-FIRST. The memo may only SKIP a date, never reorder the walk:
    the function's contract is the MOST RECENT valued snapshot, and trying an older date earlier
    because it happens to be uncached would return a stale one that looks entirely normal.
    """
    from airs_scanner import download_vermogensoverzicht_sync  # noqa: PLC0415

    global _NEWEST_VALUED
    # ⚠⚠ BEFORE ANYTHING ELSE, AND ON EVERY CALL. The memo is process-global and only ONE of the
    # three entry points ever reset it, so a per-row Refresh ran against a previous run's ruled-out
    # dates — which are exactly the dates that have since been valued. See `_MEMO_STARTED_AT`.
    _expire_valuation_memo()
    last_err: Exception | None = None
    tried = 0
    for back in range(_WALK_BACK_DAYS):
        tot = (date.today() - timedelta(days=back)).isoformat()
        # ⚠ BOTH CONDITIONS, AND THE SECOND IS THE LOAD-BEARING ONE — see `_UNVALUED_QUORUM`.
        if (_NEWEST_VALUED is not None and tot > _NEWEST_VALUED
                and len(_UNVALUED_DATES.get(tot, ())) >= _UNVALUED_QUORUM):
            continue
        try:
            tried += 1
            blob = download_vermogensoverzicht_sync(name, van, tot)
        except RuntimeError as e:
            # Unvalued FOR THIS BOOK → empty body / error page. Try the day before, and record which
            # account it was: the quorum above is what stops this one failure speaking for the
            # fleet. A real auth failure returns the same on EVERY date, exhausts the walk, and is
            # raised below.
            _UNVALUED_DATES.setdefault(tot, set()).add(name)
            last_err = e
            continue
        # ⚠ A SUCCESS IS THE ONLY PROOF THE BATCH RAN FOR A DATE, which is what licenses ruling out
        # anything newer. Recorded before returning, so the next account benefits from it.
        if _NEWEST_VALUED is None or tot > _NEWEST_VALUED:
            _NEWEST_VALUED = tot
        return tot, blob
    # ⚠ THE MESSAGE SAYS HOW HARD IT LOOKED. "no valued Vermogensoverzicht in the last 7 days" was
    # printed identically whether the walk made seven requests or zero — and zero is what it made
    # once the memo had ruled out the whole horizon, which is the failure that hid the bug above.
    raise RuntimeError(
        f"no valued Vermogensoverzicht in the last {_WALK_BACK_DAYS} days for {name} "
        f"({tried} date(s) tried, "
        f"{_WALK_BACK_DAYS - tried} already ruled out by ≥{_UNVALUED_QUORUM} other accounts)"
        + (f": {last_err}" if last_err else ""))


def dependent_accounts(portefeuille: str) -> list[str]:
    """The accounts this one's figures are BUILT FROM — transitively, nearest first.

    ⚠ A CERTIFICATE IS ANOTHER BOOK, AND REFRESHING ONLY THE PARENT LEAVES IT HALF FRESH. Some
    holdings are not instruments: they are Leonteq AMCs wrapping another strategy, and everything
    the modal shows through one — the looked-through positions, their returns, the attribution —
    is read from the WRAPPED book's own scan. Measured 2026-08-05: BUS_Offensief_Dyn is built on
    StarTopSelectie OFF DYN; TOPS_BEOFF_BEH_DYN on NINE other books. Re-scanning the parent alone
    re-reads AIRS for the 12 lines it stores and leaves the 40 instruments behind them dated to
    whenever those books were last touched.

    The chain is holding -> linked model portfolio -> the ACCOUNT paired with that model. All three
    hops already exist; nothing here decides a link, it only follows them.

    ⚠ READ FROM THE DB, NEVER FRESHENED (`freshen=False`). Working out WHAT to refresh must not
    itself hit AIRS — that would put a scrape in front of every scrape, and it would need the very
    session the refresh is about to use.

    ⚠ CYCLE-SAFE, AND THE CYCLE IS REAL. `_airs_portfolio_links` records it: TOPS_STS_L holds the
    certificate of the strategy it IS, so following links walks back to the row you started from.
    A `seen` set is what stops a refresh recursing until the session dies.
    """
    from routers._airs_account_links import list_account_links  # noqa: PLC0415
    from routers._airs_holding_isin import resolve_account_isins  # noqa: PLC0415

    try:
        by_model = {a["model_portfolio_id"]: a["portefeuille"]
                    for a in list_account_links()["accounts"] if a.get("model_portfolio_id")}
    except Exception as e:  # noqa: BLE001 — a refresh must not fail because the map is unreadable
        _log.warning("[airs_vermogen] could not resolve dependencies for %s (%s: %s)",
                     portefeuille, type(e).__name__, e)
        return []

    seen, order, queue = {portefeuille}, [], [portefeuille]
    while queue:
        cur = queue.pop(0)
        try:
            rows = resolve_account_isins(cur, freshen=False).get("rows") or []
        except Exception as e:  # noqa: BLE001 — one unreadable book must not lose the rest
            _log.warning("[airs_vermogen] %s: dependency scan failed (%s: %s)",
                         cur, type(e).__name__, e)
            continue
        for name in sorted({by_model[r["linked_portfolio_id"]] for r in rows
                            if r.get("linked_portfolio_id") in by_model}):
            if name in seen:
                continue
            seen.add(name)
            order.append(name)
            queue.append(name)
    return order


def refresh_one_portfolio(portefeuille: str, cascade: bool = True,
                          on_step: Callable[[int, int, str], None] | None = None,
                          should_stop: Callable[[], bool] | None = None,
                          wait: float | None = None) -> dict:
    """Re-scan ONE portfolio's Rendement (ATT) + Vermogensoverzicht (VOLK) and store both — the
    per-row "Refresh" on the overview table.

    ⚠ AND THE BOOKS IT IS BUILT FROM, unless `cascade=False`. A holding that is a certificate is
    another book, and the parent's own scan says nothing about what is inside it — see
    `dependent_accounts`. The cost is real and proportional: BUS_Offensief_Dyn pulls in one more
    account, TOPS_BEOFF_BEH_DYN nine, at four downloads each. It is reported per account rather
    than hidden in a single "done".

    ⚠ THE TARGET IS SCANNED FIRST. It is the row the user clicked, so its answer should not be
    held hostage to nine dependencies — and if one of those fails, the primary result is already
    in hand and the failure is reported beside it rather than replacing it.

    Reuses the exact download → parse → save path the full daily scan uses, so a single row's
    refresh and the whole-fleet refresh can never diverge. Serialized against the full scan (and
    other single refreshes) via `_LOCK` — they share ONE AirSPMS session, which must not be driven
    by two threads at once. A few seconds: two downloads (plus a login only if the session lapsed).

    ⚠ `on_say(done, total, message)` IS OPTIONAL AND CHANGES NOTHING ELSE. It exists because the
    cascade makes this unbounded from the reader's side — TOPS_BEOFF_BEH_DYN is NINE accounts at
    five downloads each — and a button that sits disabled for a minute with no line moving is
    indistinguishable from a broken one. It is a hook rather than a second, streaming copy of this
    function: two implementations of "refresh one portfolio" is exactly what the ⚠ above says this
    body exists to prevent.

    ⚠⚠ `should_stop()` MAKES CANCEL REAL, AND IT REVERSES A PREVIOUS REFUSAL (2026-08-13). The job
    wrapper used to pass no such hook and documented why: stopping mid-cascade leaves the parent
    fresh against stale children, "the state this endpoint exists to avoid". The argument does not
    survive contact with the alternative — `cascade=False` is a supported mode that produces the
    IDENTICAL state deliberately, and the one thing the refusal actually guaranteed was that a
    Cancel button changed nothing for minutes while its toast read "cancelling…". A control that
    does nothing is worse than a documented compromise: it teaches the reader that Cancel is
    decorative everywhere else too.

    So it stops BETWEEN accounts (never inside one — an account's reports are downloaded and stored
    as a unit) and the outcome SAYS SO: `cancelled_at` names the book it stopped before and
    `stale_books` names the ones left un-refreshed, so a half-cascade can never be mistaken for a
    clean one. Same hook, same shape and same `cancelled_at` key as `run_airs_vermogen_refresh_sync`
    — one vocabulary for cancellation, not two.

    ⚠ `wait` IS FOR CALLERS THAT ARE PART OF A LARGER REFRESH, and `None` (refuse immediately) stays
    the default because a BUTTON must answer. A person who pressed Refresh and gets "another AIRS
    refresh is running" has learned something true and can press again; the same person watching a
    disabled button for the four minutes of a fleet scan has not. But `refresh_portfolio_fully` is
    a different caller: it is one leg of a job that has already started, and refusing it there
    abandons a portfolio half-refreshed — the exact split state the whole function exists to close.
    So it waits. See `_acquire_session`.
    """
    if not _acquire_session(wait):
        return {"status": "busy", "message": "An AIRS refresh is already running", "portefeuille": portefeuille}

    # ⚠⚠ THE SAME RELAY THE FLEET RUN USES — `_say` moves the bar, `_emit` narrates at the position
    # it left. This function used to own a private `_step` that only the four call sites below
    # reached, so everything INSIDE an account was silent: the toast read
    # "AITopSelectie OFF DYN — scanning AIRS reports" at 0% for the whole scan, which for a
    # nine-book cascade at five downloads apiece is minutes of one unchanging line.
    #
    # ⚠ AND THE CAUSE WAS ONE MISSING ARGUMENT. `scan_one` already narrates every download the
    # moment it lands (`on_report`, with per-report timings) and the fleet loop passes it; this
    # caller did not, so the work it shares with the fleet reported half as much.
    global _PROGRESS  # noqa: PLW0603 — see `_PROGRESS`: one run at a time, guarded by `_LOCK`
    _PROGRESS = on_step
    _PROGRESS_AT.update(done=0, total=0)

    def _report(acct: str, report: str, status: str, detail: str = "") -> None:
        """One download's outcome, as it lands — the same shape the fleet emits.

        ⚠ `report` IS ALREADY THE HUMAN LABEL ("Vermogensoverzicht"), not the `att`/`volk` code —
        `scan_one._step` passes `label` to its `_say`. Mapping it again would print the label
        unchanged for every report and look like it worked.

        `detail` arrives with the download's own timing ("31 holdings as of 2026-08-15 (3.2s)"),
        which is the number that answers "is this slow, and which part".
        """
        _emit("report", account=acct, report=report, status=status, detail=detail,
              message=f"{acct} · {report}: {status}" + (f" — {detail}" if detail else ""))

    try:
        # ⚠ BEFORE `dependent_accounts`, WHICH IS NOT FREE. It walks the certificate chain through
        # `resolve_account_isins` per book — a lookup, but a lookup over up to nine accounts, and
        # it runs before the old first line was emitted.
        _say(0, 0, f"{portefeuille} — working out which books it is built from…")
        today = date.today()
        van, tot = f"{today.year}-01-01", today.isoformat()

        # ⚠ THE SAME FUNCTION THE FLEET SCAN CALLS. This used to be a second, near-identical copy
        # of the four downloads — and the two had already drifted (only one recorded which reports
        # arrived). One body, so "refresh this row" and "refresh everything" cannot mean different
        # things.
        # ⚠ THE TOTAL IS KNOWN BEFORE THE FIRST DOWNLOAD, so the bar is a real fraction from the
        # start rather than a spinner that suddenly acquires a denominator. `dependent_accounts`
        # is a lookup, not a scan.
        deps = list(dependent_accounts(portefeuille)) if cascade else []
        total = 1 + len(deps)
        _say(0, total, f"{portefeuille} — scanning AIRS reports"
                        + (f" (+{len(deps)} book{'s' if len(deps) != 1 else ''} it is built from)"
                           if deps else ""))
        # ⚠ THE FIRST BOUNDARY, AND THE ONE THAT MATTERS MOST — it is where a misclick is undone.
        # Nothing has been downloaded or written yet, so stopping here is not a compromise at all:
        # the account is exactly as it was. `dependent_accounts` above is a lookup, not a scan.
        if should_stop is not None and should_stop():
            _say(0, total, f"{portefeuille} — cancelled before anything was read")
            return {"status": "cancelled", "portefeuille": portefeuille,
                    "cancelled_at": portefeuille, "cascaded": [], "stale_books": deps,
                    "holdings_rows": 0, "errors": [], "reports_ok": []}
        res = scan_one(portefeuille, van, tot, on_report=_report)
        ok = res["reports_ok"]
        _say(1, total, f"{portefeuille} — {res['holdings']} holdings, "
                        f"{', '.join(sorted(ok)) or 'no reports'}")

        # ⚠ THE PER-ROW REFRESH RECORDS ITS VERDICT TOO — it is how an account short a report gets
        # its badge cleared without waiting for the next full scan. `_MIN_ROSTER` does not apply:
        # this is a deliberate request for one named account, not a discovery whose size might mean
        # the scrape failed.
        _record_reports({portefeuille: ok}, datetime.now(timezone.utc).isoformat())

        # ⚠ INSIDE THE SAME LOCK HOLD. `scan_one` deliberately does not take `_LOCK` (both callers
        # already hold it), so the dependencies run on the session this call already owns. Taking
        # and releasing per account would let the fleet scan interleave halfway through a cascade
        # and leave the parent fresh against half-stale children.
        cascaded: list[dict] = []
        cancelled_at: str | None = None
        stale_books: list[str] = []
        for i, dep in enumerate(deps, 1):
            # ⚠ BEFORE THE ACCOUNT, NOT INSIDE IT, and `break` rather than `return` — the run still
            # falls through to the result below and reports what it DID store. The same idiom the
            # fleet scan uses; abandoning here would lose the parent's own refresh, which is
            # already downloaded, stored, and the reason the button was pressed.
            if should_stop is not None and should_stop():
                cancelled_at = dep
                stale_books = deps[i - 1:]
                _say(i, total, f"cancelled before {dep} — {i - 1} of {len(deps)} book(s) refreshed")
                break
            _say(i, total, f"{dep} — book {i} of {len(deps)} behind {portefeuille}")
            try:
                sub = scan_one(dep, van, tot, on_report=_report)
            except Exception as e:  # noqa: BLE001 — one child must not lose the parent's result
                _log.warning("[airs_vermogen] cascade %s failed: %s: %s",
                             dep, type(e).__name__, e)
                cascaded.append({"portefeuille": dep, "status": "error",
                                 "errors": [f"{type(e).__name__}: {e}"]})
                # ⚠ A FAILED CHILD IS NAMED ON THE BAR, not folded into the count. A parent
                # refreshed against a book that did not scan is not fresh.
                _say(i + 1, total, f"{dep} — FAILED ({type(e).__name__})")
                continue
            _record_reports({dep: sub["reports_ok"]}, datetime.now(timezone.utc).isoformat())
            cascaded.append({
                "portefeuille": dep,
                "status": "ok" if ("att" in sub["reports_ok"] or "volk" in sub["reports_ok"])
                else "error",
                "holdings_rows": sub["holdings"],
                # The reason the cascade exists at all: this child is the book that actually
                # trades what the parent holds through a certificate, so its Transacties are what
                # make the parent's look-through possible.
                "transacties_rows": sub.get("transacties"),
                "as_of": sub["as_of"],
                "errors": [f"{e['report']}: {e['error_type']}: {e['message']}"
                           for e in sub["errors"]],
            })
        if cascaded:
            _log.warning("[airs_vermogen] %s: also refreshed %d book(s) it is built from — %s",
                         portefeuille, len(cascaded),
                         ", ".join(c["portefeuille"] for c in cascaded))

        return {
            # ⚠ CANCELLED OUTRANKS OK. The parent's own reports are stored and fresh, so `ok` would
            # be defensible on its own terms and completely misleading: the books its look-through
            # figures are computed FROM were not re-read. One word for "we stopped", named the same
            # way the fleet scan names it.
            "status": "cancelled" if cancelled_at
            else ("ok" if ("att" in ok or "volk" in ok) else "error"),
            "cancelled_at": cancelled_at,
            # The books the cancel left behind — the reason the outcome cannot be read as clean.
            "stale_books": stale_books,
            "portefeuille": portefeuille,
            # ⚠ The books BEHIND this one, each with its own outcome. A cascade that half-failed
            # must not read as a clean refresh — the parent's figures are only as fresh as the
            # child they are computed from.
            "cascaded": cascaded,
            "as_of": res["as_of"] if "volk" in ok else tot,
            "holdings_rows": res["holdings"],
            "mutatie_rows": res["mutaties"],
            "model_weight_rows": res["model_weights"],
            "rendement_stored": "att" in ok,
            "vermogen_stored": "volk" in ok,
            "transacties_stored": "trans" in ok,
            "transacties_rows": res.get("transacties"),
            "reports_ok": ok,
            "complete": len(ok) == len(REPORTS),
            # Formatted for the single-row toast; the structured form rides along beside it.
            "errors": [f"{e['report']}: {e['error_type']}: {e['message']}" for e in res["errors"]],
            "error_details": res["errors"],
        }
    finally:
        # ⚠ CLEARED WITH THE LOCK — the same rule the fleet run follows. A sink left pointing
        # at a finished job would put the NEXT refresh's lines on an already-green card.
        _PROGRESS = None
        _LOCK.release()


def get_status() -> dict:
    """Current status + the persistent freshest snapshot: its date, distinct
    portfolios, and total holding rows."""
    latest_date = None
    portfolios = 0
    holdings = 0
    try:
        resp = (
            supabase.table("airs_holding")
            .select("as_of_date").order("as_of_date", desc=True).limit(1).execute()
        )
        if resp.data:
            latest_date = resp.data[0]["as_of_date"]
            # Total holding rows at that date.
            cnt = (
                supabase.table("airs_holding")
                .select("id", count="exact")
                .eq("as_of_date", latest_date).limit(0).execute()
            )
            holdings = getattr(cnt, "count", 0) or 0
            # Distinct portfolios — paginate the portefeuille column + dedupe
            # (PostgREST has no DISTINCT count; the row set at one date is small).
            seen: set[str] = set()
            offset, page = 0, 1000
            for _ in range(20):
                rows = (
                    supabase.table("airs_holding")
                    .select("portefeuille").eq("as_of_date", latest_date)
                    .range(offset, offset + page - 1).execute()
                ).data or []
                if not rows:
                    break
                seen.update(r["portefeuille"] for r in rows)
                if len(rows) < page:
                    break
                offset += page
            portfolios = len(seen)
    except Exception:
        pass
    return {**_STATUS, "latest_snapshot_date": latest_date,
            "latest_snapshot_portfolios": portfolios,
            "latest_snapshot_holdings": holdings}
