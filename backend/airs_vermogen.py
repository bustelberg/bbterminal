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
import time as _time
from datetime import date, datetime, timedelta, timezone

from deps import supabase

_log = logging.getLogger(__name__)
_LOCK = threading.Lock()

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

# Dates proved to have NO valuation during the current run — see `_vermogen_most_recent`. Cleared
# at the start of every run: "unvalued" is true until AirSPMS's next end-of-day batch, not for ever.
_UNVALUED_DATES: set[str] = set()


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


# The four reports an account needs for every figure on the portfolios page to describe the same
# moment. Order is display order, not fetch order.
REPORTS = ("att", "volk", "mut", "model")


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
    # ⚠ UPDATE ONLY — AN ACCOUNT DISCOVERY HAS NEVER SEEN CANNOT BE INSERTED HERE, AND TRYING
    # TOOK THE WHOLE BATCH DOWN WITH IT. PostgREST's `upsert` is INSERT ... ON CONFLICT, so for a
    # portefeuille with no roster row the INSERT ran without `last_seen_at` — a NOT NULL column
    # with no default, by design (see the migration: the live set IS `last_seen_at = max(...)`,
    # so a per-row default would let this function redefine it). Measured in production
    # 2026-08-03, refreshing one account:
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

    rows = [{"portefeuille": n, "reports_ok": sorted(outcomes[n]), "reports_at": stamp}
            for n in names if n in known]
    if not rows:
        return
    try:
        for i in range(0, len(rows), 200):
            supabase.table("airs_account_roster").upsert(
                rows[i:i + 200], on_conflict="portefeuille").execute()
    except Exception as e:  # noqa: BLE001
        _log.warning("[airs_vermogen] could not record report outcomes: %s: %s",
                     type(e).__name__, e)


# How long a COMPLETE scan of an account stays good. AIRS publishes at most one valuation a day,
# so re-downloading four reports for an account we fully scanned this morning buys nothing and
# costs ~44× that. Env-tunable; the daily job's interval is far longer, so it still scans the fleet
# once a day exactly as before — this only collapses the repeat presses in between.
# ⚠ 20, NOT 12 — AIRS VALUES ONCE A DAY. The window only has to be shorter than the gap between
# two valuations; at 12h a mid-afternoon press re-downloaded the whole fleet for a valuation that
# had not moved since the morning. 20h still guarantees the daily job (a fixed 10:00 Amsterdam
# tick, ~24h apart) never skips a real one, and collapses every repeat press in between.
AIRS_FRESH_HOURS = float(os.environ.get("AIRS_FRESH_HOURS", "20"))

# Fewer holdings than this and a book is not a portfolio: the AIRS benchmarks carry exactly 1 and
# the `_MV` / `WTS test` shells carry none, against 10-29 for every real book. Same threshold the
# model-portfolios table uses, so "too small to be real" means one thing across the app.
MIN_REAL_HOLDINGS = int(os.environ.get("AIRS_MIN_REAL_HOLDINGS", "5"))


def bogus_accounts(counts: dict[str, int], verdicts: dict[str, dict]) -> set[str]:
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
    """
    out: set[str] = set()
    for name, v in verdicts.items():
        key = (name or "").strip().lower()
        if "volk" not in set(v.get("reports_ok") or ()):
            continue                      # never fetched its holdings — unknown, not empty
        if counts.get(name, counts.get(key, 0)) < MIN_REAL_HOLDINGS:
            out.add(key)
    return out


def _emit(kind: str, **fields) -> None:
    """Append one step to the run's live log — what the scan is doing, as it does it.

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
# ⚠ `airs_crm_relatie` IS DELIBERATELY ABSENT. It is keyed on `portefeuille` too, but it is a CRM
# record about a RELATION — a client — not a scraped report, and no refresh recreates it. Deleting
# it here would quietly destroy data this button cannot restore.
#
# ⚠ SO IS `airs_account_hidden`. That row is a human DECISION to keep an account off the list;
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

    The four counts partition the discovered fleet exactly (`skipped + len(outcomes)`), so they can
    be read as a whole without wondering where the missing accounts went.
    """
    added = updated = failed = 0
    for name, ok in outcomes.items():
        if not ok:
            failed += 1
        elif name in known:
            updated += 1
        else:
            added += 1
    return {"added": added, "updated": updated, "up_to_date": len(skipped), "failed": failed}


def format_run_message(counts: dict[str, int]) -> str:
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
    """
    a = counts["added"]
    line = (f"{a} portfolio{'' if a == 1 else 's'} added, "
            f"{counts['updated']} updated, {counts['up_to_date']} already up to date")
    return line + (f", {counts['failed']} failed" if counts.get("failed") else "")


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
    holdings = mutaties = model_weights = 0
    as_of = tot

    from airs_scanner import AirsNoData  # noqa: PLC0415

    def _step_detail(code: str) -> str:
        """What the successful download actually yielded — the number that makes "ok" verifiable."""
        return {"att": "stored", "volk": f"{holdings} holdings as of {as_of}",
                "mut": f"{mutaties} mutations", "model": f"{model_weights} model weights",
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

    def _model() -> None:
        nonlocal model_weights
        model_weights = _save_model_weights(name, van, tot)

    _step("att", "Rendement", _att)
    _step("volk", "Vermogensoverzicht", _volk)
    _step("mut", "Mutaties", _mut)
    _step("model", "Model", _model)

    return {"reports_ok": ok, "holdings": holdings, "mutaties": mutaties,
            "model_weights": model_weights, "as_of": as_of, "errors": errors}


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


def run_airs_vermogen_refresh_sync(triggered_by: str = "manual", force: bool = False) -> dict:
    """Discover → download → parse → store, for every live portfolio that NEEDS it. Serialized
    via `_LOCK` (a second trigger while one runs returns busy). Returns the final
    status dict. Call from a thread — it does blocking Playwright + DB work.

    ⚠ INCREMENTAL BY DEFAULT. Discovery always runs against AIRS — the live list is the point, and
    it is what refills an account somebody deleted — but an account whose last pass got all four
    reports within `AIRS_FRESH_HOURS` is skipped rather than re-downloaded (`accounts_to_scan`).
    `force=True` scans every discovered account regardless.
    """
    if not _LOCK.acquire(blocking=False):
        return {"status": "busy", "message": "An AIRS refresh is already running"}
    try:

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
        _UNVALUED_DATES.clear()
        _STATUS.update({
        })

        try:
            names = _discover_portfolios()
        except Exception as e:
            _STATUS.update({
                "status": "error",
                "message": f"Portfolio discovery failed: {type(e).__name__}: {e}",
                "finished_at": datetime.now(timezone.utc).isoformat(),
            })
            _log.warning("[airs_vermogen] discovery failed: %s: %s", type(e).__name__, e)
            return dict(_STATUS)

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
        verdicts = _roster_verdicts()
        known = set(verdicts)
        todo, current = accounts_to_scan(
            names, verdicts, datetime.now(timezone.utc), force=force)
        # ⚠ THE SECOND SKIP, AND IT IS ABOUT THE BOOK RATHER THAN THE CLOCK. `accounts_to_scan`
        # answers "is this account's data fresh"; this answers "is this account worth fetching at
        # all". A benchmark with one holding and a `_MV` shell with none are re-downloaded four
        # times each, every run, for ever — and they are precisely the books that can never be
        # complete, so the freshness skip never catches them either. `force` re-checks everything.
        from routers._airs_accounts import _holding_counts  # noqa: PLC0415

        bogus: set[str] = set()
        if not force:
            try:
                counts, _, _isin = _holding_counts()
                bogus = bogus_accounts(counts, verdicts)
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
        for i, name in enumerate(names, 1):
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

        # ⚠ THIS JOB DOES NOT TOUCH CRM. It used to also download CRM → Relaties → Alle
        # relaties inline, which is a different report about different objects (relations, not
        # portfolios) and already has its own daily job at 11:00
        # (`airs_crm.run_crm_relaties_refresh_sync`, wired in `scheduler._fire_crm_relaties`).
        # Running it here meant a second scrape of the same export every time anyone refreshed
        # the holdings, and — worse — a CRM failure was appended to THIS job's `errors` and
        # counted in its "N report(s) failed", so a portfolio refresh reported a fault in a
        # report it was never asked to fetch.
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
        counts = count_outcomes(current, known, outcomes)
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
        )
        _STATUS.update({
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "status": "ok" if any_stored else "error",
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
            "message": format_run_message(counts),
            "detail": detail,
        })
        _log.info("[airs_vermogen] %s refresh — %s (%s)",
                  triggered_by, _STATUS["message"], detail)
        return dict(_STATUS)
    finally:
        _STATUS["running"] = False
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

    ⚠ AND THE WALK IS SHARED ACROSS THE RUN, BECAUSE VALUATION IS FLEET-WIDE. AirSPMS values in one
    end-of-day batch, so a date that has no valuation has none for ANY book — yet every account
    re-discovered that from scratch, starting at today. Measured 2026-07-30: today's valuation had
    not run, so all ~25 books with holdings paid one wasted request before landing on the 29th; on
    a Monday it is three (Mon, Sun, Sat) before Friday. `_UNVALUED_DATES` remembers the misses for
    the duration of a run and skips them, which removes 44-130 round trips from a full scan.

    ⚠ ONLY MISSES ARE CACHED, NEVER HITS. A book valued monthly legitimately sits weeks behind a
    daily-valued one, so "this date worked for account A" says nothing about account B and caching
    it would hand B a stale snapshot. A date that returned NOTHING is the only fleet-wide fact here.
    """
    from airs_scanner import download_vermogensoverzicht_sync  # noqa: PLC0415

    last_err: Exception | None = None
    for back in range(0, 7):
        tot = (date.today() - timedelta(days=back)).isoformat()
        if tot in _UNVALUED_DATES:
            continue                      # another account already proved this day has no valuation
        try:
            return tot, download_vermogensoverzicht_sync(name, van, tot)
        except RuntimeError as e:
            # Unvalued date → empty body / error page. Try the day before. A real auth failure
            # returns the same on EVERY date, exhausts the loop, and is raised below.
            _UNVALUED_DATES.add(tot)
            last_err = e
    raise RuntimeError(f"no valued Vermogensoverzicht in the last 7 days ({last_err})")


def refresh_one_portfolio(portefeuille: str) -> dict:
    """Re-scan ONE portfolio's Rendement (ATT) + Vermogensoverzicht (VOLK) and store both — the
    per-row "Refresh" on the overview table.

    Reuses the exact download → parse → save path the full daily scan uses, so a single row's
    refresh and the whole-fleet refresh can never diverge. Serialized against the full scan (and
    other single refreshes) via `_LOCK` — they share ONE AirSPMS session, which must not be driven
    by two threads at once. A few seconds: two downloads (plus a login only if the session lapsed).
    """
    if not _LOCK.acquire(blocking=False):
        return {"status": "busy", "message": "An AIRS refresh is already running", "portefeuille": portefeuille}
    try:
        today = date.today()
        van, tot = f"{today.year}-01-01", today.isoformat()

        # ⚠ THE SAME FUNCTION THE FLEET SCAN CALLS. This used to be a second, near-identical copy
        # of the four downloads — and the two had already drifted (only one recorded which reports
        # arrived). One body, so "refresh this row" and "refresh everything" cannot mean different
        # things.
        res = scan_one(portefeuille, van, tot)
        ok = res["reports_ok"]

        # ⚠ THE PER-ROW REFRESH RECORDS ITS VERDICT TOO — it is how an account short a report gets
        # its badge cleared without waiting for the next full scan. `_MIN_ROSTER` does not apply:
        # this is a deliberate request for one named account, not a discovery whose size might mean
        # the scrape failed.
        _record_reports({portefeuille: ok}, datetime.now(timezone.utc).isoformat())
        return {
            "status": "ok" if ("att" in ok or "volk" in ok) else "error",
            "portefeuille": portefeuille,
            "as_of": res["as_of"] if "volk" in ok else tot,
            "holdings_rows": res["holdings"],
            "mutatie_rows": res["mutaties"],
            "model_weight_rows": res["model_weights"],
            "rendement_stored": "att" in ok,
            "vermogen_stored": "volk" in ok,
            "reports_ok": ok,
            "complete": len(ok) == len(REPORTS),
            # Formatted for the single-row toast; the structured form rides along beside it.
            "errors": [f"{e['report']}: {e['error_type']}: {e['message']}" for e in res["errors"]],
            "error_details": res["errors"],
        }
    finally:
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
