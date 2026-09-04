"""Async ingest queue for the asset pipeline.

Uploading a CSV writes ISINs to `asset_ingest_queue` as `pending` (instant, no
Yahoo). A SINGLE in-process background worker (`process_slice`, driven by an
APScheduler tick) is the ONLY Yahoo/OpenFIGI consumer — it drains the queue
through the throttled resolver + store, so nothing competes for the Yahoo
throttle and resolutions never run on throttle-degraded data (which is what
corrupted the concurrent re-resolve).

Everything here is best-effort + idempotent: an ISIN is marked `done`/`failed`
only after its resolve+store completes, so a restart resumes cleanly from
`pending`."""
from __future__ import annotations

from collections.abc import Callable

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from deps import IN_CHUNK_SIZE, supabase

from . import openfigi, store
from .resolve import resolve
from .yahoo import YahooThrottled

log = logging.getLogger(__name__)

_CHUNK = 500
# Max ISINs per worker tick — bounds each tick's runtime; the next tick continues
# where this left off (each ISIN is marked done as it completes).
SLICE = 40


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_stored_ok() -> set[str]:
    """All ISINs already ingested with status='ok' — loaded once, small + bounded
    (the set-membership skip check, cheaper than per-ISIN lookups)."""
    stored: set[str] = set()
    off = 0
    while True:
        r = supabase.table("asset_execution").select("isin").eq("status", "ok").range(off, off + 999).execute()
        batch = r.data or []
        stored.update(x["isin"] for x in batch)
        if len(batch) < 1000:
            break
        off += 1000
    return stored


def enqueue(identifiers: list[str], skip_existing: bool = True) -> dict:
    """Dedupe + upsert ISINs as `pending` (fast — no Yahoo). Skips ones already
    ingested ok unless `skip_existing=False` (used to re-queue mis-mapped rows).
    Returns counts so the UI can report the split immediately."""
    ids = sorted({x.strip().upper() for x in identifiers if x and x.strip()})
    skipped = 0
    if skip_existing and ids:
        stored = _load_stored_ok()
        keep = [x for x in ids if x not in stored]
        skipped = len(ids) - len(keep)
        ids = keep
    now = _now()
    rows = [{"isin": x, "status": "pending", "reason": None, "updated_at": now} for x in ids]
    for i in range(0, len(rows), _CHUNK):
        supabase.table("asset_ingest_queue").upsert(rows[i:i + _CHUNK], on_conflict="isin").execute()
    return {"queued": len(ids), "skipped_existing": skipped, "input": len(ids) + skipped}


def status() -> dict:
    """Queue counts by status + whether there's outstanding work.

    ⚠ `working` means "there is work OUTSTANDING", NOT "a worker is running". A backlog nobody is
    draining reports `working: True` for ever — this queue sat at 9,945 pending, untouched since
    2026-07-07, and still called itself working. To ask whether a worker is ALIVE (e.g. before
    adding Yahoo load of your own), use `is_worker_active()`, which reads the heartbeat.
    """
    def _count(st: str) -> int:
        return supabase.table("asset_ingest_queue").select("isin", count="exact").eq("status", st).limit(1).execute().count or 0
    pending, done, failed = _count("pending"), _count("done"), _count("failed")
    return {"pending": pending, "done": done, "failed": failed, "total": pending + done + failed,
            "working": pending > 0}


def last_activity() -> str | None:
    """When the worker last MOVED a row out of `pending` — the queue's only heartbeat.

    A worker stamps `updated_at` as it marks each ISIN done/failed, so the newest of those IS the
    last moment anything was actually being resolved. `pending` cannot tell you this: it counts
    what is LEFT, which stays high precisely when nobody is working.
    """
    r = (supabase.table("asset_ingest_queue").select("updated_at")
         .in_("status", ["done", "failed"])
         .order("updated_at", desc=True).limit(1).execute().data or [])
    return r[0]["updated_at"] if r else None


def is_worker_active(within_minutes: int = 10) -> bool:
    """Is something draining this queue RIGHT NOW — i.e. consuming Yahoo?

    THE QUESTION THAT MATTERS TO ANY OTHER YAHOO CALLER. The ingest queue is *the* single Yahoo
    consumer by design: Yahoo answers an overloaded caller with an EMPTY result rather than a 429,
    and an empty candidate set is how a resolution silently lands on a thin foreign listing
    (NVDA-on-Stuttgart, Alphabet-on-Vienna). So a second consumer must stand down while the
    resolver is mid-search.

    But it must stand down for the WORKER, not for the BACKLOG. Gating on `pending > 0` reads a
    week-old abandoned queue as "busy" and disables the other job for ever — which is exactly what
    happened the first time the price refresh tried it (9,945 pending, last touched seven days
    earlier, and the refresh skipped every tick).
    """
    from datetime import datetime, timedelta, timezone  # noqa: PLC0415

    seen = last_activity()
    if not seen:
        return False
    try:
        ts = datetime.fromisoformat(seen.replace("Z", "+00:00"))
    except ValueError:
        return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts > datetime.now(timezone.utc) - timedelta(minutes=within_minutes)


def _mark(isin: str, st: str, reason: str | None = None) -> None:
    supabase.table("asset_ingest_queue").update(
        {"status": st, "reason": reason, "updated_at": _now()}
    ).eq("isin", isin).execute()


def _reapply_symbol_overrides() -> None:
    """Put every `asset_symbol_override` back after a resolution slice."""
    from .symbol_override import apply_symbol_overrides  # noqa: PLC0415
    n = apply_symbol_overrides()
    if n:
        log.warning("[queue] re-applied %d symbol override(s) this slice overwrote", n)


def _reapply_aliases() -> None:
    """Put every `asset_isin_alias` back after a resolution slice."""
    from .isin_alias import apply_aliases  # noqa: PLC0415
    n = apply_aliases()
    if n:
        log.warning("[queue] re-applied %d ISIN alias(es) this slice overwrote", n)


def process_slice(limit: int = SLICE, verbose: bool = False,
                  isins: list[str] | None = None,
                  on_each: Callable[[str, str], None] | None = None) -> dict:
    """Process up to `limit` pending ISINs through the throttled resolve+store,
    marking each done/failed. THE worker step (one Yahoo consumer). On a Yahoo
    ban (`YahooThrottled`) it leaves the current ISIN pending and stops — the
    next tick retries once Yahoo recovers. `verbose` prints each ISIN's OpenFIGI
    + yfinance result to stdout. Returns per-tick counts.

    ⚠ `isins` PICKS THE SLICE BY IDENTITY INSTEAD OF BY AGE, AND WITHOUT IT AN INTERACTIVE CALLER
    CANNOT REACH ITS OWN WORK. The default order is `added_at` — right for a background worker
    chewing through a backlog, and useless for "resolve THIS benchmark now": the queue holds ~10,000
    pending ISINs, so a benchmark's 71 constituents enqueued a second ago are ~10,000 places from
    the front. Measured 2026-07-30: Fill enqueued 71 ACWI names, drained a slice, and resolved 25
    unrelated week-old rows instead. Same worker step either way — only the selection differs.
    """
    if isins:
        want, pend = [i.strip().upper() for i in isins if i], []
        for i in range(0, len(want), IN_CHUNK_SIZE):
            if len(pend) >= limit:
                break
            pend += (supabase.table("asset_ingest_queue").select("isin")
                     .eq("status", "pending").in_("isin", want[i:i + IN_CHUNK_SIZE])
                     .limit(limit - len(pend)).execute().data or [])
    else:
        pend = (
            supabase.table("asset_ingest_queue").select("isin")
            .eq("status", "pending").order("added_at").limit(limit).execute().data
        ) or []
    if not pend:
        return {"processed": 0, "ok": 0, "failed": 0, "unmapped": 0, "remaining": 0}
    ids = [r["isin"] for r in pend]
    try:
        figi_map = openfigi.lookup_isins(ids)
    except Exception:  # noqa: BLE001
        figi_map = {}

    # Process the slice CONCURRENTLY — the shared Yahoo throttle paces + caps the
    # aggregate request rate, so N in-flight requests overlap the network latency.
    # `banned` short-circuits the rest of the slice once Yahoo hard-bans us (the
    # throttle raised YahooThrottled after its cooldowns); those stay pending and
    # retry next tick. `plock` keeps verbose lines from interleaving.
    workers = max(1, int(os.environ.get("YAHOO_CONCURRENCY", "4")))
    banned = threading.Event()
    plock = threading.Lock()

    def _vp(msg: str) -> None:
        if verbose:
            with plock:
                print(msg, flush=True)

    def _report(isin: str, outcome: str, detail: str = "") -> None:
        """Tell the CALLER, per ISIN, as it happens.

        ⚠⚠ WITHOUT THIS A SLICE IS SILENT FOR MINUTES. The refresh emitted one line — "resolving 16
        unmapped ISIN(s)…" — and then nothing until all sixteen were done. Each ISIN is a paced
        Yahoo resolve (search + quote + profile, 10-30s of timeouts each in the worst case) run
        `YAHOO_CONCURRENCY`-wide, so a slice legitimately takes minutes; with no line in between it
        is indistinguishable from a hang, and the operator's only move is to give up on a job that
        was working. Reported as "this one seems to be just stuck".

        ⚠ IT MUST NOT THROW. It runs inside a worker thread mid-slice; an exception here would fail
        an ISIN that actually resolved. Swallowed, like every other reporting hook in this repo.
        """
        if not on_each:
            return
        try:
            with plock:
                on_each(isin, f"{outcome}{f' — {detail}' if detail else ''}")
        except Exception:  # noqa: BLE001 — narration must never fail the work it narrates
            pass

    def _one(isin: str) -> str:
        if banned.is_set():
            return "throttled"  # leave pending — Yahoo is banned
        fig = openfigi.extract_columns(figi_map.get(isin.strip().upper(), []))
        of = f"OpenFIGI {fig.get('openfigi_figi') or '—'} {fig.get('openfigi_name') or '(none)'}"
        try:
            res = resolve(isin, with_candles=False, figi_hint=fig)
            an = res.get("analysis") or {}
            if not an.get("symbol"):
                ac = res.get("asset_class")
                db_status = "bond" if ac == "bond" else "not_found"
                store.upsert_unmapped(isin, db_status, res.get("reason"), ac, res.get("sector"), figi=fig)
                _mark(isin, "done", res.get("reason"))
                _vp(f"  skip {isin}  [{of}]  ->  {db_status}: {(res.get('reason') or '')[:70]}")
                _report(isin, db_status, (res.get("reason") or "")[:60])
                return "unmapped"
            ids_ = store.upsert_asset(res, figi=fig)
            rows = store.store_series(ids_["analysis_id"], an["symbol"], an.get("first_ts"))
            _mark(isin, "done")
            _vp(f"  ok   {isin}  [{of}]  ->  yfinance {an['symbol']} "
                f"({an.get('exchange') or '—'}, {an.get('currency') or '—'}) · "
                f"{rows:,} bars since {an.get('first_date') or '?'}")
            _report(isin, "ok", f"{an['symbol']} · {rows:,} bars")
            return "ok"
        except YahooThrottled:
            banned.set()  # stop the rest of the slice; this ISIN stays pending
            _vp(f"  ... {isin}  Yahoo rate-limited — pausing the slice, will resume")
            _report(isin, "Yahoo rate-limited", "the rest of the slice stays pending")
            return "throttled"
        except Exception as e:  # noqa: BLE001
            try:
                store.upsert_unmapped(isin, "error", f"{type(e).__name__}: {e}", figi=fig)
            except Exception:  # noqa: BLE001
                pass
            _mark(isin, "failed", f"{type(e).__name__}: {e}")
            _vp(f"  ERR  {isin}  [{of}]  ->  {type(e).__name__}: {e}")
            _report(isin, "failed", f"{type(e).__name__}: {e}"[:60])
            return "failed"

    with ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(_one, ids))
    ok = results.count("ok")
    failed = results.count("failed")
    # ⚠ `unmapped` IS AN OUTCOME, NOT A GAP IN THE TALLY. OpenFIGI identified the security and
    # Yahoo has no daily series for it (a bond, a structured product, a delisted line) — the row is
    # marked done and will never be retried, which is correct and is NOT a failure. It was counted
    # in neither `ok` nor `failed`, so a slice that resolved 25 such ISINs reported `processed: 0`
    # and read exactly like a slice that did nothing at all.
    unmapped = results.count("unmapped")
    try:
        store.set_default_executions()
    except Exception:  # noqa: BLE001
        pass
    # ⚠ MANUAL OVERRIDES GO BACK ON LAST, OR THIS SLICE JUST UNDID THEM. A resolution writes
    # `asset_execution` per ISIN and picks the listing BY NAME, which is exactly how a wrong share
    # class gets chosen (iShares Global Corp Bond "EUR" hedged vs "USD (Dist)" — three characters
    # apart, different currency exposure). Both are no-ops when nothing drifted: they compare the
    # stored row first and only then reach for the network.
    for _fn, _what in ((_reapply_symbol_overrides, "symbol override"),
                       (_reapply_aliases, "ISIN alias")):
        try:
            _fn()
        except Exception as e:  # noqa: BLE001 — never fail a completed slice on a re-assert
            log.warning("[queue] re-applying %ss failed: %s: %s", _what, type(e).__name__, e)
    remaining = supabase.table("asset_ingest_queue").select("isin", count="exact").eq("status", "pending").limit(1).execute().count or 0
    return {"processed": ok + failed + unmapped, "ok": ok, "failed": failed,
            "unmapped": unmapped, "remaining": remaining}


# OpenFIGI securityTypes that have no Yahoo daily price series — don't bother
# re-trying these (individual bonds/gilts, rights, warrants).
_UNPRICEABLE_TYPES = {"Bond", "Right", "Warrant", "Bill", "Note", "Debenture"}


def requeue_unmapped() -> dict:
    """Re-queue unmapped rows (status not_found/error) that OpenFIGI DID identify
    as a real, likely-priceable security — a re-try. Many were only not_found
    because Yahoo was throttled while the old batch processed them; the single
    clean worker resolves them (e.g. Sealed Air, Amicus). Skips clearly-
    unpriceable OpenFIGI types (bonds, rights, warrants) — those stay unmapped."""
    rows: list[dict] = []
    off = 0
    while True:
        r = (
            supabase.table("asset_execution").select("isin, openfigi_figi, openfigi_type, reason")
            .in_("status", ["not_found", "error"]).range(off, off + 999).execute().data
        ) or []
        rows += r
        if len(r) < 1000:
            break
        off += 1000
    isins = [
        r["isin"] for r in rows
        if r.get("openfigi_figi") and (r.get("openfigi_type") or "") not in _UNPRICEABLE_TYPES
        # ⚠⚠ A DELIBERATE UNMAP IS NOT A THROTTLE CASUALTY — see `store.MANUAL_UNMAP_PREFIX`. This
        # function's premise is that a `not_found` row failed only because Yahoo was busy, which is
        # true of almost all of them and false of exactly the ones somebody unmapped because the
        # resolver had them on a different instrument. Re-queueing those runs the same resolver
        # over the same candidates and can restore the same wrong ticker.
        and not (r.get("reason") or "").startswith(store.MANUAL_UNMAP_PREFIX)
    ]
    res = enqueue(isins, skip_existing=False)
    return {"unmapped": len(rows), "retryable": len(isins), **res}


def requeue_suspects(only: list[str] | None = None, apply: bool = False) -> dict:
    """The wrong-company mis-mapped rows — LISTED by default, re-queued only when asked.

    ⚠⚠ IT USED TO RE-QUEUE ALL OF THEM, UNASKED, AND THAT IS THE DESTRUCTIVE RE-RESOLVE THIS
    PIPELINE IS BUILT AROUND AVOIDING. `same_company` is the right test for an operating company
    and it is WRONG far more often than it is right on this population: measured 2026-09-04 on the
    live grid, 110 rows fail it and only ~15 are genuinely the wrong company. The other ~95 are
    OpenFIGI's own spelling — `MUENCHENER RUECKVER AG-REG` for Münchener Rückversicherungs-
    Gesellschaft, `IND & COMM BK OF CHINA-H` for ICBC, `SAMSUNG ELECTRO-REGS GDR PFD`, `DHL GROUP`
    for Deutsche Post (a rename), `VANG FTSE JPN USDA` for a Vanguard ETF. Re-resolving those is
    the Alphabet-to-Vienna failure waiting to happen: Yahoo answers an overloaded caller with an
    EMPTY list, so a re-resolution of a CORRECT row can only move it to a thinner listing.

    ⚠ AND NO THRESHOLD RESCUES IT — three rules were scored against the 15 hand-checked errors:
    the type allowlist alone catches all 15 but would re-resolve 38 correct rows; type AND a
    country mismatch cuts that to 11 false positives but misses 3 real ones (including the Abu
    Dhabi bank that started this); "a bare US ticker for a non-US ISIN" is structural and clean but
    catches only 4 of 15, because most of these resolved onto a non-US venue. There is no safe
    automatic gate in the data on the row, so the decision stays with a person and this function's
    job is to make it a SHORT list rather than to act on a long one.

    ⚠ THE VERDICT IS READ, NOT RE-DERIVED. `identity_status` is stamped at resolve time by
    `resolve.identity_status`, which is this same `same_company` call; computing it again here was
    a second copy of the detector, free to drift from the badge the grid shows.

    `only` re-queues exactly the named ISINs (the reviewed ones). `apply=True` with no `only`
    re-queues every suspect — the old behaviour, now something a caller has to ask for by name.
    """
    rows: list[dict] = []
    off = 0
    while True:
        r = (
            supabase.table("asset_execution")
            .select("isin, name, openfigi_name, openfigi_type, yahoo_symbol, med_adv_eur")
            .eq("status", "ok").eq("identity_status", "mismatch")
            .range(off, off + 999).execute().data
        ) or []
        rows += r
        if len(r) < 1000:
            break
        off += 1000

    if only is not None:
        wanted = {x.strip().upper() for x in only}
        suspects = [r["isin"] for r in rows if r["isin"] in wanted]
        unknown = sorted(wanted - {r["isin"] for r in rows})
    else:
        suspects = [r["isin"] for r in rows]
        unknown = []

    if not apply and only is None:
        # ⚠ REPORT, NOT ACT. Returning the rows lets the caller print them; `queued` is 0 and says
        # so, rather than a dry run that looks like it did something.
        return {"suspects": len(suspects), "rows": rows, "queued": 0, "skipped": 0,
                "applied": False, "unknown": unknown}
    res = enqueue(suspects, skip_existing=False)
    return {"suspects": len(suspects), "rows": rows, "applied": True, "unknown": unknown, **res}
