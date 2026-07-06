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

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from deps import supabase

from . import openfigi, store
from .resolve import resolve, same_company
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
    """Queue counts by status + whether there's outstanding work."""
    def _count(st: str) -> int:
        return supabase.table("asset_ingest_queue").select("isin", count="exact").eq("status", st).limit(1).execute().count or 0
    pending, done, failed = _count("pending"), _count("done"), _count("failed")
    return {"pending": pending, "done": done, "failed": failed, "total": pending + done + failed,
            "working": pending > 0}


def _mark(isin: str, st: str, reason: str | None = None) -> None:
    supabase.table("asset_ingest_queue").update(
        {"status": st, "reason": reason, "updated_at": _now()}
    ).eq("isin", isin).execute()


def process_slice(limit: int = SLICE, verbose: bool = False) -> dict:
    """Process up to `limit` pending ISINs through the throttled resolve+store,
    marking each done/failed. THE worker step (one Yahoo consumer). On a Yahoo
    ban (`YahooThrottled`) it leaves the current ISIN pending and stops — the
    next tick retries once Yahoo recovers. `verbose` prints each ISIN's OpenFIGI
    + yfinance result to stdout. Returns per-tick counts."""
    pend = (
        supabase.table("asset_ingest_queue").select("isin")
        .eq("status", "pending").order("added_at").limit(limit).execute().data
    ) or []
    if not pend:
        return {"processed": 0, "ok": 0, "failed": 0, "remaining": 0}
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
                return "unmapped"
            ids_ = store.upsert_asset(res, figi=fig)
            rows = store.store_series(ids_["analysis_id"], an["symbol"], an.get("first_ts"))
            _mark(isin, "done")
            _vp(f"  ok   {isin}  [{of}]  ->  yfinance {an['symbol']} "
                f"({an.get('exchange') or '—'}, {an.get('currency') or '—'}) · "
                f"{rows:,} bars since {an.get('first_date') or '?'}")
            return "ok"
        except YahooThrottled:
            banned.set()  # stop the rest of the slice; this ISIN stays pending
            _vp(f"  ... {isin}  Yahoo rate-limited — pausing the slice, will resume")
            return "throttled"
        except Exception as e:  # noqa: BLE001
            try:
                store.upsert_unmapped(isin, "error", f"{type(e).__name__}: {e}", figi=fig)
            except Exception:  # noqa: BLE001
                pass
            _mark(isin, "failed", f"{type(e).__name__}: {e}")
            _vp(f"  ERR  {isin}  [{of}]  ->  {type(e).__name__}: {e}")
            return "failed"

    with ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(_one, ids))
    ok = results.count("ok")
    failed = results.count("failed")
    try:
        store.set_default_executions()
    except Exception:  # noqa: BLE001
        pass
    remaining = supabase.table("asset_ingest_queue").select("isin", count="exact").eq("status", "pending").limit(1).execute().count or 0
    return {"processed": ok + failed, "ok": ok, "failed": failed, "remaining": remaining}


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
            supabase.table("asset_execution").select("isin, openfigi_figi, openfigi_type")
            .in_("status", ["not_found", "error"]).range(off, off + 999).execute().data
        ) or []
        rows += r
        if len(r) < 1000:
            break
        off += 1000
    isins = [
        r["isin"] for r in rows
        if r.get("openfigi_figi") and (r.get("openfigi_type") or "") not in _UNPRICEABLE_TYPES
    ]
    res = enqueue(isins, skip_existing=False)
    return {"unmapped": len(rows), "retryable": len(isins), **res}


def requeue_suspects() -> dict:
    """Re-queue the wrong-company mis-mapped rows (stored analysis name is a
    DIFFERENT company than the ISIN's OpenFIGI name) for a clean worker pass —
    fixes the throttle-corrupted re-resolutions without a second competing Yahoo
    process. `skip_existing=False` so these already-ok rows actually re-process."""
    rows: list[dict] = []
    off = 0
    while True:
        r = (
            supabase.table("asset_grid").select("isin, name, openfigi_name")
            .eq("status", "ok").range(off, off + 999).execute().data
        ) or []
        rows += r
        if len(r) < 1000:
            break
        off += 1000
    suspects = [
        r["isin"] for r in rows
        if r.get("openfigi_name") and not same_company(r.get("name"), r.get("openfigi_name"))
    ]
    res = enqueue(suspects, skip_existing=False)
    return {"suspects": len(suspects), **res}
