"""One account's AIRS Transacties, cached — the read behind the /portfolios "Transactions" panel.

⚠ CACHED BY DEFAULT, LIVE ONLY WHEN ASKED. A live fetch is one download behind a headless AirSPMS
session and takes seconds; the panel is opened by a click, and a click that costs seconds every
time is a panel nobody opens twice. Same shape as the model portfolios' position cache: read the
stored snapshot, hit AIRS only on `refresh=true`, on a window that differs from the stored one, or
when nothing is stored at all.

⚠ AND THE ANSWER SAYS WHICH IT WAS. `cached_at` is returned and the UI prints it. A cached answer
shown as fresh is how a stale figure gets trusted — the rule this repo already applies to the
model-portfolio positions and to every price mark.

⚠ ZERO ROWS IS AN ANSWER AND IT IS CACHED LIKE ANY OTHER. A book that has not traded has an empty
Transacties report. Storing the empty snapshot is what stops every expand re-asking AIRS for a
report we already know is empty — and it is why "no snapshot stored" and "stored, and empty" are
kept apart in the table rather than both reading as absent.

⚠ THE AIRS SESSION IS SINGLE-THREADED, so a live fetch takes `airs_vermogen._LOCK` — the same lock
the fleet scan and the per-row refresh hold. Two threads driving one headless browser is how a
download arrives as somebody else's report. A contended fetch does NOT queue: it falls back to
whatever is cached and says so, because a request that blocks for the length of a fleet scan is
indistinguishable from a hang.
"""
from __future__ import annotations

import logging
from datetime import date

from deps import supabase

_log = logging.getLogger(__name__)

# ⚠ THE SAME WINDOW THE REST OF THE PANEL IS MEASURED OVER — 1 January to today, exactly what
# `refresh_one_portfolio` and the fleet scan pass for ATT/VOLK/MUT/MODEL. A transactions list on a
# different window from the returns beside it invites a reader to explain one with the other, and
# the arithmetic would not work.
def ytd_window() -> tuple[str, str]:
    today = date.today()
    return f"{today.year}-01-01", today.isoformat()


def _stored(portefeuille: str) -> tuple[dict | None, str | None]:
    """The cached snapshot, and WHY there isn't one when there isn't.

    ⚠ A BROKEN CACHE IS NOT A BROKEN PANEL. This 500'd on the very first click, because the table
    did not exist yet (the migration had not been applied) and an unhandled `APIError` came out as
    `{"detail":"Internal Server Error"}` — 34 bytes that name neither the table nor the cause. The
    cache is an OPTIMISATION: it makes the second open instant. Its absence means one more download,
    not no answer, so a read fault degrades to a live fetch.

    ⚠ AND IT IS LOUD, because the failure that put this comment here is a SETUP one that never
    heals on its own. Swallowed silently, a missing table would look exactly like a cold cache: the
    panel would work, every open would re-download from AIRS, and nobody would ever find out. The
    reason is logged at WARNING (uvicorn's root logger sits there, so `info` is invisible in
    production) and returned so it reaches the screen.
    """
    try:
        rows = (supabase.table("airs_transactie_snapshot")
                .select("portefeuille,datum_van,datum_tot,columns,kinds,rows,fetched_at")
                .eq("portefeuille", portefeuille).limit(1).execute().data or [])
        return (rows[0] if rows else None), None
    except Exception as e:  # noqa: BLE001 — the cache must never be the thing that breaks the read
        _log.warning("[airs_transacties] cache READ failed for %s (%s: %s) — falling back to a "
                     "live fetch. Has migration 20260805000000_airs_transactie_snapshot been "
                     "applied?", portefeuille, type(e).__name__, e)
        return None, f"The transactions cache is unavailable ({type(e).__name__}), so this was fetched live."


def _store(portefeuille: str, van: str, tot: str, sheet) -> str | None:
    """Delete-then-insert, so a transaction that vanished upstream actually disappears.

    ⚠ ONE SNAPSHOT PER ACCOUNT, THE NEWEST. Keeping a history here would mean serving one of
    several windows as though it were current — the same rot-backwards failure the model-portfolio
    position cache refuses by never writing back a historical `datum`.

    ⚠ A FAILED WRITE MUST NOT LOSE THE ROWS WE ALREADY HAVE. The download has happened and the
    answer is in hand; throwing here would spend an AIRS fetch and then return nothing, which is
    the worst of both. Returns the reason instead, for the caller to surface.
    """
    try:
        supabase.table("airs_transactie_snapshot").delete().eq(
            "portefeuille", portefeuille).execute()
        supabase.table("airs_transactie_snapshot").insert({
            "portefeuille": portefeuille,
            "datum_van": van,
            "datum_tot": tot,
            "columns": sheet.columns,
            "kinds": sheet.kinds,
            "rows": sheet.rows,
        }).execute()
        return None
    except Exception as e:  # noqa: BLE001 — see the docstring: the rows are already paid for
        _log.warning("[airs_transacties] cache WRITE failed for %s (%s: %s) — serving the rows "
                     "anyway; the next open will re-download.", portefeuille, type(e).__name__, e)
        return f"Fetched, but could not be cached ({type(e).__name__}) — the next open re-downloads."


def _fetch_live(portefeuille: str, van: str, tot: str):
    """Download + parse ONE account's Transacties. Raises on a fault; `AirsNoData` for "this book
    has no such report", which is an answer and must not be reported as a failure."""
    from airs_scanner import download_transacties_sync  # noqa: PLC0415
    from airs_transacties import parse_transacties  # noqa: PLC0415

    # ⚠ LOGGED AT WARNING WHEN IT ACTUALLY GOES OUT TO AIRS. uvicorn leaves the root logger at
    # WARNING, so an `info` line here is invisible in production — and "why did this take eight
    # seconds" is exactly the question this line answers. The cached path stays silent.
    _log.warning("[airs_transacties] %s: downloading TRANS %s..%s (live)", portefeuille, van, tot)
    return parse_transacties(download_transacties_sync(portefeuille, van, tot))


def account_transactions(portefeuille: str, refresh: bool = False) -> dict:
    """One account's transactions, from the cache unless a fetch is asked for or needed.

    Returns `{portefeuille, datum_van, datum_tot, columns, kinds, rows, cached_at, source, note}`.

    ⚠ `source` IS PART OF THE ANSWER, not diagnostics. `cache` | `live` | `unavailable` — and the
    third is NOT an error state to hide: an account whose report AIRS does not produce, or one we
    could not reach while a scan holds the session, has a real reason and the reader gets it.
    """
    from airs_scanner import AirsNoData  # noqa: PLC0415
    from airs_vermogen import _LOCK  # noqa: PLC0415

    van, tot = ytd_window()
    cached, cache_note = _stored(portefeuille)
    # ⚠ A CACHED SNAPSHOT OF A DIFFERENT WINDOW IS NOT THIS ANSWER. The window rolls forward every
    # day, so yesterday's cache is missing today's trades — served silently, that is a transaction
    # list that is quietly one day short. Same window, or re-fetch.
    fresh_enough = bool(cached) and cached["datum_van"] == van and cached["datum_tot"] == tot
    if fresh_enough and not refresh:
        return {**_shape(cached), "source": "cache", "note": None}

    if not _LOCK.acquire(blocking=False):
        # ⚠ FALL BACK, NEVER BLOCK. A fleet scan runs for minutes; waiting on it would make this
        # request indistinguishable from a hang. Whatever is cached is served, with the reason.
        note = ("An AIRS refresh is running, so this could not be re-fetched"
                + (" — showing the last stored snapshot." if cached else "."))
        return ({**_shape(cached), "source": "cache", "note": note} if cached
                else _empty(portefeuille, van, tot, "unavailable", _join(note, cache_note)))
    try:
        sheet = _fetch_live(portefeuille, van, tot)
    except AirsNoData as e:
        # ⚠ AIRS ANSWERED: THIS BOOK HAS NO SUCH REPORT. Stored as an empty snapshot so the next
        # open is instant — the same reasoning that makes `scan_one` count `no_data` as ok rather
        # than re-scanning the account for ever.
        from airs_transacties import ParsedSheet  # noqa: PLC0415
        write_note = _store(portefeuille, van, tot, ParsedSheet())
        _log.info("[airs_transacties] %s: no Transacties report — %s", portefeuille, e)
        return _empty(portefeuille, van, tot, "live", _join(
            "AIRS has no Transacties report for this book in this period.", write_note))
    except Exception as e:  # noqa: BLE001 — a download fault must not 500 a panel
        # ⚠ NOT STORED. A failure is not an empty report, and caching it as one would turn a
        # transient session problem into a permanent "this book never traded".
        note = f"Could not fetch transactions from AIRS ({type(e).__name__}: {e})"
        _log.warning("[airs_transacties] %s failed: %s: %s", portefeuille, type(e).__name__, e)
        return ({**_shape(cached), "source": "cache", "note": note + " — showing the last stored snapshot."}
                if cached else _empty(portefeuille, van, tot, "unavailable", note))
    finally:
        _LOCK.release()

    write_note = _store(portefeuille, van, tot, sheet)
    return {"portefeuille": portefeuille, "datum_van": van, "datum_tot": tot,
            "columns": sheet.columns, "kinds": sheet.kinds, "rows": sheet.rows,
            "cached_at": None, "source": "live", "note": _join(cache_note, write_note)}


def _join(*notes: str | None) -> str | None:
    """One line out of the reasons there are. ⚠ Two independent things can go wrong at once — the
    cache can be unreachable AND the report absent — and dropping either leaves the reader with
    half an explanation for what they are looking at."""
    parts = [n for n in notes if n]
    return " ".join(parts) if parts else None


def _shape(row: dict) -> dict:
    """A stored snapshot in the response's shape. ONE shaping function for cached and live, so the
    two cannot drift — the same rule the model-portfolio position cache follows."""
    return {"portefeuille": row["portefeuille"],
            "datum_van": row["datum_van"], "datum_tot": row["datum_tot"],
            "columns": row.get("columns") or [], "kinds": row.get("kinds") or {},
            "rows": row.get("rows") or [], "cached_at": row.get("fetched_at")}


def _empty(portefeuille: str, van: str, tot: str, source: str, note: str) -> dict:
    """No rows, and the REASON. ⚠ An empty table with no explanation reads as "this book never
    traded", which is a claim — and for an unreachable report it is a false one."""
    return {"portefeuille": portefeuille, "datum_van": van, "datum_tot": tot,
            "columns": [], "kinds": {}, "rows": [], "cached_at": None,
            "source": source, "note": note}
