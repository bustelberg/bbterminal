"""Bounded PostgREST readiness gate; never retries already-started domain work."""
import logging
import os

_log = logging.getLogger('scheduler')
_DB_WARMUP_ATTEMPTS = int(os.environ.get('DB_WARMUP_ATTEMPTS', '6'))
_DB_WARMUP_DELAY_SECONDS = float(os.environ.get('DB_WARMUP_DELAY_SECONDS', '5'))

def _is_db_warming(exc: BaseException) -> bool:
    """Is PostgREST still BOOTING (wait for it), or did the query genuinely fail (report it)?

    Matched on the PostgREST error CODE, never on its prose: PGRST002 = the schema cache is not
    loaded yet, PGRST001 = it cannot reach the database at all. Both are states a booting stack
    passes THROUGH. Every other code is a real answer about our query and must surface — a
    substring match on "Retrying." would swallow those too.
    """
    code = str(getattr(exc, "code", "") or "")
    if code in {"PGRST001", "PGRST002"}:
        return True
    # postgrest's APIError carries the raw dict in `str(exc)`; keep a fallback in case the
    # attribute moves, but stay anchored on the codes.
    text = str(exc)
    return "PGRST001" in text or "PGRST002" in text


def _await_db_ready(what: str) -> bool:
    """Block (on a daemon thread) until PostgREST serves a trivial read, or give up saying so.

     THIS IS A GATE, NOT A RETRY WRAPPER AROUND THE WORK. It spends one tiny query per attempt;
    wrapping the job itself would re-spend whatever the job had already done (the asset-price
    refresh costs ~1.5s of Yahoo per instrument). Once the gate opens, the job runs exactly once
    and any later error is a real one.

    Returns True when the database answered, False when it never did — the caller then logs one
    clear line instead of a traceback that reads like a bug in the job.
    """
    import time  # noqa: PLC0415

    from deps import supabase  # noqa: PLC0415

    for attempt in range(1, _DB_WARMUP_ATTEMPTS + 1):
        try:
            supabase.table("asset_analysis").select("analysis_id").limit(1).execute()
            return True
        except Exception as e:  # noqa: BLE001
            if not _is_db_warming(e):
                return True  # a real error — let the job hit it and report it properly
            if attempt == _DB_WARMUP_ATTEMPTS:
                _log.warning(
                    "[scheduler] %s: the database was still warming up after %s attempt(s) "
                    "(%s) — standing down; the daily tick is the fallback.",
                    what, _DB_WARMUP_ATTEMPTS, e,
                )
                return False
            _log.info(
                "[scheduler] %s: database still warming up (%s), retrying in %ss [%s/%s]",
                what, getattr(e, "code", type(e).__name__),
                _DB_WARMUP_DELAY_SECONDS, attempt, _DB_WARMUP_ATTEMPTS,
            )
            time.sleep(_DB_WARMUP_DELAY_SECONDS)
    return False


