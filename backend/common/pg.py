"""Direct-Postgres COPY transport.

PostgREST caps responses at 1000 rows (prod) / 10000 (local), so the default
loaders page over millions of rows — thousands of HTTP round-trips through
Cloudflare, plus per-row JSON. When a direct Postgres connection string is
configured (`SUPABASE_DB_URL` / `DATABASE_URL`), this streams an entire result in
a SINGLE `COPY ... TO STDOUT` query: no row cap, no `IN`-chunking (the whole id
list goes as one `= ANY($)` array), binary wire instead of JSON. Measured at ~12x
on the same query (1,079 ms paged vs 89 ms COPY).

Everything here is strictly opt-in and self-healing: `_run_copy` returns `None`
when the env var is absent, psycopg isn't installed, or ANYTHING goes wrong — the
caller then falls back to its PostgREST path (or raises, if it has none). So a
bad connection degrades to "as before", never to an error.

Lives in `common/` rather than `momentum/` because it is infrastructure, not a
momentum concern: `timeseries` and the ingest phases use it too, and a
`timeseries -> momentum` import would be a dependency cycle. `momentum.data._pg`
re-exports these three names for its existing callers.
"""
from __future__ import annotations

import io
import logging
import os

log = logging.getLogger(__name__)

# One-shot guard so the "COPY path disabled" warning is logged once per process
# rather than on every chunked load.
_warned_no_db_url = False


def _db_url() -> str | None:
    """Direct-Postgres connection string, if configured. `SUPABASE_DB_URL`
    takes precedence; `DATABASE_URL` is accepted as a common alias."""
    return os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")


def copy_path_enabled() -> bool:
    """True when a direct-Postgres connection string is configured."""
    return bool(_db_url())


def _run_copy(sql: str, params: tuple) -> io.BytesIO | None:
    """Execute one `COPY (...) TO STDOUT` over a fresh direct connection and
    return the raw CSV bytes (or `None` to signal fall-back: unconfigured,
    psycopg missing, or any connection/query error)."""
    url = _db_url()
    if not url:
        global _warned_no_db_url
        if not _warned_no_db_url:
            log.warning(
                "[common.pg] SUPABASE_DB_URL/DATABASE_URL is not set — using the slower "
                "PostgREST loader. Large-universe backtests can hit Postgres statement "
                "timeouts (57014) on this path. Set SUPABASE_DB_URL (the Supabase direct / "
                "session-pooler connection string) to enable the fast single-COPY loader."
            )
            _warned_no_db_url = True
        return None
    try:
        import psycopg  # local import so the dependency stays optional
    except ImportError:
        log.warning(
            "[common.pg] SUPABASE_DB_URL is set but psycopg isn't installed; "
            "using the PostgREST loader instead."
        )
        return None
    try:
        buf = io.BytesIO()
        with psycopg.connect(url, connect_timeout=15) as conn:
            with conn.cursor() as cur:
                # Disable the server/role statement timeout for THIS session — a
                # bulk metric COPY over thousands of companies runs for many
                # seconds and must not be cancelled (57014). Set via an explicit
                # query (not a libpq `options=` startup param) because Supavisor
                # — the Supabase pooler — can reject startup options.
                cur.execute("SET statement_timeout = 0")
                with cur.copy(sql, params) as copy:
                    while (block := copy.read()):
                        buf.write(block)
        buf.seek(0)
        return buf
    except Exception as e:  # noqa: BLE001 — any failure → fall back, never raise
        log.warning(
            "[common.pg] direct COPY failed (%s: %s); falling back to PostgREST.",
            type(e).__name__, e,
        )
        return None
