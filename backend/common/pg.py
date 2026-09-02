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

import csv
import io
import json
import logging
import os
import re
import threading
from contextlib import contextmanager
from contextvars import ContextVar

log = logging.getLogger(__name__)

# Only ever matched against module-level literals — see `load_rows_via_copy`.
_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# ---------------------------------------------------------------------------
# Per-request connection reuse
#
# ⚠⚠ A FRESH CONNECTION PER COPY COSTS 220ms IN PRODUCTION AND 24ms LOCALLY — SO A LOCAL PROFILE
# CANNOT SEE THIS AT ALL. `_run_copy_uncached` used to `psycopg.connect()` every time. Measured
# 2026-08-11 from this machine:
#
#     connect + SET statement_timeout + SELECT 1     local 24.0ms     production 220.7ms
#
# The Analyse modal issues 17 COPYs, so it spent **~3.75s in production purely opening
# connections** — TCP, then TLS, then Supavisor's auth — against 0.41s locally. That is not a
# tuning detail, it was the single largest remaining cost on the page, and every profile taken on
# a laptop reported it as 4% of the time.
#
# So a connection is opened once per REQUEST and reused. Scope comes from `read_cache`, which is
# already exactly "one request" and is already entered by every endpoint that does bulk reads.
#
# ⚠ KEYED PER THREAD, NOT PER CONTEXT. A ContextVar is COPIED into a worker thread by
# `asyncio.to_thread`, so several workers can share one context — and a psycopg connection is NOT
# thread-safe. Sharing one across threads would interleave two COPY streams on a single socket,
# which does not raise, it returns the wrong bytes. The scope holds a dict keyed by thread id, so
# concurrent workers get their own connection and still avoid reconnecting per COPY.
#
# ⚠ A BROKEN CONNECTION MUST NOT POISON THE REST OF THE REQUEST. Anything that fails on a reused
# connection drops it from the scope so the next COPY opens a fresh one; the caller still gets the
# normal PostgREST fallback for that one query.
_CONN_SCOPE: ContextVar[dict | None] = ContextVar("pg_conn_scope", default=None)


@contextmanager
def copy_connection_scope():
    """Reuse one direct-Postgres connection per thread for the duration of this block.

    Entered by `read_cache`, so callers get it for free. Nests by doing nothing (an inner block
    keeps the outer scope) — closing on the inner exit would shut the connection the outer block
    is still using.
    """
    if _CONN_SCOPE.get() is not None:
        yield
        return
    scope: dict = {}
    token = _CONN_SCOPE.set(scope)
    try:
        yield
    finally:
        _CONN_SCOPE.reset(token)
        for conn in list(scope.values()):
            try:
                conn.close()
            except Exception:  # noqa: BLE001 — closing must never raise into the caller
                pass
        scope.clear()


def _scoped_connection(url: str):
    """The live connection for this thread, or None when no scope is open."""
    scope = _CONN_SCOPE.get()
    if scope is None:
        return None
    key = threading.get_ident()
    conn = scope.get(key)
    if conn is not None and getattr(conn, "closed", False):
        conn = None
        scope.pop(key, None)
    if conn is None:
        import psycopg  # noqa: PLC0415 — optional dependency, same as below

        conn = psycopg.connect(url, connect_timeout=15)
        with conn.cursor() as cur:
            # Once per connection rather than once per COPY (see `_run_copy_uncached`).
            cur.execute("SET statement_timeout = 0")
        conn.commit()
        scope[key] = conn
    return conn


def load_rows_via_copy(table: str, columns: str, key_col: str, values: list,
                       *, where: dict | None = None) -> list[dict] | None:
    """`SELECT columns FROM table WHERE key_col = ANY(values)` as dicts, in ONE COPY.

    Returns `None` to mean "fall back to PostgREST" — unconfigured, psycopg missing, or any
    error. Never raises, never returns a partial list.

    ⚠ WHY THIS EXISTS: PostgREST encodes an IN-clause into the URL, so a long id list must be
    chunked (`IN_CHUNK_SIZE` = 200) and each chunk is a round trip. `asset_grid` alone costs 11 of
    them on one Analyse open — 502 analysis ids in three chunks plus eight ISIN batches — and in
    production a round trip is ~50ms. A COPY has no URL, so the whole list goes in one.

    ⚠⚠ THE ROWS ARE SHIPPED AS JSON, NOT AS CSV COLUMNS, AND THAT IS NOT A STYLE CHOICE. The other
    COPY loaders in this codebase parse with `line.split(",")`, which is safe only because they
    select numbers and dates. These selects include `name`, `gf_company_name`, `openfigi_name`,
    `leonteq_name` — and **1,948 rows in `asset_grid` have a comma in `name`** ("Alphabet, Inc.").
    Splitting on commas would silently shift every field after the name, producing rows that parse
    but describe the wrong instrument. `row_to_json` also preserves TYPES (a numeric stays a
    number, NULL stays None) and distinguishes NULL from the empty string, which bare CSV cannot —
    so the dicts match what PostgREST returns field for field, rather than approximately.

    ⚠ `table`, `columns` and `key_col` are INTERPOLATED and must never come from a request. Every
    caller passes a module-level literal; `values` is the only parameterised part.
    """
    if not _db_url() or not values:
        return None
    if not _SAFE_IDENT.match(table) or not _SAFE_IDENT.match(key_col):
        log.warning("[common.pg] refusing a COPY with a non-identifier table/key: %r/%r",
                    table, key_col)
        return None
    # ⚠ `where` EXISTS BECAUSE DROPPING A SERVER-SIDE FILTER AND RE-APPLYING IT IN PYTHON IS A
    # SILENT DATA REGRESSION — right answer, far more bytes. Measured on `airs_holding`: filtering
    # on `portefeuille` alone and picking the snapshot afterwards fetched **788 rows instead of
    # 42**, an 18.8x over-fetch, because the table keeps 28 historical snapshots per book. Extra
    # equality predicates belong in the query.
    clauses = [f"{key_col} = ANY(%s)"]
    params: list = [list(values)]
    for col, val in (where or {}).items():
        if not _SAFE_IDENT.match(col):
            log.warning("[common.pg] refusing a COPY with a non-identifier filter: %r", col)
            return None
        clauses.append(f"{col} = %s")
        params.append(val)
    sql = (f"COPY (SELECT row_to_json(t)::text FROM "
           f"(SELECT {columns} FROM {table} WHERE {' AND '.join(clauses)}) t) "
           f"TO STDOUT WITH (FORMAT csv)")
    buf = _run_copy(sql, tuple(params))
    return None if buf is None else _rows_from_copy(buf)


def _rows_from_copy(buf: io.BytesIO) -> list[dict]:
    """One `row_to_json`-per-line COPY buffer -> dicts. ⚠ ONE PARSER, shared by every JSON COPY
    loader here, because the reason it is `csv.reader` and not `line.split(',')` is a measured bug
    (1,948 `asset_grid` names contain a comma) and a second copy of the loop is where that lesson
    gets quietly re-broken."""
    out: list[dict] = []
    # csv.reader unquotes the one JSON column, so embedded commas, quotes and newlines survive.
    for row in csv.reader(io.TextIOWrapper(buf, encoding="utf-8", newline="")):
        if row and row[0]:
            out.append(json.loads(row[0]))
    return out


def load_table_via_copy(table: str, columns: str = "*",
                        *, order_by: str | None = None) -> list[dict] | None:
    """The WHOLE relation as dicts, in ONE statement over the direct connection.

    Returns `None` to mean "fall back to PostgREST" — unconfigured, psycopg missing, or any error.
    Never raises, never returns a partial list. Same JSON transport, and the same interpolation
    rule, as `load_rows_via_copy`: `table`, `columns` and `order_by` must never come from a request.

    ⚠⚠ WHY THIS EXISTS, AND IT IS NOT ONLY SPEED. Offset-paging a wide view through PostgREST costs
    one statement PER PAGE and each one re-materializes the whole view before discarding the rows
    ahead of the offset — against the 8s `statement_timeout` on the `authenticator` role, which
    `service_role` inherits. That is what took `/asset-pipeline` down: `asset_grid` slowed to 10.3s
    (a correlated LATERAL over a view — see migration `20260817010000`), every page from offset
    ~8,000 on returned **57014**, and the grid never loaded. Measured after the view fix, same 16,613
    rows: **17 PostgREST pages 12.68s · one COPY 0.80s**.

    So the pager is not merely slower, it is the fragile shape: its per-page budget is a fraction of
    the whole read's, so the next column somebody adds to the view breaks the endpoint again. One
    statement also reads ONE MVCC snapshot, which is strictly stronger than the pager's
    order-by-append-only-PK trick for staying consistent while a batch inserts underneath it.
    """
    if not _db_url():
        return None
    if not _SAFE_IDENT.match(table):
        log.warning("[common.pg] refusing a COPY with a non-identifier table: %r", table)
        return None
    if order_by is not None and not _SAFE_IDENT.match(order_by):
        log.warning("[common.pg] refusing a COPY with a non-identifier order: %r", order_by)
        return None
    order = f" ORDER BY {order_by}" if order_by else ""
    sql = (f"COPY (SELECT row_to_json(t)::text FROM "
           f"(SELECT {columns} FROM {table}{order}) t) TO STDOUT WITH (FORMAT csv)")
    buf = _run_copy(sql, ())
    return None if buf is None else _rows_from_copy(buf)


def load_distinct_via_copy(table: str, column: str) -> list | None:
    """The DISTINCT non-null values of ONE column, in ONE statement.

    Returns `None` to mean "fall back to PostgREST" — unconfigured, psycopg missing, or any error.
    Same interpolation rule as its siblings: `table` and `column` must never come from a request.

    ⚠⚠ WHY THIS EXISTS: A DROPDOWN'S OPTION LIST IS AN AGGREGATE, AND PULLING THE ROWS TO BUILD IT
    IN PYTHON IS BOTH SLOWER AND WRONG. `/api/companies/field-options` built its sector list as
    `universe_membership.select("sector").limit(10000)` and then `{r["sector"] for r in rows}` —
    8,444 rows over the wire to produce 43 strings.

    ⚠⚠ AND `.limit()` IS NOT WHAT DECIDES HOW MANY ROWS COME BACK — PostgREST's `db-max-rows` is,
    and it is **1,000 on the cloud project** against 10,000 locally (`project_postgrest_max_rows_trap`).
    So production was deriving that list from the first 1,000 rows and shipping **40 of the 43
    sectors**, with no empty cell and no error anywhere: three filter options simply did not exist,
    and WHICH three depended on physical row order, so a VACUUM could change the answer. The local
    dataset returned all 43 and could never reproduce it — the exact shape of that trap.

    A `SELECT DISTINCT` cannot truncate, because the aggregate happens before the row limit rather
    than after it: 43 rows leave the server, and they are all of them.

    ⚠ NULLs and blanks are dropped HERE rather than by the caller, so every caller gets the same
    answer and none of them has to remember to.

    ⚠⚠ SORTED IN PYTHON, NOT BY `ORDER BY` — the two do not agree, and the DATABASE's answer is the
    one that can move. Postgres sorts under the database COLLATION, which is locale-aware and need
    not match between the local container and the cloud project; Python sorts by codepoint. On the
    live sector list they already differ:

        Postgres:  Industrials, Information Technology, Internet Services, IT Services & Consulting
        Python:    IT Services & Consulting, IT Services & Software, Industrials, Information …

    Either order is defensible for a dropdown. What is not defensible is the order depending on
    which environment answered, so it is decided HERE — which also keeps this byte-identical to the
    `sorted({...})` the callers used before, so swapping the transport in cannot reorder a UI.
    """
    if not _db_url():
        return None
    if not _SAFE_IDENT.match(table) or not _SAFE_IDENT.match(column):
        log.warning("[common.pg] refusing a COPY with a non-identifier table/column: %r/%r",
                    table, column)
        return None
    sql = (f"COPY (SELECT DISTINCT {column} FROM {table} "
           f"WHERE {column} IS NOT NULL AND btrim({column}::text) <> '') "
           f"TO STDOUT WITH (FORMAT csv)")
    buf = _run_copy(sql, ())
    if buf is None:
        return None
    out: list = []
    for row in csv.reader(io.TextIOWrapper(buf, encoding="utf-8", newline="")):
        if row and row[0].strip():
            out.append(row[0].strip())
    return sorted(out)


def _drop_scoped_connection() -> None:
    """Forget this thread's connection after an error, so the next COPY reconnects."""
    scope = _CONN_SCOPE.get()
    if scope is None:
        return
    conn = scope.pop(threading.get_ident(), None)
    if conn is not None:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass

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
    psycopg missing, or any connection/query error).

    ⚠ INSIDE A `read_cache()` BLOCK AN IDENTICAL COPY IS SERVED FROM THE FIRST ONE. Measured on
    the Analyse modal: the benchmark's price panel — the single most expensive read on that screen
    — was loaded THREE times with a byte-identical id list and window, because three collaborating
    modules each correctly asked for it. Outside such a block this is unchanged: no memo, no TTL,
    every call hits Postgres. See `common/read_cache.py`.
    """
    from common import read_cache  # noqa: PLC0415  (module-level would be a cycle via deps)

    if read_cache.active() is not None:
        # `repr` on the params, because they are lists of ids and dates — unhashable as-is, and
        # what identifies the query is exactly their printed form.
        return read_cache.copy_bytes(("COPY", sql, repr(params)), _run_copy_uncached, sql, params)
    return _run_copy_uncached(sql, params)


def _run_copy_uncached(sql: str, params: tuple) -> io.BytesIO | None:
    """The COPY itself. Split out so the memo above wraps it without duplicating any of it."""
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
    # Reuse this request's connection when a scope is open (see `copy_connection_scope`), which
    # is where the 220ms-per-connect production cost goes. Falls back to a fresh, self-closing
    # connection when there is no scope — a script, a scheduler tick, a test.
    try:
        conn = _scoped_connection(url)
    except Exception as e:  # noqa: BLE001 — reuse is an optimisation, never a failure mode
        log.warning("[common.pg] could not open a scoped connection (%s: %s); "
                    "falling back to a per-COPY connection", type(e).__name__, e)
        conn = None

    try:
        buf = io.BytesIO()
        if conn is not None:
            with conn.cursor() as cur:
                # `statement_timeout` was set once when this connection was opened.
                with cur.copy(sql, params) as copy:
                    while (block := copy.read()):
                        buf.write(block)
        else:
            with psycopg.connect(url, connect_timeout=15) as fresh:
                with fresh.cursor() as cur:
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
        # ⚠ DROP THE REUSED CONNECTION ON ANY FAILURE. A COPY that errors can leave the session in
        # a state the next one cannot use, and a broken socket would otherwise fail every
        # remaining COPY in the request instead of just this one. Reconnecting costs 220ms once;
        # poisoning the request costs all of them.
        _drop_scoped_connection()
        log.warning(
            "[common.pg] direct COPY failed (%s: %s); falling back to PostgREST.",
            type(e).__name__, e,
        )
        return None
