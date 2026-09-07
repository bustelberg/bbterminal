"""Drifted identity/serial sequences: detect one from the insert that hit it, and repair it.

⚠⚠ THE FAILURE THIS EXISTS FOR DOES NOT LOOK LIKE ITS CAUSE. A sequence that has fallen behind
its table reads fine, deploys fine, and then fails on the NEXT INSERT with

    duplicate key value violates unique constraint "current_picks_snapshot_pkey"
    Key (snapshot_id)=(3408) already exists.

which reads as a bug in the application — a race, a double-write, a retry gone wrong — and is
none of those. It is one number in `pg_sequence` being smaller than `MAX(id)`, and EVERYTHING
that inserts into that table stops at the same instant.

Measured in production 2026-09-07, twice on one database from one incident:
  * `ingest_run` — `_create_run` raised on every call, so the scheduler could not open a run row
    and NEITHER COULD ANY BUTTON. /schedule reported `daily_pipeline` 26 days stale beside a
    Rebalance button that flashed "Starting…" and did nothing: two surfaces both looking like a
    scheduler fault when the schedule was never the problem.
  * `current_picks_snapshot` — the 05:00 pipeline failed one strategy of three with the error
    above. It surfaced later than the first only because this table is inserted into twice a day
    rather than on every click.

⚠ HOW IT HAPPENS: rows inserted with EXPLICIT ids do not advance the sequence. The local→prod
clone does exactly that, and `scripts/clone-local-to-prod.ps1` step **[7b]** — the reset that
repairs it — is the LAST step, so any abort (Ctrl-C, a dropped connection, a disk preflight
failure) leaves every sequence in the database drifted at once. A hand-written INSERT naming the
id column and a restore from a data-only dump do the same thing.

⚠ THE FIX IS STRICTLY SAFE AND ONLY EVER MOVES FORWARD. `setval(seq, MAX(id))` never reassigns an
id and never touches a row; moving a sequence BACKWARDS is the dangerous direction (ids of deleted
rows would be reissued) and nothing here can do it — the repair reads `MAX(id)` and sets exactly
that, and a sequence already ahead of its table lands on the same value it had.

⚠ THE WHOLE-DATABASE VERSION IS `scripts/resync-sequences.sql`, and it stays the tool of record:
this module repairs the ONE table whose insert just failed, because that is all the failing insert
proves. After an aborted clone every sequence is drifted and they should be fixed in one pass, not
one 05:00 failure at a time.
"""
from __future__ import annotations

import logging
import re

log = logging.getLogger(__name__)

#: Postgres' unique_violation. PostgREST passes it through in the error body, and psycopg exposes
#: it as `sqlstate`, so matching on the code covers both transports.
_UNIQUE_VIOLATION = "23505"


def _message_of(exc: BaseException) -> str:
    """Everything an exception can tell us, as one lowercase string.

    ⚠ THE TRANSPORTS DISAGREE ABOUT WHERE THE CODE LIVES. A supabase-py `APIError` carries a dict
    (`code`, `message`, `details`) and stringifies to its repr; a psycopg error carries `sqlstate`
    and puts the constraint name in `str(e)`. Reading `str(exc)` plus the attributes we know about
    is what makes one test work for both, rather than a match that quietly only ever fires on one.
    """
    parts = [str(exc)]
    for attr in ("code", "sqlstate", "message", "details"):
        value = getattr(exc, attr, None)
        if value:
            parts.append(str(value))
    args0 = exc.args[0] if exc.args else None
    if isinstance(args0, dict):
        parts.extend(str(v) for v in args0.values() if v)
    return " ".join(parts).lower()


def is_sequence_drift(exc: BaseException, table: str) -> bool:
    """Is this exception `table`'s primary key colliding on an id the sequence already passed?

    ⚠ IT MUST NAME THE PRIMARY KEY, NOT JUST THE TABLE. A duplicate on a business unique
    constraint — `universe.template_key`, `(benchmark_id, target_date)` — is a real application
    conflict and re-issuing the insert after a setval would be wrong: it would either fail again
    or, worse, succeed at writing a row somebody's uniqueness rule exists to refuse. Only a
    `_pkey` collision on a sequence-backed id is the drift this module repairs.
    """
    blob = _message_of(exc)
    if _UNIQUE_VIOLATION not in blob:
        return False
    return f"{table}_pkey" in blob


def repair_sequence(table: str, column: str) -> int | None:
    """Move `table`'s sequence forward to `MAX(column)`. Returns the new value, or None.

    None means "could not", never "did not need to": no direct-Postgres URL, psycopg absent, or
    the statement failed. The caller must then RAISE rather than retry — an insert re-issued
    against an unrepaired sequence fails identically, and a silent second failure is how a
    diagnosis gets lost.

    ⚠ IT DISCOVERS THE SEQUENCE THE SAME TWO WAYS `resync-sequences.sql` AND THE CLONE DO.
    `pg_get_serial_sequence` MISSES a manually-created sequence (CREATE SEQUENCE + DEFAULT
    nextval, not OWNED BY — `company_id_seq` is one), so the default expression is parsed as a
    fallback. A version of this that only asked `pg_get_serial_sequence` would work on most
    tables and silently refuse to repair exactly the hand-made ones.

    ⚠ `GREATEST(max, 1)` because a sequence cannot be set below its minimum, and `is_called :=
    max > 0` so an EMPTY table's next insert gets id 1 rather than 2.

    ⚠ Identifiers are matched against `^[a-z_][a-z0-9_]*$` before they reach the SQL. They are
    module-level literals at every call site today; the check is what keeps that true if one ever
    stops being.
    """
    ident = re.compile(r"^[a-z_][a-z0-9_]*$")
    if not ident.match(table) or not ident.match(column):
        log.warning("[sequences] refusing to repair a non-identifier target: %r.%r", table, column)
        return None

    from common.pg import _db_url  # noqa: PLC0415 — module-level would be a cycle via deps

    url = _db_url()
    if not url:
        log.warning(
            "[sequences] %s.%s looks like a drifted sequence, but SUPABASE_DB_URL/DATABASE_URL "
            "is not set so it cannot be repaired from here — run scripts/resync-sequences.sql",
            table, column,
        )
        return None
    try:
        import psycopg  # noqa: PLC0415 — optional dependency, same as common.pg
    except ImportError:
        log.warning("[sequences] psycopg isn't installed; run scripts/resync-sequences.sql")
        return None

    sql = f"""
        SELECT setval(seq::regclass, GREATEST(m, 1), m > 0)
        FROM (
            SELECT COALESCE(
                       pg_get_serial_sequence('public.{table}', '{column}'),
                       (SELECT (regexp_match(column_default, 'nextval\\(''([^'']+)'''))[1]
                          FROM information_schema.columns
                         WHERE table_schema = 'public'
                           AND table_name = '{table}'
                           AND column_name = '{column}')
                   ) AS seq,
                   (SELECT COALESCE(MAX({column}), 0) FROM public.{table}) AS m
        ) q
        WHERE seq IS NOT NULL
    """
    try:
        # ⚠ A FRESH CONNECTION, DELIBERATELY NOT THE REQUEST-SCOPED ONE. We are here because a
        # statement just failed; inside a transaction that psycopg has marked aborted every
        # further statement raises `InFailedSqlTransaction`, and the repair would be refused for
        # a reason that has nothing to do with the sequence.
        with psycopg.connect(url, connect_timeout=15) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
            conn.commit()
    except Exception as e:  # noqa: BLE001 — the caller raises with the diagnosis either way
        log.warning("[sequences] repair of %s.%s failed: %s: %s", table, column, type(e).__name__, e)
        return None
    if not row or row[0] is None:
        log.warning("[sequences] no sequence found behind %s.%s", table, column)
        return None
    log.warning("[sequences] %s.%s sequence moved forward to %s", table, column, row[0])
    return int(row[0])


def insert_repairing_sequence(table: str, column: str, row: dict | list[dict]):
    """`supabase.table(table).insert(row).execute()`, retried ONCE past a drifted sequence.

    Use it where a failed insert costs more than the insert — the pipeline's run row and the
    schedule's snapshots, both of which take a whole surface down with them.

    ⚠ ONE RETRY, AND ONLY AFTER A REPAIR THAT REPORTED SUCCESS. A retry loop over a duplicate-key
    error is how a real uniqueness conflict becomes an infinite one.

    ⚠ WHEN IT CANNOT REPAIR, THE ERROR IT RAISES SAYS SO IN WORDS. The raw message names a
    constraint and an id and reads as an application bug; someone then has to already know the
    whole story to act on it. This one names the cause and the file that fixes it — which is the
    entire difference between a five-minute repair and a morning.
    """
    from deps import supabase  # noqa: PLC0415 — module-level would be a cycle

    try:
        return supabase.table(table).insert(row).execute()
    except Exception as e:
        if not is_sequence_drift(e, table):
            raise
        log.warning(
            "[sequences] insert into %s hit a duplicate primary key — this is a DRIFTED "
            "SEQUENCE, not a race: %s", table, e,
        )
        if repair_sequence(table, column) is None:
            raise RuntimeError(
                f"{table}.{column}'s sequence has fallen behind the table, so every insert into "
                f"{table} fails with a duplicate primary key. This is not an application bug: "
                f"rows were loaded with explicit ids (an aborted local→prod clone is the usual "
                f"cause — scripts/clone-local-to-prod.ps1 step [7b] is the reset that follows "
                f"the load) and the sequence was never moved past them. Fix it by running "
                f"scripts/resync-sequences.sql in the SQL editor: query [1] reports every "
                f"drifted sequence, query [2] repairs them. Original error: {e}"
            ) from e
        return supabase.table(table).insert(row).execute()
