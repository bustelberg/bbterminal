"""Turn opaque database errors into production-friendly explanations.

The PostgREST client surfaces a Postgres statement timeout as a bare
``{'message': 'canceling statement due to statement timeout', 'code':
'57014'}`` — which tells you *a* query timed out but not WHICH operation
or WHY. `explain_db_error` re-wraps such errors with the operation
context and, for the timeout case, the actionable root cause (the
direct-Postgres COPY fast path being disabled or its connection
failing), so the message that reaches the UI / logs is self-explanatory.

Dependency-free (no momentum/router imports) so any layer can use it;
the caller passes `copy_enabled` (from
`momentum.data._pg.copy_path_enabled()`).

Pinned by tests/test_db_errors.py.
"""
from __future__ import annotations


def _looks_like_timeout(e: Exception) -> bool:
    blob = f"{getattr(e, 'code', '')} {getattr(e, 'message', '')} {e}".lower()
    return (
        "57014" in blob
        or "statement timeout" in blob
        or "canceling statement" in blob
    )


def explain_db_error(e: Exception, *, what: str, copy_enabled: bool) -> Exception:
    """Return the exception to raise IN PLACE OF `e`, carrying a clear
    message about WHAT failed and — for a statement timeout — WHY plus how
    to fix it. Unknown errors still get the operation context prepended, so
    nothing reaches production context-free. Always pair with `raise … from e`
    so the original traceback is preserved in the logs.

    `what` should read naturally after "while …", e.g.
    "loading close_price for 1700 companies (2002-01-01..2026-06-08)".
    """
    if _looks_like_timeout(e):
        if copy_enabled:
            why = (
                "SUPABASE_DB_URL is set, but the direct-Postgres COPY connection "
                "failed, so this fell back to PostgREST (which has a short "
                "statement_timeout). Run GET /api/admin/copy-status to see the "
                "exact COPY connection error."
            )
        else:
            why = (
                "the direct-Postgres COPY fast path is disabled (SUPABASE_DB_URL / "
                "DATABASE_URL not set), so this large read went through PostgREST "
                "and exceeded its statement_timeout. Set SUPABASE_DB_URL to the "
                "Supabase Session pooler (host on port 5432) so big reads stream "
                "via COPY in a single query."
            )
        return RuntimeError(f"Database statement timeout while {what}. Why: {why}")
    return RuntimeError(f"Database error while {what}: {type(e).__name__}: {e}")
