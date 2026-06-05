"""Optional fast path: load tables via a direct Postgres COPY stream.

PostgREST caps responses at 1000 rows (prod) / 10000 (local), so the default
loaders page over millions of rows — thousands of HTTP round-trips through
Cloudflare, plus per-row JSON. When a direct Postgres connection string is
configured (`SUPABASE_DB_URL` / `DATABASE_URL`), this module streams the entire
result in a SINGLE `COPY ... TO STDOUT` query: no row cap, no `IN`-chunking (the
whole id list goes as one `= ANY($)` array), binary wire instead of JSON.
Typically several × faster on the up-front load.

Everything here is strictly opt-in and self-healing: each loader returns `None`
when the env var is absent, psycopg isn't installed, or ANYTHING goes wrong —
the caller then falls back to the existing PostgREST path. So shipping this
can't change behaviour until the connection string is set, and a bad connection
degrades to "as before", never to an error.
"""
from __future__ import annotations

import io
import logging
import os
from datetime import date

import pandas as pd

log = logging.getLogger(__name__)


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
        return None
    try:
        import psycopg  # local import so the dependency stays optional
    except ImportError:
        log.warning(
            "[data._pg] SUPABASE_DB_URL is set but psycopg isn't installed; "
            "using the PostgREST loader instead."
        )
        return None
    try:
        buf = io.BytesIO()
        with psycopg.connect(url, connect_timeout=15) as conn:
            with conn.cursor() as cur:
                with cur.copy(sql, params) as copy:
                    while (block := copy.read()):
                        buf.write(block)
        buf.seek(0)
        return buf
    except Exception as e:  # noqa: BLE001 — any failure → fall back, never raise
        log.warning(
            "[data._pg] direct COPY failed (%s: %s); falling back to PostgREST.",
            type(e).__name__, e,
        )
        return None


def load_metric_df_via_copy(
    company_ids: list[int],
    metric_code: str,
    start_date: date,
    end_date: date,
    value_col: str,
) -> pd.DataFrame | None:
    """Stream one `metric_data` series for the given companies/date-range via a
    single COPY. Returns a DataFrame with columns ``[company_id, target_date,
    <value_col>]`` sorted by (company_id, target_date), or ``None`` to signal
    the caller to use the PostgREST path."""
    if not _db_url() or not company_ids:
        return None
    sql = (
        "COPY (SELECT company_id, target_date, numeric_value FROM metric_data "
        "WHERE metric_code = %s AND source_code = 'gurufocus' "
        "AND company_id = ANY(%s) "
        "AND target_date BETWEEN %s AND %s "
        "ORDER BY company_id, target_date) TO STDOUT WITH (FORMAT csv)"
    )
    params = (metric_code, list(company_ids), start_date.isoformat(), end_date.isoformat())
    buf = _run_copy(sql, params)
    if buf is None:
        return None

    cols = ["company_id", "target_date", value_col]
    if buf.getbuffer().nbytes == 0:
        return pd.DataFrame(columns=cols)
    df = pd.read_csv(buf, names=cols, header=None)
    df["company_id"] = df["company_id"].astype(int)
    df["target_date"] = pd.to_datetime(df["target_date"])
    df[value_col] = df[value_col].astype(float)
    return df.reset_index(drop=True)


def load_fx_rate_df_via_copy(
    currency_codes: list[str],
    start_date: date,
    end_date: date,
) -> pd.DataFrame | None:
    """Stream `fx_rate` rows for the given currencies/date-range via a single
    COPY. Returns a DataFrame ``[currency_code, rate_date, rate]`` (rate_date as
    datetime, rate as float), or ``None`` for the PostgREST fall-back. The
    caller is responsible for the EUR constant series + daily reindex/ffill;
    this only replaces the raw row fetch."""
    needed = [c for c in currency_codes if c and c != "EUR"]
    if not _db_url() or not needed:
        return None
    sql = (
        "COPY (SELECT currency_code, rate_date, rate FROM fx_rate "
        "WHERE currency_code = ANY(%s) "
        "AND rate_date BETWEEN %s AND %s "
        "ORDER BY currency_code, rate_date) TO STDOUT WITH (FORMAT csv)"
    )
    buf = _run_copy(sql, (needed, start_date.isoformat(), end_date.isoformat()))
    if buf is None:
        return None

    cols = ["currency_code", "rate_date", "rate"]
    if buf.getbuffer().nbytes == 0:
        return pd.DataFrame(columns=cols)
    df = pd.read_csv(buf, names=cols, header=None)
    df["currency_code"] = df["currency_code"].astype(str)
    df["rate_date"] = pd.to_datetime(df["rate_date"])
    df["rate"] = df["rate"].astype(float)
    return df.reset_index(drop=True)
