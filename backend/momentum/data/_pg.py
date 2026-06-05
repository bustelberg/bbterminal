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


def copy_universe_memberships_via_pg(src_universe_id: int, dst_universe_id: int) -> int | None:
    """Copy every `universe_membership` row from one universe to another in a
    single direct-Postgres `INSERT ... SELECT` (used to freeze a template into
    a static snapshot). Returns the number of rows copied, or `None` to signal
    fall-back (unconfigured / psycopg missing / error)."""
    url = _db_url()
    if not url:
        return None
    try:
        import psycopg  # noqa: PLC0415
    except ImportError:
        return None
    try:
        with psycopg.connect(url, connect_timeout=30) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO universe_membership "
                    "(universe_id, company_id, target_month, universe_ticker, sector, industry) "
                    "SELECT %s, company_id, target_month, universe_ticker, sector, industry "
                    "FROM universe_membership WHERE universe_id = %s",
                    (dst_universe_id, src_universe_id),
                )
                n = cur.rowcount
            conn.commit()
        return n
    except Exception as e:  # noqa: BLE001 — fall back, never raise
        log.warning(
            "[data._pg] membership copy failed (%s: %s); falling back.",
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


# ISO-8601 with microseconds + explicit +00:00 offset, matching how
# supabase-py/PostgREST serializes a `timestamptz` ("2026-05-27T07:15:19.577638+00:00").
# `to_char(.US)` always pads to 6 digits; PostgREST trims trailing zeros (and
# drops the fraction entirely when all-zero), so `_match_postgrest_ts` strips
# them back off to keep the API response byte-identical to the paged path.
_TS_ISO_FMT = "to_char(%s AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US+00:00')"


def _match_postgrest_ts(s: str | None) -> str | None:
    """Trim trailing-zero microseconds from a `to_char`'d ISO timestamp so it
    matches PostgREST's serialization (`.693290` → `.69329`, `.000000` → no
    fraction). Input shape is always `...SS.UUUUUU+00:00`."""
    if not s or "." not in s:
        return s
    head, rest = s.split(".", 1)
    frac, off = rest[:6], rest[6:]  # 6-digit micros, then the fixed +00:00
    frac = frac.rstrip("0")
    return f"{head}.{frac}{off}" if frac else f"{head}{off}"


def load_companies_via_copy() -> list[dict] | None:
    """Stream the `/companies` list via a single COPY, returning the exact
    row shape `routers.companies.list_companies` produces from PostgREST:
    flat company columns + `gurufocus_exchange` (the exchange_code) +
    `country` (the country_name), ordered by company_name. Returns `None`
    to signal the PostgREST fall-back (unconfigured, psycopg missing, or any
    error). `timestamptz` columns are `to_char`'d to PostgREST's ISO format
    so the API response is byte-identical to the paged path."""
    if not _db_url():
        return None
    sql = (
        "COPY (SELECT c.company_id, c.company_name, c.gurufocus_ticker, c.exchange_id, "
        f"{_TS_ISO_FMT % 'c.delisted_at'}, "
        f"{_TS_ISO_FMT % 'c.gurufocus_lookup_failed_at'}, "
        f"{_TS_ISO_FMT % 'c.out_of_scope_at'}, "
        "c.out_of_scope_reason, e.exchange_code, co.country_name "
        "FROM company c "
        "LEFT JOIN gurufocus_exchange e ON e.exchange_id = c.exchange_id "
        "LEFT JOIN country co ON co.country_code = e.country_code "
        "ORDER BY c.company_name) TO STDOUT WITH (FORMAT csv)"
    )
    buf = _run_copy(sql, ())
    if buf is None:
        return None

    import csv as _csv  # noqa: PLC0415 — stdlib, local to keep boot cheap
    out: list[dict] = []
    reader = _csv.reader(io.TextIOWrapper(buf, encoding="utf-8"))
    for row in reader:
        if len(row) != 10:
            continue
        cid, name, ticker, exch_id, delisted, gf_failed, oos_at, oos_reason, exch_code, country = row
        out.append({
            "company_id": int(cid),
            "company_name": name or None,
            "gurufocus_ticker": ticker,
            "exchange_id": int(exch_id) if exch_id else None,
            "delisted_at": _match_postgrest_ts(delisted or None),
            "gurufocus_lookup_failed_at": _match_postgrest_ts(gf_failed or None),
            "out_of_scope_at": _match_postgrest_ts(oos_at or None),
            "out_of_scope_reason": oos_reason or None,
            "gurufocus_exchange": exch_code or None,
            "country": country or None,
        })
    return out


def load_latest_close_dates_via_copy(company_ids: list[int]) -> dict[int, str] | None:
    """Latest `close_price` `target_date` per company, for a SMALL set of
    company ids (e.g. a strategy's ~24 held names) — a single indexed
    `GROUP BY max(target_date)` via COPY. Returns `{company_id: 'YYYY-MM-DD'}`
    (companies with no close_price are simply absent), or `None` for the
    PostgREST fall-back. Replaces the full-table `company_latest_close_price_dates`
    RPC for the held-companies freshness view, which times out on the whole
    metric_data table."""
    if not _db_url() or not company_ids:
        return None
    sql = (
        "COPY (SELECT company_id, max(target_date)::text FROM metric_data "
        "WHERE metric_code = 'close_price' AND company_id = ANY(%s) "
        "GROUP BY company_id) TO STDOUT WITH (FORMAT csv)"
    )
    buf = _run_copy(sql, (list(company_ids),))
    if buf is None:
        return None

    import csv as _csv  # noqa: PLC0415
    out: dict[int, str] = {}
    for row in _csv.reader(io.TextIOWrapper(buf, encoding="utf-8")):
        if len(row) != 2 or not row[0] or not row[1]:
            continue
        out[int(row[0])] = row[1]
    return out


def load_all_latest_close_dates_via_copy() -> dict[int, str] | None:
    """Latest `close_price` `target_date` for EVERY company — a single
    `GROUP BY max(target_date)` over the whole `metric_data` table via COPY.
    Returns `{company_id: 'YYYY-MM-DD'}` (companies with no close are absent),
    or `None` for the fall-back. The PostgREST RPC equivalent
    (`company_latest_close_price_dates`) times out on the full table; the
    direct-Postgres GROUP BY is indexed + fast. Used by the delisting sweep."""
    if not _db_url():
        return None
    sql = (
        "COPY (SELECT company_id, max(target_date)::text FROM metric_data "
        "WHERE metric_code = 'close_price' GROUP BY company_id) "
        "TO STDOUT WITH (FORMAT csv)"
    )
    buf = _run_copy(sql, ())
    if buf is None:
        return None

    import csv as _csv  # noqa: PLC0415
    out: dict[int, str] = {}
    for row in _csv.reader(io.TextIOWrapper(buf, encoding="utf-8")):
        if len(row) != 2 or not row[0] or not row[1]:
            continue
        out[int(row[0])] = row[1]
    return out


def load_universe_membership_via_copy(
    universe_id: int, grouping_field: str,
) -> dict[str, dict[int, str | None]] | None:
    """Stream a universe's FULL membership panel (every month × company) via
    a single COPY, returning the same `{YYYY-MM: {company_id: grouping_value}}`
    shape as the PostgREST pager in `universe_loader._load_index_universe`.
    Returns `None` for the fall-back. This is the heaviest universe read —
    ACWI spans ~2k companies × ~290 months (~hundreds of thousands of rows),
    so the PostgREST pager makes hundreds of round-trips; COPY is one.

    `grouping_field` is validated to `sector`/`industry` before it's
    interpolated into the SQL (no other caller-controlled SQL text)."""
    if grouping_field not in ("sector", "industry"):
        return None
    if not _db_url():
        return None
    sql = (
        f"COPY (SELECT target_month, company_id, {grouping_field} "
        "FROM universe_membership WHERE universe_id = %s "
        "ORDER BY target_month) TO STDOUT WITH (FORMAT csv)"
    )
    buf = _run_copy(sql, (universe_id,))
    if buf is None:
        return None

    import csv as _csv  # noqa: PLC0415
    result: dict[str, dict[int, str | None]] = {}
    reader = _csv.reader(io.TextIOWrapper(buf, encoding="utf-8"))
    for row in reader:
        if len(row) != 3:
            continue
        month_raw, cid_raw, group_val = row
        m = (month_raw or "")[:7]
        if not m or not cid_raw:
            continue
        result.setdefault(m, {})[int(cid_raw)] = group_val or None
    return result
