"""Bespoke COPY-backed loaders for tables the `timeseries` façade doesn't cover.

The COPY transport itself moved to `common.pg` (infrastructure, not a momentum
concern — `timeseries` needs it too, and importing `momentum` from there would
be a cycle). `_db_url`, `copy_path_enabled` and `_run_copy` are re-exported below
for this module's many existing callers.

What stays here: the one-off queries that aren't per-entity time series —
company lists, universe memberships, latest-date lookups, FX rows. Everything
that IS a per-entity series (`close_price`, `volume`, `asset_price.close/volume`)
now goes through `timeseries.load_series`.

Each loader returns `None` when the env var is absent, psycopg isn't installed,
or anything goes wrong — the caller then falls back to its PostgREST path.
"""
from __future__ import annotations

import io
import logging
from datetime import date

import pandas as pd

# Re-exported for the callers that still import them from here.
from common.pg import _db_url, _run_copy, copy_path_enabled  # noqa: F401

log = logging.getLogger(__name__)


def copy_universe_memberships_via_pg(src_universe_id: int, dst_universe_id: int) -> int | None:
    """Copy every `universe_membership` row from one universe to another in a
    single direct-Postgres `INSERT ... SELECT` (used to freeze a template into
    a static snapshot). Delisted / out-of-scope companies are filtered out (a
    JOIN to `company`) so a frozen snapshot only holds tradeable securities —
    mirrors the `load_universe` backtest filter. Returns the number of rows
    copied, or `None` to signal fall-back (unconfigured / psycopg missing /
    error)."""
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
                cur.execute("SET statement_timeout = 0")
                cur.execute(
                    "INSERT INTO universe_membership "
                    "(universe_id, company_id, target_month, universe_ticker, sector, industry) "
                    "SELECT %s, um.company_id, um.target_month, um.universe_ticker, um.sector, um.industry "
                    "FROM universe_membership um "
                    "JOIN company c ON c.company_id = um.company_id "
                    "WHERE um.universe_id = %s "
                    "AND c.delisted_at IS NULL AND c.out_of_scope_at IS NULL",
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
        "COPY (SELECT c.company_id, c.company_name, c.gurufocus_ticker, c.exchange_id, c.isin, "
        f"{_TS_ISO_FMT % 'c.delisted_at'}, "
        f"{_TS_ISO_FMT % 'c.gurufocus_lookup_failed_at'}, "
        f"{_TS_ISO_FMT % 'c.out_of_scope_at'}, "
        "c.out_of_scope_reason, c.market_cap_eur, c.market_cap_date::text, "
        "c.market_cap_native, c.market_cap_currency, c.market_cap_fx_rate, "
        "c.openfigi_status, c.openfigi_name, "
        f"{_TS_ISO_FMT % 'c.openfigi_checked_at'}, "
        "e.exchange_code, e.currency_code, co.country_name "
        "FROM company c "
        "LEFT JOIN gurufocus_exchange e ON e.exchange_id = c.exchange_id "
        "LEFT JOIN country co ON co.country_code = e.country_code "
        "ORDER BY c.company_name) TO STDOUT WITH (FORMAT csv)"
    )
    buf = _run_copy(sql, ())
    if buf is None:
        return None

    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415
    import csv as _csv  # noqa: PLC0415 — stdlib, local to keep boot cheap
    out: list[dict] = []
    reader = _csv.reader(io.TextIOWrapper(buf, encoding="utf-8"))
    for row in reader:
        if len(row) != 20:
            continue
        (cid, name, ticker, exch_id, isin, delisted, gf_failed, oos_at, oos_reason,
         mktcap_eur, mktcap_date, mktcap_native, mktcap_currency, mktcap_fx_rate,
         openfigi_status, openfigi_name, openfigi_checked,
         exch_code, currency, country) = row
        out.append({
            "company_id": int(cid),
            "company_name": name or None,
            "gurufocus_ticker": ticker,
            "exchange_id": int(exch_id) if exch_id else None,
            "isin": isin or None,
            "delisted_at": _match_postgrest_ts(delisted or None),
            "gurufocus_lookup_failed_at": _match_postgrest_ts(gf_failed or None),
            "out_of_scope_at": _match_postgrest_ts(oos_at or None),
            "out_of_scope_reason": oos_reason or None,
            "market_cap_eur": float(mktcap_eur) if mktcap_eur else None,
            "market_cap_date": mktcap_date or None,
            "market_cap_native": float(mktcap_native) if mktcap_native else None,
            "market_cap_currency": mktcap_currency or None,
            "market_cap_fx_rate": float(mktcap_fx_rate) if mktcap_fx_rate else None,
            "openfigi_status": openfigi_status or None,
            "openfigi_name": openfigi_name or None,
            "openfigi_checked_at": _match_postgrest_ts(openfigi_checked or None),
            "gurufocus_exchange": exch_code or None,
            "currency": currency or None,
            "country": country or None,
            "gf_unsubscribed": not is_gf_subscribed_exchange(exch_code or None),
        })
    return out


def load_latest_metric_dates_via_copy(
    company_ids: list[int], metric_code: str,
) -> dict[int, str] | None:
    """Latest `target_date` per company for a given `metric_code` (source
    'gurufocus'), for a set of company ids — ONE grouped COPY. Returns
    `{company_id: 'YYYY-MM-DD'}` (companies with no such metric are absent), or
    `None` for the PostgREST fall-back (SUPABASE_DB_URL unset / psycopg missing /
    error). Both `close_price` and `volume` are 100% source 'gurufocus', so the
    source filter is a no-op that just unlocks the single-seek index path.

    Per-company lateral `ORDER BY target_date DESC LIMIT 1` (a "loose index
    scan") so the PK index seeks straight to each company's latest row — one row
    read per company, all in a single round-trip instead of N PostgREST calls."""
    if not _db_url() or not company_ids:
        return None
    sql = (
        "COPY (SELECT cid AS company_id, l.d::text FROM unnest(%s::int[]) AS cid "
        "CROSS JOIN LATERAL (SELECT md.target_date AS d FROM metric_data md "
        "WHERE md.company_id = cid AND md.metric_code = %s "
        "AND md.source_code = 'gurufocus' ORDER BY md.target_date DESC LIMIT 1) l) "
        "TO STDOUT WITH (FORMAT csv)"
    )
    buf = _run_copy(sql, (list(company_ids), metric_code))
    if buf is None:
        return None

    import csv as _csv  # noqa: PLC0415
    out: dict[int, str] = {}
    for row in _csv.reader(io.TextIOWrapper(buf, encoding="utf-8")):
        if len(row) != 2 or not row[0] or not row[1]:
            continue
        out[int(row[0])] = row[1]
    return out


def load_latest_close_dates_via_copy(company_ids: list[int]) -> dict[int, str] | None:
    """Latest `close_price` `target_date` per company (thin wrapper over
    `load_latest_metric_dates_via_copy` — kept for existing callers)."""
    return load_latest_metric_dates_via_copy(company_ids, "close_price")


def load_latest_close_prices_via_copy(
    company_ids: list[int],
) -> dict[int, dict] | None:
    """Latest `close_price` row (date + native-currency value) per company,
    for a SMALL set of company ids (e.g. a strategy's ~24 held names) — a
    single indexed `DISTINCT ON` via COPY. Returns
    `{company_id: {"date": 'YYYY-MM-DD', "price": float}}` (companies with no
    close_price are absent), or `None` for the PostgREST fall-back. The value
    is the raw GuruFocus close in the security's native trading currency (no
    FX conversion). Sibling of `load_latest_close_dates_via_copy`, returning
    the value alongside the date for the held-companies panel.

    Per-company lateral `ORDER BY target_date DESC LIMIT 1` (one row read per
    company) instead of `DISTINCT ON` over each company's full date range."""
    if not _db_url() or not company_ids:
        return None
    sql = (
        "COPY (SELECT cid AS company_id, l.d::text, l.v FROM unnest(%s::int[]) AS cid "
        "CROSS JOIN LATERAL (SELECT md.target_date AS d, md.numeric_value AS v "
        "FROM metric_data md WHERE md.company_id = cid AND md.metric_code = 'close_price' "
        "AND md.source_code = 'gurufocus' ORDER BY md.target_date DESC LIMIT 1) l) "
        "TO STDOUT WITH (FORMAT csv)"
    )
    buf = _run_copy(sql, (list(company_ids),))
    if buf is None:
        return None

    import csv as _csv  # noqa: PLC0415
    out: dict[int, dict] = {}
    for row in _csv.reader(io.TextIOWrapper(buf, encoding="utf-8")):
        if len(row) != 3 or not row[0] or not row[1]:
            continue
        try:
            price = float(row[2]) if row[2] else None
        except ValueError:
            price = None
        out[int(row[0])] = {"date": row[1], "price": price}
    return out


def load_all_latest_close_dates_via_copy() -> dict[int, str] | None:
    """Latest `close_price` `target_date` for EVERY company via COPY. Returns
    `{company_id: 'YYYY-MM-DD'}` (companies with no close are absent), or `None`
    for the fall-back. Used by the delisting sweep (runs every pipeline tick).

    A per-company lateral `ORDER BY target_date DESC LIMIT 1` over the `company`
    table: one indexed row-read per company (~2.4k seeks) instead of the old
    `GROUP BY max(target_date)` that scanned the whole ~13M-row close_price range
    (~2.3 GB, the prior delisting-sweep IO hog). close_price is 100% source
    'gurufocus', so the source filter only narrows to the single-seek index path."""
    if not _db_url():
        return None
    sql = (
        "COPY (SELECT c.company_id, l.d::text FROM company c "
        "CROSS JOIN LATERAL (SELECT md.target_date AS d FROM metric_data md "
        "WHERE md.company_id = c.company_id AND md.metric_code = 'close_price' "
        "AND md.source_code = 'gurufocus' ORDER BY md.target_date DESC LIMIT 1) l) "
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
    """Stream a universe's membership for its LATEST month via a single COPY,
    returning `{YYYY-MM: {company_id: grouping_value}}` (one key — the newest
    captured month). Returns `None` for the fall-back.

    Fixed-basket model: universes are frozen snapshots, so a backtest uses only
    the newest month's membership (`broadcast_constant` then applies it across
    all of history). Restricting the COPY to `max(target_month)` keeps this cheap
    even for a legacy multi-month universe (e.g. an old ACWI reconstruction with
    hundreds of months still in the table).

    `grouping_field` is validated to `sector`/`industry` before it's
    interpolated into the SQL (no other caller-controlled SQL text)."""
    if grouping_field not in ("sector", "industry"):
        return None
    if not _db_url():
        return None
    sql = (
        f"COPY (SELECT target_month, company_id, {grouping_field} "
        "FROM universe_membership WHERE universe_id = %s "
        "AND target_month = (SELECT max(target_month) FROM universe_membership WHERE universe_id = %s) "
        "ORDER BY target_month) TO STDOUT WITH (FORMAT csv)"
    )
    buf = _run_copy(sql, (universe_id, universe_id))
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
