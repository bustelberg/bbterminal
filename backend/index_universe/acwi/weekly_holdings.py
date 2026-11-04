"""Download and atomically publish the official iShares ACWI equity holdings."""
from __future__ import annotations

import hashlib
from contextlib import contextmanager
from datetime import date, datetime
from io import StringIO
from zoneinfo import ZoneInfo

import requests
import psycopg
from psycopg.types.json import Jsonb

from index_universe.acwi.asset_membership import ACWI_LABEL, SOURCE, resolve
from index_universe.acwi.holdings import load_acwi_holdings, parse_holdings_csv

SOURCE_URL = "https://www.ishares.com/us/products/239600/ishares-msci-acwi-etf/latest-holdings.csv"


@contextmanager
def publication_connection():
    from common.pg import _db_url

    url = _db_url()
    if not url:
        raise RuntimeError("ACWI holdings import requires a database connection")
    with psycopg.connect(url, autocommit=True, connect_timeout=5,
                         options="-c statement_timeout=5000") as conn:
        acquired = conn.execute(
            "SELECT pg_try_advisory_lock(hashtext('bbterminal.acwi_weekly_coverage'))"
        ).fetchone()[0]
        try:
            yield conn if acquired else None
        finally:
            if acquired:
                conn.execute("SELECT pg_advisory_unlock(hashtext('bbterminal.acwi_weekly_coverage'))")


def needs_bootstrap(conn) -> bool:
    latest = conn.execute("SELECT max(as_of) FROM public.acwi_holdings_snapshot").fetchone()[0]
    today = datetime.now(ZoneInfo("America/New_York")).date()
    return latest is None or (today - latest).days > 7


def refresh(conn, ctx) -> dict:
    ctx.check()
    response = requests.get(SOURCE_URL, timeout=(10, 45), headers={"User-Agent": "bbterminal/1.0"})
    response.raise_for_status()
    raw = response.content
    checksum = hashlib.sha256(raw).hexdigest()
    holdings, stamp = parse_holdings_csv(StringIO(raw.decode("utf-8-sig"), newline=""))
    as_of = date.fromisoformat(stamp)
    today = datetime.now(ZoneInfo("America/New_York")).date()
    if not 0 <= (today - as_of).days <= 10:
        raise ValueError(f"iShares returned an out-of-date ACWI snapshot ({stamp})")

    latest = conn.execute(
        """SELECT as_of, source_sha256, jsonb_array_length(holdings), resolved_count
           FROM public.acwi_holdings_snapshot ORDER BY as_of DESC LIMIT 1"""
    ).fetchone()
    if latest and as_of < latest[0]:
        raise ValueError(f"iShares returned {stamp}, older than the archived ACWI snapshot")
    if latest and as_of == latest[0] and checksum == latest[1]:
        return {"status": "unchanged", "as_of": stamp, "equities": latest[2], "resolved": latest[3]}

    if latest:
        previous_count = latest[2]
    else:
        baseline, baseline_stamp = load_acwi_holdings()
        baseline_date = (date.fromisoformat(baseline_stamp) if baseline_stamp[4] == "-"
                         else datetime.strptime(baseline_stamp, "%d-%b-%Y").date())
        if as_of < baseline_date:
            raise ValueError(f"iShares returned {stamp}, older than the bundled ACWI snapshot")
        previous_count = len(baseline)
    if len(holdings) < previous_count * .9:
        raise ValueError(f"iShares returned only {len(holdings)} equity rows, versus {previous_count} previously")
    rows, stats = resolve((holdings, stamp))
    if not rows or (latest and len(rows) < latest[3] * .9):
        raise ValueError(f"ACWI identity coverage fell to {len(rows)} resolved assets")

    ctx.check()
    with conn.transaction():
        universe = conn.execute("SELECT universe_id FROM public.universe WHERE label = %s", (ACWI_LABEL,)).fetchone()
        if not universe:
            raise RuntimeError("No ACWI universe is configured")
        conn.execute(
            """INSERT INTO public.acwi_holdings_snapshot (as_of, source_url, source_sha256, holdings, resolved_count)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (as_of) DO UPDATE SET source_url = EXCLUDED.source_url,
                 source_sha256 = EXCLUDED.source_sha256, holdings = EXCLUDED.holdings,
                 resolved_count = EXCLUDED.resolved_count, imported_at = now()""",
            (as_of, SOURCE_URL, checksum, Jsonb(holdings), len(rows)),
        )
        conn.execute("DELETE FROM public.index_file_membership WHERE universe_id = %s AND source = %s",
                     (universe[0], SOURCE))
        conn.execute(
            """INSERT INTO public.index_file_membership
                 (universe_id, analysis_id, source, ticker, exchange, yahoo_symbol, source_as_of)
               SELECT %s, analysis_id, source, ticker, exchange, yahoo_symbol, source_as_of
               FROM jsonb_to_recordset(%s) AS r(analysis_id integer, source text, ticker text,
                   exchange text, yahoo_symbol text, source_as_of text)
               ON CONFLICT (universe_id, analysis_id) DO NOTHING""",
            (universe[0], Jsonb(rows)),
        )
    return {"status": "published", **stats}
