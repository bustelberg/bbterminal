"""
src/quick_insight/web/helpers/gurufocus_data_helpers/repo.py
repo.py — DB queries for the GuruFocus Data page.

Reads:
  - All companies with their metric coverage (facts_number row counts per company)
  - Companies that have zero facts_number rows (no data yet)
"""
from __future__ import annotations

from dataclasses import dataclass

import duckdb
import pandas as pd

from quick_insight.config.config import settings


def _db_path() -> str:
    p = getattr(settings, "db_path", None) or getattr(settings, "duckdb_path", None)
    if not p:
        raise RuntimeError("No DuckDB path configured.")
    return str(p)


def _connect() -> duckdb.DuckDBPyConnection:
    # Always read_only=False to avoid connection conflict
    return duckdb.connect(_db_path(), read_only=False)


@dataclass(frozen=True)
class GFDataRepo:

    def company_metric_coverage(self) -> pd.DataFrame:
        """
        For every company: how many facts_number rows, facts_text rows,
        and distinct metric_codes exist.

        Returns columns:
          company_id, company_name, primary_ticker, primary_exchange,
          sector, country,
          facts_number_rows, facts_text_rows, distinct_metrics, has_data
        """
        sql = """
        SELECT
            c.company_id,
            c.company_name,
            c.primary_ticker,
            c.primary_exchange,
            c.sector,
            c.country,
            COALESCE(fn.n, 0)  AS facts_number_rows,
            COALESCE(ft.n, 0)  AS facts_text_rows,
            COALESCE(dm.n, 0)  AS distinct_metrics,
            CASE WHEN COALESCE(fn.n, 0) + COALESCE(ft.n, 0) > 0
                 THEN TRUE ELSE FALSE END AS has_data
        FROM company c
        LEFT JOIN (
            SELECT company_id, COUNT(*) AS n
            FROM facts_number
            GROUP BY company_id
        ) fn ON fn.company_id = c.company_id
        LEFT JOIN (
            SELECT company_id, COUNT(*) AS n
            FROM facts_text
            GROUP BY company_id
        ) ft ON ft.company_id = c.company_id
        LEFT JOIN (
            SELECT company_id, COUNT(DISTINCT metric_id) AS n
            FROM facts_number
            GROUP BY company_id
        ) dm ON dm.company_id = c.company_id
        ORDER BY c.company_name
        """
        con = _connect()
        try:
            df = con.execute(sql).df().fillna(0)
        finally:
            con.close()

        df["facts_number_rows"] = df["facts_number_rows"].astype(int)
        df["facts_text_rows"]   = df["facts_text_rows"].astype(int)
        df["distinct_metrics"]  = df["distinct_metrics"].astype(int)
        df["has_data"]          = df["has_data"].astype(bool)
        df["company_name"]      = df["company_name"].fillna("").astype(str)
        df["primary_ticker"]    = df["primary_ticker"].fillna("").astype(str)
        df["primary_exchange"]  = df["primary_exchange"].fillna("").astype(str)
        df["sector"]            = df["sector"].fillna("").astype(str)
        df["country"]           = df["country"].fillna("").astype(str)
        return df

    def latest_snapshot_per_company(self) -> pd.DataFrame:
        """
        For each company: the most recent target_date across all their facts_number rows.
        Returns: company_id, latest_snapshot_date
        """
        sql = """
        SELECT
            fn.company_id,
            MAX(s.target_date) AS latest_snapshot_date
        FROM facts_number fn
        JOIN snapshot s ON s.snapshot_id = fn.snapshot_id
        GROUP BY fn.company_id
        """
        con = _connect()
        try:
            df = con.execute(sql).df()
        finally:
            con.close()
        df["latest_snapshot_date"] = pd.to_datetime(df["latest_snapshot_date"], errors="coerce").dt.date
        return df
