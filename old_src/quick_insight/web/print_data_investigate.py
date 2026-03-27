# scripts/print_debt_to_equity_msft.py
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd


def _find_db_path() -> str:
    """
    Tries common env vars first, then a local quick_insight.config import,
    then falls back to ./quick_insight.duckdb if present.
    """
    import os

    for k in ("DB_PATH", "DUCKDB_PATH", "QUICK_INSIGHT_DB_PATH"):
        v = os.getenv(k)
        if v and Path(v).exists():
            return str(Path(v))

    try:
        from quick_insight.config.config import settings  # type: ignore

        p = getattr(settings, "db_path", None) or getattr(settings, "duckdb_path", None)
        if p and Path(str(p)).exists():
            return str(p)
    except Exception:
        pass

    candidates = [
        Path.cwd() / "quick_insight.duckdb",
        Path.cwd() / "data.duckdb",
        Path.cwd() / "quick_insight.db",
        Path.cwd() / "data.db",
    ]
    for c in candidates:
        if c.exists():
            return str(c)

    raise SystemExit(
        "Could not find DuckDB file. Set DB_PATH or DUCKDB_PATH env var, "
        "or ensure quick_insight.config.settings.(db_path|duckdb_path) is configured."
    )


def main() -> int:
    db_path = _find_db_path()
    print(f"[INFO] Using DuckDB: {db_path}")

    ticker = "MSFT"
    metric_code = "annuals__Balance Sheet__Debt-to-Equity"

    sql = """
    SELECT
      c.company_id,
      c.primary_ticker,
      c.primary_exchange,
      c.company_name,
      m.metric_code,
      s.snapshot_id,
      s.target_date,
      s.published_at,
      s.imported_at,
      fn.source_id,
      fn.is_prediction,
      fn.metric_value
    FROM facts_number fn
    JOIN company  c ON c.company_id  = fn.company_id
    JOIN metric   m ON m.metric_id   = fn.metric_id
    JOIN snapshot s ON s.snapshot_id = fn.snapshot_id
    WHERE c.primary_ticker = ?
      AND m.metric_code = ?
    ORDER BY s.target_date ASC, s.published_at ASC, c.primary_exchange ASC, fn.source_id ASC
    """

    with duckdb.connect(db_path, read_only=True) as con:
        df = con.execute(sql, [ticker, metric_code]).df()

    if df.empty:
        print("[WARN] No rows found.")
        return 0

    df["target_date"] = pd.to_datetime(df["target_date"], errors="coerce")
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    df["imported_at"] = pd.to_datetime(df["imported_at"], errors="coerce")

    print(f"[INFO] Rows: {len(df)}")
    print()

    exch = (
        df[["company_id", "primary_exchange"]]
        .drop_duplicates()
        .sort_values(["primary_exchange", "company_id"])
        .to_string(index=False)
    )
    print("[INFO] company_id / exchange combos:")
    print(exch)
    print()

    pd.set_option("display.max_rows", 5000)
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.width", 200)

    print(df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
