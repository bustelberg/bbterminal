# src\quick_insight\ingest\gurufocus\get_companies_from_db.py
from __future__ import annotations

import duckdb
import pandas as pd

from quick_insight.config.config import settings


# ============================================================
# CONNECTION
# ============================================================
def _db_path() -> str:
    p = getattr(settings, "db_path", None) or getattr(settings, "duckdb_path", None)
    if not p:
        raise RuntimeError("No DuckDB path configured. Expected settings.db_path or settings.duckdb_path.")
    return str(p)


def _connect(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(_db_path(), read_only=read_only)


# ============================================================
# FETCH ALL TICKER / EXCHANGE PAIRS
# ============================================================
def fetch_all_tickers_and_exchanges() -> pd.DataFrame:
    """
    Return all distinct primary_ticker / primary_exchange pairs.
    """
    sql = """
    SELECT DISTINCT
        primary_ticker,
        primary_exchange
    FROM company
    WHERE primary_ticker IS NOT NULL
      AND primary_exchange IS NOT NULL
    ORDER BY primary_exchange, primary_ticker
    """
    with _connect(read_only=True) as con:
        return con.execute(sql).df()

# ============================================================
# COUNT BY US VS NON-US
# ============================================================
def count_us_vs_non_us(df: pd.DataFrame) -> None:
    """
    Print counts of companies listed on:
    - NASDAQ or NYSE (US)
    - All other exchanges (Non-US)
    """
    us_mask = df["primary_exchange"].isin(["NASDAQ", "NYSE"])

    us_count = int(us_mask.sum())
    non_us_count = int((~us_mask).sum())

    print("\n--- Company Counts ---")
    print(f"US (NASDAQ + NYSE): {us_count}")
    print(f"Non-US:             {non_us_count}")
    print(f"Total:              {len(df)}")

def count_per_exchange(df: pd.DataFrame) -> None:
    counts = (
        df.groupby("primary_exchange")
        .size()
        .sort_values(ascending=False)
    )

    print("\n--- Companies per exchange ---\n")
    print(counts.to_string())
# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    df = fetch_all_tickers_and_exchanges()

    if df.empty:
        print("No tickers found in database.")
    else:
        print("\nAll tickers in database:\n")
        print(df.to_string(index=False))
        print(f"\nTotal companies: {len(df)}")

        # Insights
        count_us_vs_non_us(df)
        count_per_exchange(df)
