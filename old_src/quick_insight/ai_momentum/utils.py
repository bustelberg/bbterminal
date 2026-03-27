# src\quick_insight\ai_momentum\utils.py

from __future__ import annotations

import pandas as pd

from quick_insight.config.config import settings
from quick_insight.db import fetch_df

QUERY = """
WITH latest_snapshot AS (
    SELECT
        sn.snapshot_id,
        sn.target_date
    FROM snapshot sn
    JOIN facts_number fn ON fn.snapshot_id = sn.snapshot_id
    JOIN source       sr ON sr.source_id   = fn.source_id
    WHERE sr.source_code = 'longequity'
    ORDER BY sn.target_date DESC
    LIMIT 1
)
SELECT
    c.sector,
    c.company_name,
    c.primary_ticker,
    c.primary_exchange,
    ls.target_date AS snapshot_date
FROM company c
JOIN facts_number fn    ON fn.company_id   = c.company_id
JOIN source       sr    ON sr.source_id    = fn.source_id
JOIN latest_snapshot ls ON ls.snapshot_id  = fn.snapshot_id
WHERE sr.source_code = 'longequity'
GROUP BY
    c.sector,
    c.company_name,
    c.primary_ticker,
    c.primary_exchange,
    ls.target_date
ORDER BY
    c.sector,
    c.company_name
"""


def get_universe() -> tuple[pd.Timestamp, pd.DataFrame]:
    """
    Returns the latest longequity snapshot date and a DataFrame with columns:
      sector | company_name | primary_ticker | primary_exchange
    """
    raw = fetch_df(settings.db_path, QUERY)

    if raw.empty:
        raise RuntimeError("No data found for source 'longequity'.")

    snapshot_date = pd.Timestamp(raw["snapshot_date"].iloc[0])
    df = raw[["sector", "company_name", "primary_ticker", "primary_exchange"]].copy()

    return snapshot_date, df


if __name__ == "__main__":
    snapshot_date, df = get_universe()

    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_colwidth", 40)

    print(f"Universe as of: {snapshot_date.date()}  ({len(df)} companies)\n")
    print(df.to_string(index=False))


def aggregate_to_sector(
    df: pd.DataFrame,
    score_cols: list[str],
    *,
    group_col: str = "sector",
) -> pd.DataFrame:
    return (
        df.groupby(group_col)[score_cols]
        .mean()
        .reset_index()
        .rename(columns={c: c for c in score_cols})  # passthrough, names stay the same
    )