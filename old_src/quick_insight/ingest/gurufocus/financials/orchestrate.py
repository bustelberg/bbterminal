# src/quick_insight/ingest/gurufocus/financials/orchestrate.py
from __future__ import annotations

from quick_insight.ingest.gurufocus.financials.data_lake import (
    ensure_financials_in_data_lake,
)
from quick_insight.ingest.gurufocus.financials.prep_for_db import (
    load_financials_long_df,
)
from quick_insight.ingest.gurufocus.load_into_db import load_facts_df_into_duckdb


def orchestrate_financials(
    primary_ticker: str,
    primary_exchange: str,
) -> None:
    cache_path = ensure_financials_in_data_lake(
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
    )
    df = load_financials_long_df(
        cache_path=cache_path,
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
    )
    res = load_facts_df_into_duckdb(df=df)
    print(res)


if __name__ == "__main__":
    orchestrate_financials(primary_ticker="AAPL", primary_exchange="NASDAQ")