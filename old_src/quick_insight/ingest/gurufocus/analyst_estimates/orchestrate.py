# src/quick_insight/ingest/gurufocus/analyst_estimates/orchestrate.py
from __future__ import annotations
from quick_insight.ingest.gurufocus.analyst_estimates.data_lake import ensure_analyst_estimate_in_data_lake
from quick_insight.ingest.gurufocus.analyst_estimates.prep_for_db import load_analyst_estimates_long_df
from quick_insight.ingest.gurufocus.load_into_db import load_facts_df_into_duckdb


def orchestrate_analyst_estimates(
    primary_ticker: str,
    primary_exchange: str,
) -> bool | None:
    cache_path = ensure_analyst_estimate_in_data_lake(
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
    )

    if cache_path is None:
        return None

    df = load_analyst_estimates_long_df(
        cache_path=cache_path,
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
    )
    res = load_facts_df_into_duckdb(df=df)
    print(res)
    return True


if __name__ == "__main__":
    orchestrate_analyst_estimates(primary_ticker="00388", primary_exchange="HKSE")