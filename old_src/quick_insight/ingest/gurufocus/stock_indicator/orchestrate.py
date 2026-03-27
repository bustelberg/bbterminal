# src/quick_insight/ingest/gurufocus/stock_indicator/orchestrate.py
from __future__ import annotations

from collections.abc import Iterable

from quick_insight.config.indicators import INDICATOR_ALLOWLIST
from quick_insight.ingest.gurufocus.load_into_db import load_facts_df_into_duckdb
from quick_insight.ingest.gurufocus.stock_indicator.data_lake import (
    ensure_indicator_in_data_lake,
)
from quick_insight.ingest.gurufocus.stock_indicator.prep_for_db import (
    load_indicator_dir_long_df,
)


def orchestrate_indicator(
    *,
    primary_ticker: str,
    primary_exchange: str,
    indicator_key: str,
    use_cache: bool = False,
) -> object:
    """
    Run the full stock-indicator pipeline for a single indicator:
    cache -> prep df -> load into db
    """
    indicator_path = ensure_indicator_in_data_lake(
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
        indicator_key=indicator_key,
        use_cache=use_cache,
    )
    print(indicator_path)

    df = load_indicator_dir_long_df(
        indicators_dir=indicator_path.parent,
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
        indicator_files=[indicator_path.name],
    )

    res = load_facts_df_into_duckdb(df=df)
    print(res)
    return res


def orchestrate_indicators(
    *,
    primary_ticker: str,
    primary_exchange: str,
    indicator_keys: Iterable[str],
    use_cache: bool = False,
) -> dict[str, object]:
    """
    Run the full stock-indicator pipeline for the provided indicator keys.
    """
    results: dict[str, object] = {}

    for indicator_key in indicator_keys:
        print(f"\n=== indicator: {indicator_key} ===")
        results[indicator_key] = orchestrate_indicator(
            primary_ticker=primary_ticker,
            primary_exchange=primary_exchange,
            indicator_key=indicator_key,
            use_cache=use_cache,
        )

    return results


def orchestrate_indicator_allowlist(
    *,
    primary_ticker: str,
    primary_exchange: str,
    use_cache: bool = False,
) -> dict[str, object]:
    """
    Run the full stock-indicator pipeline for all indicators in INDICATOR_ALLOWLIST.
    """
    return orchestrate_indicators(
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
        indicator_keys=INDICATOR_ALLOWLIST,
        use_cache=use_cache,
    )


if __name__ == "__main__":
    orchestrate_indicator(
        primary_ticker="NVDA",
        primary_exchange="NASDAQ",
        indicator_key="price",
        use_cache=False,
    )

    # or:
    # orchestrate_indicators(
    #     primary_ticker="NVDA",
    #     primary_exchange="NASDAQ",
    #     indicator_keys=["price", "free_cash_flow", "forward_pe_ratio"],
    #     use_cache=False,
    # )

    # or:
    # orchestrate_indicator_allowlist(
    #     primary_ticker="NVDA",
    #     primary_exchange="NASDAQ",
    #     use_cache=False,
    # )