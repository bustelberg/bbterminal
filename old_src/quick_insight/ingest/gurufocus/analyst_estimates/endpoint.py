# src\quick_insight\ingest\gurufocus\analyst_estimates\endpoint.py
from __future__ import annotations

from pathlib import Path

from quick_insight.ingest.gurufocus.utils import (
    GFOutcome,
    GFSpec,
    build_symbol,
    company_cache_dir,
    fetch_guru,
)


def analyst_estimate_spec(primary_ticker: str, primary_exchange: str) -> GFSpec:
    """GET /stock/{symbol}/analyst_estimate"""
    symbol = build_symbol(primary_ticker, primary_exchange)
    return GFSpec(
        path_template="stock/{symbol}/analyst_estimate",
        cache_path=company_cache_dir(primary_ticker, primary_exchange) / "analyst_estimate.json",
        params={"symbol": symbol},
        block_on_unsubscribed=True,
    )


def fetch_analyst_estimate(
    primary_ticker: str,
    primary_exchange: str,
    use_cache: bool = True,
) -> tuple[GFOutcome, Path | None]:
    return fetch_guru(
        analyst_estimate_spec(primary_ticker, primary_exchange),
        use_cache=use_cache,
    )


if __name__ == "__main__":
    fetch_analyst_estimate(primary_ticker="AAPL", primary_exchange="NASDAQ")