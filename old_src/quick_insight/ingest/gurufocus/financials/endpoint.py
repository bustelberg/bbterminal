# src\quick_insight\ingest\gurufocus\financials\endpoint.py
from __future__ import annotations

from pathlib import Path

from quick_insight.ingest.gurufocus.utils import (
    GFOutcome,
    GFSpec,
    build_symbol,
    company_cache_dir,
    fetch_guru,
)


def financials_spec(
    primary_ticker: str,
    primary_exchange: str,
    *,
    order: str = "desc",
) -> GFSpec:
    """GET /stock/{symbol}/financials?order={order}"""
    symbol = build_symbol(primary_ticker, primary_exchange)
    return GFSpec(
        path_template="stock/{symbol}/financials",
        cache_path=company_cache_dir(primary_ticker, primary_exchange) / "financials.json",
        params={"symbol": symbol, "order": order},
    )


def fetch_financials(
    primary_ticker: str,
    primary_exchange: str,
    use_cache: bool = True,
    *,
    order: str = "desc",
) -> tuple[GFOutcome, Path | None]:
    return fetch_guru(
        financials_spec(primary_ticker, primary_exchange, order=order),
        use_cache=use_cache,
    )


if __name__ == "__main__":
    fetch_financials(primary_ticker="AAPL", primary_exchange="NASDAQ")