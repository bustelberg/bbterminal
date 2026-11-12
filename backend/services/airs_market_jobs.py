"""AIRS market-data job workflows; HTTP adapters own validation and job identity."""
from __future__ import annotations


def repair_risk_history(ctx, *, isins, years, full_history, yahoo_only) -> str:
    from asset_pipeline.price_refresh import repair_price_histories

    ctx.progress(0, len(isins), "Checking price and FX history for the risk views…")
    ctx.check()
    repaired = repair_price_histories(isins, years=years, full_history=full_history, yahoo_only=yahoo_only)
    ctx.check()
    ctx.progress(len(isins), len(isins), "Risk price history checked")
    return (f"Risk price history checked for {len(isins)} instruments "
            f"({repaired.get('backfill', {}).get('stored', 0)} price histories stored)")


def refresh_monthly_benchmark(ctx, *, portefeuille, month) -> str:
    from routers._monthly_benchmark import ensure

    ensure(portefeuille, month, ctx)
    return 'Benchmark updated.'


def refresh_account_prices(ctx, *, portefeuille) -> str:
    from routers._airs_price_check import save_price_check

    ctx.progress(0, 0, f"Refreshing stale price checks for {portefeuille}...")
    ctx.check()
    result = save_price_check(portefeuille)['result']
    ctx.check()
    count = len(result.get("rows") or [])
    ctx.progress(1, 1, f"Price checks refreshed for {count} holdings")
    return f"Price checks refreshed for {count} holdings"


def refresh_holding_price(ctx, *, portefeuille, normalized) -> str:
    from routers._airs_price_check import recheck_price_check_row

    ctx.progress(0, 0, f"Refreshing Yahoo price for {normalized}...")
    ctx.check()
    recheck_price_check_row(portefeuille, normalized)
    ctx.check()
    ctx.progress(1, 1, f"Price refreshed for {normalized}")
    return f"Price refreshed for {normalized}"


def refresh_sold_price(ctx, *, normalized) -> str:
    from routers._airs_price_check import refresh_sold_holding_price

    ctx.check()
    ctx.progress(0, 1, "Checking the latest stock price…")
    summary = refresh_sold_holding_price(normalized)
    ctx.progress(1, 1, summary)
    return summary
