"""Automatic AIRS/Yahoo price-check flow, shared by timer and manual dispatch.

Admission, queue waiting policy and run recording belong to the caller. This
flow refreshes watched prices once, saves comparisons from those stored prices,
and preserves partial results before reporting an incomplete outcome.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from scheduler_runtime.execution import Cancelled, require_complete


@dataclass(frozen=True)
class PriceCheckProviders:
    watched_isins: Callable
    refresh_prices: Callable
    save_checks: Callable


def _default_providers() -> PriceCheckProviders:
    from asset_pipeline import price_refresh
    from routers._airs_price_check import refresh_price_checks

    return PriceCheckProviders(price_refresh.watched_isins,
                               price_refresh.refresh_stale, refresh_price_checks)


def run(ctx=None, *, report: Callable, wait_for_queue: Callable,
        providers: PriceCheckProviders | None = None) -> tuple[str, dict]:
    providers = providers or _default_providers()
    step = report
    step(0, 0, "Refreshing Yahoo closes and saved AIRS price checks")
    stop = (lambda: bool(getattr(ctx, "cancelled", False))) if ctx is not None else None
    if stop and stop():
        raise Cancelled("stopped before automatic price checks")
    wait_for_queue(ctx)
    # Do not rely on _freshen's per-process daily memo: noon may already have checked these
    # instruments before today's close existed. One batch also avoids the per-account fetch cap.
    prices = providers.refresh_prices(isins=providers.watched_isins(), stale_days=1)
    try:
        result = providers.save_checks(should_stop=stop, freshen=False, only_if_changed=True, on_step=step)
    except Cancelled:
        raise
    except Exception as exc:
        require_complete(
            f"Automatic price-check persistence failed: {type(exc).__name__}: {exc}. "
            "Completed Yahoo updates are retained; next: inspect the saved checks error, then retry airs_price_checks.",
            {'prices': prices, 'errors': {'checks': f'{type(exc).__name__}: {exc}'}},
        )
    result["prices"] = prices
    if stop and stop():
        raise Cancelled("stopped automatic price checks; saved checks are kept")
    if result.get("errors") or prices.get("failed"):
        instruments = [
            f"{row.get('symbol') or 'symbol absent'} / {row.get('isin') or 'ISIN absent'} "
            f"(analysis_id={row.get('analysis_id')}, stored={row.get('latest_close')}, "
            f"expected={row.get('expected_close')}, provider_latest={row.get('vendor_latest_close') or 'unconfirmed'}): "
            f"{row.get('error') or row.get('reason') or 'no failure detail recorded'}"
            for row in prices.get('results', []) if row.get('status') == 'error'
        ]
        detail = (f"Automatic price checks incomplete: {prices.get('failed', 0)} Yahoo refreshes failed; "
                  f"{result.get('saved', 0)} account checks saved. "
                  + '; '.join(instruments)
                  + (f"; account check errors: {result['errors']}" if result.get('errors') else '')
                  + ' Completed updates are retained. Next: resolve the instrument/provider or account-check error, then retry airs_price_checks.')
        require_complete(detail, result)
    return f"{result['saved']} account price checks refreshed", result
