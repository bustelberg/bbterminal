# src\quick_insight\ingest\gurufocus\stock_indicator\endpoint.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from urllib.parse import quote

from quick_insight.config.indicators import (
    INDICATOR_ALLOWLIST,
)
from quick_insight.config.config import settings
from quick_insight.ingest.gurufocus.utils import (
    GFOutcome,
    GFSpec,
    build_symbol,
    cache_company_key,
    company_cache_dir,
    fetch_guru,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iter_unique_keep_order(keys: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for k in keys:
        k = str(k).strip()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out


def indicator_spec(
    primary_ticker: str,
    primary_exchange: str,
    key: str,
    type_: str | None = None,
) -> GFSpec:
    """
    Build spec for:
        GET /stock/{symbol}/{indicator_key}
    optionally with:
        ?type={type_}

    Behavior
    --------
    - If type_ is None:
        - omit the type query parameter
        - omit '__none' from the cache filename
    - If type_ is provided:
        - include type in params
        - include it in the cache filename
    """
    symbol = build_symbol(primary_ticker, primary_exchange)
    safe_key = key.replace("/", "_")

    params: dict[str, str] = {
        "symbol": symbol,
        "indicator_key": quote(key, safe=""),
    }

    if type_ is None:
        cache_name = f"indicator__{safe_key}.json"
    else:
        params["type"] = type_
        cache_name = f"indicator__{safe_key}__{type_}.json"

    return GFSpec(
        path_template="stock/{symbol}/{indicator_key}",
        cache_path=company_cache_dir(primary_ticker, primary_exchange) / cache_name,
        params=params,
    )


# ---------------------------------------------------------------------------
# Single-indicator fetcher
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FetchIndicatorResult:
    ok: bool
    error: str | None
    cache_path: Path | None


def fetch_indicator_series(
    primary_ticker: str,
    primary_exchange: str,
    indicator_key: str,
    *,
    type_: str | None = None,
    company_dir: Path | None = None,
    use_cache: bool = True,
    timeout: int = 60,
) -> FetchIndicatorResult:
    """
    Fetch a single indicator series.

    Behavior
    --------
    - If type_ is specified, that exact type is requested
    - If type_ is None, the API request omits type entirely
    """
    del company_dir  # unused

    outcome, cache_path = fetch_guru(
        indicator_spec(primary_ticker, primary_exchange, indicator_key, type_),
        use_cache=use_cache,
        timeout=timeout,
    )

    if outcome == GFOutcome.OK:
        return FetchIndicatorResult(ok=True, error=None, cache_path=cache_path)

    return FetchIndicatorResult(
        ok=False,
        error=f"Fetch failed for {indicator_key} (outcome={outcome})",
        cache_path=None,
    )


# ---------------------------------------------------------------------------
# Parallel bulk fetcher
# ---------------------------------------------------------------------------

def fetch_selected_indicators_for_company(
    primary_ticker: str,
    primary_exchange: str,
    use_cache: bool = True,
    *,
    indicator_keys: Iterable[str] = INDICATOR_ALLOWLIST,
    max_workers: int = 100,
    timeout: int = 60,
    type_: str | None = None,
) -> Path:
    """
    Fetch the provided indicator_keys for a single company in parallel.

    Behavior
    --------
    - If type_ is specified, that exact type is requested for every key
    - If type_ is None, the type parameter is omitted entirely
    """
    company_dir = Path(settings.gurufocus_dir) / cache_company_key(
        primary_ticker,
        primary_exchange,
    )
    company_dir.mkdir(parents=True, exist_ok=True)

    keys = _iter_unique_keep_order(indicator_keys)
    errors: dict[str, dict[str, str]] = {}
    ok_count = 0
    fail_count = 0
    lock = Lock()

    def _fetch_one(key: str) -> tuple[str, bool, dict[str, str]]:
        outcome, _ = fetch_guru(
            indicator_spec(primary_ticker, primary_exchange, key, type_),
            use_cache=use_cache,
            timeout=timeout,
        )
        if outcome == GFOutcome.OK:
            return key, True, {}
        return key, False, {type_ or "omitted": f"outcome={outcome}"}

    print(f"\n[PARALLEL] Fetching {len(keys)} indicators with max_workers={max_workers}")

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_fetch_one, key): key for key in keys}
        done = 0
        for fut in as_completed(futures):
            key = futures[fut]
            done += 1
            try:
                key, any_ok, errors_for_key = fut.result()
                with lock:
                    if any_ok:
                        ok_count += 1
                        print(f"[{done:03d}/{len(keys)}] [OK]   {key}")
                    else:
                        fail_count += 1
                        errors[key] = errors_for_key
                        print(f"[{done:03d}/{len(keys)}] [FAIL] {key} → {errors_for_key}")
            except Exception as e:
                with lock:
                    fail_count += 1
                    errors[key] = {"exception": f"{type(e).__name__}: {e}"}
                    print(f"[{done:03d}/{len(keys)}] [EXC]  {key} → {type(e).__name__}: {e}")

    (company_dir / "errors.json").write_text(
        json.dumps(errors, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"\n--- DONE | {primary_exchange}:{primary_ticker} ---")
    print(f"  ok={ok_count}  failed={fail_count}  see errors.json")

    return company_dir


if __name__ == "__main__":
    result = fetch_indicator_series(
        primary_ticker="NVDA",
        primary_exchange="NASDAQ",
        indicator_key="price",
        use_cache=False,
    )
    print(result)

    # Example: force a specific type
    # result = fetch_indicator_series(
    #     primary_ticker="AAPL",
    #     primary_exchange="NASDAQ",
    #     indicator_key="ebitda_growth",
    #     type_="quarterly",
    #     use_cache=False,
    # )
    # print(result)

    # Example: bulk fetch for one company
    # fetch_selected_indicators_for_company(
    #     primary_ticker="AAPL",
    #     primary_exchange="NASDAQ",
    #     indicator_keys=["price", "volume", "market_cap"],
    #     use_cache=False,
    # )