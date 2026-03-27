# src\quick_insight\ingest\gurufocus\stock_indicator\data_lake.py
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Iterable

from quick_insight.config.config import settings
from quick_insight.config.indicators import (
    INDICATOR_ALLOWLIST,
)
from quick_insight.ingest.gurufocus.stock_indicator.endpoint import (
    indicator_spec,
)
from quick_insight.ingest.gurufocus.stock_indicator.utils import (
    should_request_guru,
    stock_indicator_min_days,
)
from quick_insight.ingest.gurufocus.utils import (
    GFOutcome,
    build_cache_path_with_min_days,
    cache_company_key,
    fetch_guru,
    resolve_cache_file_path,
    write_json_cache,
)


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------

class GuruFocusForbiddenRegionError(RuntimeError):
    """Raised when GuruFocus returns a region/exchange access restriction."""
    pass


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


def _pick_probe_key(keys: list[str]) -> str:
    """
    Prefer 'price' as the cheap/representative probe.
    Otherwise use the first key in order.
    """
    if "price" in keys:
        return "price"
    return keys[0]


# ---------------------------------------------------------------------------
# Single-indicator data-lake resolver
# ---------------------------------------------------------------------------

def ensure_indicator_in_data_lake(
    primary_ticker: str,
    primary_exchange: str,
    indicator_key: str,
    *,
    type_: str | None = None,
    timeout: int = 60,
    use_cache: bool = False,
) -> Path:
    """
    Ensure a single stock-indicator cache file is available and return its path.

    Behavior
    --------
    - If use_cache=True:
        - return any existing cache file immediately, even if outdated
    - If use_cache=False:
        - use existing fresh cache if possible
        - otherwise fetch from Guru
    - Treats [] / {} as valid empty payloads
    - Raises GuruFocusForbiddenRegionError on blocked exchange
    - By default omits `type` and lets the API determine the best fit
    """
    spec = indicator_spec(
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
        key=indicator_key,
        type_=type_,
    )

    # -------------------------------------------------------------------
    # Hard cache short-circuit:
    # if use_cache=True, return any existing cache regardless of age
    # -------------------------------------------------------------------
    if use_cache:
        try:
            return resolve_cache_file_path(spec=spec)
        except (FileNotFoundError, ValueError):
            pass

    # -------------------------------------------------------------------
    # Freshness-aware cache short-circuit
    # -------------------------------------------------------------------
    try:
        if not should_request_guru(spec=spec):
            return resolve_cache_file_path(spec=spec)
    except (FileNotFoundError, ValueError):
        pass

    # -------------------------------------------------------------------
    # Stale or missing -> fetch
    # -------------------------------------------------------------------
    old_cache_path: Path | None = None
    try:
        old_cache_path = resolve_cache_file_path(spec=spec)
    except FileNotFoundError:
        old_cache_path = None

    outcome, response_data = fetch_guru(
        spec=spec,
        use_cache=False,
        timeout=timeout,
        return_data=True,
    )

    if outcome == GFOutcome.BLOCK_EXCHANGE:
        raise GuruFocusForbiddenRegionError(
            f"GuruFocus access forbidden for {primary_exchange}:{primary_ticker} "
            f"| indicator={indicator_key} | type={type_ or 'omitted'} "
            f"| outcome={outcome}"
        )

    if outcome != GFOutcome.OK or response_data is None:
        raise RuntimeError(
            f"Fetch failed for {primary_exchange}:{primary_ticker} "
            f"| indicator={indicator_key} | type={type_ or 'omitted'} "
            f"| outcome={outcome}"
        )

    is_empty_payload = response_data == [] or response_data == {}

    if is_empty_payload:
        new_cache_path = spec.cache_path
        new_cache_path.parent.mkdir(parents=True, exist_ok=True)
        write_json_cache(cache_path=new_cache_path, data=response_data)

        if old_cache_path is not None and old_cache_path != new_cache_path:
            old_cache_path.unlink(missing_ok=True)

        return new_cache_path

    min_days = stock_indicator_min_days(response_data)
    if min_days is None:
        raise ValueError(
            f"Could not determine stock-indicator min_days for "
            f"{primary_exchange}:{primary_ticker} | "
            f"indicator={indicator_key} | type={type_ or 'omitted'}"
        )

    new_cache_path = build_cache_path_with_min_days(
        spec=spec,
        min_days=min_days,
    )
    new_cache_path.parent.mkdir(parents=True, exist_ok=True)
    write_json_cache(cache_path=new_cache_path, data=response_data)

    if old_cache_path is not None and old_cache_path != new_cache_path:
        old_cache_path.unlink(missing_ok=True)

    return new_cache_path


# ---------------------------------------------------------------------------
# Bulk data-lake resolver
# ---------------------------------------------------------------------------

def ensure_selected_indicators_in_data_lake(
    primary_ticker: str,
    primary_exchange: str,
    *,
    indicator_keys: Iterable[str] = INDICATOR_ALLOWLIST,
    max_workers: int = 20,
    timeout: int = 60,
    type_: str | None = None,
) -> Path:
    """
    Ensure the provided indicator_keys are up to date for a single company.

    Behavior
    --------
    1. Probe one indicator first (prefer 'price').
    2. If probe hits blocked exchange access, stop immediately.
    3. Otherwise run the remaining indicators in parallel.
    4. If one of the parallel tasks later hits blocked exchange, cancel pending
       work where possible and raise GuruFocusForbiddenRegionError.
    """
    company_dir = Path(settings.gurufocus_dir) / cache_company_key(
        primary_ticker,
        primary_exchange,
    )
    company_dir.mkdir(parents=True, exist_ok=True)

    keys = _iter_unique_keep_order(indicator_keys)
    if not keys:
        (company_dir / "errors.json").write_text(
            json.dumps({}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"\n--- DONE | {primary_exchange}:{primary_ticker} ---")
        print("  ok=0  failed=0  see errors.json")
        return company_dir

    errors: dict[str, dict[str, str]] = {}
    ok_count = 0
    fail_count = 0
    lock = Lock()

    print(
        f"\n[PARALLEL] Ensuring {len(keys)} indicators "
        f"for {primary_exchange}:{primary_ticker} with max_workers={max_workers}"
    )

    # -----------------------------------------------------------------------
    # Probe first
    # -----------------------------------------------------------------------
    probe_key = _pick_probe_key(keys)
    print(f"[PROBE] Trying {probe_key} first for {primary_exchange}:{primary_ticker}")

    try:
        probe_path = ensure_indicator_in_data_lake(
            primary_ticker=primary_ticker,
            primary_exchange=primary_exchange,
            indicator_key=probe_key,
            type_=type_,
            timeout=timeout,
        )
        ok_count += 1
        print(f"[001/{len(keys)}] [OK]   {probe_key} → {probe_path}")

    except GuruFocusForbiddenRegionError as e:
        fail_count += 1
        errors[probe_key] = {"error": str(e)}
        print(f"[001/{len(keys)}] [FORBIDDEN] {probe_key} → {e}")

        (company_dir / "errors.json").write_text(
            json.dumps(errors, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        raise

    except Exception as e:
        fail_count += 1
        errors[probe_key] = {"error": f"{type(e).__name__}: {e}"}
        print(f"[001/{len(keys)}] [FAIL] {probe_key} → {type(e).__name__}: {e}")

    # -----------------------------------------------------------------------
    # Parallel for the rest
    # -----------------------------------------------------------------------
    remaining_keys = [k for k in keys if k != probe_key]
    if not remaining_keys:
        (company_dir / "errors.json").write_text(
            json.dumps(errors, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"\n--- DONE | {primary_exchange}:{primary_ticker} ---")
        print(f"  ok={ok_count}  failed={fail_count}  see errors.json")
        return company_dir

    def _ensure_one(key: str) -> tuple[str, Path]:
        path = ensure_indicator_in_data_lake(
            primary_ticker=primary_ticker,
            primary_exchange=primary_exchange,
            indicator_key=key,
            type_=type_,
            timeout=timeout,
        )
        return key, path

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_ensure_one, key): key for key in remaining_keys}
        done = 1  # probe already counted
        forbidden_error: GuruFocusForbiddenRegionError | None = None

        try:
            for fut in as_completed(futures):
                key = futures[fut]
                done += 1

                try:
                    key, path = fut.result()
                    with lock:
                        ok_count += 1
                        print(f"[{done:03d}/{len(keys)}] [OK]   {key} → {path}")

                except GuruFocusForbiddenRegionError as e:
                    with lock:
                        fail_count += 1
                        errors[key] = {"error": str(e)}
                        print(f"[{done:03d}/{len(keys)}] [FORBIDDEN] {key} → {e}")

                    forbidden_error = e

                    for other_fut in futures:
                        if other_fut is not fut:
                            other_fut.cancel()

                    break

                except Exception as e:
                    with lock:
                        fail_count += 1
                        errors[key] = {"error": f"{type(e).__name__}: {e}"}
                        print(
                            f"[{done:03d}/{len(keys)}] [FAIL] {key} → "
                            f"{type(e).__name__}: {e}"
                        )

        finally:
            (company_dir / "errors.json").write_text(
                json.dumps(errors, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )

        if forbidden_error is not None:
            raise forbidden_error

    print(f"\n--- DONE | {primary_exchange}:{primary_ticker} ---")
    print(f"  ok={ok_count}  failed={fail_count}  see errors.json")

    return company_dir


if __name__ == "__main__":
    ensure_selected_indicators_in_data_lake(
        primary_ticker="NVDA",
        primary_exchange="NASDAQ",
        max_workers=20,
    )