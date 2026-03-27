# src/quick_insight/ingest/gurufocus/analyst_estimates/data_lake.py
from __future__ import annotations

from pathlib import Path

from quick_insight.ingest.gurufocus.analyst_estimates.endpoint import (
    analyst_estimate_spec,
)
from quick_insight.ingest.gurufocus.analyst_estimates.utils import (
    analyst_estimate_min_days,
    should_request_guru,
)
from quick_insight.ingest.gurufocus.utils import (
    build_cache_path_with_min_days,
    fetch_guru,
    resolve_cache_file_path,
    write_json_cache,
    GFOutcome
)


def ensure_analyst_estimate_in_data_lake(
    primary_ticker: str,
    primary_exchange: str,
) -> Path | None:
    """
    Ensure the analyst_estimate cache file in the data lake is up to date
    and return the resolved cache path.

    Returns
    -------
    Path | None
        The up-to-date cache file path to use for downstream DB prep/load,
        or None when the exchange is blocked/unsubscribed.
    """
    spec = analyst_estimate_spec(
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
    )

    should_request = should_request_guru(spec=spec)

    if not should_request:
        return resolve_cache_file_path(spec=spec)

    old_cache_path: Path | None = None
    try:
        old_cache_path = resolve_cache_file_path(spec=spec)
    except FileNotFoundError:
        old_cache_path = None

    outcome, response_data = fetch_guru(
        spec=spec,
        use_cache=False,
        return_data=True,
    )

    if outcome == GFOutcome.BLOCK_EXCHANGE:
        return None

    if response_data is None:
        raise RuntimeError(
            f"Guru fetch failed for {primary_ticker} {primary_exchange} "
            f"(outcome={outcome})"
        )

    min_days = analyst_estimate_min_days(data=response_data)
    if min_days is None:
        raise ValueError(
            f"Could not determine analyst estimate min_days for "
            f"{primary_ticker} {primary_exchange}"
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


if __name__ == "__main__":
    path = ensure_analyst_estimate_in_data_lake(
        primary_ticker="AMP",
        primary_exchange="MIL",
    )
    print(path)