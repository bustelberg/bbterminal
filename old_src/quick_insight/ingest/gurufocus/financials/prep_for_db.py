# src/quick_insight/ingest/gurufocus/financials/prep_for_db.py
from __future__ import annotations

import json
from datetime import datetime
from itertools import zip_longest
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from quick_insight.ingest.gurufocus.prep_for_db import (
    finalise_long_df,
    make_row,
    resolve_timestamps,
    yyyy_mm_to_month_end,
)


def _iter_blocks(financials: dict[str, Any]) -> Iterable[tuple[str, dict[str, Any]]]:
    for k in ("annuals", "quarterly", "quarterlys", "quarters"):
        block = financials.get(k)
        if isinstance(block, dict) and block:
            yield k, block


def _detect_period_key(block: dict[str, Any]) -> str:
    for c in ("Fiscal Year", "Fiscal Quarter", "Quarter", "Date", "date", "fiscal_year", "fiscal_quarter"):
        if c in block and isinstance(block[c], list) and block[c]:
            return c
    raise ValueError(f"Could not find period column in block keys: {list(block.keys())[:30]}")


def _flatten_leaf_series(node: Any, *, prefix_parts: list[str]) -> Iterable[tuple[list[str], Any]]:
    if isinstance(node, dict):
        for k, v in node.items():
            yield from _flatten_leaf_series(v, prefix_parts=prefix_parts + [str(k)])
    else:
        yield prefix_parts, node


def load_financials_long_df(
    *,
    cache_path: str | Path,
    primary_ticker: str,
    primary_exchange: str,
    source_code: str = "gurufocus_api",
    published_at: datetime | None = None,
    imported_at: datetime | None = None,
    include_ttm: bool = True,
) -> pd.DataFrame:
    published_at_date, imported_at_dt = resolve_timestamps(published_at, imported_at)

    cache_path = Path(cache_path).expanduser().resolve()
    if not cache_path.exists():
        raise FileNotFoundError(f"Financials cache file not found: {cache_path}")

    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    financials = payload.get("financials")
    if not isinstance(financials, dict):
        raise ValueError("JSON does not contain a top-level 'financials' object.")

    rows = []

    for block_name, block in _iter_blocks(financials):
        period_key = _detect_period_key(block)
        period_strs = [str(p).strip() for p in (block.get(period_key) or [])]

        target_dates: dict[str, Any] = {}
        latest_non_ttm = None
        for ps in period_strs:
            if ps.upper() == "TTM":
                continue
            try:
                td = yyyy_mm_to_month_end(ps).date()
            except Exception:
                continue
            target_dates[ps] = td
            if latest_non_ttm is None or td > latest_non_ttm:
                latest_non_ttm = td

        if include_ttm and any(ps.upper() == "TTM" for ps in period_strs):
            target_dates["TTM"] = latest_non_ttm or published_at_date

        for top_key, top_val in block.items():
            if top_key == period_key:
                continue
            for path_parts, leaf in _flatten_leaf_series(top_val, prefix_parts=[block_name, str(top_key)]):
                metric_code = "__".join(path_parts)
                if isinstance(leaf, list):
                    for ps, v in zip_longest(period_strs, leaf, fillvalue=None):
                        if ps is None or (ps.upper() == "TTM" and not include_ttm):
                            continue
                        td = target_dates.get(ps)
                        if td is None:
                            continue
                        rows.append(make_row(
                            primary_ticker=primary_ticker, primary_exchange=primary_exchange,
                            metric_code=metric_code, target_date=td,
                            published_at=published_at_date, imported_at=imported_at_dt,
                            source_code=source_code, value=v, is_prediction=False,
                        ))
                else:
                    rows.append(make_row(
                        primary_ticker=primary_ticker, primary_exchange=primary_exchange,
                        metric_code=metric_code, target_date=published_at_date,
                        published_at=published_at_date, imported_at=imported_at_dt,
                        source_code=source_code, value=leaf, is_prediction=False,
                    ))

    return finalise_long_df(rows)