# src/quick_insight/ingest/gurufocus/analyst_estimates/prep_for_db.py
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from quick_insight.ingest.gurufocus.prep_for_db import (
    finalise_long_df,
    make_row,
    resolve_timestamps,
    yyyy_mm_to_month_end,
)


def load_analyst_estimates_long_df(
    *,
    cache_path: str | Path,
    primary_ticker: str,
    primary_exchange: str,
    source_code: str = "gurufocus_api",
    published_at: datetime | None = None,
    imported_at: datetime | None = None,
) -> pd.DataFrame:
    published_at_date, imported_at_dt = resolve_timestamps(published_at, imported_at)

    cache_path = Path(cache_path).expanduser().resolve()
    if not cache_path.exists():
        raise FileNotFoundError(f"Analyst estimate cache file not found: {cache_path}")

    analyst_json = json.loads(cache_path.read_text(encoding="utf-8"))
    rows = []

    for freq in ("annual", "quarterly"):
        block = analyst_json.get(freq) or {}
        dates = block.get("date") or []
        target_dates = {d: yyyy_mm_to_month_end(d).date() for d in dates}
        scalar_target_date = max(target_dates.values()) if target_dates else published_at_date

        for key, value in block.items():
            if key == "date":
                continue
            metric_code = f"{freq}_{key}"
            if isinstance(value, list):
                for d, v in zip(dates, value):
                    td = target_dates.get(d)
                    if td is None:
                        continue
                    rows.append(make_row(
                        primary_ticker=primary_ticker, primary_exchange=primary_exchange,
                        metric_code=metric_code, target_date=td,
                        published_at=published_at_date, imported_at=imported_at_dt,
                        source_code=source_code, value=v, is_prediction=True,
                    ))
            else:
                rows.append(make_row(
                    primary_ticker=primary_ticker, primary_exchange=primary_exchange,
                    metric_code=metric_code, target_date=scalar_target_date,
                    published_at=published_at_date, imported_at=imported_at_dt,
                    source_code=source_code, value=value, is_prediction=True,
                ))

    return finalise_long_df(rows)