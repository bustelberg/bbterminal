"""One interface for every per-entity time series, whatever table it lives in.

Phase 1 of the engine unification (see the module docstrings in `registry.py`
and `loader.py`). Nothing here changes what any query returns — it centralizes
WHERE the query is written, so a single signal engine can later consume price,
volume and fundamental series through the same call without learning which
vendor or id space each came from.

    from timeseries import load_series, to_panel

    df = load_series(company_ids, "gf.close", start, end)   # entity_id, date, close
    panel = to_panel(df, "close")                            # index=date, cols=entity
"""
from .loader import DATE_COL, ENTITY_COL, SeriesUnavailable, load_series
from .panel import to_panel
from .registry import SERIES, SeriesSpec, resolve

__all__ = [
    "DATE_COL",
    "ENTITY_COL",
    "SERIES",
    "SeriesSpec",
    "SeriesUnavailable",
    "load_series",
    "resolve",
    "to_panel",
]
