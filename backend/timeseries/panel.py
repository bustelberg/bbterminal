"""Long -> wide. The step that actually costs time.

Measured on 1,000 assets x 5.58M rows (2026-07-10):

    COPY load          10,180 ms  (85%)
    pandas .pivot()     1,591 ms  (13%)
    12-1 momentum         200 ms  ( 2%)

`pandas.pivot`/`pivot_table` build an intermediate MultiIndex and re-sort. A
`factorize` on each axis plus a single numpy scatter produces the identical
matrix in 232 ms — 7x faster — because it never materializes the index.

The signal itself is 2% of the run. Optimize the load and the reshape; never the
signal.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .loader import DATE_COL, ENTITY_COL


def to_panel(
    df: pd.DataFrame,
    value: str,
    *,
    entity_col: str = ENTITY_COL,
    date_col: str = DATE_COL,
    dtype: str = "float64",
) -> pd.DataFrame:
    """Long `[entity, date, value]` -> wide `index=date, columns=entity`.

    Equivalent to `df.pivot_table(index=date_col, columns=entity_col,
    values=value).sort_index()` for de-duplicated input, which is guaranteed by
    the primary keys upstream (`metric_data` on
    `(company_id, metric_code, source_code, target_date)`, `asset_price` on
    `(analysis_id, target_date)`).

    DIVERGENCE ON DUPLICATES: `pivot_table` averages them, this takes the last
    write. Duplicates cannot occur through `load_series`, but if you hand this a
    frame from somewhere else, know which one you want.
    """
    if df.empty:
        return pd.DataFrame(dtype=dtype)

    dates, date_uniq = pd.factorize(df[date_col], sort=True)
    ents, ent_uniq = pd.factorize(df[entity_col], sort=True)

    arr = np.full((len(date_uniq), len(ent_uniq)), np.nan, dtype=dtype)
    arr[dates, ents] = df[value].to_numpy(dtype=dtype)

    return pd.DataFrame(
        arr,
        index=pd.Index(date_uniq, name=date_col),
        columns=pd.Index(ent_uniq, name=entity_col),
    )
