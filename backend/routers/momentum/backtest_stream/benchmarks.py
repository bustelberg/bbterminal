"""Sector-ETF benchmark price prefetch.

When `selection_mode == "sector_etf"` the backtest holds the mapped ETF
for each picked sector instead of individual stocks. Both the
variants-sweep and single-run paths need the same `(price_index, meta)`
shape, so the read is consolidated here.

Pagination per benchmark_id is critical: a single ETF since 1998 has
~6,886 daily bars and 11 ETFs together exceed 75k rows. A single
`.in_()` query would truncate to ~90 days each and every entry/exit
lookup downstream would silently return None."""
from __future__ import annotations

import asyncio

import pandas as pd

from deps import supabase


async def fetch_benchmark_price_index(
    sector_etfs: dict[str, int] | None,
) -> tuple[dict[int, pd.Series], dict[int, tuple[str, str]]] | tuple[None, None]:
    """Return `(price_index, meta)` for every benchmark referenced by
    `sector_etfs`. Returns `(None, None)` when there are no benchmarks
    to load (mode != sector_etf, or empty mapping).

    `price_index` is `{benchmark_id: pd.Series(price, DatetimeIndex)}`.
    `meta` is `{benchmark_id: (ticker, name)}`.
    """
    if not sector_etfs:
        return None, None
    bm_ids = sorted({int(v) for v in sector_etfs.values()})
    if not bm_ids:
        return None, None

    meta_resp = await asyncio.to_thread(
        lambda: supabase.table("benchmark")
        .select("benchmark_id, ticker, name")
        .in_("benchmark_id", bm_ids)
        .execute()
    )
    meta = {
        int(r["benchmark_id"]): (r["ticker"], r["name"])
        for r in (meta_resp.data or [])
    }

    px_rows: list[dict] = []
    page_size = 1000
    for bid in bm_ids:
        offset = 0
        while True:
            px_resp = await asyncio.to_thread(
                lambda b=bid, o=offset: supabase.table("benchmark_price")
                .select("benchmark_id, target_date, price")
                .eq("benchmark_id", b)
                .order("target_date")
                .range(o, o + page_size - 1)
                .execute()
            )
            batch = px_resp.data or []
            px_rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

    price_index: dict[int, pd.Series] = {}
    if px_rows:
        df_bm = pd.DataFrame(px_rows)
        for bid, group in df_bm.groupby("benchmark_id"):
            price_index[int(bid)] = pd.Series(
                group["price"].values,
                index=pd.DatetimeIndex(group["target_date"]),
                dtype="float64",
            ).sort_index()
    return price_index, meta
