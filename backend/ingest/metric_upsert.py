"""Shared batched upsert into `metric_data`.

The price/volume loaders (`ingest.prices`) and the earnings loaders
(`ingest.earnings`) both write time-series rows to `metric_data` keyed on the
same composite natural key. This is the single batched-upsert loop they used to
duplicate verbatim — prices wraps each batch in the transient-retry primitive;
earnings passes pre-validated rows and doesn't.
"""
from __future__ import annotations

from supabase import Client

from common.retry import retry

# metric_data's natural key — the conflict target every metric upsert uses.
METRIC_CONFLICT = "company_id,metric_code,source_code,target_date"


def upsert_metric_rows(
    supabase: Client,
    rows: list[dict],
    *,
    batch_size: int = 500,
    with_retry: bool = False,
    description: str = "metric_data.upsert",
) -> int:
    """Upsert `rows` into `metric_data` in `batch_size` chunks, conflict-keyed
    on `METRIC_CONFLICT`. Returns the number of rows the DB reported written.

    `with_retry=True` wraps each batch in `common.retry.retry` (linear 5xx /
    timeout backoff) — the price/volume path wants that resilience; the
    earnings path passes pre-validated rows and doesn't."""
    if not rows:
        return 0
    total = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]

        def _do(b: list[dict] = batch):
            return supabase.table("metric_data").upsert(
                b, on_conflict=METRIC_CONFLICT, ignore_duplicates=False,
            ).execute()

        resp = (
            retry(_do, base_delay=2, backoff="linear", description=description)
            if with_retry else _do()
        )
        total += len(resp.data or [])
    return total
