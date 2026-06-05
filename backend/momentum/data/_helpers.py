"""Retry + chunked metric loader shared across price/volume loads.

`_query_with_retry` wraps a single Supabase call with 502/timeout
retries — kept here because every other loader in this package uses it.
`_load_metric_chunks` is the parallel page-and-chunk fetcher that the
price + volume loaders both call (with different metric codes)."""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from supabase import Client

from common.retry import retry
from deps import IN_CHUNK_SIZE


_MAX_RETRIES = 3
_RETRY_DELAY = 5  # seconds; multiplied by attempt number (linear backoff)

# Worker count for parallel Supabase chunk loads (price + volume reads,
# paginated 50 company_ids per chunk). Bottleneck is the Supabase client's
# connection pool / Cloudflare 502s, not the per-request work. 8 is a
# comfortable default for local Supabase and Cloudflare-fronted prod alike.
_LOAD_PARALLELISM = 8

# Worker count for ECB FX history sync. Bottleneck is the ECB Statistical
# Data Warehouse, which is free and has no documented rate limit but
# regularly times out on full-history XML responses when too many requests
# fire concurrently (we observed CNY read-timeout-60 at 8 workers). 4 still
# gives ~4× speedup over sequential while leaving headroom for ECB to keep
# up. Combined with the retry helper in `fx_rates._ecb_get`, transient
# blips are recovered automatically.
_FX_SYNC_PARALLELISM = 4


def _query_with_retry(query_fn, description: str = "query"):
    """Execute a Supabase query with retry on transient errors (502, timeouts)
    and linear backoff. Thin binding over `common.retry.retry`."""
    return retry(
        query_fn,
        attempts=_MAX_RETRIES,
        base_delay=_RETRY_DELAY,
        backoff="linear",
        description=description,
    )


def _load_metric_chunks(
    supabase: Client,
    company_ids: list[int],
    metric_code: str,
    start_date: date,
    end_date: date,
    on_progress,
    *,
    description_prefix: str,
) -> list[dict]:
    """Bulk-load metric_data rows for the given (metric_code, company_ids,
    date range), running chunk loads in parallel for ~N× wall-time speedup.

    Returns the raw row list (un-deduped, unsorted). Chunks of IN_CHUNK_SIZE
    keep .in_() URLs short enough for Cloudflare; chunks run on a small
    worker pool so we get the benefit of overlapped network RTT without
    saturating the connection pool or upstream rate limits."""
    if not company_ids:
        return []

    page_size = 1000
    chunk_size = IN_CHUNK_SIZE
    chunks = [
        company_ids[i : i + chunk_size]
        for i in range(0, len(company_ids), chunk_size)
    ]
    chunks_total = len(chunks)

    rows: list[dict] = []
    rows_lock = threading.Lock()
    page_counter = [0]
    chunks_done_counter = [0]

    def _load_chunk(chunk_idx_and_chunk: tuple[int, list[int]]) -> None:
        chunk_idx, chunk = chunk_idx_and_chunk
        offset = 0
        while True:
            resp = _query_with_retry(
                lambda o=offset, c=chunk: (
                    supabase.table("metric_data")
                    .select("company_id, target_date, numeric_value")
                    .eq("metric_code", metric_code)
                    .eq("source_code", "gurufocus")
                    .in_("company_id", c)
                    .gte("target_date", start_date.isoformat())
                    .lte("target_date", end_date.isoformat())
                    .order("company_id")
                    .order("target_date")
                    .range(o, o + page_size - 1)
                    .execute()
                ),
                description=f"{description_prefix} chunk {chunk_idx + 1}",
            )
            if not resp.data:
                # Empty chunk still counts as completed for progress.
                with rows_lock:
                    chunks_done_counter[0] += 1
                    chunks_done_now = chunks_done_counter[0]
                if on_progress:
                    on_progress(len(rows), page_counter[0], chunks_done_now, chunks_total)
                break
            is_last_page = len(resp.data) < page_size
            with rows_lock:
                rows.extend(resp.data)
                page_counter[0] += 1
                page_num = page_counter[0]
                total_so_far = len(rows)
                if is_last_page:
                    chunks_done_counter[0] += 1
                chunks_done_now = chunks_done_counter[0]
            if on_progress:
                on_progress(total_so_far, page_num, chunks_done_now, chunks_total)
            if is_last_page:
                break
            offset += page_size

    with ThreadPoolExecutor(max_workers=_LOAD_PARALLELISM) as executor:
        # `list(executor.map(...))` propagates exceptions from worker threads.
        list(executor.map(_load_chunk, list(enumerate(chunks))))

    return rows
