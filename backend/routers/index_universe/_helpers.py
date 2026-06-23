"""Shared helpers for the index-universe routers.

`_enrich_tickers` is the small "join company info onto raw membership
rows" used by the generic per-index reads. The two SSE drainers cover
the existing patterns in the original file — one drains a queue fed by
an executor-launched `_run`, the other drains a queue fed by a daemon
thread (the older ACWI write paths). Behavior is byte-identical to the
inline versions; consolidated here so the per-domain files stay focused
on what they actually do."""
from __future__ import annotations

import asyncio
import queue as _queue

from deps import supabase, fetch_in_chunks
from routers._sse import sse_keepalive, sse_raw


# Module-level cache for the universe-stats list. The underlying view does
# COUNT(DISTINCT universe_ticker) over the full universe_membership table,
# which sometimes trips Supabase's 8s statement_timeout once the table grows
# past ~500k rows (S&P 500 history × ACWI × monthly entries). Reads change
# rarely (only after an index ingest), so a 5-minute TTL avoids paying that
# cost on every dropdown render. On timeout we fall back to a stale cached
# entry if we have one, then to a cheap universe-table-only read so the UI
# still loads — month/ticker counts come back as 0 in that degraded mode.
_UNIVERSE_STATS_CACHE: dict = {"ts": 0.0, "data": None}
_UNIVERSE_STATS_TTL = 300.0


def fetch_all_membership(
    universe_id: int,
    select_cols: str,
    *,
    month: str | None = None,
    order: str | None = None,
) -> list[dict]:
    """Fetch ALL `universe_membership` rows for a universe, paginating past the
    PostgREST `db-max-rows` cap (1000 on cloud, 10000 local).

    A single `.limit(100000)` does NOT bypass that cap — the server truncates the
    response regardless — which is why a 1487-company frozen universe only showed
    1000 rows. A windowed `.range()` loop does, since each page requests a window
    within the cap and we keep going until a short page. A `company_id` tiebreaker
    is always appended to the sort: `range()` over a non-unique order silently
    skips/duplicates rows across page boundaries (see project_postgrest_max_rows_trap).
    """
    rows: list[dict] = []
    offset = 0
    page = 1000
    while True:
        q = (
            supabase.table("universe_membership")
            .select(select_cols)
            .eq("universe_id", universe_id)
        )
        if month is not None:
            q = q.eq("target_month", month)
        if order is not None:
            q = q.order(order)
        resp = q.order("company_id").range(offset, offset + page - 1).execute()
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return rows


def _enrich_tickers(rows: list[dict]) -> list[dict]:
    """Join company info onto raw membership ticker rows.

    Returns each company's display attributes — the same set the `/companies`
    table shows — so a frozen-universe inspection (FrozenUniversesPanel) can
    render Country / Sector / Mkt Cap / OpenFIGI columns identical to /companies.
    `sector` is read from the membership row (it's stored per-universe); the
    rest come from the `company` dimension. All fields are additive."""
    from ingest.gurufocus_url import gurufocus_url, pad_hkse_ticker  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415
    company_ids = [r["company_id"] for r in rows if r["company_id"]]
    company_info: dict[int, dict] = {}
    for c in fetch_in_chunks(
        company_ids,
        lambda chunk: supabase.table("company").select(
            "company_id, company_name, isin, gurufocus_ticker, "
            "delisted_at, out_of_scope_at, out_of_scope_reason, "
            "market_cap_eur, market_cap_date, market_cap_native, "
            "market_cap_currency, market_cap_fx_rate, "
            "openfigi_status, openfigi_name, openfigi_checked_at, "
            "gurufocus_exchange:gurufocus_exchange("
            "exchange_code, country:country(country_name))"
        ).in_("company_id", chunk).execute(),
    ):
        exch_info = c.get("gurufocus_exchange") or {}
        country_info = exch_info.get("country") or {}
        code = exch_info.get("exchange_code") or ""
        company_info[c["company_id"]] = {
            "company_name": c.get("company_name") or "",
            "isin": c.get("isin") or "",
            "exchange": code,
            "gurufocus_ticker": c.get("gurufocus_ticker") or "",
            "country": country_info.get("country_name"),
            "delisted_at": c.get("delisted_at"),
            "out_of_scope_at": c.get("out_of_scope_at"),
            "out_of_scope_reason": c.get("out_of_scope_reason"),
            "market_cap_eur": c.get("market_cap_eur"),
            "market_cap_date": c.get("market_cap_date"),
            "market_cap_native": c.get("market_cap_native"),
            "market_cap_currency": c.get("market_cap_currency"),
            "market_cap_fx_rate": c.get("market_cap_fx_rate"),
            "openfigi_status": c.get("openfigi_status"),
            "openfigi_name": c.get("openfigi_name"),
            "openfigi_checked_at": c.get("openfigi_checked_at"),
            # AU/NZ, Russia, Africa, LatAm are outside our GuruFocus subscription;
            # flag them so an empty market cap reads as "unsubscribed" not "missing".
            "gf_unsubscribed": not is_gf_subscribed_exchange(code),
        }

    result = []
    for r in rows:
        info = company_info.get(r["company_id"], {}) if r["company_id"] else {}
        # Fall back to the company's gurufocus_ticker when the membership row
        # carries no `universe_ticker` (e.g. the LongEquity frozen union, whose
        # rows are company-id-based) so the ticker column always populates.
        ticker = r.get("ticker") or info.get("gurufocus_ticker") or ""
        exchange = info.get("exchange") or None
        # Display HKSE tickers in their canonical zero-padded form (1 → 00001),
        # matching the GuruFocus link and the stored gurufocus_ticker.
        ticker = pad_hkse_ticker(ticker, exchange)
        result.append({
            "ticker": ticker,
            "company_id": r["company_id"],
            "company_name": info.get("company_name") or None,
            "isin": info.get("isin") or None,
            "exchange": exchange,
            "gurufocus_url": gurufocus_url(ticker, exchange),
            # Sector is stored per-universe on the membership row.
            "sector": r.get("sector"),
            "country": info.get("country"),
            "delisted_at": info.get("delisted_at"),
            "out_of_scope_at": info.get("out_of_scope_at"),
            "out_of_scope_reason": info.get("out_of_scope_reason"),
            "market_cap_eur": info.get("market_cap_eur"),
            "market_cap_date": info.get("market_cap_date"),
            "market_cap_native": info.get("market_cap_native"),
            "market_cap_currency": info.get("market_cap_currency"),
            "market_cap_fx_rate": info.get("market_cap_fx_rate"),
            "openfigi_status": info.get("openfigi_status"),
            "openfigi_name": info.get("openfigi_name"),
            "openfigi_checked_at": info.get("openfigi_checked_at"),
            "gf_unsubscribed": info.get("gf_unsubscribed", False),
        })
    return result


async def drain_executor_queue(q: _queue.Queue, task):
    """Drain a queue fed by an executor-launched `_run`. The executor task
    eventually finishes; the queue's sentinel is None. Used by the SSE
    endpoints whose worker is launched via `loop.run_in_executor`."""
    yield sse_keepalive()
    while True:
        try:
            msg = await asyncio.to_thread(q.get, timeout=0.15)
        except Exception:
            if task.done():
                while not q.empty():
                    msg = q.get_nowait()
                    if msg is not None:
                        yield sse_raw(msg)
                break
            continue
        if msg is None:
            break
        yield sse_raw(msg)


async def drain_thread_queue(q: _queue.Queue):
    """Drain a queue fed by a daemon `threading.Thread` worker. The thread
    pushes None when done so we just block on `q.get` and exit on the
    sentinel — there's no task handle to inspect."""
    yield sse_keepalive()
    while True:
        msg = await asyncio.to_thread(q.get)
        if msg is None:
            break
        yield sse_raw(msg)
