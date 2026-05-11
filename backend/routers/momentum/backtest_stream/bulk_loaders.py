"""Streamed bulk loaders for prices, volumes, and FX.

Each helper drives a background `to_thread` task while polling a
progress queue so SSE updates stream as the load proceeds. Throttling
caps the emit rate so very large loads don't drown the SSE stream in
near-identical "X rows loaded" updates.

Each function is an async generator that yields SSE event strings and
finally yields a sentinel `("__result__", <value>)` tuple carrying the
loaded DataFrame / dict so the orchestrator can pick it up without a
second helper call."""
from __future__ import annotations

import asyncio
import json
import queue as _queue

import pandas as pd

from deps import supabase
from momentum.data import (
    convert_prices_to_eur,
    load_all_prices,
    load_all_volumes,
    load_fx_rates,
    sync_fx_rates_to_db,
)


_PROGRESS_THROTTLE = 25


def _emit(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


def _keepalive() -> str:
    return ": keepalive\n\n"


async def load_prices_streamed(
    company_ids: list[int],
    price_start,
    price_end,
):
    """Yield SSE progress events while bulk-loading EUR-input prices
    from the DB. Finally yields `("__result__", prices_df)`."""
    yield _emit({"type": "progress", "pct": 62, "message": f"Loading prices from DB ({price_start} to {price_end}, starts early for 200-day MA)..."})
    yield _keepalive()

    prices_progress_q: _queue.Queue = _queue.Queue()

    def _on_progress(rows_so_far: int, page_num: int, chunks_done: int = 0, chunks_total: int = 0):
        prices_progress_q.put({
            "rows": rows_so_far,
            "page": page_num,
            "chunks_done": chunks_done,
            "chunks_total": chunks_total,
        })

    prices_task = asyncio.create_task(asyncio.to_thread(
        load_all_prices, supabase, company_ids, price_start, price_end,
        on_progress=_on_progress,
    ))

    last_emitted_page = 0

    def _fmt(p: dict) -> str:
        ct = p.get("chunks_total", 0)
        cd = p.get("chunks_done", 0)
        pct_str = f" ≈ {round(cd / ct * 100)}%" if ct else ""
        return f"  Loaded {p['rows']:,} price rows ({cd}/{ct} chunks{pct_str})..."

    while not prices_task.done():
        drained = []
        while True:
            try:
                drained.append(prices_progress_q.get_nowait())
            except _queue.Empty:
                break
        if drained:
            latest = drained[-1]
            if latest["page"] - last_emitted_page >= _PROGRESS_THROTTLE:
                last_emitted_page = latest["page"]
                yield _emit({"type": "progress", "pct": 63, "message": _fmt(latest)})
        await asyncio.sleep(0.1)

    final_total = None
    while True:
        try:
            final_total = prices_progress_q.get_nowait()
        except _queue.Empty:
            break
    if final_total is not None and final_total["page"] != last_emitted_page:
        yield _emit({"type": "progress", "pct": 64, "message": _fmt(final_total)})

    prices_df = await prices_task
    yield ("__result__", prices_df)


async def sync_fx_streamed(currencies_needed: list[str], price_start, price_end):
    """Yield SSE progress while ECB syncing the FX rates. Returns a sync
    summary dict via the `("__result__", fx_sync)` sentinel."""
    yield _emit({"type": "progress", "pct": 65, "message": f"Syncing FX rates from ECB (through {price_end})..."})
    yield _keepalive()

    fx_progress_q: _queue.Queue = _queue.Queue()
    fx_done = [0]
    fx_total = len(currencies_needed)

    def _on_progress(code: str, status: dict):
        fx_done[0] += 1
        fx_progress_q.put({
            "code": code,
            "done": fx_done[0],
            "total": fx_total,
            "status": status.get("status"),
        })

    fx_task = asyncio.create_task(asyncio.to_thread(
        sync_fx_rates_to_db, supabase, currencies_needed, price_start, price_end,
        on_progress=_on_progress,
    ))
    while not fx_task.done():
        drained = []
        while True:
            try:
                drained.append(fx_progress_q.get_nowait())
            except _queue.Empty:
                break
        if drained:
            latest = drained[-1]
            pct = round(latest["done"] / max(1, latest["total"]) * 100)
            yield _emit({
                "type": "progress",
                "pct": 65,
                "message": f"  FX sync {latest['done']}/{latest['total']} ≈ {pct}% (latest: {latest['code']} → {latest['status']})",
            })
        await asyncio.sleep(0.15)
    fx_sync = await fx_task
    synced_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "synced")
    cached_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "cached")
    failed_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "error")
    nodata_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "no_data")
    total_rows = sum(s.get("rows", 0) for s in fx_sync.values())
    yield _emit({
        "type": "progress",
        "pct": 65,
        "message": (
            f"FX sync done: {len(synced_codes)} updated ({total_rows:,} rows), "
            f"{len(cached_codes)} already current, "
            f"{len(failed_codes)} failed, {len(nodata_codes)} no_data"
        ),
    })
    if failed_codes:
        for code in failed_codes:
            err = fx_sync[code].get("error", "unknown")
            yield _emit({"type": "warning", "scope": "fx", "message": f"FX sync failed for {code}: {err}"})
    if nodata_codes:
        _ccy_names = {
            "AED": "UAE Dirham", "ARS": "Argentine Peso", "AUD": "Australian Dollar",
            "BRL": "Brazilian Real", "CAD": "Canadian Dollar", "CHF": "Swiss Franc",
            "CLP": "Chilean Peso", "CNY": "Chinese Yuan", "COP": "Colombian Peso",
            "CZK": "Czech Koruna", "DKK": "Danish Krone", "EGP": "Egyptian Pound",
            "EUR": "Euro", "GBP": "British Pound", "GBX": "British Penny",
            "HKD": "Hong Kong Dollar", "HUF": "Hungarian Forint", "IDR": "Indonesian Rupiah",
            "ILS": "Israeli Shekel", "INR": "Indian Rupee", "ISK": "Icelandic Krona",
            "JPY": "Japanese Yen", "KRW": "South Korean Won", "MXN": "Mexican Peso",
            "MYR": "Malaysian Ringgit", "NOK": "Norwegian Krone", "NZD": "New Zealand Dollar",
            "PEN": "Peruvian Sol", "PHP": "Philippine Peso", "PKR": "Pakistani Rupee",
            "PLN": "Polish Zloty", "QAR": "Qatari Riyal", "RON": "Romanian Leu",
            "RUB": "Russian Ruble", "SAR": "Saudi Riyal", "SEK": "Swedish Krona",
            "SGD": "Singapore Dollar", "THB": "Thai Baht", "TRY": "Turkish Lira",
            "TWD": "Taiwan Dollar", "USD": "US Dollar", "VND": "Vietnamese Dong",
            "ZAR": "South African Rand",
        }
        labeled = ", ".join(
            f"{c} ({_ccy_names[c]})" if c in _ccy_names else c for c in nodata_codes
        )
        yield _emit({
            "type": "warning",
            "scope": "fx",
            "message": f"No FX data returned for: {labeled} (ECB may not cover these)",
        })
    yield ("__result__", fx_sync)


async def load_fx_and_convert(
    prices_df: pd.DataFrame,
    company_currency: dict[int, str | None],
    currencies_needed: list[str],
    price_start,
    price_end,
):
    """Yield SSE progress while loading FX rates from the DB and
    converting prices to EUR. Returns `(prices_eur_df, prices_local_df, fx_rates)`
    via `("__result__", (eur, local, rates))`."""
    yield _emit({"type": "progress", "pct": 65, "message": f"Loading FX rates ({price_start} to {price_end}) for {len(currencies_needed)} currencies..."})
    yield _keepalive()
    fx_rates = await asyncio.to_thread(
        load_fx_rates, supabase, currencies_needed, price_start, price_end,
    )
    loaded_codes = [c for c, s in fx_rates.items() if s is not None and not s.empty]
    missing_codes = sorted(set(currencies_needed) - set(loaded_codes))
    yield _emit({"type": "progress", "pct": 65, "message": f"FX rates loaded for {len(loaded_codes)} currencies"})
    if missing_codes:
        yield _emit({
            "type": "warning",
            "scope": "fx",
            "message": f"No FX history for: {', '.join(missing_codes)} — companies on those currencies will be dropped",
        })

    yield _emit({"type": "progress", "pct": 65, "message": f"Converting {len(prices_df):,} price rows to EUR..."})
    yield _keepalive()
    prices_local_df = prices_df
    prices_eur_df, fx_stats = await asyncio.to_thread(
        convert_prices_to_eur, prices_df, company_currency, fx_rates,
    )
    yield _emit({
        "type": "progress",
        "pct": 65,
        "message": (
            f"FX done: {fx_stats['converted_rows']:,} rows converted "
            f"({', '.join(fx_stats['converted_currencies']) or 'none'}), "
            f"{fx_stats['passthrough_rows']:,} already EUR, "
            f"{fx_stats['dropped_no_currency']:,} dropped (no currency), "
            f"{fx_stats['dropped_no_fx']:,} dropped (no FX rate)"
        ),
    })
    if fx_stats["missing_currencies"]:
        yield _emit({
            "type": "warning",
            "scope": "fx",
            "message": f"Currencies with no FX series in date range: {', '.join(fx_stats['missing_currencies'])}",
        })
    yield ("__result__", (prices_eur_df, prices_local_df, fx_rates))


async def load_volumes_streamed(
    company_ids: list[int],
    price_start,
    price_end,
):
    """Yield SSE progress events while bulk-loading volumes from the DB.
    Finally yields `("__result__", volumes_df)`."""
    yield _emit({"type": "progress", "pct": 66, "message": "Loading volumes from DB..."})
    yield _keepalive()

    volumes_progress_q: _queue.Queue = _queue.Queue()

    def _on_progress(rows_so_far: int, page_num: int, chunks_done: int = 0, chunks_total: int = 0):
        volumes_progress_q.put({
            "rows": rows_so_far,
            "page": page_num,
            "chunks_done": chunks_done,
            "chunks_total": chunks_total,
        })

    volumes_task = asyncio.create_task(asyncio.to_thread(
        load_all_volumes, supabase, company_ids, price_start, price_end,
        on_progress=_on_progress,
    ))

    def _fmt(p: dict) -> str:
        ct = p.get("chunks_total", 0)
        cd = p.get("chunks_done", 0)
        pct_str = f" ≈ {round(cd / ct * 100)}%" if ct else ""
        return f"  Loaded {p['rows']:,} volume rows ({cd}/{ct} chunks{pct_str})..."

    last_emitted_vpage = 0
    while not volumes_task.done():
        drained = []
        while True:
            try:
                drained.append(volumes_progress_q.get_nowait())
            except _queue.Empty:
                break
        if drained:
            latest = drained[-1]
            if latest["page"] - last_emitted_vpage >= _PROGRESS_THROTTLE:
                last_emitted_vpage = latest["page"]
                yield _emit({"type": "progress", "pct": 66, "message": _fmt(latest)})
        await asyncio.sleep(0.1)
    final_v = None
    while True:
        try:
            final_v = volumes_progress_q.get_nowait()
        except _queue.Empty:
            break
    if final_v is not None and final_v["page"] != last_emitted_vpage:
        yield _emit({"type": "progress", "pct": 67, "message": _fmt(final_v)})

    volumes_df = await volumes_task
    yield ("__result__", volumes_df)
