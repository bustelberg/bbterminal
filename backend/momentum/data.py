"""Bulk data loaders for the momentum backtester."""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pandas as pd
from supabase import Client

_logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRY_DELAY = 5  # seconds

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
    """Execute a Supabase query with retry on transient errors (502, etc.)."""
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return query_fn()
        except Exception as e:
            err = str(e).lower()
            is_transient = "502" in err or "bad gateway" in err or "timeout" in err
            if is_transient and attempt < _MAX_RETRIES:
                wait = _RETRY_DELAY * attempt
                _logger.warning(f"{description}: attempt {attempt} failed ({e}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def load_universe(
    supabase: Client,
    *,
    universe_label: str | None = None,
    target_month: str | None = None,
) -> pd.DataFrame:
    """Load companies for backtesting.

    If universe_label and target_month are given, loads from universe_membership
    for that specific universe/month (with sector from the membership row).
    Otherwise loads all companies joined with their exchange info.

    Returns DataFrame with columns:
        company_id, company_name, gurufocus_ticker, gurufocus_exchange, sector, country
    """
    rows: list[dict] = []
    page_size = 1000
    offset = 0

    # Load companies with exchange info
    while True:
        resp = _query_with_retry(
            lambda o=offset: (
                supabase.table("company")
                .select("company_id, company_name, gurufocus_ticker, exchange_id, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))")
                .range(o, o + page_size - 1)
                .execute()
            ),
            description="load_universe",
        )
        if not resp.data:
            break
        rows.extend(resp.data)
        if len(resp.data) < page_size:
            break
        offset += page_size

    if not rows:
        return pd.DataFrame(
            columns=["company_id", "company_name", "gurufocus_ticker", "gurufocus_exchange", "sector", "country"]
        )

    # Flatten the nested exchange/country join
    flat_rows = []
    for r in rows:
        exchange_info = r.get("gurufocus_exchange") or {}
        country_info = exchange_info.get("country") or {}
        flat_rows.append({
            "company_id": r["company_id"],
            "company_name": r["company_name"],
            "gurufocus_ticker": r["gurufocus_ticker"],
            "gurufocus_exchange": exchange_info.get("exchange_code"),
            "country": country_info.get("country_name"),
        })

    df = pd.DataFrame(flat_rows)

    # If a universe is specified, join with universe_membership for sector
    if universe_label and target_month:
        # Get universe_id
        u_resp = supabase.table("universe").select("universe_id").eq("label", universe_label).limit(1).execute()
        if u_resp.data:
            universe_id = u_resp.data[0]["universe_id"]
            # Load membership rows
            m_rows: list[dict] = []
            m_offset = 0
            while True:
                m_resp = _query_with_retry(
                    lambda o=m_offset: (
                        supabase.table("universe_membership")
                        .select("company_id, sector, universe_ticker")
                        .eq("universe_id", universe_id)
                        .eq("target_month", target_month)
                        .range(o, o + page_size - 1)
                        .execute()
                    ),
                    description="load_universe_membership",
                )
                if not m_resp.data:
                    break
                m_rows.extend(m_resp.data)
                if len(m_resp.data) < page_size:
                    break
                m_offset += page_size

            if m_rows:
                membership_df = pd.DataFrame(m_rows)
                df = df.merge(membership_df[["company_id", "sector"]], on="company_id", how="inner")
                df = df.dropna(subset=["sector"]).reset_index(drop=True)
                return df

    # Fallback: no sector info available, return without sector filtering
    df["sector"] = None
    return df


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

    Returns the raw row list (un-deduped, unsorted). Chunks of 50 keep
    .in_() URLs short enough for Cloudflare; chunks run on a small worker
    pool so we get the benefit of overlapped network RTT without saturating
    the connection pool or upstream rate limits."""
    if not company_ids:
        return []

    page_size = 1000
    chunk_size = 50
    chunks = [
        company_ids[i : i + chunk_size]
        for i in range(0, len(company_ids), chunk_size)
    ]

    rows: list[dict] = []
    rows_lock = threading.Lock()
    page_counter = [0]

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
                break
            with rows_lock:
                rows.extend(resp.data)
                page_counter[0] += 1
                page_num = page_counter[0]
                total_so_far = len(rows)
            if on_progress:
                on_progress(total_so_far, page_num)
            if len(resp.data) < page_size:
                break
            offset += page_size

    with ThreadPoolExecutor(max_workers=_LOAD_PARALLELISM) as executor:
        # `list(executor.map(...))` propagates exceptions from worker threads.
        list(executor.map(_load_chunk, list(enumerate(chunks))))

    return rows


def load_all_prices(
    supabase: Client,
    company_ids: list[int],
    start_date: date,
    end_date: date,
    on_progress: callable = None,
) -> pd.DataFrame:
    """Bulk-load daily closing prices for all companies.

    Args:
        on_progress: Optional callback(rows_so_far, page_num) called after
            each page. Called from worker threads; must be thread-safe.

    Returns DataFrame with columns: company_id, target_date, price
    sorted by (company_id, target_date).
    """
    if not company_ids:
        return pd.DataFrame(columns=["company_id", "target_date", "price"])

    rows = _load_metric_chunks(
        supabase, company_ids, "close_price", start_date, end_date,
        on_progress, description_prefix="load_all_prices",
    )

    if not rows:
        return pd.DataFrame(columns=["company_id", "target_date", "price"])

    df = pd.DataFrame(rows)
    df.rename(columns={"numeric_value": "price"}, inplace=True)
    df["target_date"] = pd.to_datetime(df["target_date"])
    df["price"] = df["price"].astype(float)
    df = df.sort_values(["company_id", "target_date"]).reset_index(drop=True)
    return df


def load_company_currency(
    supabase: Client,
    company_ids: list[int],
) -> dict[int, str | None]:
    """Load the trading currency for each company via its exchange.

    Returns {company_id: currency_code}. Companies with no exchange resolve
    to None and won't be FX-converted.
    """
    if not company_ids:
        return {}

    result: dict[int, str | None] = {}
    chunk_size = 50
    for chunk_start in range(0, len(company_ids), chunk_size):
        chunk = company_ids[chunk_start : chunk_start + chunk_size]
        resp = _query_with_retry(
            lambda c=chunk: (
                supabase.table("company")
                .select("company_id, gurufocus_exchange:gurufocus_exchange(currency_code)")
                .in_("company_id", c)
                .execute()
            ),
            description=f"load_company_currency chunk {chunk_start // chunk_size + 1}",
        )
        for row in (resp.data or []):
            exch = row.get("gurufocus_exchange") or {}
            result[int(row["company_id"])] = exch.get("currency_code")
    return result


def sync_fx_rates_to_db(
    supabase: Client,
    currency_codes: list[str],
    start_date: date,
    end_date: date,
    on_progress=None,
) -> dict[str, dict]:
    """Ensure `fx_rate` table covers [start_date, end_date] for each currency.

    For each currency:
      - Look up the latest existing rate_date in `fx_rate`.
      - If already >= end_date, skip (already covered).
      - Otherwise fetch from ECB / pegs / Yahoo starting at (existing_max + 1 day)
        or `start_date` if no rows exist, and upsert.

    `on_progress(code, status, details)` is called after each currency.

    Returns per-currency status dict for logging. EUR is skipped (base currency).
    """
    # Imported lazily so this module stays independent of fx_rates's HTTP side
    # effects unless sync is actually requested.
    from fx_rates import fetch_history
    from datetime import date as _date, timedelta as _timedelta

    today = _date.today()
    end_iso = end_date.isoformat()

    def _sync_one(code: str) -> tuple[str, dict]:
        if not code or code == "EUR":
            return code, {"status": "skipped", "rows": 0}

        try:
            resp = (
                supabase.table("fx_rate")
                .select("rate_date")
                .eq("currency_code", code)
                .order("rate_date", desc=True)
                .limit(1)
                .execute()
            )
        except Exception as e:
            return code, {"status": "error", "error": f"db read: {e}", "rows": 0}

        existing_max = resp.data[0]["rate_date"] if resp.data else None
        if existing_max and str(existing_max) >= end_iso:
            return code, {"status": "cached", "rows": 0, "max_date": str(existing_max)}

        # Fetch from ECB starting the day after what we have, or from start_date
        # if the table is empty for this currency. ECB is free and daily, so
        # re-fetching a wide window is cheap.
        if existing_max:
            next_day = _date.fromisoformat(str(existing_max)) + _timedelta(days=1)
            # ECB rejects startPeriod strictly in the future with a 400. If we
            # already have data up through today, there's nothing to ask for —
            # treat the local cache as current.
            if next_day > today:
                return code, {"status": "cached", "rows": 0, "max_date": str(existing_max)}
            fetch_start = next_day.isoformat()
        else:
            fetch_start = start_date.isoformat()

        try:
            rates = fetch_history(code, fetch_start)
        except Exception as e:
            return code, {"status": "error", "error": f"ecb fetch: {e}", "rows": 0}

        if not rates:
            # Truly missing only when we have nothing in the DB at all.
            # Otherwise ECB just hasn't published new rates yet (weekends,
            # holidays, or a few-day publishing lag) — existing data is fine.
            if existing_max is None:
                return code, {"status": "no_data", "rows": 0, "max_date": None}
            return code, {"status": "cached", "rows": 0, "max_date": str(existing_max)}

        rows_to_upsert = [
            {"currency_code": code, "rate_date": r["date"], "rate": r["rate"]}
            for r in rates
        ]
        upserted = 0
        try:
            for i in range(0, len(rows_to_upsert), 500):
                supabase.table("fx_rate").upsert(
                    rows_to_upsert[i : i + 500],
                    on_conflict="currency_code,rate_date",
                ).execute()
                upserted += len(rows_to_upsert[i : i + 500])
            return code, {
                "status": "synced",
                "rows": upserted,
                "max_date": rows_to_upsert[-1]["rate_date"],
            }
        except Exception as e:
            return code, {"status": "error", "error": f"db upsert: {e}", "rows": upserted}

    status: dict[str, dict] = {}
    if not currency_codes:
        return status

    # See `_FX_SYNC_PARALLELISM` — capped at the currency count so we don't
    # spawn idle workers for small batches.
    workers = min(_FX_SYNC_PARALLELISM, len(currency_codes))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for code, st in pool.map(_sync_one, currency_codes):
            status[code] = st
            if on_progress:
                on_progress(code, st)

    return status


def load_fx_rates(
    supabase: Client,
    currency_codes: list[str],
    start_date: date,
    end_date: date,
) -> dict[str, pd.Series]:
    """Bulk-load daily FX rates for the given currencies from the fx_rate table.

    Returns {currency_code: Series indexed by date} where rate = units of
    currency per 1 EUR. EUR is returned as a constant-1 series.
    """
    if not currency_codes:
        return {}

    result: dict[str, pd.Series] = {}
    needed = [c for c in currency_codes if c and c != "EUR"]

    for code in currency_codes:
        if code == "EUR":
            # Constant 1.0 series — conversion is a no-op for EUR-denominated
            # prices. We still populate it so callers can look up uniformly.
            idx = pd.date_range(start=start_date, end=end_date, freq="D")
            result["EUR"] = pd.Series(1.0, index=idx, dtype="float64")

    if not needed:
        return result

    rows: list[dict] = []
    page_size = 1000
    chunk_size = 50
    for chunk_start in range(0, len(needed), chunk_size):
        chunk = needed[chunk_start : chunk_start + chunk_size]
        offset = 0
        while True:
            resp = _query_with_retry(
                lambda o=offset, c=chunk: (
                    supabase.table("fx_rate")
                    .select("currency_code, rate_date, rate")
                    .in_("currency_code", c)
                    .gte("rate_date", start_date.isoformat())
                    .lte("rate_date", end_date.isoformat())
                    .order("currency_code")
                    .order("rate_date")
                    .range(o, o + page_size - 1)
                    .execute()
                ),
                description=f"load_fx_rates chunk {chunk_start // chunk_size + 1}",
            )
            if not resp.data:
                break
            rows.extend(resp.data)
            if len(resp.data) < page_size:
                break
            offset += page_size

    if rows:
        df = pd.DataFrame(rows)
        df["rate_date"] = pd.to_datetime(df["rate_date"])
        df["rate"] = df["rate"].astype(float)
        for code, grp in df.groupby("currency_code"):
            series = grp.set_index("rate_date")["rate"].sort_index()
            # Reindex onto a daily grid and forward-fill so weekends/holidays
            # pick up the last available rate — prices traded on Monday use
            # Friday's close rate, which is how most back-office systems
            # report it anyway.
            idx = pd.date_range(start=start_date, end=end_date, freq="D")
            result[code] = series.reindex(idx).ffill().bfill()
    return result


def convert_prices_to_eur(
    prices_df: pd.DataFrame,
    company_currency: dict[int, str | None],
    fx_rates: dict[str, pd.Series],
) -> tuple[pd.DataFrame, dict]:
    """Convert local-currency prices to EUR in-place via lookup on (currency, date).

    Returns (converted_df, stats) where stats has:
        - converted_rows: count of non-EUR rows converted
        - passthrough_rows: count of EUR rows left as-is
        - dropped_no_currency: rows dropped because the company has no currency
        - dropped_no_fx: rows dropped because the currency has no FX series
        - missing_currencies: sorted list of currencies that had no FX series
        - converted_currencies: sorted list of currencies that were converted
    """
    stats = {
        "converted_rows": 0,
        "passthrough_rows": 0,
        "dropped_no_currency": 0,
        "dropped_no_fx": 0,
        "missing_currencies": [],
        "converted_currencies": [],
    }
    if prices_df.empty:
        return prices_df, stats

    df = prices_df.copy()
    df["currency"] = df["company_id"].map(company_currency)

    no_currency_mask = df["currency"].isna()
    stats["dropped_no_currency"] = int(no_currency_mask.sum())
    df = df[~no_currency_mask].copy()

    missing: set[str] = set()
    converted: set[str] = set()
    dropped_no_fx = 0
    kept_frames: list[pd.DataFrame] = []

    for code, group in df.groupby("currency", sort=False):
        if code == "EUR":
            stats["passthrough_rows"] += len(group)
            kept_frames.append(group.drop(columns=["currency"]))
            continue

        series = fx_rates.get(code)
        if series is None or series.empty:
            missing.add(code)
            dropped_no_fx += len(group)
            continue

        # Align rates onto each price row via reindex.
        rates = series.reindex(group["target_date"]).ffill().bfill()
        rates_arr = rates.to_numpy()
        if pd.isna(rates_arr).all():
            missing.add(code)
            dropped_no_fx += len(group)
            continue

        converted_group = group.drop(columns=["currency"]).copy()
        converted_group["price"] = group["price"].to_numpy() / rates_arr
        # Drop rows where the rate was NaN after ffill/bfill (shouldn't happen
        # in practice but guards against partial FX history).
        valid = ~pd.isna(converted_group["price"])
        dropped_no_fx += int((~valid).sum())
        converted_group = converted_group[valid]
        stats["converted_rows"] += len(converted_group)
        converted.add(code)
        kept_frames.append(converted_group)

    stats["dropped_no_fx"] = dropped_no_fx
    stats["missing_currencies"] = sorted(missing)
    stats["converted_currencies"] = sorted(converted)

    if not kept_frames:
        return prices_df.iloc[0:0].copy(), stats

    out = pd.concat(kept_frames, ignore_index=True)
    out = out.sort_values(["company_id", "target_date"]).reset_index(drop=True)
    return out, stats


def self_heal_missing_data(
    supabase: Client,
    company_ids: list[int],
    ticker_lookup: dict[int, str],
    exchange_lookup: dict[int, str],
    *,
    on_progress=None,
) -> dict:
    """For each company in `company_ids`, ensure both close_price and volume
    are present in `metric_data` by re-running the ingest pipeline (Storage
    cache check → GF API fetch → cache + DB load).

    Use this on the small subset of universe companies that came back empty
    from a bulk DB load — calling it on every company would be wasteful
    (hundreds of redundant HEAD calls). The downstream `ensure_*` helpers
    already short-circuit if the DB is fresh, so even a misuse just costs
    extra DB round-trips, not API calls.

    A 403/"unsubscribed region" response on any company causes the helper
    to mark its exchange as forbidden and skip every subsequent company on
    the same exchange (ingest already does the same thing in its own
    pipeline). A 403 for a single bad ticker (delisted, wrong symbol) does
    NOT taint the whole exchange.

    `on_progress(cid, status, message)` is called from worker threads —
    callbacks must be thread-safe.

    Returns:
        {"healed_company_ids": [...], "stats": {...}}
        where "stats" includes processed/prices_fetched/volumes_fetched/
        forbidden_exchanges/errors counts.
    """
    # Imported lazily to avoid making `data.py` always pay the ingest module's
    # transitive imports (urllib, supabase storage helpers, etc.).
    from ingest.prices import (  # noqa: PLC0415
        ensure_prices_for_company, ensure_volume_for_company,
    )

    if not company_ids:
        return {
            "healed_company_ids": [],
            "stats": {
                "processed": 0, "prices_fetched": 0, "volumes_fetched": 0,
                "forbidden_exchanges": [], "errors": 0,
            },
        }

    forbidden_exchanges: set[str] = set()
    healed: list[int] = []
    stats = {
        "processed": 0,
        "prices_fetched": 0,
        "volumes_fetched": 0,
        "errors": 0,
    }
    lock = threading.Lock()

    def _heal_one(cid: int) -> None:
        ticker = ticker_lookup.get(cid)
        exch = exchange_lookup.get(cid)
        if not ticker or not exch:
            with lock:
                stats["errors"] += 1
            if on_progress:
                on_progress(cid, "skipped", "missing ticker/exchange")
            return
        with lock:
            if exch in forbidden_exchanges:
                if on_progress:
                    on_progress(cid, "skipped", f"exchange {exch} known forbidden")
                return
        try:
            r_p = ensure_prices_for_company(supabase, cid, ticker, exch)
            if r_p.is_forbidden:
                with lock:
                    forbidden_exchanges.add(exch)
                if on_progress:
                    on_progress(cid, "forbidden", f"{exch}: unsubscribed")
                return
            r_v = ensure_volume_for_company(supabase, cid, ticker, exch)
        except Exception as e:  # noqa: BLE001
            with lock:
                stats["errors"] += 1
            if on_progress:
                on_progress(cid, "error", str(e))
            return
        any_loaded = r_p.rows_loaded > 0 or r_v.rows_loaded > 0
        with lock:
            stats["processed"] += 1
            if r_p.rows_loaded > 0:
                stats["prices_fetched"] += 1
            if r_v.rows_loaded > 0:
                stats["volumes_fetched"] += 1
            if any_loaded:
                healed.append(cid)
        if on_progress:
            on_progress(
                cid,
                "ok" if any_loaded else "noop",
                f"prices={r_p.source}({r_p.rows_loaded}) volumes={r_v.source}({r_v.rows_loaded})",
            )

    # Use fewer workers than the bulk load: each call hits the GF API,
    # which is rate-limit-sensitive — overdoing parallelism risks 429s.
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(_heal_one, company_ids))

    return {
        "healed_company_ids": sorted(healed),
        "stats": {**stats, "forbidden_exchanges": sorted(forbidden_exchanges)},
    }


def load_all_volumes(
    supabase: Client,
    company_ids: list[int],
    start_date: date,
    end_date: date,
    on_progress: callable = None,
) -> pd.DataFrame:
    """Bulk-load daily volume for all companies.

    Args:
        on_progress: Optional callback(rows_so_far, page_num) called after
            each page. Called from worker threads; must be thread-safe.

    Returns DataFrame with columns: company_id, target_date, volume
    sorted by (company_id, target_date).
    """
    if not company_ids:
        return pd.DataFrame(columns=["company_id", "target_date", "volume"])

    rows = _load_metric_chunks(
        supabase, company_ids, "volume", start_date, end_date,
        on_progress, description_prefix="load_all_volumes",
    )

    if not rows:
        return pd.DataFrame(columns=["company_id", "target_date", "volume"])

    df = pd.DataFrame(rows)
    df.rename(columns={"numeric_value": "volume"}, inplace=True)
    df["target_date"] = pd.to_datetime(df["target_date"])
    df["volume"] = df["volume"].astype(float)
    df = df.sort_values(["company_id", "target_date"]).reset_index(drop=True)
    return df
