"""FX rate sync + load + price-to-EUR conversion.

Three pieces tied together:
  - `sync_fx_rates_to_db` keeps the `fx_rate` table fresh by reading
    ECB / Yahoo for any currency whose latest row is older than the
    requested end_date. Skipped under db_only mode.
  - `load_fx_rates` reads the synced table into per-currency
    `pd.Series` ready for in-memory conversion (weekends/holidays are
    forward-filled to the last available rate).
  - `convert_prices_to_eur` divides each row's local price by the
    matching FX rate, returning the converted frame plus a stats dict
    so the UI can show how many rows were converted vs dropped."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pandas as pd
from supabase import Client

from deps import IN_CHUNK_SIZE
from ._helpers import _FX_SYNC_PARALLELISM, _query_with_retry
from ._pg import load_fx_rate_df_via_copy


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

    # Fast path: one direct-Postgres COPY when SUPABASE_DB_URL is configured;
    # returns None (→ PostgREST paging below) when unconfigured or on error.
    df = load_fx_rate_df_via_copy(needed, start_date, end_date)
    if df is None:
        rows: list[dict] = []
        page_size = 1000
        chunk_size = IN_CHUNK_SIZE
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
        df = pd.DataFrame(rows) if rows else None

    if df is not None and not df.empty:
        # Conversions are idempotent for the COPY frame (already typed) and
        # necessary for the PostgREST frame (string columns).
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
