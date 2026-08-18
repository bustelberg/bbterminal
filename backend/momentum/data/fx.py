"""FX rate sync + load + price-to-EUR conversion.

Three pieces tied together:
  - `sync_fx_rates_to_db` keeps the `fx_rate` table covering the whole
    requested window — it extends FORWARD from the stored max and
    BACKWARDS from the stored min. Skipped under db_only mode.
    ⚠ The backwards leg was missing until 2026-08-18, and `load_fx_rates`
    below hides the gap rather than showing it: see its `.ffill().bfill()`.
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

from deps import IN_CHUNK_SIZE, chunked
from ._helpers import _FX_SYNC_PARALLELISM, _query_with_retry
from ._pg import load_fx_rate_df_via_copy


def sync_fx_rates_to_db(
    supabase: Client,
    currency_codes: list[str],
    start_date: date,
    end_date: date,
    on_progress=None,
) -> dict[str, dict]:
    """Ensure `fx_rate` covers [start_date, end_date] for each currency — at BOTH ends.

    Per currency:
      - read the stored min and max `rate_date`;
      - if the stored min is LATER than `start_date`, fetch from `start_date` and upsert
        (the backwards leg — see below);
      - if the stored max is EARLIER than `end_date`, fetch from max+1 and upsert
        (the forwards leg).

    `on_progress(code, status)` is called after each currency. Returns a per-currency status
    dict for logging. EUR is skipped (base currency).

    ⚠⚠ THE BACKWARDS LEG EXISTS BECAUSE THERE WAS NONE, AND ITS ABSENCE PRODUCED A NUMBER RATHER
        THAN A BLANK (2026-08-18). This function only ever extended FORWARD: it read the stored max
        and fetched from max+1, so a currency whose stored history simply STARTS too late was never
        repaired by anything, in any environment, for ever. And `load_fx_rates` HIDES that — its
        `.ffill().bfill()` extends the earliest stored rate backwards to whatever `start_date` was
        asked for, so a backtest window opening before a currency's stored minimum converts that
        whole stretch at ONE wrong rate. No empty cell, no error, just a return that is wrong by
        however much the currency moved before its first stored day.

    ⚠ THE FORWARD LEG'S "ALREADY COVERED" SHORT-CIRCUIT USED TO RETURN FROM THE FUNCTION, which is
        precisely why the head gap was unreachable: a currency current to today — every currency,
        most days — returned `cached` before anything could look at where its history began. The
        two legs are now independent and both are evaluated.

    ⚠ THE BACKWARDS FETCH UPSERTS EVERYTHING IT GETS, not just the rows before the old minimum.
        ECB has no end parameter, so the response spans `start_date` → today anyway; writing all of
        it repairs interior HOLES as a side effect, which nothing else in the codebase does. The
        cost is bounded and one-off: after the first successful run the stored min is at or before
        `start_date` and this leg never fires for that currency again.

    ⚠ IT IS THE SAME `fetch_history` THE FORWARD LEG USES — ECB, the USD pegs and the TWD special
        case behind one call. A second fetcher for "old" rates would be a second place for a peg to
        be derived differently at the two ends of one series.
    """
    # Imported lazily so this module stays independent of fx_rates's HTTP side
    # effects unless sync is actually requested.
    from fx_rates import fetch_history
    from datetime import date as _date, timedelta as _timedelta

    today = _date.today()
    end_iso = end_date.isoformat()
    start_iso = start_date.isoformat()

    def _edge(code: str, *, newest: bool) -> str | None:
        """The stored min or max `rate_date` for one currency, or None when we hold nothing."""
        resp = (
            supabase.table("fx_rate")
            .select("rate_date")
            .eq("currency_code", code)
            .order("rate_date", desc=newest)
            .limit(1)
            .execute()
        )
        return str(resp.data[0]["rate_date"]) if resp.data else None

    def _upsert(code: str, rates: list[dict]) -> int:
        rows = [
            {"currency_code": code, "rate_date": r["date"], "rate": r["rate"]}
            for r in (rates or [])
            if r.get("date") and r.get("rate") is not None
        ]
        n = 0
        for chunk in chunked(rows, 500):
            supabase.table("fx_rate").upsert(
                chunk, on_conflict="currency_code,rate_date",
            ).execute()
            n += len(chunk)
        return n

    def _sync_one(code: str) -> tuple[str, dict]:
        if not code or code == "EUR":
            return code, {"status": "skipped", "rows": 0}

        try:
            existing_max = _edge(code, newest=True)
            existing_min = _edge(code, newest=False) if existing_max else None
        except Exception as e:
            return code, {"status": "error", "error": f"db read: {e}", "rows": 0}

        rows = 0
        back_rows = 0
        note: str | None = None

        # ── BACKWARDS ────────────────────────────────────────────────────────────────────────
        # Only when we hold something: an empty table is the forwards leg's job (it fetches from
        # `start_date`), and running both would be two identical requests.
        if existing_min is not None and existing_min > start_iso:
            try:
                back = fetch_history(code, start_iso)
                back_rows = _upsert(code, back)
                rows += back_rows
                if back_rows:
                    # ⚠ THE EARLIEST DATE ECB ACTUALLY RETURNED, NOT THE ONE WE ASKED FOR. A
                    # currency whose published history begins in 2005 does not gain a 2000 start
                    # by being asked for one, and this table's whole problem is coverage being
                    # reported as wider than it is.
                    got_from = min(r["date"] for r in back if r.get("date"))
                    note = f"backfilled to {got_from} (was {existing_min})"
                    existing_min = min(str(existing_min), got_from)
            except Exception as e:  # noqa: BLE001
                # ⚠ NOT FATAL TO THE FORWARDS LEG. A failed head repair must not cost today's rate
                # — that would turn a long-standing gap into a fresh one.
                note = f"backfill failed: {e}"

        # ── FORWARDS ─────────────────────────────────────────────────────────────────────────
        covered_forward = bool(existing_max) and str(existing_max) >= end_iso
        if not covered_forward:
            if existing_max:
                next_day = _date.fromisoformat(str(existing_max)) + _timedelta(days=1)
                # ECB rejects startPeriod strictly in the future with a 400. If we already have
                # data up through today, there is nothing to ask for.
                fetch_start = next_day.isoformat() if next_day <= today else None
            else:
                fetch_start = start_iso
            if fetch_start is not None:
                try:
                    fwd = fetch_history(code, fetch_start)
                except Exception as e:
                    return code, {"status": "error", "error": f"ecb fetch: {e}", "rows": rows,
                                  "backfilled_rows": back_rows, "note": note}
                if fwd:
                    try:
                        rows += _upsert(code, fwd)
                        existing_max = max(str(existing_max or ""), fwd[-1]["date"]) or None
                    except Exception as e:
                        return code, {"status": "error", "error": f"db upsert: {e}",
                                      "rows": rows, "backfilled_rows": back_rows, "note": note}
                elif existing_max is None:
                    # Truly missing only when we have nothing at all. Otherwise ECB simply has not
                    # published yet (weekend, holiday, publishing lag) and what we hold is fine.
                    return code, {"status": "no_data", "rows": rows, "max_date": None,
                                  "backfilled_rows": back_rows, "note": note}

        out = {
            "status": "synced" if rows else "cached",
            "rows": rows,
            "backfilled_rows": back_rows,
            "max_date": str(existing_max) if existing_max else None,
            "min_date": str(existing_min) if existing_min else None,
        }
        if note:
            out["note"] = note
        return code, out

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
        for ci, chunk in enumerate(chunked(needed, chunk_size)):
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
                    description=f"load_fx_rates chunk {ci + 1}",
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
