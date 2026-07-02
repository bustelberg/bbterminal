"""Persistence for the asset pipeline.

Upserts the analysis asset (dedup by symbol) + the execution instrument (by
ISIN, many->one), and stores the analysis instrument's daily CLOSE + VOLUME
series (once per asset). Idempotent — safe to re-run over the same ISIN list."""
from __future__ import annotations

import time
from datetime import datetime, timezone

from deps import supabase

from . import yahoo

_CHUNK = 500

# Fields persisted to asset_price, as (Yahoo-quote-key, db-column). We store
# CLOSE + VOLUME only for now. To store full OHLCV later: (1) add open/high/low
# columns in a migration, (2) add ("open","open"),("high","high"),("low","low")
# here. The row builder, the /store + /storage responses, and the UI all read
# this one list, so nothing else changes. `close` always gates "a real bar" — so
# keep it present regardless.
STORED_FIELDS: list[tuple[str, str]] = [("close", "close"), ("volume", "volume")]


def stored_columns() -> list[str]:
    """The db columns currently persisted per bar (drives the 'what we store'
    messaging everywhere)."""
    return [col for _yk, col in STORED_FIELDS]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _analysis_id(symbol: str) -> int | None:
    r = supabase.table("asset_analysis").select("analysis_id").eq("symbol", symbol).limit(1).execute()
    return r.data[0]["analysis_id"] if r.data else None


def upsert_asset(res: dict) -> dict:
    """Upsert asset_analysis (by symbol) + — for an ISIN input — asset_execution
    (by ISIN, linked to the analysis asset). Returns ids + the analysis symbol."""
    an = res.get("analysis") or {}
    symbol = an.get("symbol")
    if not symbol:
        raise ValueError("no analysis symbol to store")

    supabase.table("asset_analysis").upsert(
        {
            "symbol": symbol,
            "asset_class": res.get("asset_class"),
            "label": an.get("name"),
            "sector": res.get("sector"),
            "currency": an.get("currency"),
            "first_date": an.get("first_date"),
            "years": an.get("years"),
            "updated_at": _now_iso(),
        },
        on_conflict="symbol",
    ).execute()
    analysis_id = _analysis_id(symbol)

    execution_id = None
    if res.get("id_type") == "isin":
        ex = res.get("execution") or {}
        eup = supabase.table("asset_execution").upsert(
            {
                "isin": res["input"].strip().upper(),
                "analysis_id": analysis_id,
                "yahoo_symbol": ex.get("symbol"),
                "name": ex.get("name"),
                "exchange": ex.get("exchange"),
                "currency": ex.get("currency"),
                "med_adv_eur": ex.get("med_adv_eur"),
                "first_date": ex.get("first_date"),
                "years": ex.get("years"),
                "wrapper": res.get("wrapper"),
                "is_leveraged": bool(res.get("is_leveraged")),
                "updated_at": _now_iso(),
            },
            on_conflict="isin",
        ).execute()
        execution_id = eup.data[0]["execution_id"] if eup.data else None

    return {"analysis_id": analysis_id, "execution_id": execution_id, "symbol": symbol}


def store_series(analysis_id: int, symbol: str, first_ts: int | None) -> int:
    """Fetch the FULL daily close+volume series for `symbol` and upsert into
    asset_price. Uses a period-window fetch (range=max coarsens to quarterly).
    Returns rows stored."""
    if not first_ts:
        r0 = yahoo.chart(symbol, rng="3mo")
        first_ts = ((r0 or {}).get("meta") or {}).get("firstTradeDate")
    if not first_ts:
        return 0
    w = yahoo.chart_window(symbol, int(first_ts), int(time.time()), "1d")
    if not w:
        return 0
    ts = w.get("timestamp") or []
    q = ((w.get("indicators") or {}).get("quote") or [{}])[0]
    closes = q.get("close") or []
    series = {ykey: (q.get(ykey) or []) for ykey, _col in STORED_FIELDS}
    rows = []
    for i in range(len(ts)):
        c = closes[i] if i < len(closes) else None
        if c is None or not yahoo.is_closed_bar(ts[i]):  # close gates a real, closed bar
            continue
        row: dict = {
            "analysis_id": analysis_id,
            "target_date": datetime.fromtimestamp(ts[i], timezone.utc).date().isoformat(),
        }
        for ykey, col in STORED_FIELDS:
            arr = series[ykey]
            val = arr[i] if i < len(arr) else None
            if col == "volume" and val is None:
                val = 0  # keep volume aligned 1:1 with the stored close (no-volume day = 0)
            row[col] = val
        rows.append(row)
    # Trim the leading zero-volume backfill head (settlement-only / carried
    # rows) so the stored series starts at real trading data. No-op for FX/index.
    rows = yahoo.trim_leading_no_volume(rows)
    stored = 0
    for i in range(0, len(rows), _CHUNK):
        supabase.table("asset_price").upsert(
            rows[i:i + _CHUNK], on_conflict="analysis_id,target_date",
        ).execute()
        stored += len(rows[i:i + _CHUNK])
    # Drop any previously-stored current/future (partial) bar so a re-run that
    # now excludes today doesn't leave a stale partial close behind.
    today = datetime.now(timezone.utc).date().isoformat()
    supabase.table("asset_price").delete().eq("analysis_id", analysis_id).gte("target_date", today).execute()
    return stored


def store_one(identifier: str) -> dict:
    """Resolve + persist ONE identifier: the analysis asset (dedup by symbol) +
    the execution (by ISIN) + the analysis series' close+volume. Returns what was
    stored, including the exact `stored_fields`. Used by the single-ISIN 'Store'
    action; the batch flow reuses upsert_asset + store_series directly."""
    from .resolve import resolve  # noqa: PLC0415
    res = resolve(identifier, with_candles=False)  # store doesn't need candles
    an = res.get("analysis") or {}
    if not an.get("symbol"):
        raise ValueError(res.get("reason") or "no analysis instrument resolved")
    ids = upsert_asset(res)
    rows = store_series(ids["analysis_id"], an["symbol"], an.get("first_ts"))
    try:
        set_default_executions()
    except Exception:  # noqa: BLE001
        pass
    return {
        "analysis_id": ids["analysis_id"],
        "analysis": an.get("symbol"),
        "execution": (res.get("execution") or {}).get("symbol"),
        "asset_class": res.get("asset_class"),
        "rows": rows,
        "stored_fields": stored_columns(),
    }


def set_default_executions() -> int:
    """Per analysis asset, flag the most-liquid execution `is_default=true` (the
    rest false) — the auto 'best listing to trade', keeping the others on the
    table. Cheap end-of-batch pass. Returns rows touched."""
    rows = (
        supabase.table("asset_execution")
        .select("execution_id, analysis_id, med_adv_eur, is_default")
        .execute()
    ).data or []
    by: dict[int, list[dict]] = {}
    for r in rows:
        by.setdefault(r["analysis_id"], []).append(r)
    touched = 0
    for execs in by.values():
        best = max(execs, key=lambda r: (r.get("med_adv_eur") or 0))
        for r in execs:
            want = r["execution_id"] == best["execution_id"]
            if bool(r.get("is_default")) != want:  # only write on change
                supabase.table("asset_execution").update({"is_default": want}).eq(
                    "execution_id", r["execution_id"]
                ).execute()
                touched += 1
    return touched


def storage_summary() -> dict:
    """Live counts + a rough on-disk estimate for the asset-pipeline tables."""
    a = supabase.table("asset_analysis").select("analysis_id", count="exact").limit(1).execute()
    e = supabase.table("asset_execution").select("execution_id", count="exact").limit(1).execute()
    p = supabase.table("asset_price").select("analysis_id", count="exact").limit(1).execute()
    pv = supabase.table("asset_price").select("analysis_id", count="exact").not_.is_("volume", "null").limit(1).execute()
    n_assets, n_exec, n_rows, n_vol = (a.count or 0), (e.count or 0), (p.count or 0), (pv.count or 0)
    # asset_price is the only large table: ~48B heap + ~40B PK index per row.
    est_mb = round(n_rows * 88 / 1e6, 1)
    return {
        "analysis_assets": n_assets,
        "executions": n_exec,
        "price_rows": n_rows,
        "volume_rows": n_vol,   # rows with a positive traded volume
        "est_price_mb": est_mb,
        "stored_fields": stored_columns(),  # what we persist per bar (close, volume)
    }
