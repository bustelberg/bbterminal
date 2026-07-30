"""Parquet OHLCV+actions archive for the asset pipeline.

One parquet blob per analysis asset in the `asset-parquet` Supabase Storage
bucket, holding the standard column set: date, open, high, low, close,
adj_close, volume, dividends, splits. Written ALONGSIDE the close+volume
`asset_price` table (which still powers the chart + grid coverage) — parquet is
the fuller archive. Built from the SAME full-history `yahoo.chart_window` fetch
that `store.store_series` already makes, so it costs zero extra Yahoo calls.

Best-effort: Yahoo is an unofficial API and Storage can hiccup, so every failure
logs + returns None and never breaks ingest."""
from __future__ import annotations

import io
import logging
import time

import pandas as pd

from deps import supabase

from . import yahoo

log = logging.getLogger(__name__)

BUCKET = "asset-parquet"

# The per-asset column set, in order.
COLUMNS = ["date", "open", "high", "low", "close", "adj_close", "volume", "dividends", "splits"]


def _safe_path(symbol: str) -> str:
    """Storage object key for a Yahoo symbol — `/` and `:` can't appear in a path
    segment, so map them (and whitespace) to `_`. e.g. BRK/B -> BRK_B.parquet."""
    safe = symbol.strip().replace("/", "_").replace(":", "_").replace(" ", "_")
    return f"{safe}.parquet"


def build_frame(result: dict | None) -> pd.DataFrame | None:
    """Full daily OHLCV + adj_close + dividends + splits from one Yahoo v8
    `chart.result[0]` (as returned by `yahoo.chart_window`, which requests
    events). Drops today's partial bar + the leading zero-volume backfill head
    (same hygiene as `store_series`). adj_close falls back to close when Yahoo
    omits it (FX/crypto/index). Returns None when there's nothing usable."""
    if not result:
        return None
    ts = result.get("timestamp") or []
    if not ts:
        return None
    ind = result.get("indicators") or {}
    q = (ind.get("quote") or [{}])[0]
    o, h, low_, c, v = (q.get(k) or [] for k in ("open", "high", "low", "close", "volume"))
    adj = ((ind.get("adjclose") or [{}])[0]).get("adjclose") or []
    events = result.get("events") or {}
    div_by_ts = {int(k): (val or {}).get("amount") for k, val in (events.get("dividends") or {}).items()}
    split_by_ts: dict[int, float] = {}
    for k, val in (events.get("splits") or {}).items():
        val = val or {}
        num, den = val.get("numerator"), val.get("denominator")
        if num and den:
            split_by_ts[int(k)] = num / den  # yfinance "Stock Splits" = ratio (4:1 -> 4.0)

    def g(a: list, i: int):
        return a[i] if i < len(a) else None

    rows: list[dict] = []
    for i in range(len(ts)):
        close_v = g(c, i)
        if close_v is None or not yahoo.is_closed_bar(ts[i]):  # skip null + today's partial
            continue
        t = int(ts[i])
        adj_v = g(adj, i)
        rows.append({
            "date": yahoo.utc_dt(t).date(),
            "open": g(o, i), "high": g(h, i), "low": g(low_, i), "close": close_v,
            "adj_close": adj_v if adj_v is not None else close_v,
            "volume": g(v, i),
            "dividends": div_by_ts.get(t, 0.0),
            "splits": split_by_ts.get(t, 0.0),
        })
    if not rows:
        return None
    # Trim the leading zero-volume backfill head (settlement-only / carried rows)
    # — reuse the same helper store_series uses. Operates on the 'volume' key.
    rows = yahoo.trim_leading_no_volume(rows)
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=COLUMNS)
    for col in ("open", "high", "low", "close", "adj_close"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
    df["dividends"] = pd.to_numeric(df["dividends"], errors="coerce").fillna(0.0).astype("float32")
    df["splits"] = pd.to_numeric(df["splits"], errors="coerce").fillna(0.0).astype("float32")
    return df


def _retry(fn, *, tries: int = 3):
    last: Exception | None = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(0.5 * (i + 1))
    raise last  # type: ignore[misc]


def _upload(path: str, content: bytes) -> bool:
    """Upsert `content` to the bucket at `path` — create, falling back to update
    on a 409/'already exists' (mirrors ingest/prices.py). True on success."""
    file_options = {"content-type": "application/octet-stream"}
    try:
        _retry(lambda: supabase.storage.from_(BUCKET).upload(path, content, file_options=file_options))
        return True
    except Exception as e:  # noqa: BLE001
        msg = str(e).lower()
        if "already exists" in msg or "duplicate" in msg or "409" in msg:
            try:
                _retry(lambda: supabase.storage.from_(BUCKET).update(path, content, file_options=file_options))
                return True
            except Exception as ue:  # noqa: BLE001
                log.warning("[asset-parquet] update fallback failed for %s: %s", path, ue)
                return False
        log.warning("[asset-parquet] upload failed for %s: %s: %s", path, type(e).__name__, e)
        return False


def write(symbol: str, df: pd.DataFrame | None) -> tuple[str, int] | None:
    """Serialize `df` to parquet + upsert it to Storage. Returns (path, rows) or
    None on empty/failure. Best-effort — never raises."""
    if df is None or df.empty:
        return None
    path = _safe_path(symbol)
    try:
        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", index=False)
    except Exception as e:  # noqa: BLE001
        log.warning("[asset-parquet] serialize failed for %s: %s: %s", symbol, type(e).__name__, e)
        return None
    if not _upload(path, buf.getvalue()):
        return None
    return path, len(df)


def signed_url(path: str, expires: int = 3600) -> str | None:
    """Short-lived download URL for a stored parquet blob (for the grid's OHLCV
    link). None on failure."""
    if not path:
        return None
    try:
        res = supabase.storage.from_(BUCKET).create_signed_url(path, expires)
    except Exception as e:  # noqa: BLE001
        log.warning("[asset-parquet] signed_url failed for %s: %s", path, e)
        return None
    if isinstance(res, dict):  # supabase-py key casing varies across versions
        return res.get("signedURL") or res.get("signedUrl") or res.get("signed_url")
    return None
