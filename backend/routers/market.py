"""Read-only market snapshots from the shared Yahoo Finance transport."""
from __future__ import annotations

import math
import threading
import time
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from asset_pipeline.yahoo import chart_window, utc_dt

router = APIRouter(prefix="/api/market", tags=["market"])
_locks = {symbol: threading.Lock() for symbol in ("^IXIC", "ACWI", "IWQU.L")}
_cached: dict[str, tuple[float, MarketSnapshot]] = {}


class MarketPoint(BaseModel):
    date: str
    close: float


class MarketSnapshot(BaseModel):
    symbol: str
    name: str
    source: str = "Yahoo Finance"
    latest: float
    latest_at: str
    peak: float
    peak_date: str
    from_peak_pct: float
    history_start: str
    fetched_at: str
    points: list[MarketPoint]


def _positive(value: object) -> float | None:
    if isinstance(value, (int, float)) and math.isfinite(value) and value > 0:
        return float(value)
    return None


def build_snapshot(raw: dict, now: float, symbol: str = "^IXIC",
                   name: str = "Nasdaq Composite", *, fx: dict | None = None) -> MarketSnapshot:
    """Daily highs define the peak; daily closes define the chart. No adjusted prices."""
    meta = raw.get("meta") or {}
    quotes = ((raw.get("indicators") or {}).get("quote") or [{}])[0]
    closes, highs = quotes.get("close") or [], quotes.get("high") or []
    exchange = ZoneInfo("Europe/London" if symbol == "IWQU.L" else "America/New_York")

    def converted(value, day):
        price = _positive(value)
        if price is None or fx is None:
            return price
        from routers._benchmark_index import _rate
        rate = _rate(fx, "USD", day)
        if not rate:
            raise ValueError(f"Missing USD/EUR conversion for {day}")
        return price / rate
    points: dict[str, MarketPoint] = {}
    peak, peak_date = 0.0, ""
    last_stamp = 0
    for index, stamp in enumerate(raw.get("timestamp") or []):
        if not isinstance(stamp, (int, float)) or stamp > now:
            continue
        day = utc_dt(stamp).astimezone(exchange).date().isoformat()
        close = converted(closes[index], day) if index < len(closes) else None
        high = converted(highs[index], day) if index < len(highs) else None
        if high is not None and high > peak:
            peak, peak_date = high, day
        if close is not None:
            points[day] = MarketPoint(date=day, close=close)
            last_stamp = max(last_stamp, int(stamp))
    if not points or not peak_date:
        raise ValueError("Yahoo Finance returned no usable market history.")
    ordered = sorted(points.values(), key=lambda point: point.date)
    latest, latest_stamp = ordered[-1].close, last_stamp
    quote_price = _positive(meta.get("regularMarketPrice"))
    quote_stamp = meta.get("regularMarketTime")
    if (quote_price is not None and isinstance(quote_stamp, (int, float))
            and last_stamp <= quote_stamp <= now):
        latest, latest_stamp = quote_price, int(quote_stamp)
        day = utc_dt(latest_stamp).astimezone(exchange).date().isoformat()
        latest = converted(latest, day)
        points[day] = MarketPoint(date=day, close=latest)
        ordered = sorted(points.values(), key=lambda point: point.date)
    if latest > peak:
        peak, peak_date = latest, ordered[-1].date
    return MarketSnapshot(
        symbol=symbol, name=name,
        latest=latest, latest_at=utc_dt(latest_stamp).isoformat(), peak=peak,
        peak_date=peak_date, from_peak_pct=(latest / peak - 1) * 100,
        history_start=ordered[0].date, fetched_at=utc_dt(now).isoformat(), points=ordered,
    )


def _snapshot(symbol: str, name: str) -> MarketSnapshot:
    """Cache the shared vendor read for five minutes, including concurrent page opens."""
    with _locks[symbol]:
        cached = _cached.get(symbol)
        if cached and time.monotonic() - cached[0] < 300:
            return cached[1]
        try:
            now = time.time()
            raw = chart_window(symbol, 0, int(now), "1d")
            if not raw:
                raise ValueError("No Yahoo Finance response")
            fx = None
            if symbol == "IWQU.L":
                from routers._benchmark_index import _fx_to_eur
                if (raw.get("meta") or {}).get("currency") != "USD":
                    raise ValueError("IWQU.L must be quoted in USD")
                stamps = [s for s in raw.get("timestamp") or [] if isinstance(s, (int, float)) and s <= now]
                if not stamps:
                    raise ValueError("No IWQU.L history")
                from datetime import timedelta
                start = (utc_dt(min(stamps)).date() - timedelta(days=7)).isoformat()
                fx = _fx_to_eur({"USD"}, start, utc_dt(now).date().isoformat())
            snapshot = build_snapshot(raw, now, symbol, name, fx=fx)
        except Exception as exc:
            raise HTTPException(503, f"{name} data is temporarily unavailable from Yahoo Finance.") from exc
        _cached[symbol] = (time.monotonic(), snapshot)
        return snapshot


@router.get("/nasdaq", response_model=MarketSnapshot)
def nasdaq() -> MarketSnapshot:
    return _snapshot("^IXIC", "Nasdaq Composite")


@router.get("/acwi", response_model=MarketSnapshot)
def acwi() -> MarketSnapshot:
    return _snapshot("ACWI", "iShares MSCI ACWI ETF")


@router.get("/iwqu", response_model=MarketSnapshot)
def iwqu() -> MarketSnapshot:
    return _snapshot("IWQU.L", "iShares Edge MSCI World Quality Factor UCITS ETF · EUR")
