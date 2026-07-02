"""Thin Yahoo Finance HTTP helpers (unofficial API).

curl_cffi Chrome impersonation to dodge blocks, urllib fallback. Best-effort:
callers handle None/empty. Not a production SLA — for research/prototyping."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.parse import quote as _urlquote


def trim_leading_no_volume(bars: list[dict]) -> list[dict]:
    """Drop the LEADING run of zero/None-volume bars — the settlement-only /
    carried-forward backfill head some Yahoo series carry (e.g. ALL.AX's flat
    1996 rows, GC=F's early tail): no real trades, so the close isn't a traded
    price. No-op when NO bar has volume (FX / indices legitimately have none),
    so the whole series is kept. Interior zero-volume days (real no-trade days)
    are preserved — only the leading run is removed."""
    if not any((b.get("volume") or 0) > 0 for b in bars):
        return bars
    i = 0
    while i < len(bars) and not ((bars[i].get("volume") or 0) > 0):
        i += 1
    return bars[i:]


def is_closed_bar(ts: int) -> bool:
    """True when the daily bar at epoch `ts` is for a fully-elapsed UTC day. The
    current UTC day's bar is still forming (partial close/volume) — Yahoo returns
    it for 24/7 crypto and intraday for equities — so exclude it from stored
    series, candles, and liquidity."""
    return datetime.fromtimestamp(ts, timezone.utc).date() < datetime.now(timezone.utc).date()

try:
    from curl_cffi import requests as _creq
    _HAS_CURL = True
except Exception:  # noqa: BLE001
    _HAS_CURL = False

_SEARCH = "https://query2.finance.yahoo.com/v1/finance/search"
_CHART = "https://query1.finance.yahoo.com/v8/finance/chart"


def _get(url: str) -> tuple[int | None, str]:
    if _HAS_CURL:
        try:
            r = _creq.get(url, impersonate="chrome", timeout=30)
            return r.status_code, (r.text or "")
        except Exception:  # noqa: BLE001
            return None, ""
    from urllib.request import Request, urlopen  # noqa: PLC0415
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with urlopen(req, timeout=30) as resp:  # noqa: S310
            return resp.status, resp.read().decode("utf-8", "replace")
    except Exception:  # noqa: BLE001
        return None, ""


def search(query: str, count: int = 10) -> list[dict]:
    """Yahoo symbol search. Returns the `quotes` list (or [])."""
    s, t = _get(f"{_SEARCH}?q={_urlquote(query)}&quotesCount={count}&newsCount=0")
    if s != 200:
        return []
    try:
        return json.loads(t).get("quotes") or []
    except Exception:  # noqa: BLE001
        return []


def _parse_chart(status: int | None, text: str) -> dict | None:
    if status != 200:
        return None
    try:
        res = (json.loads(text).get("chart") or {}).get("result") or []
        return res[0] if res else None
    except Exception:  # noqa: BLE001
        return None


def chart(symbol: str, rng: str = "3mo", interval: str = "1d") -> dict | None:
    """Chart for `symbol` over a named range. Returns the first `chart.result`
    (or None). NOTE: `range=max` COARSENS the interval (Yahoo returns ~quarterly
    bars), so use `chart_window` for true daily candles at the far end."""
    s, t = _get(f"{_CHART}/{_urlquote(symbol, safe='=^.:-')}?range={rng}&interval={interval}")
    return _parse_chart(s, t)


def chart_window(symbol: str, period1: int, period2: int, interval: str = "1d") -> dict | None:
    """Chart for an explicit epoch-seconds window. Unlike `range=max`, a bounded
    `period1/period2` window returns TRUE daily bars — used to fetch the oldest
    candles right at the listing's inception."""
    s, t = _get(
        f"{_CHART}/{_urlquote(symbol, safe='=^.:-')}"
        f"?period1={int(period1)}&period2={int(period2)}&interval={interval}"
    )
    return _parse_chart(s, t)


_FX: dict[str, float | None] = {"EUR": 1.0}


def fx_to_eur(ccy: str | None) -> float | None:
    """EUR per 1 unit of `ccy` (GBp pence handled = GBP/100). Cached per run."""
    if not ccy:
        return None
    base, div = ("GBP", 100.0) if ccy == "GBp" else (ccy, 1.0)
    if base not in _FX:
        c = chart(f"{base}EUR=X", rng="5d")
        rate = None
        if c:
            closes = [x for x in (((c.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []) if x]
            rate = closes[-1] if closes else (c.get("meta") or {}).get("regularMarketPrice")
        _FX[base] = rate
    r = _FX.get(base)
    return None if r is None else r / div
