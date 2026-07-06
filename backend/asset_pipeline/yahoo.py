"""Thin Yahoo Finance HTTP helpers (unofficial API).

curl_cffi Chrome impersonation to dodge blocks, urllib fallback. Best-effort:
callers handle None/empty. Not a production SLA — for research/prototyping."""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote as _urlquote

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def utc_dt(ts: float) -> datetime:
    """Epoch seconds → aware UTC datetime, robust to pre-1970 (negative)
    timestamps. `datetime.fromtimestamp(neg, utc)` raises OSError [Errno 22] on
    Windows for negative epochs — and old US tickers (e.g. XOM/CVX) have Yahoo
    firstTradeDates in the 1960s — so use the cross-platform epoch+timedelta
    form everywhere in the pipeline."""
    return _EPOCH + timedelta(seconds=int(ts))


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
    return utc_dt(ts).date() < datetime.now(timezone.utc).date()

try:
    from curl_cffi import requests as _creq
    _HAS_CURL = True
except Exception:  # noqa: BLE001
    _HAS_CURL = False

_SEARCH = "https://query2.finance.yahoo.com/v1/finance/search"
_CHART = "https://query1.finance.yahoo.com/v8/finance/chart"


def _raw_get(url: str) -> tuple[int | None, str]:
    """One Yahoo GET, NO throttle. curl_cffi Chrome impersonation, urllib
    fallback. Used both by the throttled `_get` and by the throttle's own canary
    probe (so the canary can't recurse back into the throttle)."""
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


class YahooThrottled(RuntimeError):
    """Yahoo has rate-limited us past recovery — the AAPL canary stays throttled
    after `MAX_THROTTLES` cooldowns. Deliberately NOT caught in `resolve()` so a
    batch ingest stops hammering a banned endpoint rather than grinding through
    every remaining ISIN; `/resolve` + `/store` surface it as their 502."""


def _envf(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


class _Throttle:
    """Adaptive rate-limiter + ban detector for the whole Yahoo layer.

    Every `_get` funnels through here, so ONE instance paces all Yahoo traffic
    (search + per-candidate score fetches + candles + FX). Ported from the
    etoro-yfinance canary/backoff design, adapted to our curl_cffi HTTP layer:
    the throttle signal is an HTTP 429/999 or a network error, NOT an
    empty-but-200 body (an empty search result is legitimate, not a ban).

    Mechanism: pace each request by `delay`; on a throttle signal retry once;
    if still throttled probe a known-liquid canary (AAPL) — if the canary is
    fine the original ticker is just dead/missing (return; callers degrade to
    None/[]); if the canary is ALSO throttled we're banned, so cool down
    (doubling), permanently slow `delay` (×1.5, capped), and retry; give up
    (raise `YahooThrottled`) after `MAX_THROTTLES` bans.

    Concurrency: request STARTS are paced `delay` apart (a reserved-slot token
    bucket under a short lock), but the network call itself runs OUTSIDE the lock
    under a semaphore (`YAHOO_CONCURRENCY` in flight) — so N requests overlap the
    network latency and throughput ≈ min(1/delay, concurrency/latency) instead of
    the serial ~1/latency. A ban is recovered under the lock (single-threaded
    canary + cooldown, which blocks everyone's pacing), and the recovery raw_gets
    run WITHOUT the semaphore to avoid a lock↔semaphore deadlock. Tune via env:
    YAHOO_CONCURRENCY / _RPS / _MIN_DELAY / _MAX_DELAY / _COOLDOWN / _MAX_COOLDOWN
    / _MAX_THROTTLES / _CANARY."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.min_delay = _envf("YAHOO_MIN_DELAY", 0.1)
        self.max_delay = _envf("YAHOO_MAX_DELAY", 5.0)
        self.cooldown0 = _envf("YAHOO_COOLDOWN", 90.0)
        self.max_cooldown = _envf("YAHOO_MAX_COOLDOWN", 900.0)
        self.max_throttles = int(_envf("YAHOO_MAX_THROTTLES", 12))
        self.canary = os.environ.get("YAHOO_CANARY", "AAPL")
        rps = _envf("YAHOO_RPS", 10.0)
        self.delay = max(self.min_delay, (1.0 / rps) if rps > 0 else self.min_delay)
        self._next_ok = 0.0  # earliest monotonic time the next request may START
        self._sem = threading.Semaphore(max(1, int(_envf("YAHOO_CONCURRENCY", 4))))

    @staticmethod
    def _is_throttled(status: int | None) -> bool:
        # 429 = Too Many Requests; 999 = Yahoo's "unusual traffic" code; None =
        # network error / dropped connection (often a soft block). A 200 with an
        # empty body is NOT a throttle (legitimate empty search result).
        return status is None or status in (429, 999)

    def _canary_throttled(self) -> bool:
        """Probe AAPL via the NON-throttled raw GET (no recursion). True = Yahoo
        is throttling us; False = it's fine and the caller's ticker is just
        dead/missing."""
        s, _ = _raw_get(f"{_CHART}/{self.canary}?range=1d&interval=1d")
        return self._is_throttled(s)

    def _reserve(self) -> None:
        """Reserve the next paced request slot, then sleep until it. The lock is
        held only for the (instant) reservation, so slots hand out concurrently."""
        with self._lock:
            start = max(time.monotonic(), self._next_ok)
            self._next_ok = start + self.delay
        wait = start - time.monotonic()
        if wait > 0:
            time.sleep(wait)

    def get(self, url: str) -> tuple[int | None, str]:
        self._reserve()
        with self._sem:  # cap in-flight requests; network runs OUTSIDE _lock
            status, text = _raw_get(url)
        if not self._is_throttled(status):
            return status, text

        # Possible throttle / ban. Recover UNDER THE LOCK so only one thread runs
        # the canary + cooldown (its sleeps block everyone's pacing = all wait).
        # Recovery raw_gets DON'T take the semaphore (would deadlock vs a normal
        # thread holding the sem and waiting on the lock).
        with self._lock:
            time.sleep(min(self.delay, 0.5))  # one quick retry first
            status, text = _raw_get(url)
            if not self._is_throttled(status):
                return status, text
            cooldown = self.cooldown0
            throttles = 0
            while True:
                if not self._canary_throttled():
                    # Canary is fine → the ticker itself is dead/missing, not a
                    # ban. Hand back the non-200 tuple; callers map it to None/[].
                    return status, text
                throttles += 1
                if throttles >= self.max_throttles:
                    raise YahooThrottled(
                        f"Yahoo rate-limited past recovery after {throttles} cooldowns "
                        f"(canary {self.canary} still throttled)."
                    )
                time.sleep(cooldown)
                cooldown = min(self.max_cooldown, cooldown * 2)
                self.delay = min(self.max_delay, self.delay * 1.5)
                status, text = _raw_get(url)
                if not self._is_throttled(status):
                    return status, text


_throttle = _Throttle()


def _get(url: str) -> tuple[int | None, str]:
    """Throttled Yahoo GET — every `search`/`chart`/`chart_window`/`fx` call
    funnels through here, so the one `_Throttle` paces the whole layer and
    detects a ban via the AAPL canary. Raises `YahooThrottled` when Yahoo has
    banned us past recovery."""
    return _throttle.get(url)


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
    candles right at the listing's inception.

    Requests `events=div,splits` so the result carries `events.dividends` /
    `events.splits` (used to build the full parquet OHLCV+actions archive) and
    `indicators.adjclose`. Inert for callers that only read OHLCV/candles."""
    s, t = _get(
        f"{_CHART}/{_urlquote(symbol, safe='=^.:-')}"
        f"?period1={int(period1)}&period2={int(period2)}&interval={interval}"
        f"&events=div%2Csplits"
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
