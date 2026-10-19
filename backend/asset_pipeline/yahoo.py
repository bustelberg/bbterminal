"""Thin Yahoo Finance HTTP helpers (unofficial API).

curl_cffi Chrome impersonation to dodge blocks, urllib fallback. Best-effort:
callers handle None/empty. Not a production SLA — for research/prototyping."""
from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
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
_QUOTE = "https://query1.finance.yahoo.com/v7/finance/quote"
_PROFILE = "https://query1.finance.yahoo.com/v10/finance/quoteSummary"

# v7/quote (market cap, shares, avg volume) 401s without a consent cookie + crumb.
# Prime once, cache the session + crumb, refresh on a 401.
_quote_sess = None
_quote_crumb: str | None = None
# Priming races when several threads call asset_profile() concurrently (two
# sessions, one wasted crumb fetch). Double-checked lock around the prime only.
_sess_lock = threading.Lock()


def _usage_job_for_url(url: str) -> str:
    """Low-cardinality fallback when the caller is not inside a named job context."""
    from ingest.api_usage import current_api_job  # noqa: PLC0415

    fallback = ("yahoo_search" if "/finance/search" in url
                else "yahoo_chart" if "/finance/chart" in url
                else "yahoo_profile" if "quoteSummary" in url
                else "yahoo_quote" if "/finance/quote" in url
                else "yahoo_session")
    return current_api_job(fallback)


def _track_request(url: str, status: int | None, body: str = "") -> None:
    """Count one actual Yahoo HTTP attempt. Telemetry never changes the request result."""
    try:
        from deps import supabase  # noqa: PLC0415
        from ingest.api_usage import classify_outcome, track_api_call  # noqa: PLC0415

        track_api_call(
            supabase, source="yahoo", region="global", job=_usage_job_for_url(url),
            outcome=classify_outcome(status, has_data=bool(body)),
        )
    except Exception:  # noqa: BLE001
        pass


def _quote_session():
    """Cached (curl_cffi session, crumb) primed with Yahoo's cookie. (None, None)
    when curl_cffi is missing or priming failed — quote() then no-ops."""
    global _quote_sess, _quote_crumb
    if _quote_sess is not None and _quote_crumb:
        return _quote_sess, _quote_crumb
    if not _HAS_CURL:
        return None, None
    with _sess_lock:
        if _quote_sess is not None and _quote_crumb:  # primed while we waited
            return _quote_sess, _quote_crumb
        try:
            s = _creq.Session(impersonate="chrome")
            consent_url = "https://fc.yahoo.com"
            try:
                consent = s.get(consent_url, timeout=10)  # sets the A1 consent cookie
                _track_request(consent_url, consent.status_code, consent.text or "")
            except Exception:  # noqa: BLE001
                _track_request(consent_url, None)
            crumb_url = "https://query1.finance.yahoo.com/v1/test/getcrumb"
            try:
                r = s.get(crumb_url, timeout=10)
            except Exception:  # noqa: BLE001
                _track_request(crumb_url, None)
                raise
            _track_request(crumb_url, r.status_code, r.text or "")
            crumb = (r.text or "").strip()
            if not crumb or len(crumb) > 40:  # an HTML error page isn't a crumb
                return None, None
            _quote_sess, _quote_crumb = s, crumb
            return s, crumb
        except Exception:  # noqa: BLE001
            return None, None


def _profile_raw(sym: str) -> tuple[int | None, str]:
    """ONE assetProfile GET on the cached session+crumb, refreshing the crumb once
    on a 401. Returns `(status, text)` so it can be driven by `_Throttle.run` —
    a transport failure returns `(None, "")`, which the throttle reads as a
    possible ban and settles with the canary."""
    global _quote_sess, _quote_crumb
    s, crumb = _quote_session()
    if not s:
        return None, ""
    url = f"{_PROFILE}/{_urlquote(sym, safe='=^.:-')}"
    try:
        r = s.get(url, params={"modules": "assetProfile", "crumb": crumb}, timeout=20)
        _track_request(url, r.status_code, r.text or "")
        if r.status_code == 401:  # crumb expired — refresh once, retry
            _quote_sess = _quote_crumb = None
            s, crumb = _quote_session()
            if not s:
                return 401, ""
            r = s.get(url, params={"modules": "assetProfile", "crumb": crumb}, timeout=20)
            _track_request(url, r.status_code, r.text or "")
        return r.status_code, (r.text or "")
    except Exception:  # noqa: BLE001
        _track_request(url, None)
        return None, ""


def asset_profile(symbols: list[str]) -> dict[str, dict]:
    """Per-symbol v10 quoteSummary assetProfile -> {symbol: {sector, industry,
    country}}. One request/symbol (no batch), cookie+crumb, paced + ban-detected
    by the shared `_Throttle`.

    A symbol is PRESENT in the result iff Yahoo gave a DEFINITIVE answer:
      * 200 — a profile (or, for ETFs/crypto/futures, a 200 carrying no
        assetProfile at all): present, with the missing values as None.
      * 404 — "No fundamentals data found for symbol" / "Quote not found".
        That IS an answer: this symbol will never have a profile. Present, all
        values None, so a caller records it once and stops asking.
    Anything else (transport error, 401 after a crumb refresh, 429/999) is
    ABSENT, so a caller retries it instead of recording a false "no country".
    Raises `YahooThrottled` when Yahoo has banned us past recovery, rather than
    silently returning a run of empty profiles."""
    out: dict[str, dict] = {}
    if not _HAS_CURL:
        return out
    empty = {"sector": None, "industry": None, "country": None}
    for sym in dict.fromkeys(x for x in symbols if x):
        status, text = _throttle.run(lambda s=sym: _profile_raw(s))
        if status == 404:  # definitive: no profile exists for this symbol
            out[sym] = dict(empty)
            continue
        if status != 200:
            continue  # unanswered — omit, so the caller can retry
        try:
            res = (json.loads(text).get("quoteSummary") or {}).get("result") or []
        except Exception:  # noqa: BLE001
            continue
        ap = (res[0].get("assetProfile") or {}) if res else {}
        out[sym] = {"sector": ap.get("sector") or None,
                    "industry": ap.get("industry") or None,
                    "country": ap.get("country") or None}
    return out


_FUND_SECTOR_LABELS = {
    "realestate": "Real Estate",
    "consumer_cyclical": "Consumer Cyclical",
    "basic_materials": "Basic Materials",
    "consumer_defensive": "Consumer Defensive",
    "technology": "Technology",
    "communication_services": "Communication Services",
    "financial_services": "Financial Services",
    "utilities": "Utilities",
    "industrials": "Industrials",
    "energy": "Energy",
    "healthcare": "Healthcare",
}


def _fund_sector_weightings(symbol: str, *, vehicle: str) -> list[dict]:
    """One explicitly registered fund's Yahoo sector weights, as percentage rows.

    Yahoo identifies ETFs through ``fundProfile.legalType`` and mutual funds through
    ``price.quoteType``. Keep those as separate accepted identities: callers hold a mixture of
    exchange-traded funds, mutual funds and in-house certificates, and a similarly shaped
    ``topHoldings`` object alone is not permission to expose look-through.
    """
    global _quote_sess, _quote_crumb
    if not symbol or not _HAS_CURL:
        return []

    def request() -> tuple[int | None, str]:
        global _quote_sess, _quote_crumb
        session, crumb = _quote_session()
        if not session:
            return None, ""
        url = f"{_PROFILE}/{_urlquote(symbol, safe='=^.:-')}"
        try:
            response = session.get(
                url,
                params={"modules": "topHoldings,fundProfile,price", "crumb": crumb},
                timeout=20,
            )
            _track_request(url, response.status_code, response.text or "")
            if response.status_code == 401:
                _quote_sess = _quote_crumb = None
                session, crumb = _quote_session()
                if not session:
                    return 401, ""
                response = session.get(
                    url,
                    params={"modules": "topHoldings,fundProfile,price", "crumb": crumb},
                    timeout=20,
                )
                _track_request(url, response.status_code, response.text or "")
            return response.status_code, response.text or ""
        except Exception:  # noqa: BLE001
            _track_request(url, None)
            return None, ""

    status, text = _throttle.run(request)
    if status != 200:
        return []
    try:
        result = ((json.loads(text).get("quoteSummary") or {}).get("result") or [])[0]
        legal_type = (result.get("fundProfile") or {}).get("legalType")
        quote_type = (result.get("price") or {}).get("quoteType")
        if vehicle == "etf" and legal_type != "Exchange Traded Fund":
            return []
        if vehicle == "mutual_fund" and quote_type != "MUTUALFUND":
            return []
        raw_rows = (result.get("topHoldings") or {}).get("sectorWeightings") or []
    except (IndexError, TypeError, ValueError):
        return []

    rows = []
    for item in raw_rows:
        if not isinstance(item, dict) or len(item) != 1:
            continue
        key, value = next(iter(item.items()))
        raw = (value or {}).get("raw") if isinstance(value, dict) else None
        if key not in _FUND_SECTOR_LABELS or not isinstance(raw, (int, float)) or raw <= 0:
            continue
        rows.append({"sector": _FUND_SECTOR_LABELS[key], "weight_pct": float(raw) * 100.0})
    return rows


def fund_sector_weightings(symbol: str) -> list[dict]:
    """Yahoo sector weights for an explicitly registered ETF only."""
    return _fund_sector_weightings(symbol, vehicle="etf")


def mutual_fund_sector_weightings(symbol: str) -> list[dict]:
    """Yahoo sector weights for an explicitly registered mutual-fund share class only."""
    return _fund_sector_weightings(symbol, vehicle="mutual_fund")


def quote(symbols: list[str]) -> dict[str, dict]:
    """Batch v7 quote -> {symbol: {marketCap, currency, sharesOutstanding, …}}.
    Chunks of 100, cookie+crumb auth (refreshed once on a 401), shared circuit.
    Best-effort — symbols Yahoo doesn't return are simply absent. This is the
    market-cap source (listing-INDEPENDENT: query a company's PRIMARY listing)."""
    syms = [s for s in dict.fromkeys(symbols) if s]
    out: dict[str, dict] = {}
    if not syms:
        return out
    for i in range(0, len(syms), 100):
        chunk = syms[i:i + 100]

        def request():
            global _quote_sess, _quote_crumb
            for attempt in range(2):
                s, crumb = _quote_session()
                if not s:
                    return None, ""
                try:
                    r = s.get(_QUOTE, params={"symbols": ",".join(chunk), "crumb": crumb}, timeout=20)
                    _track_request(_QUOTE, r.status_code, r.text or "")
                except Exception:  # noqa: BLE001
                    _track_request(_QUOTE, None)
                    return None, ""
                if r.status_code != 401 or attempt == 1:
                    return r.status_code, r.text or ""
                _quote_sess = _quote_crumb = None
            return 401, ""

        status, text = _throttle.run(request)
        if status != 200:
            continue
        try:
            for q in (json.loads(text).get("quoteResponse") or {}).get("result", []) or []:
                if q.get("symbol"):
                    out[q["symbol"]] = q
        except (ValueError, TypeError, AttributeError):
            continue
    return out


def _raw_get(url: str) -> tuple[int | None, str]:
    """One Yahoo GET, NO throttle. curl_cffi Chrome impersonation, urllib
    fallback. Used both by the throttled `_get` and by the throttle's own canary
    probe (so the canary can't recurse back into the throttle)."""
    if _HAS_CURL:
        try:
            r = _creq.get(url, impersonate="chrome", timeout=30)
            _track_request(url, r.status_code, r.text or "")
            return r.status_code, (r.text or "")
        except Exception:  # noqa: BLE001
            _track_request(url, None)
            return None, ""
    from urllib.error import HTTPError  # noqa: PLC0415
    from urllib.request import Request, urlopen  # noqa: PLC0415
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with urlopen(req, timeout=30) as resp:  # noqa: S310
            body = resp.read().decode("utf-8", "replace")
            _track_request(url, resp.status, body)
            return resp.status, body
    except HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        _track_request(url, exc.code, body)
        return exc.code, body
    except Exception:  # noqa: BLE001
        _track_request(url, None)
        return None, ""


class YahooThrottled(RuntimeError):
    """The shared Yahoo circuit is open. Deliberately NOT caught in `resolve()` so a
    batch ingest stops hammering a banned endpoint rather than grinding through
    every remaining ISIN; `/resolve` + `/store` surface it as their 502."""


def _envf(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


class _Throttle:
    """Pace requests and share one fail-fast circuit across all Yahoo callers.

    A confirmed ban opens the circuit until a monotonic deadline; no worker sleeps
    through that cooldown. After it expires only one caller probes recovery. Failed
    probes double the cooldown (capped); other callers fail immediately. Locks guard
    state only, never network calls or sleeps. MAX_THROTTLES is intentionally retired.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.min_delay = _envf("YAHOO_MIN_DELAY", 0.1)
        self.max_delay = _envf("YAHOO_MAX_DELAY", 5.0)
        self.cooldown0 = max(1.0, _envf("YAHOO_COOLDOWN", 90.0))
        self.max_cooldown = max(self.cooldown0, _envf("YAHOO_MAX_COOLDOWN", 900.0))
        self._cooldown = self.cooldown0
        self._open_until = 0.0
        self._recovering = False
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
        """Probe AAPL without recursion. Only HTTP 200 certifies recovery."""
        s, _ = _raw_get(f"{_CHART}/{self.canary}?range=1d&interval=1d")
        # An unavailable/unauthorized canary cannot certify provider recovery.
        return s != 200

    def _unavailable(self) -> YahooThrottled:
        remaining = max(0.0, self._open_until - time.monotonic())
        return YahooThrottled(
            f"Yahoo circuit open; retry in {remaining:.0f}s"
            if remaining else "Yahoo recovery probe already running; retry later"
        )

    def _trip(self) -> None:
        with self._lock:
            self._open_until = time.monotonic() + self._cooldown
            self._cooldown = min(self.max_cooldown, self._cooldown * 2)
            self.delay = min(self.max_delay, self.delay * 1.5)
            self._recovering = False

    def _reset(self) -> None:
        with self._lock:
            self._open_until = 0.0
            self._cooldown = self.cooldown0
            self._recovering = False

    def _available(self) -> None:
        with self._lock:
            if self._recovering or time.monotonic() < self._open_until:
                raise self._unavailable()
            probe = self._open_until > 0
            if probe:
                self._recovering = True
        if probe:
            try:
                if self._canary_throttled():
                    raise YahooThrottled("Yahoo recovery canary still unavailable")
            except BaseException:
                self._trip()
                raise
            self._reset()

    def _reserve(self) -> None:
        """Reserve the next paced request slot, then sleep until it. The lock is
        held only for the (instant) reservation, so slots hand out concurrently."""
        with self._lock:
            start = max(time.monotonic(), self._next_ok)
            self._next_ok = start + self.delay
        wait = start - time.monotonic()
        if wait > 0:
            time.sleep(wait)

    def run(self, do_request: "Callable[[], tuple[int | None, str]]") -> tuple[int | None, str]:
        """Pace + ban-guard ONE request. `do_request()` performs it and returns
        `(status, text)`; it is retried as-is on a throttle signal, so it must be
        idempotent. The URL-fetching `get()` is the common case, but the
        cookie+crumb assetProfile fetch drives the same machinery through here."""
        self._available()
        with self._sem:  # cap in-flight requests; network runs OUTSIDE _lock
            self._available()
            self._reserve()
            self._available()  # another request may have opened the circuit while pacing
            status, text = do_request()
        if not self._is_throttled(status):
            return status, text

        with self._lock:
            if self._recovering or self._open_until:
                raise self._unavailable()
            self._recovering = True
        try:
            # One immediate retry distinguishes a transient transport failure from
            # a ban. Peers fail fast while this caller checks the provider.
            status, text = do_request()
            if not self._is_throttled(status):
                self._reset()
                return status, text
            if self._canary_throttled():
                raise YahooThrottled(f"Yahoo rate-limited; canary {self.canary} unavailable")
            self._reset()  # ticker-specific failure, not a provider ban
            return status, text
        except BaseException:
            self._trip()
            raise

    def get(self, url: str) -> tuple[int | None, str]:
        return self.run(lambda: _raw_get(url))


_throttle = _Throttle()


def _get(url: str) -> tuple[int | None, str]:
    """Throttled Yahoo GET — every `search`/`chart`/`chart_window`/`fx` call
    funnels through here, so the one `_Throttle` paces the whole layer and
    detects a ban via the AAPL canary. Raises `YahooThrottled` while the shared
    circuit is open or another caller is probing recovery."""
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


def closed_daily_bars(symbol: str) -> list[tuple[date, float, float | None]]:
    """All completed Yahoo daily bars as ``(date, close, volume)``.

    This is the common price/volume feed for user-added ETF benchmarks. It
    deliberately uses the raw close (not adjusted close): Diversifier reports
    price returns and must not quietly turn into a dividend-inclusive series.
    """
    chart_data = chart_window(symbol, 0, int(time.time()), "1d")
    if not chart_data:
        return []
    timestamps = chart_data.get("timestamp") or []
    quote = ((chart_data.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []
    out: list[tuple[date, float, float | None]] = []
    for index, (timestamp, close) in enumerate(zip(timestamps, closes)):
        if close is None or not is_closed_bar(timestamp):
            continue
        try:
            volume = volumes[index] if index < len(volumes) else None
            out.append((utc_dt(timestamp).date(), float(close),
                        float(volume) if volume is not None else None))
        except (TypeError, ValueError):
            continue
    return out


_FX: dict[str, float | None] = {"EUR": 1.0}


def fx_to_eur(ccy: str | None) -> float | None:
    """EUR per 1 unit of `ccy`, where `ccy` is a QUOTE currency — a minor unit stays a minor
    unit (GBp -> GBP/100), because that is what a PRICE is denominated in. Cached per run.

     THIS RATE IS FOR A PRICE, NOT FOR A MARKET CAP. Yahoo quotes a London listing in PENCE but
    reports its `marketCap` in POUNDS — same payload, same `currency: "GBp"`, two different units.
    A caller converting a cap must normalise to the MAJOR unit first (`SUBUNIT[ccy][0]`) and ask
    for THAT rate; passing "GBp" here divides an already-major figure by 100 and yields a market
    cap 100x too small that still looks like a number (Shell: EUR 1.95bn for a EUR 195bn company).
    See `scripts/asset_backfill_marketcap.py`.

     The minor-unit map is `asset_pipeline.fx.SUBUNIT` — SHARED, never re-derived. This function
    special-cased "GBp" inline until 2026-07-16 and therefore knew nothing of `ZAc` / `ILA`: it
    asked Yahoo for a nonexistent "ZAcEUR=X", got None, and every caller read that as "cannot be
    priced". That silently zeroed `med_adv_eur` for Johannesburg listings — a liquidity of zero
    is not a missing value, it loses the ranking in `resolve()` outright.
    """
    if not ccy:
        return None
    from .fx import SUBUNIT  # noqa: PLC0415 — module-level would be fine; kept local & cheap

    base, div = SUBUNIT.get(ccy, (ccy, 1.0))
    if base not in _FX:
        c = chart(f"{base}EUR=X", rng="5d")
        rate = None
        if c:
            closes = [x for x in (((c.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []) if x]
            rate = closes[-1] if closes else (c.get("meta") or {}).get("regularMarketPrice")
        _FX[base] = rate
    r = _FX.get(base)
    return None if r is None else r / div
