"""Pull the full tradable-instrument universe from the lynqs (Leonteq) eportal
AMC-gateway search API into a CSV.

The API (`/eportal/amc-gateway/api/v2/instruments/search`) is a SEARCH endpoint
(`terms` + `offset` + `limit`), grouped by `productType` - there is no bulk
export. So we ENUMERATE: probe the broadest query the API accepts, paginate each
search term, and dedupe by `id`. A gentle rate-limit calibration finds a safe
request rate and we stay well under it (the endpoint sits behind Imperva - be a
good citizen: this is deliberately conservative).

Auth = your browser session. Copy the `cookie` request header from a logged-in
lynqs request into the LYNQS_COOKIE env var (or a --cookie-file). It expires, so
refresh it when you start seeing 401/403.

    # PowerShell:  $env:LYNQS_COOKIE = 'eportal-single-app=...; lynqs-auth=...'
    # bash:        export LYNQS_COOKIE='eportal-single-app=...; lynqs-auth=...'
    uv run python scripts/lynqs_universe.py --out lynqs_universe.csv

Output columns: id, ticker, name, productType, ric, isin, currency. Then upload
the CSV on /asset-pipeline and pick the `isin` column to ingest it as a universe.

Notes / etiquette:
  * Conservative by design - calibrated rate, exponential backoff, retries.
  * Imperva may still challenge a non-browser client; if you get HTML instead of
    JSON, refresh the cookie (and run from the same network as your browser).
  * --depth 2 enumerates all 2-char prefixes (~1,300 searches) for completeness;
    --depth 1 is a quick pass (36 searches) that may miss the long tail.
"""
from __future__ import annotations

import argparse
import csv
import itertools
import os
import statistics
import string
import sys
import time
from collections import Counter
from pathlib import Path

try:
    from curl_cffi import requests as creq  # browser-TLS impersonation (Imperva)
except ImportError:  # pragma: no cover
    creq = None

BASE = "https://www.lynqs.com/eportal/amc-gateway/api/v2/instruments/search"
FIELDS = ["id", "ticker", "name", "productType", "ric", "isin", "currency"]


# ---------------------------------------------------------------- HTTP session
def _session(cookie: str, currency: str, language: str):
    s = creq.Session(impersonate="chrome")
    s.headers.update({
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "referer": "https://www.lynqs.com/eportal/",
        "x-ltq-currency": currency,
        "x-ltq-language": language,
        "cookie": cookie,
    })
    return s


def _search(s, terms: str, offset: int, limit: int, timeout: int = 30):
    """Returns (status_code, json_or_None). Never raises."""
    try:
        r = s.get(BASE, params={"terms": terms, "offset": offset, "limit": limit}, timeout=timeout)
    except Exception:  # noqa: BLE001
        return None, None
    if r.status_code != 200:
        return r.status_code, None
    try:
        return 200, r.json()
    except Exception:  # noqa: BLE001 - Imperva HTML challenge etc.
        return 200, None


def _flatten(payload) -> list[dict]:
    """Group-by-type payload -> flat list of {FIELDS}."""
    out: list[dict] = []
    if not isinstance(payload, dict):
        return out
    for items in payload.values():
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    out.append({k: (it.get(k) or "") for k in FIELDS})
    return out


# --------------------------------------------------------------------- pacing
class Pacer:
    """Min-interval pacing + exponential backoff on throttle signals."""

    def __init__(self, rate: float) -> None:
        self.interval = 1.0 / max(0.2, rate)
        self._next = 0.0
        self.backoff = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        target = max(now, self._next)
        if target > now:
            time.sleep(target - now)
        self._next = time.monotonic() + self.interval

    def penalize(self) -> float:
        self.backoff = min(60.0, (self.backoff or 1.0) * 2)
        return self.backoff

    def relax(self) -> None:
        self.backoff = self.backoff * 0.5 if self.backoff > 1.0 else 0.0


def _search_retry(s, term, offset, limit, pacer: Pacer, tries: int = 5):
    """Paced GET with retry/backoff on 429/5xx/network. (code, data)."""
    code = None
    for _ in range(tries):
        pacer.wait()
        code, data = _search(s, term, offset, limit)
        if code == 200 and data is not None:
            pacer.relax()
            return code, data
        if code in (401, 403):
            return code, None
        # 429 / 5xx / None / non-JSON-200 -> back off and retry
        time.sleep(pacer.penalize())
    return code, None


# ---------------------------------------------------------------- calibration
def calibrate(s, probe: str = "aa") -> float:
    """Gently ramp the request rate; stop at the first throttle sign or latency
    spike; return 70% of the last comfortable rate. Deliberately caps at 8/s."""
    print("[calibrate] probing a safe request rate...", flush=True)
    safe = 1.0
    for rate in (1, 2, 3, 4, 6, 8):
        interval = 1.0 / rate
        lat: list[float] = []
        throttled = False
        for _ in range(max(3, rate * 2)):  # ~2 seconds of traffic per step
            t0 = time.time()
            code, _data = _search(s, probe, 0, 5)
            lat.append(time.time() - t0)
            if code in (401, 403):
                print(f"[calibrate] auth/blocked ({code}) - refresh LYNQS_COOKIE.", flush=True)
                sys.exit(2)
            if code == 429 or (code or 0) >= 500 or code is None:
                throttled = True
                break
            time.sleep(interval)
        med = statistics.median(lat) if lat else 99.0
        if throttled:
            print(f"[calibrate] throttled near ~{rate} req/s - stopping ramp.", flush=True)
            break
        if med > 2.5:
            print(f"[calibrate] latency rising ({med:.1f}s) at ~{rate} req/s - near limit.", flush=True)
            break
        print(f"[calibrate] ~{rate} req/s ok (median {med:.2f}s)", flush=True)
        safe = rate
        time.sleep(1.0)  # cool-down between steps
    rate = max(0.5, round(safe * 0.7, 1))
    print(f"[calibrate] using {rate} req/s (70% of {safe:.0f}).", flush=True)
    return rate


# -------------------------------------------------------------------- probe
def probe(s) -> dict:
    """A handful of cheap requests to learn the endpoint's shape:
      1. which (if any) 'match-all' term returns the broadest result, and
      2. whether a large per-group `limit` is honored (-> one-shot fetch).
    Returns {'term': str|None, 'limit': int, 'counts': {type: n}, 'truncated': [...]}"""
    print("[probe] discovering match-all term + limit ceiling (a few requests)...", flush=True)
    best_term, best_n = None, -1
    for t in ("", "*", "%", ".", "a", "e"):
        code, data = _search(s, t, 0, 50)
        if code in (401, 403):
            print(f"[probe] auth/blocked ({code}) - refresh LYNQS_COOKIE.")
            sys.exit(2)
        n = len(_flatten(data)) if code == 200 else 0
        print(f"  term {t!r:>4}: {n} rows (http {code})", flush=True)
        if n > best_n:
            best_term, best_n = t, n
        time.sleep(0.5)
    if best_n <= 0:
        print("[probe] no term returned data - API needs a real query; will enumerate prefixes.", flush=True)
        return {"term": None, "limit": 100, "counts": {}, "truncated": []}

    print(f"[probe] best term {best_term!r} ({best_n} rows @50). Testing limit ceiling...", flush=True)
    prev, chosen, counts = -1, 50, {}
    for lim in (500, 5000, 50000):
        code, data = _search(s, best_term, 0, lim)
        rows = _flatten(data) if code == 200 else []
        counts = dict(Counter(r["productType"] for r in rows))
        trunc = [t for t, n in counts.items() if n >= lim]
        total = len(rows)
        print(f"  limit={lim}: {total} rows {counts}"
              + (f"  still-truncated:{trunc}" if trunc else "  <- nothing hit the cap"), flush=True)
        chosen = lim
        if total <= prev or not trunc:
            break  # no more growth, or every group fit under the limit -> complete
        prev = total
        time.sleep(0.5)
    trunc = [t for t, n in counts.items() if n >= chosen]
    return {"term": best_term, "limit": chosen, "counts": counts, "truncated": trunc}


def fetch_minimal(s, term, limit, pacer: Pacer, keep, max_pages: int):
    """Match-all path: ONE big request; only paginate groups that came back full
    (returned exactly `limit`). Returns (seen, complete)."""
    seen: dict[str, dict] = {}

    def _absorb(rows) -> int:
        added = 0
        for r in rows:
            if keep and r["productType"] not in keep:
                continue
            k = r["id"] or f"{r['isin']}|{r['ticker']}"
            if k and k not in seen:
                seen[k] = r
                added += 1
        return added

    code, data = _search_retry(s, term, 0, limit, pacer)
    if code != 200 or not data:
        return seen, False
    first = _flatten(data)
    _absorb(first)
    full = [t for t, n in Counter(r["productType"] for r in first).items() if n >= limit]
    if not full:
        print(f"[fetch] complete in ONE request - {len(seen):,} instruments.", flush=True)
        return seen, True
    # Some group exceeded `limit` -> page the whole result set until it drains.
    print(f"[fetch] groups over the limit, paginating: {full}", flush=True)
    for page in range(1, max_pages):
        code, data = _search_retry(s, term, page * limit, limit, pacer)
        if code != 200 or not data:
            break
        rows = _flatten(data)
        if not rows or _absorb(rows) == 0:
            break
        print(f"[fetch] page {page + 1}: {len(seen):,} unique", flush=True)
    return seen, True


def enumerate_terms(depth: int) -> list[str]:
    alphabet = string.ascii_lowercase + string.digits
    terms = ["".join(p) for p in itertools.product(alphabet, repeat=max(1, depth))]
    print(f"[enumerate] no match-all term -> {len(terms)} {depth}-char prefixes.", flush=True)
    return terms


# ----------------------------------------------------------------------- main
def _write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)


def fetch_enumerated(s, terms, limit, max_pages, pacer: Pacer, keep, out_path: Path) -> dict:
    """Fallback: paginate every prefix term, dedupe by id. Checkpoints to CSV."""
    seen: dict[str, dict] = {}
    t0 = time.time()
    for ti, term in enumerate(terms, 1):
        for page in range(max_pages):
            code, data = _search_retry(s, term, page * limit, limit, pacer)
            if code in (401, 403):
                print(f"[fetch] {code} - session dead. Refresh the cookie and re-run.", flush=True)
                return seen
            if code != 200 or not data:
                break
            rows = _flatten(data)
            if not rows:
                break
            for r in rows:
                if keep and r["productType"] not in keep:
                    continue
                k = r["id"] or f"{r['isin']}|{r['ticker']}"
                if k:
                    seen.setdefault(k, r)
        if ti % 25 == 0 or ti == len(terms):
            print(f"[fetch] {ti}/{len(terms)} terms - {len(seen):,} unique - {time.time() - t0:0.0f}s", flush=True)
            _write_csv(out_path, list(seen.values()))  # checkpoint
    return seen


def main() -> None:
    ap = argparse.ArgumentParser(description="Pull the lynqs tradable universe to CSV (probe-first, min requests).")
    ap.add_argument("--out", default="lynqs_universe.csv")
    ap.add_argument("--cookie-file", help="file with the cookie string (else LYNQS_COOKIE env)")
    ap.add_argument("--currency", default="CHF")
    ap.add_argument("--language", default="en")
    ap.add_argument("--limit", type=int, default=50000, help="per-group page size for the one-shot fetch")
    ap.add_argument("--max-pages", type=int, default=40, help="pagination cap")
    ap.add_argument("--depth", type=int, default=2, help="prefix length for the enumeration fallback")
    ap.add_argument("--types", default="", help="comma-separated productTypes to keep (default: all)")
    ap.add_argument("--probe-only", action="store_true", help="just probe the endpoint's limits and exit")
    ap.add_argument("--force-enumerate", action="store_true", help="skip the one-shot path, enumerate prefixes")
    args = ap.parse_args()

    if creq is None:
        sys.exit("curl_cffi is required (it's a backend dependency - run via `uv run`).")
    cookie = (Path(args.cookie_file).read_text(encoding="utf-8").strip()
              if args.cookie_file else os.environ.get("LYNQS_COOKIE", "").strip())
    if not cookie:
        sys.exit("No cookie: set LYNQS_COOKIE (or --cookie-file) to your logged-in lynqs `cookie` header.")
    keep = {t.strip() for t in args.types.split(",") if t.strip()} or None
    out_path = Path(args.out)

    s = _session(cookie, args.currency, args.language)
    info = probe(s)
    if args.probe_only:
        print("\n[probe-only] recommendation:", flush=True)
        if info["term"] is not None and not info["truncated"]:
            print(f"  -> ONE request covers it: terms={info['term']!r} limit={info['limit']} "
                  f"({sum(info['counts'].values()):,} instruments across {info['counts']}).", flush=True)
        elif info["term"] is not None:
            print(f"  -> match-all term {info['term']!r} works but groups {info['truncated']} exceed "
                  f"limit {info['limit']} - those get paginated.", flush=True)
        else:
            print("  -> no match-all term; falls back to prefix enumeration.", flush=True)
        return

    pacer = Pacer(calibrate(s, probe=info["term"] or "aa"))

    if info["term"] is not None and not args.force_enumerate:
        seen, ok = fetch_minimal(s, info["term"], min(args.limit, max(info["limit"], 500)),
                                 pacer, keep, args.max_pages)
        if not ok or not seen:  # one-shot failed -> enumerate
            print("[fetch] one-shot path came up empty - falling back to enumeration.", flush=True)
            seen = fetch_enumerated(s, enumerate_terms(args.depth), 100, args.max_pages, pacer, keep, out_path)
    else:
        seen = fetch_enumerated(s, enumerate_terms(args.depth), 100, args.max_pages, pacer, keep, out_path)

    rows = list(seen.values())
    _write_csv(out_path, rows)
    by_type = dict(Counter(r["productType"] for r in rows))
    with_isin = sum(1 for r in rows if r["isin"])
    print(f"\n[done] {len(rows):,} unique instruments -> {out_path}", flush=True)
    print(f"[done] with ISIN: {with_isin:,}  -  by type: "
          + ", ".join(f"{k} {v}" for k, v in sorted(by_type.items(), key=lambda x: -x[1])), flush=True)


if __name__ == "__main__":
    main()
