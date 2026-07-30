"""Probe the GuruFocus API and write a local catalogue of what actually works.

WHY THIS EXISTS
    We pay for the LEGACY API (`https://api.gurufocus.com/public/user/{key}/…`),
    and it has no authoritative documentation. The docs site
    (gurufocus.com/api/overview, /data-api/*) describes a DIFFERENT, newer product
    whose paths are plural (`/stocks/{region_code}`) and whose auth we don't use.
    The Excel manual PDF documents the add-in, not the API. Meanwhile the endpoint
    that resolves an ISIN to a symbol — `isin/{ISIN}` — is real, load-bearing, and
    documented nowhere.

    So: probe it, record what came back, and keep the answer in the repo.

QUOTA
    Every probe is a real, billed call against the monthly GuruFocus quota
    (`MONTHLY_API_LIMIT` = 20,000 per region in `ingest/api_usage.py`). Two guards:

      * `--max-calls` (default 60) is a hard ceiling; the run aborts rather than
        silently overspending.
      * Responses are cached to `--cache-dir` (gitignored). A re-run costs ZERO
        calls unless you pass `--refresh`. Iterate on the report, not the API.

    `--dry-run` prints the probe plan and spends nothing.

USAGE
    cd backend && PYTHONPATH=. uv run python scripts/gurufocus_catalog.py
    cd backend && PYTHONPATH=. uv run python scripts/gurufocus_catalog.py --dry-run
    cd backend && PYTHONPATH=. uv run python scripts/gurufocus_catalog.py --refresh --max-calls 80

OUTPUT
    docs/gurufocus_api.md      — the human catalogue (committed)
    backend/gurufocus_api.json — machine-readable shapes (committed)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import deps  # noqa: F401  — loads .env before anything reads GURUFOCUS_*
from ingest.earnings._api_client import _api_request, _build_api_url

# Probe subjects. AAPL is the US bare-ticker form; WBO:ANDR exercises the
# `EXCHANGE:TICKER` form on a non-US listing (and is the Vienna listing of the
# company we currently mis-store on Prague).
SYMBOL_US = "AAPL"
SYMBOL_EU = "WBO:ANDR"
ISIN = "US0378331005"          # Apple
ISIN_EU = "AT0000730007"       # Andritz AG
CUSIP = "037833100"            # Apple

# THE TRAP THIS SCRIPT EXISTS TO CATCH
# ------------------------------------
# `stock/{sym}/<anything>` NEVER 404s. An unrecognised sub-path returns HTTP 200
# and a plausible-looking date-indexed series of zeros — the same 46 fiscal-year
# points a real indicator would return. Probing naively, `stock/AAPL/news` and
# `stock/AAPL/splits` both look like working endpoints.
#
# So every probe is compared against a canary sub-path that cannot exist. Any
# response equal to the canary's, or that is an all-zero series, is a router
# fallback and is reported as NOT REAL.
CANARY_PATH = f"stock/{SYMBOL_US}/zzz_canary_not_an_endpoint"


@dataclass
class Probe:
    path: str
    note: str = ""
    group: str = "stock"


@dataclass
class Finding:
    path: str
    group: str
    note: str
    status: int | None
    # "real"           — returned data that is not the router's fallback
    # "router-fallback"— HTTP 200, but the canary's all-zero series. NOT an endpoint.
    # "empty"          — valid response, no rows (the identifier didn't match)
    # "missing"        — 404 / non-JSON / transport failure
    verdict: str = "missing"
    kind: str = ""              # list | dict | other
    size: int | None = None     # len() of the top-level container
    keys: list[str] = field(default_factory=list)
    item_keys: list[str] = field(default_factory=list)
    sample: str = ""

    @property
    def ok(self) -> bool:
        return self.verdict == "real"


def _probes() -> list[Probe]:
    """Known-good, documented-elsewhere, and speculative paths.

    The speculative ones are the point: the only way to learn that `isin/{ISIN}`
    and `stock/{sym}/dividends` exist is to ask.
    """
    stock = [
        ("summary", "company_data incl. ISIN, currency, exchange, price — the reverse of isin/"),
        ("financials", "the 263-field blob; ~36,700 metric_data rows/company if parsed unrestricted"),
        ("keyratios", ""),
        ("quote", ""),
        ("price", ""),
        ("dividend", "PER-SHARE payments: amount, ex_date, record_date, pay_date, currency, type"),
        ("dividends", "CASH FLOW for dividends ($ millions, negative) — NOT per share"),
        ("split", "expected: router fallback"),
        ("splits", "expected: router fallback"),
        ("analyst_estimate", "used by the earnings dashboard"),
        ("analyst_estimates", "expected: router fallback (the singular is the real one)"),
        ("ownership", ""),
        ("insider", ""),
        ("guru", "expected: router fallback"),
        ("gurus", ""),
        ("news", "expected: router fallback"),
        ("filings", ""),
        ("segments", "expected: router fallback"),
        ("profile", "expected: router fallback"),
        ("valuation", "expected: router fallback"),
        ("gf_score", ""),
        ("gfvalue", ""),
        ("rank", "expected: router fallback"),
        ("earnings", "expected: router fallback"),
        ("indicator_price", "expected: router fallback — prices come from stock/{sym}/price"),
    ]
    out = [Probe(f"stock/{SYMBOL_US}/{p}", note, "stock") for p, note in stock]
    # A non-US listing on the EXCHANGE:TICKER form — does every endpoint accept it?
    out += [Probe(f"stock/{SYMBOL_EU}/{p}", f"non-US form · {note}", "stock (non-US)")
            for p, note in (("summary", ""), ("dividends", ""), ("financials", ""))]
    # Global / lookup endpoints.
    out += [
        Probe(f"isin/{ISIN}", "UNDOCUMENTED — ISIN -> [{symbol, exchange}] for every listing", "lookup"),
        Probe(f"isin/{ISIN_EU}", "UNDOCUMENTED — the Andritz case (WBO vs XPRA)", "lookup"),
        Probe("exchange_list", "UNDOCUMENTED — {region: [exchange codes]}", "lookup"),
        Probe("stock_list", "", "lookup"),
        Probe("stock_list/USA", "", "lookup"),
        Probe("country_list", "", "lookup"),
        Probe("sector_list", "", "lookup"),
        Probe("industry_list", "", "lookup"),
        Probe("guru_list", "", "lookup"),
        Probe("insider_list", "", "lookup"),
        Probe("economic_indicator_list", "", "lookup"),
        Probe(f"cusip/{CUSIP}", "UNDOCUMENTED — a CUSIP sibling of isin/?", "lookup"),
        Probe(f"cusip/{ISIN}", "an ISIN passed to cusip/ — expect an empty list", "lookup"),
        Probe(f"figi/{ISIN}", "does a FIGI sibling exist?", "lookup"),
        Probe(f"search/{SYMBOL_US}", "", "lookup"),
        Probe(CANARY_PATH, "CANARY — must not exist; defines the router-fallback payload", "canary"),
    ]
    return out


def _is_zero_series(data: Any) -> bool:
    """The router's fallback: `[[date, 0], …]` with every value zero."""
    if not isinstance(data, list) or not data:
        return False
    vals = []
    for row in data:
        if not (isinstance(row, list) and len(row) == 2):
            return False
        vals.append(row[1])
    return all(isinstance(v, (int, float)) and v == 0 for v in vals)


def _shape(data: Any) -> tuple[str, int | None, list[str], list[str], str]:
    """(kind, size, top-level keys, element keys, short sample)."""
    if isinstance(data, dict):
        keys = sorted(data)[:12]
        return "dict", len(data), keys, [], json.dumps(data, default=str)[:220]
    if isinstance(data, list):
        item_keys: list[str] = []
        if data and isinstance(data[0], dict):
            item_keys = sorted(data[0])[:12]
        return "list", len(data), [], item_keys, json.dumps(data[:2], default=str)[:220]
    return type(data).__name__, None, [], [], json.dumps(data, default=str)[:220]


def classify(path: str, data: Any, canary: Any) -> str:
    """Which of the four verdicts this response earns.

    Order matters. A response identical to the canary's is a fallback even if it
    happens to be non-degenerate, and an all-zero series is a fallback even if the
    canary itself failed to load — belt and braces, because a false "real" here
    becomes an endpoint someone builds on.
    """
    if path == CANARY_PATH:
        return "canary"
    if data is None:
        return "missing"
    if canary is not None and data == canary:
        return "router-fallback"
    if _is_zero_series(data):
        return "router-fallback"
    if hasattr(data, "__len__") and len(data) == 0:
        return "empty"
    return "real"


def _cache_path(cache_dir: Path, path: str) -> Path:
    return cache_dir / (hashlib.sha256(path.encode()).hexdigest()[:20] + ".json")


def run(probes: list[Probe], cache_dir: Path, max_calls: int, refresh: bool) -> list[Finding]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    findings: list[Finding] = []
    payloads: dict[str, Any] = {}
    spent = 0

    # The canary must be fetched first — everything else is classified against it.
    ordered = sorted(probes, key=lambda p: p.path != CANARY_PATH)

    for pr in ordered:
        cp = _cache_path(cache_dir, pr.path)
        cached = None
        if cp.exists() and not refresh:
            cached = json.loads(cp.read_text(encoding="utf-8"))

        if cached is None:
            if spent >= max_calls:
                print(f"  [budget] {max_calls} calls spent — stopping before {pr.path}", file=sys.stderr)
                break
            r = _api_request(_build_api_url(pr.path))
            spent += 1
            cached = {"status": r.status_code, "data": r.data}
            cp.write_text(json.dumps(cached, default=str), encoding="utf-8")

        data, status = cached.get("data"), cached.get("status")
        payloads[pr.path] = data
        kind, size, keys, item_keys, sample = _shape(data) if data is not None else ("", None, [], [], "")
        findings.append(Finding(
            path=pr.path, group=pr.group, note=pr.note, status=status,
            kind=kind, size=size, keys=keys, item_keys=item_keys, sample=sample,
        ))

    canary = payloads.get(CANARY_PATH)
    if canary is None:
        print("  WARNING: the canary itself returned nothing — fallback detection is off",
              file=sys.stderr)

    for f in findings:
        f.verdict = classify(f.path, payloads.get(f.path), canary)
        tag = {"real": "OK  ", "router-fallback": "FAKE", "empty": "EMPTY",
               "missing": str(f.status or "—"), "canary": "CNRY"}[f.verdict]
        print(f"  [{tag:>5}] {f.path:44} {f.kind}{'' if f.size is None else f'[{f.size}]'}")

    print(f"\n  API calls spent this run: {spent} (cache hits: {len(findings) - spent})", file=sys.stderr)
    return findings


def write_markdown(findings: list[Finding], out: Path) -> None:
    def by(v: str) -> list[Finding]:
        return sorted((f for f in findings if f.verdict == v), key=lambda x: (x.group, x.path))

    live, fake = by("real"), by("router-fallback")
    empty, missing = by("empty"), by("missing")

    lines = [
        "# GuruFocus API — what actually works",
        "",
        "Generated by `backend/scripts/gurufocus_catalog.py`. **Do not hand-edit** — re-run",
        "the script. It caches every response, so a re-run costs zero API calls.",
        "",
        "We pay for the **legacy** API: `https://api.gurufocus.com/public/user/{KEY}/<path>`.",
        "It has **no official documentation**. The docs at `gurufocus.com/api/overview` and",
        "`gurufocus.com/data-api/*` describe a *different, newer product* (plural paths like",
        "`/stocks/{region_code}`) that our key does not authenticate against; the Excel",
        "manual PDF documents the add-in, not the API.",
        "",
        "Symbol forms: US listings take a bare ticker (`AAPL`); everything else takes",
        "`EXCHANGE:TICKER` (`WBO:ANDR`) — see `ingest/earnings/_common.py::_build_symbol`.",
        "",
        "## The trap: this API does not 404",
        "",
        "`stock/{sym}/<anything>` returns **HTTP 200 and a 46-point series of zeros** for any",
        "sub-path it doesn't recognise. Probed naively, `stock/AAPL/news` looks like a working",
        "endpoint. Every probe below is compared against a canary sub-path that cannot exist;",
        "anything matching it (or any all-zero series) is listed as a fallback, not an endpoint.",
        "",
        f"## Real endpoints ({len(live)})",
        "",
        "| path | shape | note |",
        "| --- | --- | --- |",
    ]
    for f in live:
        shape = f"`{f.kind}`" + (f"[{f.size}]" if f.size is not None else "")
        if f.keys:
            shape += " keys: " + ", ".join(f"`{k}`" for k in f.keys[:6])
        elif f.item_keys:
            shape += " item keys: " + ", ".join(f"`{k}`" for k in f.item_keys[:6])
        lines.append(f"| `{f.path}` | {shape} | {f.note} |")

    lines += [
        "", f"## NOT endpoints — router fallback ({len(fake)})", "",
        "These returned HTTP 200 with the canary's all-zero series. **Do not use them.**",
        "", "| path |", "| --- |",
    ]
    lines += [f"| `{f.path}` |" for f in fake]

    if empty:
        lines += ["", f"## Real, but empty for this input ({len(empty)})", "",
                  "The endpoint exists; the identifier we probed with didn't match a row.",
                  "", "| path | note |", "| --- |  --- |"]
        lines += [f"| `{f.path}` | {f.note} |" for f in empty]

    lines += ["", f"## Missing ({len(missing)})", "",
              "404, non-JSON, or a transport failure.", "", "| path | status |", "| --- | --- |"]
    lines += [f"| `{f.path}` | {f.status or '—'} |" for f in missing]

    lines += ["", "## Samples", ""]
    for f in live:
        lines += [f"### `{f.path}`", "", "```json", f.sample, "```", ""]

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-calls", type=int, default=60, help="hard ceiling on billed API calls")
    ap.add_argument("--refresh", action="store_true", help="ignore the cache and re-probe (spends quota)")
    ap.add_argument("--dry-run", action="store_true", help="print the plan, spend nothing")
    ap.add_argument("--cache-dir", type=Path, default=Path(".gurufocus_cache"))
    ap.add_argument("--md", type=Path, default=Path("../docs/gurufocus_api.md"))
    ap.add_argument("--json", type=Path, default=Path("gurufocus_api.json"))
    a = ap.parse_args()

    probes = _probes()
    if a.dry_run:
        print(f"{len(probes)} probes; at most {min(len(probes), a.max_calls)} billed calls:\n")
        for p in probes:
            print(f"  {p.group:14} {p.path}")
        return 0

    print(f"probing {len(probes)} endpoints (max {a.max_calls} billed calls)\n")
    findings = run(probes, a.cache_dir, a.max_calls, a.refresh)

    write_markdown(findings, a.md)
    a.json.write_text(json.dumps([asdict(f) for f in findings], indent=1), encoding="utf-8")
    tally = Counter(f.verdict for f in findings)
    print("\n  " + "   ".join(f"{k}={v}" for k, v in sorted(tally.items())))
    print(f"  {a.md}\n  {a.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
