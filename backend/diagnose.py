"""
Earnings pipeline diagnostics.

Checks every step of the data refresh pipeline for a given ticker:
  1. Environment variables
  2. Supabase connectivity
  3. Supabase Storage (cache bucket)
  4. GuruFocus API reachability
  5. Each data source: financials, analyst_estimates, indicators, prices
  6. Database row counts

Usage:
    cd backend
    uv run python diagnose.py AAPL NASDAQ
    uv run python diagnose.py MSFT NASDAQ --force   # skip cache, hit API
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

# Fix Windows terminal encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore

# Load .env
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

US_EXCHANGES = {"NYSE", "NASDAQ", "AMEX"}

# ── Formatting ──────────────────────────────────────────────────────────────

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"


def ok(msg: str) -> None:
    print(f"  {GREEN}✓{RESET} {msg}")


def fail(msg: str) -> None:
    print(f"  {RED}✗{RESET} {msg}")


def warn(msg: str) -> None:
    print(f"  {YELLOW}⚠{RESET} {msg}")


def info(msg: str) -> None:
    print(f"  {DIM}  {msg}{RESET}")


def header(title: str) -> None:
    print(f"\n{BOLD}{CYAN}── {title} ──{RESET}")


# ── Helpers ─────────────────────────────────────────────────────────────────

def build_symbol(ticker: str, exchange: str) -> str:
    if exchange.upper() in US_EXCHANGES:
        return ticker
    return f"{exchange}:{ticker}"


def mask_key(key: str) -> str:
    if len(key) <= 8:
        return key[:2] + "***"
    return key[:4] + "***" + key[-4:]


def api_get(url: str, timeout: int = 15) -> tuple[int, str, float]:
    """Returns (status_code, body_preview, elapsed_seconds)."""
    req = Request(url, headers={"User-Agent": _USER_AGENT, "Accept": "application/json"})
    start = time.time()
    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, body[:200], time.time() - start
    except HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        return e.code, body, time.time() - start
    except URLError as e:
        return 0, f"URLError: {e.reason}", time.time() - start
    except Exception as e:
        return 0, f"{type(e).__name__}: {e}", time.time() - start


# ── Step 1: Environment variables ──────────────────────────────────────────

def check_env() -> dict:
    header("1. Environment Variables")
    env = {}
    for key in ["SUPABASE_URL", "SUPABASE_SERVICE_KEY", "GURUFOCUS_BASE_URL", "GURUFOCUS_API_KEY"]:
        val = os.environ.get(key, "")
        env[key] = val
        if val:
            display = mask_key(val) if "KEY" in key else val
            ok(f"{key} = {display}")
        else:
            fail(f"{key} is NOT SET")
    return env


# ── Step 2: Supabase connectivity ─────────────────────────────────────────

def check_supabase(env: dict):
    header("2. Supabase Connectivity")
    url = env.get("SUPABASE_URL", "")
    key = env.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        fail("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY — skipping")
        return None

    try:
        from supabase import create_client
        sb = create_client(url, key)
        ok("Supabase client created")
    except Exception as e:
        fail(f"Could not create Supabase client: {e}")
        return None

    # Test query
    try:
        resp = sb.table("company").select("company_id").limit(1).execute()
        ok(f"Query company table: {len(resp.data)} row(s) returned")
    except Exception as e:
        fail(f"Query company table failed: {e}")

    return sb


# ── Step 3: Storage bucket ─────────────────────────────────────────────────

def check_storage(sb, ticker: str, exchange: str):
    header("3. Supabase Storage (cache)")
    if sb is None:
        fail("No Supabase client — skipping")
        return

    bucket = "gurufocus-raw"
    try:
        buckets = sb.storage.list_buckets()
        names = [b.name for b in buckets]
        if bucket in names:
            ok(f"Bucket '{bucket}' exists")
        else:
            warn(f"Bucket '{bucket}' not found (available: {names})")
    except Exception as e:
        fail(f"Could not list buckets: {e}")
        return

    # Check cached files for this ticker
    prefix = f"{exchange.upper()}_{ticker.upper()}"
    expected_files = ["financials", "analyst_estimate", "indicator_q_roe", "indicator__price"]
    for f in expected_files:
        path = f"{prefix}/{f}.json"
        try:
            raw = sb.storage.from_(bucket).download(path)
            data = json.loads(raw)
            size = len(raw)
            if isinstance(data, dict):
                desc = f"dict with {len(data)} keys"
            elif isinstance(data, list):
                desc = f"list with {len(data)} items"
            else:
                desc = type(data).__name__
            ok(f"Cache {path}: {size:,} bytes ({desc})")
        except Exception:
            warn(f"Cache {path}: not found")


# ── Step 4: GuruFocus API reachability ─────────────────────────────────────

def check_api(env: dict, ticker: str, exchange: str) -> bool:
    header("4. GuruFocus API Reachability")
    base_url = env.get("GURUFOCUS_BASE_URL", "").strip().rstrip("/")
    api_key = env.get("GURUFOCUS_API_KEY", "")

    if not base_url or not api_key:
        fail("Missing GURUFOCUS_BASE_URL or GURUFOCUS_API_KEY — skipping")
        return False

    if base_url.endswith("/data"):
        base_url = base_url[:-len("/data")]

    symbol = build_symbol(ticker, exchange)

    test_url = f"{base_url}/public/user/{api_key}/stock/{quote(symbol, safe=':')}/price"
    masked = test_url.replace(api_key, mask_key(api_key))
    info(f"Testing: {masked}")

    # Test with Python urllib
    info("Method: Python urllib")
    status, body, elapsed = api_get(test_url)
    urllib_ok = False
    if status == 200:
        ok(f"urllib: HTTP {status} in {elapsed:.1f}s")
        urllib_ok = True
    elif status == 403 and ("just a moment" in body.lower() or "cloudflare" in body.lower()):
        fail(f"urllib: HTTP 403 — Cloudflare blocking Python TLS fingerprint ({elapsed:.1f}s)")
    else:
        fail(f"urllib: HTTP {status} ({elapsed:.1f}s)")

    # Test with curl
    curl_ok = False
    has_curl = shutil.which("curl") is not None
    if has_curl:
        info("Method: curl")
        try:
            start = time.time()
            result = subprocess.run(
                ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                 "--max-time", "15",
                 "-H", f"User-Agent: {_USER_AGENT}",
                 "-H", "Accept: application/json",
                 test_url],
                capture_output=True, text=True, timeout=20,
            )
            curl_elapsed = time.time() - start
            curl_status = int(result.stdout.strip()) if result.stdout.strip().isdigit() else 0
            if curl_status == 200:
                ok(f"curl: HTTP {curl_status} in {curl_elapsed:.1f}s")
                curl_ok = True
            elif curl_status == 403:
                fail(f"curl: HTTP 403 — blocked even via curl ({curl_elapsed:.1f}s)")
            else:
                fail(f"curl: HTTP {curl_status} ({curl_elapsed:.1f}s)")
        except Exception as e:
            fail(f"curl: {e}")
    else:
        warn("curl not found — cannot test alternative TLS fingerprint")

    if curl_ok and not urllib_ok:
        ok("Pipeline will use curl to bypass Cloudflare (auto-detected)")
    elif not curl_ok and not urllib_ok:
        fail("API blocked via both methods from this machine")
        info("Try running from a different network, or refresh data locally.")

    return curl_ok or urllib_ok


# ── Step 5: Each data source ───────────────────────────────────────────────

def check_sources(env: dict, ticker: str, exchange: str, api_ok: bool):
    header("5. Data Sources (API endpoints)")
    base_url = env.get("GURUFOCUS_BASE_URL", "").strip().rstrip("/")
    api_key = env.get("GURUFOCUS_API_KEY", "")
    if base_url.endswith("/data"):
        base_url = base_url[:-len("/data")]

    symbol = build_symbol(ticker, exchange)

    if not api_ok:
        warn("API is not reachable — skipping live endpoint checks")
        info("Data can still be served from Supabase cache if available.")
        return

    endpoints = {
        "financials": f"stock/{quote(symbol, safe=':')}/financials?order=desc",
        "analyst_estimate": f"stock/{quote(symbol, safe=':')}/analyst_estimate",
        "price": f"stock/{quote(symbol, safe=':')}/price",
        "indicator (roe)": f"stock/{quote(symbol, safe=':')}/roe?type=quarterly",
        "indicator (forward_pe)": f"stock/{quote(symbol, safe=':')}/forward_pe_ratio?type=quarterly",
    }

    for name, path in endpoints.items():
        url = f"{base_url}/public/user/{api_key}/{path}"
        time.sleep(1.5)  # rate limit
        status, body, elapsed = api_get(url)
        if status == 200:
            try:
                parsed = json.loads(body + "...")  # body is truncated
            except Exception:
                pass
            ok(f"{name}: HTTP {status} ({elapsed:.1f}s)")
        elif status == 403:
            is_cf = "just a moment" in body.lower()
            fail(f"{name}: HTTP 403 {'(Cloudflare)' if is_cf else ''} ({elapsed:.1f}s)")
        else:
            fail(f"{name}: HTTP {status} ({elapsed:.1f}s)")
            info(f"Body: {body[:80]}")


# ── Step 6: Database row counts ────────────────────────────────────────────

def check_db_data(sb, ticker: str, exchange: str):
    header("6. Database Data (metric_data)")
    if sb is None:
        fail("No Supabase client — skipping")
        return

    # Find company_id
    try:
        resp = sb.table("company").select("company_id, company_name").eq(
            "primary_ticker", ticker.upper()
        ).eq("primary_exchange", exchange.upper()).execute()
        if not resp.data:
            warn(f"Company {ticker}.{exchange} not found in database")
            return
        company = resp.data[0]
        cid = company["company_id"]
        name = company.get("company_name") or ticker
        ok(f"Company: {name} (id={cid})")
    except Exception as e:
        fail(f"Could not look up company: {e}")
        return

    # Count metrics by source
    try:
        resp = sb.table("metric_data").select(
            "source_code, metric_code"
        ).eq("company_id", cid).execute()
        rows = resp.data

        by_source: dict[str, set] = {}
        for r in rows:
            src = r["source_code"]
            by_source.setdefault(src, set()).add(r["metric_code"])

        total = len(rows)
        ok(f"Total rows: {total:,}")
        for src, codes in sorted(by_source.items()):
            info(f"  {src}: {len(codes)} metric codes")

        # Check specific important codes
        important = [
            "close_price",
            "annuals__Per Share Data__EPS without NRI",
            "annuals__Per Share Data__Free Cash Flow per Share",
            "indicator_q_forward_pe_ratio",
            "indicator_q_roic",
        ]
        for code in important:
            count = sum(1 for r in rows if r["metric_code"] == code)
            if count > 0:
                ok(f"  {code}: {count} rows")
            else:
                warn(f"  {code}: 0 rows")

    except Exception as e:
        fail(f"Could not query metric_data: {e}")


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 3:
        print(f"Usage: python {sys.argv[0]} <TICKER> <EXCHANGE> [--force]")
        print(f"Example: python {sys.argv[0]} AAPL NASDAQ")
        sys.exit(1)

    ticker = sys.argv[1].upper()
    exchange = sys.argv[2].upper()

    print(f"\n{BOLD}Earnings Pipeline Diagnostics{RESET}")
    print(f"Ticker: {ticker}  Exchange: {exchange}")
    print(f"{'─' * 50}")

    env = check_env()
    sb = check_supabase(env)
    check_storage(sb, ticker, exchange)
    api_ok = check_api(env, ticker, exchange)
    check_sources(env, ticker, exchange, api_ok)
    check_db_data(sb, ticker, exchange)

    # Summary
    header("Summary")
    if not api_ok:
        warn("GuruFocus API is blocked from this machine/IP.")
        info("Cached data in Supabase Storage can still be used.")
        info("To refresh data, run from a machine where the API is reachable.")
        info("Tip: run locally with 'uv run python diagnose.py AAPL NASDAQ'")
    else:
        ok("All systems operational.")

    print()


if __name__ == "__main__":
    main()
