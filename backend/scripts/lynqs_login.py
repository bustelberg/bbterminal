"""Log in to the lynqs (Leonteq) eportal with Playwright, capture the session
cookie, and (optionally) probe the instruments endpoint FROM INSIDE the real
browser context - which carries the genuine browser TLS fingerprint + Imperva
tokens, so it gets through where a copied cookie on a raw HTTP client may not.

RUN THIS ON YOUR OWN MACHINE (same network as your normal browser). A login from
an unfamiliar server IP can trip the account's new-location security (2FA /
verification / lock) and Imperva will likely block a datacenter IP anyway - so
this is intentionally a local, headed, human-in-the-loop tool.

Credentials come from the environment (never hardcoded / committed):
    # PowerShell
    $env:LYNQS_USER = 'reinierschep'
    $env:LYNQS_PASS = '...'
    uv run python scripts/lynqs_login.py --probe            # login + probe, headed
    uv run python scripts/lynqs_login.py --cookie-out lynqs_cookie.txt   # just capture cookie

The captured cookie is written to a file (gitignored) you can then feed to the
puller:  uv run python scripts/lynqs_universe.py --cookie-file lynqs_cookie.txt
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from collections import Counter
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover
    sync_playwright = None

EPORTAL = "https://www.lynqs.com/eportal/"
SEARCH_PATH = "/eportal/amc-gateway/api/v2/instruments/search"
FIELDS = ["id", "ticker", "name", "productType", "ric", "isin", "currency"]

# fetch executed INSIDE the page (same-origin -> real session, cookies, Imperva)
_FETCH_JS = """
async ([path, terms, limit, currency, language]) => {
  const u = new URL(path, location.origin);
  u.searchParams.set('terms', terms);
  u.searchParams.set('offset', '0');
  u.searchParams.set('limit', String(limit));
  const r = await fetch(u.toString(), { headers: {
    'accept': 'application/json, text/plain, */*',
    'x-ltq-currency': currency, 'x-ltq-language': language,
  }});
  let body = null;
  try { body = await r.json(); } catch (e) { body = null; }
  return { status: r.status, body };
}
"""


def _flatten(payload) -> list[dict]:
    out: list[dict] = []
    if isinstance(payload, dict):
        for items in payload.values():
            if isinstance(items, list):
                out.extend(x for x in items if isinstance(x, dict))
    return out


def _fetch(page, terms, limit, currency, language):
    return page.evaluate(_FETCH_JS, [SEARCH_PATH, terms, limit, currency, language])


def _cookie_header(context) -> str:
    return "; ".join(
        f"{c['name']}={c['value']}"
        for c in context.cookies()
        if c.get("domain", "").endswith("lynqs.com")
    )


def _logged_in(context) -> bool:
    return any(c["name"] == "lynqs-auth" for c in context.cookies())


def _probe(page, currency, language) -> None:
    """Match-all term + limit-ceiling probe, in-browser. Prints the shape."""
    from collections import Counter  # noqa: PLC0415
    print("\n[probe] testing match-all term + limit ceiling...", flush=True)
    best_term, best_n = None, -1
    for t in ("", "*", "%", ".", "a"):
        res = _fetch(page, t, 50, currency, language)
        n = len(_flatten(res.get("body"))) if res.get("status") == 200 else 0
        print(f"  term {t!r:>4}: {n} rows (http {res.get('status')})", flush=True)
        if n > best_n:
            best_term, best_n = t, n
        time.sleep(0.4)
    if best_n <= 0:
        print("[probe] no match-all term worked - the puller would enumerate prefixes.", flush=True)
        return
    print(f"[probe] best term {best_term!r}. Testing limit ceiling...", flush=True)
    for lim in (500, 5000, 50000):
        res = _fetch(page, best_term, lim, currency, language)
        rows = _flatten(res.get("body")) if res.get("status") == 200 else []
        counts = dict(Counter(r.get("productType") for r in rows))
        trunc = [k for k, v in counts.items() if v >= lim]
        print(f"  limit={lim}: {len(rows)} rows {counts}"
              + (f"  still-truncated:{trunc}" if trunc else "  <- nothing hit the cap"), flush=True)
        if not trunc:
            print(f"\n[probe] OK ONE request covers it: terms={best_term!r} limit={lim} "
                  f"-> {len(rows):,} instruments.", flush=True)
            return
        time.sleep(0.4)
    print("\n[probe] some groups exceed 50k - the puller will paginate those.", flush=True)


def _dump_csv(page, out, term, limit, currency, language, keep) -> None:
    """The one-shot universe pull, IN-BROWSER (proven-working session). Fetch the
    whole match-all result, dedupe by id, write CSV."""
    print(f"[dump] fetching terms={term!r} limit={limit} in-browser...", flush=True)
    res = _fetch(page, term, limit, currency, language)
    if res.get("status") != 200 or not res.get("body"):
        print(f"[dump] fetch failed (http {res.get('status')}) - not written.", flush=True)
        return
    rows = _flatten(res["body"])
    capped = [k for k, v in Counter(r.get("productType") for r in rows).items() if v >= limit]
    if capped:
        print(f"[dump] WARNING these groups hit the limit (may be truncated): {capped} - raise --limit.", flush=True)
    seen: dict[str, dict] = {}
    for r in rows:
        if keep and r.get("productType") not in keep:
            continue
        rec = {k: (r.get(k) or "") for k in FIELDS}
        key = rec["id"] or f"{rec['isin']}|{rec['ticker']}"
        if key:
            seen.setdefault(key, rec)
    with Path(out).open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(seen.values())
    by_type = dict(Counter(r["productType"] for r in seen.values()))
    with_isin = sum(1 for r in seen.values() if r["isin"])
    print(f"[dump] {len(seen):,} instruments -> {out} (with ISIN: {with_isin:,})", flush=True)
    print("[dump] by type: " + ", ".join(f"{k} {v}" for k, v in sorted(by_type.items(), key=lambda x: -x[1])), flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Playwright login to lynqs -> cookie (+ optional probe / CSV dump).")
    ap.add_argument("--cookie-out", default="lynqs_cookie.txt")
    ap.add_argument("--headless", action="store_true", help="run headless (no window; can't do 2FA)")
    ap.add_argument("--probe", action="store_true", help="after login, probe the endpoint in-browser")
    ap.add_argument("--dump-csv", metavar="PATH", help="after login, write the whole universe to this CSV (in-browser)")
    ap.add_argument("--term", default="%", help="match-all search term (default: %% wildcard)")
    ap.add_argument("--limit", type=int, default=50000, help="per-group page size for the one-shot dump")
    ap.add_argument("--types", default="", help="comma-separated productTypes to keep in the CSV (default: all)")
    ap.add_argument("--currency", default="CHF")
    ap.add_argument("--language", default="en")
    ap.add_argument("--timeout", type=int, default=90, help="seconds to wait for login to complete")
    args = ap.parse_args()

    if sync_playwright is None:
        sys.exit("playwright is required (a backend dependency). Run via `uv run`; browsers: `uv run playwright install chromium`.")
    user, password = os.environ.get("LYNQS_USER", ""), os.environ.get("LYNQS_PASS", "")
    if not user or not password:
        sys.exit("Set LYNQS_USER and LYNQS_PASS in the environment first.")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context()
        page = context.new_page()
        print(f"[login] opening {EPORTAL} ...", flush=True)
        page.goto(EPORTAL, wait_until="domcontentloaded", timeout=60000)

        # Best-effort auto-fill of the Keycloak form; fall back to manual.
        try:
            page.wait_for_selector("input#username, input[name=username]", timeout=20000)
            page.fill("input#username, input[name=username]", user)
            page.fill("input#password, input[name=password]", password)
            page.click("#kc-login, button[type=submit], input[type=submit]")
            print("[login] submitted credentials; waiting for the session...", flush=True)
        except Exception:  # noqa: BLE001
            print("[login] couldn't auto-fill the form - complete the login in the window.", flush=True)

        # Wait for the eportal session cookie (handles 2FA / consent manually in
        # headed mode - you finish in the window, we detect when you're in).
        deadline = time.time() + args.timeout
        while time.time() < deadline and not _logged_in(context):
            time.sleep(1.0)

        if not _logged_in(context):
            if args.headless:
                sys.exit("[login] not logged in within timeout (2FA/challenge?). Re-run WITHOUT --headless.")
            input("[login] finish logging in in the browser window, then press Enter here... ")

        if not _logged_in(context):
            sys.exit("[login] still no session cookie - aborting (nothing written).")

        cookie = _cookie_header(context)
        Path(args.cookie_out).write_text(cookie, encoding="utf-8")
        print(f"[login] OK logged in - cookie ({len(cookie)} chars) written to {args.cookie_out}", flush=True)
        print("[login] (secret - do not commit; it's gitignored.)", flush=True)

        if args.probe or args.dump_csv:
            page.goto(EPORTAL, wait_until="domcontentloaded")  # ensure same-origin
        if args.probe:
            _probe(page, args.currency, args.language)
        if args.dump_csv:
            keep = {t.strip() for t in args.types.split(",") if t.strip()} or None
            _dump_csv(page, args.dump_csv, args.term, args.limit, args.currency, args.language, keep)

        browser.close()


if __name__ == "__main__":
    main()
