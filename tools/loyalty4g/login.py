"""Log in to https://tops.loyalty4g.com (a Symfony form-login site).

The login flow is a standard Symfony `form_login`:
  1. GET /login          -> sets a session cookie and embeds a per-session
                            hidden `_csrf_token` in the form.
  2. POST /login_check    with `_username`, `_password`, `_csrf_token`.
       * success -> 302 redirect AWAY from /login (to the dashboard),
                    session cookie now authenticated.
       * failure -> 302 back to /login with an error flash.
We detect success by the post-login landing page no longer being the
login form.

Credentials are resolved in this order (first hit wins), for both
username and password:
    1. --username / --password CLI args
    2. $LOYALTY4G_USERNAME / $LOYALTY4G_PASSWORD env vars
    3. a KEY=VALUE creds file (default: tools/loyalty4g/loyalty4g.creds, gitignored;
       override with --creds-file). The password additionally falls back
       to a hidden prompt if still unset.

Usage:

    # fast headless login using the default tools/loyalty4g/loyalty4g.creds file:
    uv run --with requests python tools/loyalty4g/login.py

    # explicit creds on the CLI:
    uv run --with requests python tools/loyalty4g/login.py -u you@example.com -p 'secret'

    # WATCH IT LIVE in a real browser window (uses Playwright; run from the
    # backend env which already has it installed):
    uv run --project backend python tools/loyalty4g/login.py --browser

    # persist the authenticated session cookies for reuse:
    uv run --with requests python tools/loyalty4g/login.py --save-cookies tools/loyalty4g/loyalty4g_session.json

Exit code: 0 on successful login, 1 on failure.
"""
from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import sys
from urllib.parse import urljoin, urlparse

import requests

DEFAULT_BASE_URL = "https://tops.loyalty4g.com"
_LOGIN_PATH = "/login"
_LOGIN_CHECK_PATH = "/login_check"
_DEFAULT_CREDS_FILE = "tools/loyalty4g/loyalty4g.creds"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
)
_TIMEOUT = 30

# The CSRF token is rendered as a hidden <input name="_csrf_token" value="...">.
# Attribute order isn't guaranteed, so match both name-before-value and
# value-before-name.
_CSRF_PATTERNS = (
    re.compile(r'name="_csrf_token"[^>]*\bvalue="([^"]+)"', re.I),
    re.compile(r'\bvalue="([^"]+)"[^>]*name="_csrf_token"', re.I),
)
# The login form's username field — its presence after POST means we're
# still on the login page, i.e. authentication failed.
_LOGIN_FORM_MARKER = re.compile(r'name="_username"', re.I)


def _log(msg: str, *, verbose: bool) -> None:
    if verbose:
        print(f"  - {msg}", file=sys.stderr, flush=True)


def _extract_csrf(html: str) -> str | None:
    for pat in _CSRF_PATTERNS:
        m = pat.search(html)
        if m:
            return m.group(1)
    return None


def login(
    username: str,
    password: str,
    base_url: str = DEFAULT_BASE_URL,
    *,
    verbose: bool = False,
) -> tuple[bool, requests.Session, str]:
    """Attempt a login. Returns (success, session, final_url). On success
    the returned session carries the authenticated cookies."""
    session = requests.Session()
    session.headers.update({"User-Agent": _UA, "Accept-Language": "nl,en;q=0.8"})

    login_url = urljoin(base_url, _LOGIN_PATH)
    check_url = urljoin(base_url, _LOGIN_CHECK_PATH)

    # 1) Fetch the login page → session cookie + CSRF token.
    _log(f"GET {login_url}", verbose=verbose)
    resp = session.get(login_url, timeout=_TIMEOUT)
    resp.raise_for_status()
    csrf = _extract_csrf(resp.text)
    if not csrf:
        raise RuntimeError(
            "Could not find the _csrf_token on the login page — the form may "
            "have changed. Re-inspect the HTML of GET /login."
        )
    _log(f"got session cookie(s): {list(session.cookies.keys())}", verbose=verbose)
    _log(f"got _csrf_token: {csrf[:16]}...", verbose=verbose)

    # 2) Submit credentials. Symfony reads these exact field names.
    payload = {
        "_username": username,
        "_password": password,
        "_csrf_token": csrf,
    }
    _log(f"POST {check_url} as {username!r}", verbose=verbose)
    resp = session.post(
        check_url,
        data=payload,
        headers={"Referer": login_url, "Origin": base_url},
        timeout=_TIMEOUT,
        allow_redirects=True,
    )
    final_url = resp.url
    _log(f"landed on {final_url} (HTTP {resp.status_code})", verbose=verbose)

    # 3) Success = redirected away from the login page AND the response is
    # no longer the login form. Symfony bounces a failed login back to
    # /login (or re-renders /login_check with the form), so either of
    # those signals failure.
    final_path = urlparse(final_url).path.rstrip("/")
    ended_on_login = final_path.endswith("/login") or final_path.endswith("/login_check")
    form_present = bool(_LOGIN_FORM_MARKER.search(resp.text))
    success = not ended_on_login and not form_present
    _log(
        f"ended_on_login={ended_on_login} login_form_present={form_present} "
        f"-> success={success}",
        verbose=verbose,
    )
    return success, session, final_url


def _load_creds_file(path: str) -> dict[str, str]:
    """Parse a KEY=VALUE creds file. Missing file → empty dict. Lines
    starting with # and blank lines are ignored. Quotes around values are
    stripped."""
    creds: dict[str, str] = {}
    if not os.path.isfile(path):
        return creds
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            creds[key.strip()] = val.strip().strip('"').strip("'")
    return creds


def _resolve_credentials(args) -> tuple[str | None, str | None]:
    """Resolve (username, password) per the documented precedence:
    CLI arg > env var > creds file (password also falls back to prompt)."""
    file_creds = _load_creds_file(args.creds_file)
    username = (
        args.username
        or os.environ.get("LOYALTY4G_USERNAME")
        or file_creds.get("LOYALTY4G_USERNAME")
    )
    password = (
        args.password
        or os.environ.get("LOYALTY4G_PASSWORD")
        or file_creds.get("LOYALTY4G_PASSWORD")
    )
    return username, password


def login_browser(
    username: str,
    password: str,
    base_url: str = DEFAULT_BASE_URL,
    *,
    watch_seconds: int = 0,
    verbose: bool = False,
) -> bool:
    """Do the login in a REAL, visible browser window so you can watch it
    happen. Uses Playwright (install in the backend env). Fills the form,
    submits, and reports success. `watch_seconds > 0` keeps the window
    open that long after submitting; otherwise it waits until you close
    the window yourself."""
    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    login_url = urljoin(base_url, _LOGIN_PATH)
    with sync_playwright() as pw:
        _log("launching a visible Chromium window…", verbose=verbose)
        browser = pw.chromium.launch(headless=False, slow_mo=250)
        page = browser.new_page()
        _log(f"navigating to {login_url}", verbose=verbose)
        page.goto(login_url, wait_until="domcontentloaded", timeout=30_000)
        # Fill the Symfony login fields (ids from the page: _username/_password).
        page.fill("#_username", username)
        page.fill("#_password", password)
        _log("submitting the form…", verbose=verbose)
        page.click("button[type=submit], input[type=submit]")
        # Wait for the navigation triggered by the submit to settle.
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        final_url = page.url
        form_present = page.locator("#_username").count() > 0
        final_path = urlparse(final_url).path.rstrip("/")
        ended_on_login = final_path.endswith("/login") or final_path.endswith("/login_check")
        success = not ended_on_login and not form_present
        print(
            f"{'Login OK' if success else 'Login FAILED'} as {username} "
            f"(landed on {final_url})"
        )
        if watch_seconds > 0:
            print(f"Keeping the window open for {watch_seconds}s so you can look…")
            page.wait_for_timeout(watch_seconds * 1000)
        else:
            print("Close the browser window when you're done watching.")
            try:
                page.wait_for_event("close", timeout=0)
            except Exception:
                pass
        browser.close()
        return success


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Log in to tops.loyalty4g.com (Symfony form login)."
    )
    ap.add_argument("-u", "--username", help="E-mail / username (else env/creds file).")
    ap.add_argument(
        "-p", "--password",
        help="Password (else $LOYALTY4G_PASSWORD, creds file, then a hidden prompt).",
    )
    ap.add_argument(
        "--creds-file", default=_DEFAULT_CREDS_FILE,
        help=f"KEY=VALUE credentials file (default: {_DEFAULT_CREDS_FILE}).",
    )
    ap.add_argument(
        "--browser", action="store_true",
        help="Log in via a VISIBLE browser window (Playwright) so you can watch.",
    )
    ap.add_argument(
        "--watch-seconds", type=int, default=0,
        help="In --browser mode, keep the window open this many seconds after "
        "submitting (0 = wait until you close it yourself).",
    )
    ap.add_argument(
        "--base-url", default=DEFAULT_BASE_URL,
        help=f"Site base URL (default: {DEFAULT_BASE_URL}).",
    )
    ap.add_argument(
        "--save-cookies", metavar="PATH",
        help="On success, write the authenticated session cookies to PATH as JSON.",
    )
    ap.add_argument("-v", "--verbose", action="store_true", help="Log each step.")
    args = ap.parse_args()

    username, password = _resolve_credentials(args)
    if not username:
        print(
            "No username. Pass -u, set $LOYALTY4G_USERNAME, or add "
            f"LOYALTY4G_USERNAME to {args.creds_file}.",
            file=sys.stderr,
        )
        return 2
    if not password:
        password = getpass.getpass("Password: ")
    if not password:
        print("No password provided.", file=sys.stderr)
        return 2

    if args.browser:
        try:
            ok = login_browser(
                username, password, args.base_url,
                watch_seconds=args.watch_seconds, verbose=args.verbose,
            )
        except ImportError:
            print(
                "Playwright isn't available. Run --browser from the backend env:\n"
                "  uv run --project backend python tools/loyalty4g/login.py --browser",
                file=sys.stderr,
            )
            return 3
        return 0 if ok else 1

    try:
        success, session, final_url = login(
            username, password, args.base_url, verbose=args.verbose,
        )
    except requests.RequestException as e:
        print(f"Network error: {type(e).__name__}: {e}", file=sys.stderr)
        return 3
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 3

    if success:
        print(f"Login OK as {username} (landed on {final_url})")
        if args.save_cookies:
            cookies = {c.name: c.value for c in session.cookies}
            with open(args.save_cookies, "w", encoding="utf-8") as fh:
                json.dump(cookies, fh, indent=2)
            print(f"Saved {len(cookies)} cookie(s) to {args.save_cookies}")
        return 0

    print(
        f"Login FAILED for {username} — bounced back to the login page "
        f"({final_url}). Check the credentials.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
