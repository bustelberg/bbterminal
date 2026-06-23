import os
import queue
import threading
from urllib.parse import urlencode
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


BASE_URL = "https://bustelberg.airspms.cloud"

# Realistic desktop Chrome UA — Playwright's default UA contains
# "HeadlessChrome" which trips simple bot-block rules at the WAF
# layer. The `args` flag hides one of the most-checked automation
# tells. Together these are enough to get past basic bot detection
# (Cloudflare's "Just a moment" interstitial etc.) — if AirSPMS
# detects via something deeper (TLS fingerprint, behavioural), we
# need a stronger stealth approach.
_CHROMIUM_LAUNCH_ARGS = ["--disable-blink-features=AutomationControlled"]
_DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
_DESKTOP_VIEWPORT = {"width": 1280, "height": 720}


def _new_browser_page(pw):
    """Launch headless Chromium with stealth-ish options and return
    (browser, page). Single source of truth for browser config so the
    scanner and the persistent session stay in lockstep."""
    browser = pw.chromium.launch(headless=True, args=_CHROMIUM_LAUNCH_ARGS)
    context = browser.new_context(
        user_agent=_DESKTOP_UA,
        viewport=_DESKTOP_VIEWPORT,
        locale="en-US",
    )
    page = context.new_page()
    return browser, page


class AirsAccessForbiddenError(RuntimeError):
    """AirSPMS returned a 403 Forbidden / bot-block page on the login URL.

    Almost always means this server's egress IP is not on AirSPMS's
    allowlist — the Apache/Cloudflare gateway blocks before the login
    form is ever served. Distinct from a generic login failure so the
    SSE error event can carry a `kind` discriminator and the frontend
    can render IP-whitelist guidance instead of dumping the raw
    Playwright trace at the user."""

    def __init__(self, detail: str):
        super().__init__("AirSPMS blocked the login request with 403 Forbidden")
        self.detail = detail


def _looks_forbidden(diag: str) -> bool:
    """True when a `_capture_login_diagnostics` string indicates the page
    we landed on was a 403 / forbidden / access-denied gateway response
    rather than the real AirSPMS login form."""
    lower = diag.lower()
    return (
        "403 forbidden" in lower
        or "'forbidden'" in lower
        or "'access denied'" in lower
    )


def _capture_login_diagnostics(page) -> str:
    """Snapshot diagnostic signals when the login flow can't find
    `#username`. Embedded in the raised exception so the SSE error
    event lands them in the UI — useful when only prod fails and
    we can't repro locally (e.g., AirSPMS bot-blocking Railway IPs).

    Captures: current URL (in case of redirect), page title, count of
    inputs / iframes (is the form in an iframe?), and known
    bot-block phrases visible in the body."""
    try:
        url = page.url
        try:
            title = page.title()
        except Exception:
            title = "<title fetch failed>"
        try:
            html_head = (page.content() or "")[:600]
        except Exception:
            html_head = "<content fetch failed>"
        try:
            input_count = len(page.query_selector_all("input"))
        except Exception:
            input_count = -1
        try:
            iframe_count = len(page.query_selector_all("iframe"))
        except Exception:
            iframe_count = -1
        try:
            body_text = (
                page.locator("body").inner_text()
                if page.locator("body").count() else ""
            )[:400]
        except Exception:
            body_text = ""
        haystack = (body_text + " " + html_head).lower()
        hints: list[str] = []
        for needle in (
            "cloudflare", "just a moment", "checking your browser",
            "access denied", "forbidden", "attention required",
            "captcha", "blocked", "rate limit", "challenge",
        ):
            if needle in haystack:
                hints.append(needle)
        return (
            f"DIAG | url={url!r} title={title!r} "
            f"inputs={input_count} iframes={iframe_count} "
            f"bot_hints={hints} "
            f"body_excerpt={body_text[:200]!r}"
        )
    except Exception as diag_err:
        return f"(diagnostic capture failed: {diag_err})"


def _login(page):
    """Log in to AirSPMS. Raises on failure with diagnostics attached
    to the exception message — see `_capture_login_diagnostics`."""
    broker_username = os.environ.get("BROKER_USERNAME", "")
    broker_password = os.environ.get("BROKER_PASSWORD", "")

    if not broker_username or not broker_password:
        raise RuntimeError("BROKER_USERNAME or BROKER_PASSWORD not set in .env")

    page.goto(f"{BASE_URL}/login.php")
    page.wait_for_load_state("domcontentloaded")
    try:
        page.fill("#username", broker_username, timeout=30000)
    except Exception as e:
        diag = _capture_login_diagnostics(page)
        if _looks_forbidden(diag):
            raise AirsAccessForbiddenError(diag) from e
        raise RuntimeError(
            f"Could not find #username input on AirSPMS login page. "
            f"Underlying error: {type(e).__name__}: {e}. {diag}"
        ) from e
    page.fill("#password", broker_password)
    page.click("#btnFase1")
    page.wait_for_timeout(2000)

    if page.locator("#smsValid").is_visible():
        page.click("#btnFase3")
    elif page.locator("#smsDialog").is_visible():
        raise RuntimeError("SMS code required — cannot automate SMS step")
    elif page.locator("#smsOffline").is_visible():
        page.click("#btnFase4")

    page.wait_for_load_state("networkidle")


# ─── Persistent session for fast downloads ────────────────────────────────────

class AirsHttpResponse:
    """A captured response from the persistent session — body bytes plus the
    metadata needed to diagnose a non-Excel reply (HTTP status, content-type,
    final URL after any redirect)."""

    __slots__ = ("body", "status", "content_type", "url")

    def __init__(self, body: bytes, status: int, content_type: str, url: str):
        self.body = body
        self.status = status
        self.content_type = content_type
        self.url = url


def _looks_like_html(body: bytes) -> bool:
    """Heuristic: does this response body start with an HTML document marker?
    Covers `<!doctype html>`, a bare `<html>`, and a leading `<?xml`/BOM-then-tag
    that some PHP error pages emit."""
    head = body[:512].lstrip().lower()
    return head.startswith((b"<!doctype", b"<html", b"<!--", b"<head", b"<?php"))


class _AirsSession:
    """Keeps a single Playwright browser on a dedicated thread for authenticated requests."""

    def __init__(self):
        self._queue: queue.Queue = queue.Queue()
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()

    def _worker(self):
        """Runs on a dedicated thread — all Playwright calls happen here."""
        pw = None
        browser = None
        page = None

        def ensure_logged_in():
            nonlocal pw, browser, page
            if page is not None:
                return
            pw = sync_playwright().start()
            browser, page = _new_browser_page(pw)
            _login(page)

        def close_session():
            nonlocal pw, browser, page
            for obj, method in [(page, "close"), (browser, "close"), (pw, "stop")]:
                try:
                    if obj:
                        getattr(obj, method)()
                except Exception:
                    pass
            pw = browser = page = None

        while True:
            job, result_q = self._queue.get()
            try:
                ensure_logged_in()
                resp = job(page)

                # If we got HTML back (session expired / login bounce), re-login
                # once and re-run the job from scratch.
                if _looks_like_html(resp.body):
                    close_session()
                    ensure_logged_in()
                    resp = job(page)

                result_q.put(("ok", resp))
            except Exception as e:
                close_session()
                result_q.put(("error", e))

    def _submit(self, job) -> AirsHttpResponse:
        """Run `job(page) -> AirsHttpResponse` on the dedicated Playwright thread
        and return its result (or re-raise the worker's exception)."""
        result_q: queue.Queue = queue.Queue()
        self._queue.put((job, result_q))
        status, value = result_q.get()
        if status == "error":
            raise value
        return value

    def get_response(self, url: str) -> AirsHttpResponse:
        """Thread-safe GET returning the full response (body + metadata)."""
        def job(page) -> AirsHttpResponse:
            resp = page.request.get(url)
            return AirsHttpResponse(
                body=resp.body(),
                status=resp.status,
                content_type=(resp.headers or {}).get("content-type", ""),
                url=resp.url,
            )
        return self._submit(job)

    def get(self, url: str) -> bytes:
        """Thread-safe GET returning just the body bytes (back-compat)."""
        return self.get_response(url).body

    def download_via_form(
        self, page_url: str, xls_link_selector: str, timeout_ms: int = 25000,
    ) -> AirsHttpResponse:
        """Open `page_url`, click the "Naar XLS" export link (which runs the
        page's own onclick → sets a hidden `toXls=1` and submits `editForm`), and
        capture the resulting file download. This faithfully replays the manual
        click — the export is a FORM SUBMIT, not a fetchable URL.

        Returns the downloaded bytes on success; on a timeout (no download fired —
        the submit produced an inline page, e.g. a login bounce or an error) it
        returns that page's HTML so the caller can diagnose (and the worker's
        re-login-on-HTML retry kicks in)."""
        def job(page) -> AirsHttpResponse:
            page.goto(page_url, wait_until="domcontentloaded")
            link = page.locator(xls_link_selector).first
            if link.count() == 0:
                # No export control on the page we landed on — almost always the
                # login/forbidden page. Return its HTML for the diagnostic path.
                html = (page.content() or "").encode("utf-8", "replace")
                return AirsHttpResponse(html, 200, "text/html", page.url)
            try:
                with page.expect_download(timeout=timeout_ms) as dl_info:
                    link.click()
                download = dl_info.value
                with open(download.path(), "rb") as fh:
                    body = fh.read()
                return AirsHttpResponse(
                    body, 200, "application/vnd.ms-excel", download.url or page_url,
                )
            except PlaywrightTimeoutError:
                html = (page.content() or "").encode("utf-8", "replace")
                return AirsHttpResponse(html, 200, "text/html", page.url)
        return self._submit(job)


_session = _AirsSession()


def _download_report_sync(
    portfolio_name: str, datum_van: str, datum_tot: str, rapport_types: str,
) -> bytes:
    """Download one AirSPMS front-office report as XLS via the persistent
    session. `rapport_types` selects the report (e.g. 'ATT' = Rendementen)."""
    params = urlencode({
        "rapport_types": rapport_types,
        "Portefeuille": portfolio_name,
        "datum_van": datum_van,
        "datum_tot": datum_tot,
        "type": "xls",
    })
    url = f"{BASE_URL}/rapportFrontofficeClientAfdrukkenHtml.php?{params}"
    content = _session.get(url)

    if len(content) < 100:
        raise RuntimeError(f"Response too small ({len(content)} bytes)")
    if content[:15].lower().startswith(b'<!doctype'):
        raise RuntimeError("Got HTML instead of Excel — session may have expired")

    return content


def download_portfolio_sync(portfolio_name: str, datum_van: str, datum_tot: str) -> bytes:
    """Download the ATT (Rendementen) Excel report via the persistent session."""
    return _download_report_sync(portfolio_name, datum_van, datum_tot, "ATT")


# AirSPMS `rapport_types` code for the Vermogensoverzicht (holdings) report —
# 'VOLK', confirmed from a real download URL (cf. 'ATT' = Rendementen). Same
# params as ATT (Portefeuille, datum_van, datum_tot, type=xls). Overridable via
# AIRS_VERMOGEN_RAPPORT_TYPE if AirSPMS ever renames it.
VERMOGEN_RAPPORT_TYPE = "VOLK"


def download_vermogensoverzicht_sync(
    portfolio_name: str, datum_van: str, datum_tot: str,
) -> bytes:
    """Download the Vermogensoverzicht (holdings) Excel report via the
    persistent session. The bytes are parsed by `portfolio.parse_airs_excel`
    (same parser as the drag-drop path). The report code is read at call time
    so an env override takes effect on restart with no import-order gotcha."""
    code = os.environ.get("AIRS_VERMOGEN_RAPPORT_TYPE", "").strip() or VERMOGEN_RAPPORT_TYPE
    return _download_report_sync(portfolio_name, datum_van, datum_tot, code)


# CRM → Relaties → Alle relaties Excel export. There is NO direct export URL —
# the "Naar XLS" button runs an onclick that sets a hidden `toXls=1` field on the
# `editForm` and submits it (a POST). So we open the list page and click that
# link, capturing the download (see `_AirsSession.download_via_form`).
#   - AIRS_CRM_RELATIES_URL overrides the list page to open.
#   - AIRS_CRM_XLS_SELECTOR overrides the export-link selector (default matches
#     the <a> whose <img src=".../xls.gif"> is the "Naar XLS" icon).
CRM_RELATIES_PATH = "/CRM_nawList.php?sql=all"
CRM_XLS_LINK_SELECTOR = "a:has(img[src*='xls'])"


def _html_title(text: str) -> str:
    """Best-effort <title> extraction from an HTML string."""
    import re  # noqa: PLC0415

    m = re.search(r"<title[^>]*>(.*?)</title>", text, re.I | re.S)
    return m.group(1).strip() if m else "<no title>"


def _classify_html_page(text: str) -> str:
    """Guess WHAT kind of HTML page we got back, so the error says where to look."""
    low = text.lower()
    if any(n in low for n in ("403 forbidden", "access denied", "attention required")):
        return ("a 403 / access-denied gateway page — this server's egress IP may "
                "not be on the AirSPMS allowlist")
    if ('id="username"' in low or 'name="username"' in low
            or 'id="password"' in low or "login.php" in low):
        return ("the LOGIN page — the session expired and the silent re-login also "
                "failed (check BROKER_USERNAME / BROKER_PASSWORD)")
    if "crm_nawlist" in low or "relatie" in low or "nawlist" in low:
        return ("the CRM relaties LIST page (HTML) — the 'Naar XLS' export click "
                "didn't yield a file download. The export link selector may not "
                "match (override AIRS_CRM_XLS_SELECTOR) or the form submit didn't "
                "trigger a download")
    return "an unrecognised HTML page (not the Excel export)"


def _describe_non_excel(resp: "AirsHttpResponse") -> str:
    """Build an expressive, single-line diagnostic for a CRM reply that wasn't
    the expected .xls — what URL we hit, the HTTP status / content-type, the page
    title, our best guess at what the page is, and a body excerpt."""
    text = resp.body.decode("utf-8", "replace")
    title = _html_title(text)
    kind = _classify_html_page(text)
    # A compact, whitespace-collapsed body excerpt so logs stay one-ish line.
    excerpt = " ".join(text.split())[:300]
    return (
        f"requested_url={resp.url!r} http_status={resp.status} "
        f"content_type={resp.content_type!r} bytes={len(resp.body)} "
        f"page_title={title!r} — looks like {kind}. body_excerpt={excerpt!r}"
    )


def download_crm_relaties_sync() -> bytes:
    """Download the CRM 'Alle relaties' Excel export via the persistent session.
    Opens the list page and clicks its "Naar XLS" button (a form submit — there's
    no fetchable export URL), then returns the raw file bytes (stored unparsed).
    Raises with an expressive diagnostic if the result isn't Excel (login bounce /
    IP block / page changed) or is too small — so the failure says *where* it
    went wrong."""
    path = (os.environ.get("AIRS_CRM_RELATIES_URL", "").strip() or CRM_RELATIES_PATH)
    if not path.startswith(("http://", "https://")):
        path = f"{BASE_URL}/{path.lstrip('/')}"
    selector = os.environ.get("AIRS_CRM_XLS_SELECTOR", "").strip() or CRM_XLS_LINK_SELECTOR
    resp = _session.download_via_form(path, selector)
    content = resp.body

    if len(content) < 100:
        raise RuntimeError(
            f"CRM relaties response too small ({len(content)} bytes) — "
            f"{_describe_non_excel(resp)}"
        )
    if _looks_like_html(content):
        raise RuntimeError(
            "CRM relaties returned HTML, not Excel. " + _describe_non_excel(resp)
        )
    return content


# ─── Scanner (uses its own browser for DOM scraping) ──────────────────────────

def scan_portfolios_sync(send_event):
    """Run Playwright scan synchronously (call from a thread)."""
    with sync_playwright() as p:
        browser, page = _new_browser_page(p)

        try:
            send_event("progress", step="login", status="in_progress", message="Navigating to login page...")
            _login(page)
            send_event("progress", step="login", status="done", message="Logged in successfully")

            # Navigate via Rapportage > Front-Office menu
            send_event("progress", step="navigate", status="in_progress", message="Opening Rapportage menu...")
            page.hover('a[data-field="Rapportage"]')
            page.wait_for_timeout(500)

            send_event("progress", step="navigate", status="in_progress", message="Clicking Front-office...")
            page.click('a[data-field="Front-Office"]')
            page.wait_for_timeout(3000)

            content = page.frame("content")
            if not content:
                send_event("error", message="Could not find content iframe")
                return

            send_event("progress", step="navigate", status="in_progress", message="Selecting internal portfolios...")
            content.goto(f"{BASE_URL}/rapportFrontofficeClientSelectie.php?portefeuilleIntern=1")
            page.wait_for_timeout(3000)
            send_event("progress", step="navigate", status="done", message="Navigated to internal portfolio selection")

            # Scrape portfolio table across all pages
            nav = page.frame("navigatie")
            portfolios = []
            page_num = 1

            while True:
                send_event("progress", step="scrape", status="in_progress", message=f"Reading portfolio table (page {page_num})...")
                content.wait_for_selector('tr.list_dataregel', timeout=10000)

                rows = content.query_selector_all('tr.list_dataregel')
                for row in rows:
                    cells = row.query_selector_all('td.listTableData')
                    if len(cells) >= 4:
                        portfolios.append({
                            "portefeuille": cells[0].inner_text().strip(),
                            "depotbank": cells[1].inner_text().strip(),
                            "client": cells[2].inner_text().strip(),
                            "naam": cells[3].inner_text().strip(),
                        })

                # Next page link is in the navigatie frame — active ones have
                # img.simbisIcon (not .simbisIconGray) inside an <a> tag
                next_link = nav.query_selector('a:has(img[src*="navigate_right"].simbisIcon)') if nav else None
                if not next_link:
                    break

                page_num += 1
                next_link.click()
                page.wait_for_timeout(2000)

            send_event("progress", step="scrape", status="done", message=f"Read {len(portfolios)} portfolios across {page_num} page(s)")
            send_event("portfolios", data=portfolios)
            send_event("done", message=f"Scan complete. Found {len(portfolios)} portfolios.")
            return portfolios
        except AirsAccessForbiddenError as e:
            send_event(
                "error",
                kind="ip_forbidden",
                message=(
                    "AirSPMS responded with HTTP 403 Forbidden. This server's "
                    "outbound IP address is likely not on the AirSPMS allowlist "
                    "— ask your AirSPMS administrator to whitelist it, or wait "
                    "and retry if the egress IP just rotated."
                ),
                detail=e.detail,
            )
            return []
        except Exception as e:
            send_event("error", message=f"{type(e).__name__}: {e}")
            return []
        finally:
            browser.close()
