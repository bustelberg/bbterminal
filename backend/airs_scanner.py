import os
import queue
import threading
from urllib.parse import urlencode
from playwright.sync_api import sync_playwright


BASE_URL = "https://bustelberg.airspms.cloud"


def _login(page):
    """Log in to AirSPMS. Raises on failure."""
    broker_username = os.environ.get("BROKER_USERNAME", "")
    broker_password = os.environ.get("BROKER_PASSWORD", "")

    if not broker_username or not broker_password:
        raise RuntimeError("BROKER_USERNAME or BROKER_PASSWORD not set in .env")

    page.goto(f"{BASE_URL}/login.php")
    page.wait_for_load_state("domcontentloaded")
    page.fill('#username', broker_username)
    page.fill('#password', broker_password)
    page.click('#btnFase1')
    page.wait_for_timeout(2000)

    if page.locator('#smsValid').is_visible():
        page.click('#btnFase3')
    elif page.locator('#smsDialog').is_visible():
        raise RuntimeError("SMS code required — cannot automate SMS step")
    elif page.locator('#smsOffline').is_visible():
        page.click('#btnFase4')

    page.wait_for_load_state("networkidle")


# ─── Persistent session for fast downloads ────────────────────────────────────

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
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page()
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
            url, result_q = self._queue.get()
            try:
                ensure_logged_in()
                resp = page.request.get(url)
                body = resp.body()

                # If we got HTML back (session expired), re-login once
                if body[:15].lower().startswith(b"<!doctype"):
                    close_session()
                    ensure_logged_in()
                    resp = page.request.get(url)
                    body = resp.body()

                result_q.put(("ok", body))
            except Exception as e:
                close_session()
                result_q.put(("error", e))

    def get(self, url: str) -> bytes:
        """Thread-safe GET. Submits work to the dedicated Playwright thread."""
        result_q: queue.Queue = queue.Queue()
        self._queue.put((url, result_q))
        status, value = result_q.get()
        if status == "error":
            raise value
        return value


_session = _AirsSession()


def download_portfolio_sync(portfolio_name: str, datum_van: str, datum_tot: str) -> bytes:
    """Download ATT Excel report using the persistent session."""
    params = urlencode({
        "rapport_types": "ATT",
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


# ─── Scanner (uses its own browser for DOM scraping) ──────────────────────────

def scan_portfolios_sync(send_event):
    """Run Playwright scan synchronously (call from a thread)."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

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
        except Exception as e:
            send_event("error", message=f"{type(e).__name__}: {e}")
            return []
        finally:
            browser.close()
