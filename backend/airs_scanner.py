import os
from urllib.parse import urlencode
from playwright.sync_api import sync_playwright


BASE_URL = "https://bustelberg.airspms.cloud"


def _login(page):
    """Log in to AirSPMS. Returns True on success, raises on failure."""
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

            # Scrape portfolio table (inside content iframe)
            send_event("progress", step="scrape", status="in_progress", message="Reading portfolio table...")
            content.wait_for_selector('tr.list_dataregel', timeout=10000)

            rows = content.query_selector_all('tr.list_dataregel')
            portfolios = []
            for row in rows:
                cells = row.query_selector_all('td.listTableData')
                if len(cells) >= 4:
                    portfolios.append({
                        "portefeuille": cells[0].inner_text().strip(),
                        "depotbank": cells[1].inner_text().strip(),
                        "client": cells[2].inner_text().strip(),
                        "naam": cells[3].inner_text().strip(),
                    })

            send_event("portfolios", data=portfolios)
            send_event("done", message=f"Scan complete. Found {len(portfolios)} portfolios.")
        except Exception as e:
            send_event("error", message=f"{type(e).__name__}: {e}")
        finally:
            browser.close()


def download_portfolio_sync(portfolio_name: str, datum_van: str, datum_tot: str) -> bytes:
    """Log in with Playwright and download the Excel report using the browser's API context."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            _login(page)

            params = urlencode({
                "rapport_types": "ATT",
                "Portefeuille": portfolio_name,
                "datum_van": datum_van,
                "datum_tot": datum_tot,
                "type": "xls",
            })
            url = f"{BASE_URL}/rapportFrontofficeClientAfdrukkenHtml.php?{params}"

            # Use Playwright's API request context which shares the browser's cookies
            resp = page.request.get(url)
            content = resp.body()

            if len(content) < 100:
                raise RuntimeError(f"Response too small ({len(content)} bytes)")
            if content[:15].lower().startswith(b'<!doctype'):
                raise RuntimeError("Got HTML instead of Excel — session may have expired")

            return content
        finally:
            browser.close()
