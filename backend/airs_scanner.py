import os
import queue
import threading
import time
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


# AirSPMS answers a report request it has no data for with a bare HTML FRAGMENT — no doctype, no
# <html>, just a line of markup: `<br>\n30-07-2026 …`. It is a message, and it is the commonest
# non-spreadsheet reply we get.
_HTML_DOC_MARKERS = (b"<!doctype", b"<html", b"<!--", b"<head", b"<?php")
_HTML_FRAGMENT_MARKERS = (b"<br", b"<p", b"<div", b"<span", b"<b>", b"<font", b"<table")


def _looks_like_html(body: bytes) -> bool:
    """Heuristic: is this response body HTML rather than a file?

    Covers a document (`<!doctype html>`, a bare `<html>`, a leading `<?xml`/BOM-then-tag that some
    PHP error pages emit) AND a bare fragment.

    ⚠ THE FRAGMENT CASE IS NOT AN EDGE CASE — IT IS AIRS SAYING "NO DATA". A report the book has
    nothing for comes back as ~170 bytes beginning `<br>`, which matched no document marker, so it
    fell through to the raw-bytes branch and every one of them was reported as
    `RuntimeError: … 177 bytes starting b'<br>\\n30-07-2026 '`. Measured in production 2026-07-30:
    14 of 44 accounts failed their MODEL report that way — and all 14 are `_MV` (meervoudig),
    `_BM_` (benchmark) or `WTS test` books, i.e. exactly the types that HAVE no fixed model. The
    message AIRS sent was the answer, and 16 bytes of it was all anyone ever saw.

    Being HTML routes it to `_describe_non_excel`, which prints the status, content-type and a
    300-character excerpt — so the next run states what AIRS actually said instead of hinting at it.
    """
    head = body[:512].lstrip().lower()
    if head.startswith(_HTML_DOC_MARKERS):
        return True
    # A fragment only counts when the whole body is small: a real spreadsheet never starts with a
    # tag, but a truncated binary might, and calling a corrupt download "an HTML page" would hide
    # a genuine transport fault behind a tidier-looking diagnosis.
    return len(body) <= 4096 and head.startswith(_HTML_FRAGMENT_MARKERS)


# A transient connection reset drops the request but not the account — the socket
# died mid-flight (AirSPMS/Cloudflare closing an idle keep-alive, a brief blip),
# so a rebuilt session and a retry usually succeed. Distinct from a login bounce
# (HTML body → the worker's re-login retry) or a real fault (re-raised).
_MAX_TRANSIENT_RETRIES = 2
_TRANSIENT_BACKOFF_S = (0.5, 1.5)
_TRANSIENT_CONN_MARKERS = (
    "econnreset", "econnrefused", "etimedout", "epipe",
    "socket hang up", "connection closed", "connection reset",
    "connection aborted", "network error", "connection terminated",
)


def _is_transient_conn_error(e: Exception) -> bool:
    """True when an exception is a transient network reset (ECONNRESET / socket
    hang up / connection closed) worth one silent retry, rather than an auth or
    content problem. Matches on the Playwright error text (its errors carry the
    node-style errno string, e.g. `read ECONNRESET`)."""
    msg = str(e).lower()
    return any(marker in msg for marker in _TRANSIENT_CONN_MARKERS)


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

                # Retry a transient connection reset (ECONNRESET / socket hang up):
                # the socket died mid-request but the account is fine, so rebuild the
                # session and try again a couple of times with a short backoff. A
                # non-transient error (or the last attempt) re-raises to the caller.
                resp = None
                for attempt in range(_MAX_TRANSIENT_RETRIES + 1):
                    try:
                        resp = job(page)
                        break
                    except Exception as e:
                        if attempt >= _MAX_TRANSIENT_RETRIES or not _is_transient_conn_error(e):
                            raise
                        time.sleep(_TRANSIENT_BACKOFF_S[min(attempt, len(_TRANSIENT_BACKOFF_S) - 1)])
                        close_session()
                        ensure_logged_in()

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


class AirsNoData(RuntimeError):
    """AirSPMS answered the report request with "there is nothing here".

    ⚠ AN ANSWER, NOT A FAULT — AND TELLING THEM APART NEEDED EVIDENCE, NOT A GUESS. AIRS replies
    with a ~170-byte HTML fragment (`<br>\\n30-07-2026 …`) where a spreadsheet would be. That is
    roughly what an expired session or an IP block also looks like, which is why it was reported as
    a hard error and 14 of 44 accounts failed their MODEL report on every single run.

    What settles it is that the OTHER THREE REPORTS SUCCEED FOR THE SAME ACCOUNT IN THE SAME
    SESSION. Measured 2026-07-30: Rendement 44/44, Vermogensoverzicht 44/44, Mutaties 44/44 —
    Model 30/44. A dead session cannot break one report and leave three working. And all 14 are
    `_MV` (meervoudig), `_BM_` (benchmark), `WTS test` or `_Fx` books: exactly the types that HAVE
    no fixed model. The failure was per-REPORT, which makes it a fact about the report.

    Raised distinctly so a caller can record "nothing to store" rather than "the scan broke". The
    account then counts as COMPLETE, stops wearing a permanent ⚠ — and, the part that actually cost
    something, stops being re-scanned every run: an account that can never be complete is never
    skipped as fresh, so those 14 were the ONLY accounts the incremental scan ever visited
    ("1/14: BUS_WTS_SterkeMerken_Fx…" while the 30 real books were correctly skipped).
    """


# AIRS's no-data fragment carries the report date and no table. Kept narrow deliberately: a body
# that merely FAILS to be a spreadsheet is still an error — only one that positively looks like
# this message is an answer.
#
# ⚠ THESE ARE AIRS'S ACTUAL WORDS, COPIED FROM A REAL REPLY — NOT A GUESS AT THEM. The first
# version of this list guessed "geen gegevens" and matched nothing, because what AIRS really sends
# for a book with no model is (measured 2026-07-30, 177 bytes, HTTP 200):
#
#   <br> 30-07-2026 16:26:13 Modelvergelijking voor portefeuille BUS_WTS_SterkeMerken_Fx gestopt,
#   geen waarden voor modelportefeuille '' gevonden.<br>
#
# "geen WAARDEN", not "geen gegevens". Both variants of the tail appear and both are answers:
# `modelportefeuille ''` (no model assigned at all — the `_MV`/benchmark/test books) and
# `modelportefeuille 'BUS_WTS_SterkeMerken_Fx'` (a model IS named but holds no values).
_NO_DATA_MARKERS = (
    "geen waarden voor modelportefeuille",   # the measured MODEL reply, both tail variants
    "modelvergelijking",                     # …and the sentence it always opens with
    "geen gegevens", "geen data", "niets gevonden", "no data",
)


def _is_no_data(body: bytes) -> bool:
    """Is this AIRS saying the report is empty, rather than failing to produce it?

    ⚠ IT MUST NOT MATCH A LOGIN PAGE. `_classify_html_page` already recognises those, and an
    expired session returns a full document with a form in it — this requires a SHORT fragment
    (no doctype, no <html>) that contains one of AIRS's own no-data phrases. Anything longer or
    less specific stays an error, because reporting a broken session as "no model" would hide the
    one failure that needs a human.
    """
    if len(body) > 2048:
        return False
    text = body.decode("utf-8", "replace").lower()
    if "<html" in text or "<!doctype" in text or "<form" in text:
        return False
    return any(m in text for m in _NO_DATA_MARKERS)


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

    # ⚠ THE SAME STRAY BYTE THE LIST EXPORT HAS. AirSPMS prepends an APOSTROPHE before the zip
    # magic on some responses, and pandas rejects the whole file with "Excel file format cannot be
    # determined, you must specify an engine manually" — which reads as a broken download rather
    # than one junk byte. Measured 2026-07-29: 14 Model and 13 Vermogensoverzicht downloads failed
    # with exactly that message while ATT succeeded 44/44, i.e. per REPORT, not per account.
    # `_strip_spreadsheet_preamble` is a no-op on a clean file, so this is safe for all four.
    content = _strip_spreadsheet_preamble(content)

    # ⚠ AND IF IT IS STILL NOT A SPREADSHEET, SAY WHAT IT IS. The old check only caught a body
    # beginning exactly `<!doctype`; an error page with leading whitespace, a bare `<html>` or a
    # BOM sailed through into pandas, where every cause — expired session, "no data for this
    # period", an IP block — arrived as the same opaque engine error. `_describe_non_excel` prints
    # the status, content-type, page title and an excerpt, so the next failure is a diagnosis.
    if not content.startswith((_XLSX_MAGIC, _XLS_MAGIC)):
        # ⚠ ASKED BEFORE THE ERROR PATHS: "there is nothing here" is an ANSWER. See `AirsNoData`.
        if _is_no_data(content):
            raise AirsNoData(
                f"{rapport_types} for {portfolio_name!r} ({datum_van}..{datum_tot}): AIRS reports "
                f"no data — this book has no {rapport_types} for that period.")
        where = f"{rapport_types} for {portfolio_name!r} ({datum_van}..{datum_tot}) is not a spreadsheet"
        if _looks_like_html(content):
            raise RuntimeError(f"{where} — " + _describe_non_excel(AirsHttpResponse(
                body=content, status=200, content_type="", url=url)))
        # Not HTML either: show the head so the next investigation starts from the bytes rather
        # than from a guess about which of five causes it was.
        raise RuntimeError(f"{where}: {len(content)} bytes starting {content[:16]!r}")

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


def download_mutaties_sync(portfolio_name: str, datum_van: str, datum_tot: str) -> bytes:
    """Download the Mutaties (journal) Excel report — the book's dividend + withholding-tax lines.

    ⚠ AN UNKNOWN `rapport_types` RETURNS ZERO BYTES, NOT AN ERROR. Probed 2026-07-23: `MUT` and
    `TRANS` return an XLS; `MUTA`, `MUTATIES`, `MUTATIE`, `GRB`, `BOEK`, `JOURNAAL` all return an
    EMPTY body. `_download_report_sync`'s length check is what turns that into a real failure
    instead of a zero-row parse that reads as "this book earned no income".
    """
    from airs_mutaties import MUTATIES_RAPPORT_TYPE  # noqa: PLC0415  (avoid an import cycle)

    code = os.environ.get("AIRS_MUTATIES_RAPPORT_TYPE", "").strip() or MUTATIES_RAPPORT_TYPE
    return _download_report_sync(portfolio_name, datum_van, datum_tot, code)


def download_model_sync(portfolio_name: str, datum_van: str, datum_tot: str) -> bytes:
    """Download the MODEL report — a DYNAMIC portfolio's own model weights.

    This is what retires the fixed<->dynamic pairing: the weights are scoped to the book itself,
    so there is no second portfolio to guess a partner for. Same zero-bytes-on-a-wrong-code trap
    as MUT; `_download_report_sync`'s length check is what makes that loud.
    """
    from airs_model import MODEL_RAPPORT_TYPE  # noqa: PLC0415  (avoid an import cycle)

    code = os.environ.get("AIRS_MODEL_RAPPORT_TYPE", "").strip() or MODEL_RAPPORT_TYPE
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


# ── The Front-Office client selection, and the three filters that define WHICH portfolios ──────
FRONT_OFFICE_SELECTIE_PATH = "rapportFrontofficeClientSelectie.php"

# ⚠ ALL THREE, EXPLICITLY. Only `portefeuilleIntern` used to be sent; the other two happened to
# match the page's defaults, so the scan returned the right 44 by luck rather than by instruction.
# A default is not a guarantee — one AIRS UI change and this silently starts scraping a different
# population, with no error and no obvious symptom beyond a row count nobody is watching.
#
# Measured 2026-07-24 by reading the page's own radio controls:
#   actief             actief | eActief | inactief          -> `actief`  (Actieve, not alle)
#   portefeuilleIntern 0 | 1 | 10                            -> `1`       (Interne, not externe)
#   metConsolidatie    0 | 1 | 10                            -> `0`       (Zonder consolidatie)
# Together: exactly 44 portfolios across 2 pages.
FRONT_OFFICE_FILTERS = "actief=actief&portefeuilleIntern=1&metConsolidatie=0"

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
            content.goto(f"{BASE_URL}/{FRONT_OFFICE_SELECTIE_PATH}?{FRONT_OFFICE_FILTERS}")
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


# ─── Model portfolios (Stamgegevens > Onderhoud portefeuilles > Model portefeuilles) ──
#
# The list page is `modelportefeuillesList.php?page=N&=` — the URL the "next page" arrow
# pokes into `parent.frames['content'].location`. Two things about it are load-bearing and
# neither is obvious from the DOM:
#
#   1. THE NAME COLUMN IS TRUNCATED. The cell reads "BUS_WTS_Dividend..." — the full
#      "BUS_WTS_Dividend_Fx" is nowhere on the list page, not even in a title=/alt=. The
#      only place it exists is the row's edit page, as
#      `<input name="Portefeuille" value="BUS_WTS_Dividend_Fx">`. So a truncated row costs
#      one extra request; a short one (TOPS_KM) does not.
#
#   2. AN OUT-OF-RANGE PAGE DOES NOT COME BACK EMPTY. Asking for page 5 of a 4-page list
#      returns 25 rows again — AirSPMS clamps rather than 404s. A `while rows: page += 1`
#      loop therefore never terminates. We stop when a page yields no ids we haven't seen.
MODEL_PORTFOLIO_LIST_PATH = "modelportefeuillesList.php"
MODEL_PORTFOLIO_EDIT_PATH = "modelportefeuillesEdit.php"
# Safety rail for the pagination loop: 25 rows/page, so this is 2,500 portfolios.
_MAX_PORTFOLIO_PAGES = 100


def _decode_html(resp: "AirsHttpResponse") -> str:
    """Decode an AirSPMS page with the charset IT declares — NOT utf-8.

    AirSPMS serves `Content-Type: text/html; charset=ISO-8859-1`. Decoding that as utf-8
    with errors="replace" silently turns "Azië" into "Azi�": three of the 95 model
    portfolios carry an accent, and the damage is invisible until somebody reads the list.

    Falls back to cp1252 rather than strict latin-1 — cp1252 is the practical superset that
    servers claiming ISO-8859-1 actually emit (it maps the 0x80–0x9F range latin-1 leaves
    as control codes, which is where smart quotes and dashes live).
    """
    import re  # noqa: PLC0415

    m = re.search(r"charset=([\w-]+)", resp.content_type or "", re.I)
    enc = (m.group(1) if m else "").lower()
    if enc in ("", "iso-8859-1", "latin-1", "latin1", "windows-1252", "cp1252"):
        enc = "cp1252"
    try:
        return resp.body.decode(enc, "replace")
    except LookupError:                     # a charset Python doesn't know
        return resp.body.decode("cp1252", "replace")


def _clean_cell(html_fragment: str) -> str:
    """Anchor inner-HTML -> plain text. Strips tags, entities and the &nbsp; spacer."""
    import html as _html  # noqa: PLC0415
    import re  # noqa: PLC0415

    text = re.sub(r"<[^>]+>", " ", html_fragment)
    # `html.unescape` handles the whole entity table (&nbsp; &amp; &euml; …) — hand-rolling
    # a few replacements leaves the rest to leak through as literal "&euml;".
    text = _html.unescape(text).replace("\xa0", " ")
    return " ".join(text.split())


def parse_model_portfolio_rows(html: str) -> list[dict]:
    """Rows of ONE list page: `{id, name, truncated, omschrijving, fixed, fixed_datum}`.

    Every cell of a row is wrapped in an anchor pointing at the SAME edit id, and the first
    such anchor is the pencil icon (no text). So: group the anchors by row, drop the empty
    one, and the remaining four are Portefeuille / Omschrijving / Fixed / FixedDatum.
    """
    import re  # noqa: PLC0415

    rows: list[dict] = []
    for tr in re.findall(r"<tr[^>]*>.*?</tr>", html, re.I | re.S):
        if MODEL_PORTFOLIO_EDIT_PATH not in tr:
            continue
        ids = re.findall(rf"{MODEL_PORTFOLIO_EDIT_PATH}\?action=edit&(?:amp;)?id=(\d+)", tr)
        if not ids:
            continue
        cells = [
            _clean_cell(a)
            for a in re.findall(
                rf'<a[^>]+{MODEL_PORTFOLIO_EDIT_PATH}[^>]*>(.*?)</a>', tr, re.I | re.S)
        ]
        cells = [c for c in cells if c]          # drop the icon anchor
        if not cells:
            continue
        name = cells[0]
        rows.append({
            "id": int(ids[0]),
            "name": name,
            # The list truncates with a literal "..." — the row must be re-read from its
            # edit page to learn the real name.
            "truncated": name.endswith("..."),
            "omschrijving": cells[1] if len(cells) > 1 else "",
            "fixed": cells[2] if len(cells) > 2 else "",
            "fixed_datum": cells[3] if len(cells) > 3 else "",
        })
    return rows


def parse_portfolio_full_name(edit_html: str) -> str | None:
    """The UNtruncated name off an edit page: `<input name="Portefeuille" value="...">`."""
    import re  # noqa: PLC0415

    m = re.search(
        r'<input[^>]*\bname="Portefeuille"[^>]*\bvalue="([^"]*)"', edit_html, re.I)
    if not m:      # attribute order isn't guaranteed — try value-before-name too
        m = re.search(
            r'<input[^>]*\bvalue="([^"]*)"[^>]*\bname="Portefeuille"', edit_html, re.I)
    if not m:
        return None
    name = m.group(1).strip()
    return name or None


# The list page's own "Naar XLS" export — one download for ALL 95 rows, with FULL names.
# This is what makes the scan fast: the alternative is 52 edit-page round-trips (~4 minutes)
# just to un-truncate the names the list clips.
#
# It is a FORM SUBMIT (the link's onclick sets a hidden `toXls=1` and submits `editForm`),
# not a fetchable URL — hence `download_via_form`, the same path the CRM export uses.
MODEL_PORTFOLIO_XLS_SELECTOR = 'a[onclick*="toXls"]'


# The two spreadsheet magics AirSPMS emits: xlsx is a zip, legacy .xls is an OLE
# compound file. The LIST export is xlsx; the per-portfolio POSITIONS export is .xls.
# Written as byte values, not escapes, so no editor or shell can mangle them.
_XLSX_MAGIC = bytes([0x50, 0x4B, 0x03, 0x04])      # 'PK' + 03 04
_XLS_MAGIC = bytes([0xD0, 0xCF, 0x11, 0xE0])       # OLE compound file


def _strip_spreadsheet_preamble(body: bytes) -> bytes:
    """AirSPMS prepends a stray byte to the list export: the payload starts with an
    APOSTROPHE and only then the zip magic. pandas rejects the whole file with 'Excel
    file format cannot be determined' -- which reads like a broken download rather than
    one junk byte. Cut back to the real magic."""
    for magic in (_XLSX_MAGIC, _XLS_MAGIC):
        i = body.find(magic)
        if 0 <= i <= 8:            # a clean file starts at 0; tolerate a tiny preamble
            return body[i:]
    return body


def fetch_model_portfolio_names_sync() -> list[str]:
    """Every portfolio's FULL name, in list order — one XLS download instead of 52 page
    fetches. Columns: Portefeuille | Omschrijving | Fixed | FixedDatum."""
    from io import BytesIO  # noqa: PLC0415

    import pandas as pd  # noqa: PLC0415

    resp = _session.download_via_form(
        f"{BASE_URL}/{MODEL_PORTFOLIO_LIST_PATH}", MODEL_PORTFOLIO_XLS_SELECTOR)
    body = _strip_spreadsheet_preamble(resp.body)
    if _looks_like_html(body):
        raise RuntimeError(
            "the model-portfolio XLS export returned HTML, not a spreadsheet. "
            + _describe_non_excel(resp))
    df = pd.read_excel(BytesIO(body))
    return [str(v).strip() for v in df["Portefeuille"].tolist()]


def _apply_full_names(rows: list[dict], full_names: list[str]) -> list[dict]:
    """Pair the id-bearing HTML rows with the full names from the XLS.

    POSITIONALLY, because the XLS has no id and the truncated names are NOT a unique key:
    "DiTopSelectie OF..." prefix-matches both "DiTopSelectie OFF DYN" and
    "...OFF FX" (19 of the 95 rows are ambiguous like this). Both views come from the same
    query, so row N of the XLS is row N of the paginated list — verified 95/95.

    But a positional join is only as good as that assumption, so EVERY pairing is CHECKED
    against the prefix the list did show us. A row that fails the check keeps its truncated
    name and stays flagged, and the caller re-reads it from its edit page — the slow path,
    for the rows that need it rather than for all of them.
    """
    if len(full_names) != len(rows):
        return rows                    # counts disagree: trust nothing, fall back entirely

    for row, full in zip(rows, full_names):
        shown = row["name"]
        prefix = shown[:-3] if row["truncated"] else shown
        if full.startswith(prefix):
            row["name"] = full
            row["truncated"] = False
    return rows


def fetch_model_portfolios_sync(send_event=None) -> list[dict]:
    """Every model portfolio, with its FULL name.

    FAST PATH: the paginated list for the ids (4-5 requests), plus ONE "Naar XLS" download
    for the full names, paired positionally and verified. ~10s.

    SLOW PATH, per row, only when the pairing fails its check (or the XLS is unavailable):
    re-read that row's name from its edit page. That is what used to run for all 52
    truncated rows and took ~4 minutes.
    """
    def emit(kind: str, **kw):
        if send_event:
            send_event(kind, **kw)

    seen_ids: set[int] = set()
    rows: list[dict] = []

    for page in range(1, _MAX_PORTFOLIO_PAGES + 1):
        url = f"{BASE_URL}/{MODEL_PORTFOLIO_LIST_PATH}?page={page}&="
        html = _decode_html(_session.get_response(url))
        page_rows = parse_model_portfolio_rows(html)
        fresh = [r for r in page_rows if r["id"] not in seen_ids]

        # An out-of-range page REPEATS rows rather than returning none (see the header), so
        # "no new ids" — not "no rows" — is the only safe terminator.
        if not fresh:
            emit("progress", step="list", status="done",
                 message=f"{len(rows)} portfolios across {page - 1} page(s)")
            break

        seen_ids.update(r["id"] for r in fresh)
        rows.extend(fresh)
        emit("progress", step="list", status="in_progress",
             message=f"page {page}: {len(fresh)} new, {len(rows)} total")

    emit("progress", step="names", status="in_progress",
         message="downloading the full-name XLS export...")
    try:
        rows = _apply_full_names(rows, fetch_model_portfolio_names_sync())
    except Exception as e:  # noqa: BLE001 — the per-row fallback below still gets it right
        emit("progress", step="names", status="in_progress",
             message=f"XLS export unavailable ({type(e).__name__}) - falling back to edit pages")

    # Whatever the fast path could not resolve (normally nothing) is fetched the slow way.
    leftover = [r for r in rows if r["truncated"]]
    for i, r in enumerate(leftover, 1):
        url = f"{BASE_URL}/{MODEL_PORTFOLIO_EDIT_PATH}?action=edit&id={r['id']}"
        full = parse_portfolio_full_name(_decode_html(_session.get_response(url)))
        if full:
            r["name"] = full
            r["truncated"] = False
        emit("progress", step="names", status="in_progress",
             message=f"edit-page fallback {i}/{len(leftover)}: {r['name']}")

    rows.sort(key=lambda r: r["name"].lower())
    # "portfolios", not "done": the caller may follow this with the (much slower) holdings
    # count and own the terminal event. The list is complete and renderable right here.
    emit("portfolios", count=len(rows), portfolios=rows)
    return rows


def has_fixed_model(fixed: str | None) -> bool:
    """AirSPMS only stores a composition for a portfolio of type `fixed (…)`.

    A `normaal` (31) or `meervoudig` (6) one — the benchmarks and multi-model wrappers —
    has NO fixed model at all. Asking one for positions costs two round-trips and returns
    an empty sheet, which is an ANSWER, not a failure."""
    return (fixed or "").strip().lower().startswith("fixed")


def count_model_portfolio_holdings_sync(
    portfolios: list[dict], send_event=None, on_positions=None, on_error=None,
) -> list[dict]:
    """Annotate every portfolio with `holdings` — the number of INSTRUMENTS in its model.

    `on_positions(id, datum, rows)` / `on_error(id, msg)` let the caller PERSIST what this
    already downloaded. The scanner stays free of the DB (it is also driven from scripts with
    no Supabase), while the router pays nothing extra to store the positions: counting them
    requires fetching them.

    An instrument is an ISIN-bearing row. The cash line ("Liquiditeiten") carries no ISIN
    and is not an instrument, so it is not counted — which also means the count cannot be
    inflated by the `"nan"`-string trap that `_parse_positions_xls` exists to prevent.

    `holdings = None` means NO FIXED MODEL EXISTS, and that is not the same as 0. Measured:
    58 of the 95 portfolios are `fixed (…)`, and exactly ONE of those has an empty model —
    a real, empty, fixed model. Flattening both to `0` would erase the only case where the
    zero means something. Skipping the 37 non-fixed rows also makes this ~40% cheaper.

    Slow by nature: one edit-page GET + one XLS download per fixed portfolio. It streams a
    `count` event per row so the caller can fill a column in as it goes rather than block.
    """
    def emit(kind: str, **kw):
        if send_event:
            send_event(kind, **kw)

    todo = [p for p in portfolios if has_fixed_model(p.get("fixed"))]
    emit("progress", step="holdings", status="in_progress",
         message=f"counting holdings for {len(todo)} fixed portfolios "
                 f"({len(portfolios) - len(todo)} have no fixed model)")

    for i, p in enumerate(todo, 1):
        try:
            raw = fetch_portfolio_positions_sync(p["id"])
            rows = raw["rows"]
            if rows:
                # DISTINCT ISINs. A portfolio can list one instrument on TWO lines — measured:
                # VTopSelectie OFF FX holds CapitaLand (SG1M51904654) at 2% and again at 3%.
                # Counting rows would report 29 instruments for a model that holds 28.
                p["holdings"] = len({str(r["ISINCode"]).strip()
                                     for r in rows if r.get("ISINCode")})
                p["holdings_datum"] = raw["datum"]
            else:
                # Every candidate date came back empty — the dropdown had nothing but its
                # "today" placeholder. There is NO dated composition on record, which is not
                # the same as "this model holds nothing". Leave `holdings` unset.
                p["no_snapshot"] = True
            if on_positions:
                on_positions(p["id"], raw["datum"], rows, raw.get("dates"))
        except Exception as e:  # noqa: BLE001 — one bad portfolio must not kill the scan
            # Leave `holdings` unset rather than writing 0: we did not learn that it holds
            # nothing, we failed to ask. A 0 here would be a fabricated fact.
            p["holdings_error"] = f"{type(e).__name__}: {e}"
            if on_error:
                on_error(p["id"], p["holdings_error"])
        emit("count", id=p["id"], holdings=p.get("holdings"),
             datum=p.get("holdings_datum"), error=p.get("holdings_error"),
             no_snapshot=bool(p.get("no_snapshot")),
             message=f"{i}/{len(todo)} {p['name']}")

    emit("progress", step="holdings", status="done",
         message=f"counted {sum(1 for p in todo if 'holdings' in p)}/{len(todo)}")
    return portfolios


# ─── Model-portfolio POSITIONS (the XLS export behind each portfolio) ─────────────────
#
# Each portfolio's edit page carries an iframe whose src the page's own JS builds as
#     modelportefeuillefixedList.php?Portefeuille=<name>&Datum=<FixedDatum>&type=<n>
# and that page holds the "XLS-export" link (`?action=xls&…`, RELATIVE to itself — not to
# the edit page). So the export URL is:
#     modelportefeuillefixedList.php?action=xls&Portefeuille=…&Datum=…&type=1
# It returns a REAL legacy .xls (OLE compound file), not the HTML-table-named-.xls that
# many PHP apps emit — pandas reads it directly. Columns:
#     id · Portefeuille · Fonds · Percentage · ISINCode · valuta ·
#     Beleggingscategorie · Beleggingssector · regio · afmCategorie · fondsimportcode
#
# THE DATE IS THE WHOLE PROBLEM. `FixedDatum` is a <select>, and its FIRST option is always
# TODAY — an empty "new snapshot" placeholder that yields ZERO rows. The real snapshots
# follow (BUS_WTS_Dividend_Fx has 13, newest 2024-12-10). Ask for today and you get an
# empty table that looks exactly like "this portfolio has no holdings". So we try the
# candidate dates NEWEST-FIRST and take the first that actually returns rows.
#
# A portfolio whose ONLY option is the placeholder (TOPS_KM, the BUS_BM_* benchmarks —
# every one of them typed `meervoudig`/`normaal` rather than `fixed`) has no fixed model at
# all. `type=2` returns nothing for them either. That is an answer, not a failure.
MODEL_PORTFOLIO_FIXED_PATH = "modelportefeuillefixedList.php"
# How many snapshot dates to try before giving up. The placeholder costs one attempt, so 3
# leaves room for a genuinely empty latest snapshot without downloading all 14.
_MAX_DATUM_ATTEMPTS = 3


def parse_fixed_datum_options(edit_html: str) -> list[str]:
    """The `FixedDatum` <select> options — the snapshot dates this portfolio has.

    The first is always TODAY (an empty placeholder AirSPMS injects); the rest are the real
    snapshots. Returned verbatim, in page order — the caller decides what to do with them.
    """
    import re  # noqa: PLC0415

    m = re.search(r'<select[^>]*id="FixedDatum"[^>]*>(.*?)</select>', edit_html, re.I | re.S)
    if not m:
        return []
    return [
        v.strip()
        for v in re.findall(r'<option[^>]*value="([^"]*)"', m.group(1), re.I)
        if v.strip()
    ]


def _positions_xls_url(portefeuille: str, datum: str, kind: int = 1) -> str:
    from urllib.parse import quote  # noqa: PLC0415

    return (
        f"{BASE_URL}/{MODEL_PORTFOLIO_FIXED_PATH}?action=xls"
        f"&Portefeuille={quote(portefeuille)}&Datum={quote(datum)}&type={kind}"
    )


def _parse_positions_xls(body: bytes) -> list[dict]:
    """The exported .xls -> rows, with every NaN turned into a real None.

    The cash line ("Liquiditeiten") has no ISIN, no currency and no sector — those cells are
    NaN. `df.where(pd.notna(df), None)` ALONE DOES NOT WORK: on a float or mixed column
    pandas coerces the None straight back to NaN, so the NaN survives, reaches the API as a
    float, and `str()`s into the literal string "nan" — which is truthy, so the cash row
    then counts as a holding with the ISIN "nan". `astype(object)` first is what makes the
    None stick.
    """
    from io import BytesIO  # noqa: PLC0415

    import pandas as pd  # noqa: PLC0415

    df = pd.read_excel(BytesIO(_strip_spreadsheet_preamble(body)))
    df = df.astype(object).where(pd.notna(df), None)
    return df.to_dict("records")


def fetch_portfolio_positions_sync(
    portfolio_id: int, *, datum: str | None = None,
) -> dict:
    """One model portfolio's positions, ISIN and all.

    Returns `{portfolio, portfolio_id, datum, dates, rows}`. With no `datum` it walks the
    snapshot dates NEWEST-FIRST and returns the first that has rows — see the header for
    why asking for "today" silently yields an empty table.
    """
    edit_html = _decode_html(_session.get_response(
        f"{BASE_URL}/{MODEL_PORTFOLIO_EDIT_PATH}?action=edit&id={portfolio_id}"))
    name = parse_portfolio_full_name(edit_html)
    if not name:
        raise RuntimeError(
            f"portfolio {portfolio_id}: no Portefeuille field on its edit page "
            "(session expired, or the id doesn't exist)")

    dates = parse_fixed_datum_options(edit_html)
    candidates = [datum] if datum else sorted(set(dates), reverse=True)[:_MAX_DATUM_ATTEMPTS]

    rows: list[dict] = []
    used: str | None = None
    for d in candidates:
        rows = _parse_positions_xls(_session.get_response(_positions_xls_url(name, d)).body)
        used = d
        if rows:
            break

    return {
        "portfolio": name,
        "portfolio_id": portfolio_id,
        "datum": used,
        "dates": dates,
        "rows": rows,
    }
