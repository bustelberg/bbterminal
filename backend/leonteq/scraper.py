"""Playwright-based scraper for Leonteq's structured-products underlyings
listing page.

URL: https://structuredproducts-ch.leonteq.com/services/underlyings

The page is a JavaScript SPA — no useful HTML on first load. We use a
headless Chromium (same `playwright.sync_api` stack the AIRS broker
scanner already imports) to let the page fully render, then read the
equity rows out of the DOM.

Selectors are intentionally best-effort and **central to one block**
below; they're the part most likely to drift when Leonteq redesigns.
On a scrape failure the function logs the page's outer HTML title +
the visible table headers so you can pin down which selector moved
without re-running interactively.

The output shape is a flat list of dicts:
    [
        {
            "name": "Apple Inc",
            "ticker": "AAPL",
            "isin": "US0378331005",
            "sector": "Information Technology",
            "industry": "Technology Hardware, Storage & Peripherals",
        },
        ...
    ]
Every field except `name` is allowed to be empty — Leonteq sometimes
lists exotic underlyings without a clean GICS/ICB classification.

Reconciliation to `company` rows happens in `LeonteqTemplate.refresh()`,
not here — this module is purely the scrape.
"""
from __future__ import annotations

import logging
import time
from typing import Callable

from playwright.sync_api import (
    Browser,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

_log = logging.getLogger(__name__)

URL = "https://structuredproducts-ch.leonteq.com/services/underlyings"

# Maximum wait for the equity table to render after navigation. Leonteq's
# SPA is slow to bootstrap from cold cache; 60s gives us plenty of slack.
_PAGE_LOAD_TIMEOUT_MS = 60_000

# Wait between scroll-to-bottom passes when chasing lazy-loaded rows.
_SCROLL_PAUSE_MS = 800

# Hard cap on scroll passes so a misbehaving page can't lock the scraper.
_MAX_SCROLL_PASSES = 50

# Hard cap on pagination clicks. Leonteq has ~57 pages × ~30 rows = ~1700
# equities; cap at 200 pages so a navigation bug can't loop forever.
_MAX_PAGES = 200

# Minimum row count to consider the scrape valid. Leonteq's underlyings
# listing has ~1700 equities (≈34 pages × 50 rows once we switch the
# dropdown to "50 rows"); if we end with fewer than this, pagination
# almost certainly went silently wrong (button matched but didn't fire,
# the filter clicked into an empty subset, or the render-race fix
# dropped pages) and we raise instead of persisting a partial universe.
_MIN_EXPECTED_ROWS = 1500

# Wait between "click next page" and re-reading the rows. Leonteq's
# table re-renders fast but isn't instant.
_PAGE_NAV_PAUSE_MS = 600

# ── Selectors ─────────────────────────────────────────────────────
# These are best-effort. Leonteq's page is a SPA with a filterable
# equity table; on first run the structure may differ from these
# assumptions and you'll see "no rows scraped". When that happens, run
# with `LEONTEQ_DEBUG=1` in the env and the scraper will dump the
# rendered page's visible structure so we can pin down the right
# selectors.

# The page typically opens on "All asset classes". We want only
# equities — try a few obvious filter button labels.
_EQUITY_FILTER_CANDIDATES = [
    "button:has-text('Equity')",
    "button:has-text('Equities')",
    "[role='tab']:has-text('Equity')",
    "[role='tab']:has-text('Equities')",
]

# Row + cell selectors inside the rendered equity table. Try a few
# common shapes — Leonteq might use a real <table> or a CSS grid of divs.
_ROW_SELECTORS = [
    "table tbody tr",
    "[role='row']:not([role='columnheader'])",
    "[class*='underlying-row']",
    "[class*='UnderlyingRow']",
]

# "Next page" button selectors. The first matching selector that's
# visible + enabled is clicked to advance the table. We cast a wide
# net because Leonteq's DOM might use any of the common patterns
# (Material-UI, AntD, raw <ul>, custom). Order matters: most specific
# / most-likely-correct candidates come first.
_NEXT_PAGE_SELECTORS = [
    # Leonteq's own `ltq-pagination-*` scheme — observed in the
    # production DOM via the diagnostic dump. `:not(.ltq-disabled)`
    # makes the selector miss when we're on the last page, so we fall
    # through to the unfiltered variant + standard disabled-check
    # below and surface a clean "reached last page" message.
    "li.ltq-pagination-next a.ltq-pagination-link:not(.ltq-disabled)",
    "li.ltq-pagination-next a:not(.ltq-disabled)",
    "li.ltq-pagination-next a",
    ".ltq-pagination-next a",
    # Explicit aria labels — strongest semantic signal.
    "button[aria-label='Next page']",
    "button[aria-label='Next']",
    "a[aria-label='Next page']",
    "a[aria-label='Next']",
    "[aria-label*='next page' i]",
    "[aria-label*='Next page']",
    # Test-id hooks (Leonteq might use these).
    "[data-testid='pagination-next']",
    "[data-testid*='next']",
    # Text content — the literal word.
    "button:has-text('Next')",
    "a:has-text('Next')",
    # Common arrow glyphs used by icon-only Next buttons.
    "button:has-text('›')",
    "button:has-text('❯')",
    "button:has-text('→')",
    "button:has-text('▶')",
    "a:has-text('›')",
    "a:has-text('❯')",
    # Class-name patterns from common pagination libraries.
    "[class*='pagination'] [class*='next']",
    "[class*='Pagination'] [class*='next']",
    "[class*='paginat'] button:last-of-type",
    "[class*='Paginat'] button:last-of-type",
    "li.next a",
    "li.next button",
    ".page-item.next a",
    "button.next-page",
    "button.next",
    "a.next",
    # Material-UI's IconButton inside a Pagination component.
    "[class*='MuiPagination'] button[aria-label*='page']:last-of-type",
    "[class*='MuiPagination'] button[aria-label*='next' i]",
]

# Optional: increase rows-per-page if Leonteq offers a selector. Bigger
# pages = fewer click-and-wait cycles.
_PAGE_SIZE_SELECTORS = [
    "select[aria-label*='page size']",
    "select[aria-label*='per page']",
    "select[name='pageSize']",
    "[class*='page-size'] select",
]

# Leonteq's structured-products underlyings page renders a legal
# disclaimer modal (`.modal-disclaimer` containing `.ltq-modal`) over
# the entire table. Until removed, every click anywhere underneath is
# intercepted by the modal's pointer-event layer — Playwright's
# `locator.click` waits 30s for actionability and times out, and
# `force=True` clicks still fail to advance the page because the
# synthesized MouseEvent never reaches the underlying button.
#
# We deliberately DO NOT click the modal's "Accept" button: it
# navigates to Leonteq's marketing homepage, losing the underlyings
# table. DOM removal of `.modal-disclaimer` + `.ltq-modal` has no
# navigation side effects and clears all the underlying clicks.

# Per-strategy click timeout. Default Playwright `.click()` waits 30s
# for actionability, which is catastrophic when something keeps
# blocking pointer events — 23 timeouts × 30s = 11+ min and the
# browser tab dies. 5s is long enough for normal user-action waits
# but tight enough that we get to the JS-click fallback fast on the
# next page if the disclaimer ever reinserts itself.
_CLICK_TIMEOUT_MS = 5_000


def _dismiss_disclaimer_modal(
    page: Page, emit: Callable[[str, int | None], None], url: str,
) -> None:
    """Remove Leonteq's legal-disclaimer modal from the DOM. We
    deliberately do NOT click the "Accept" button — Leonteq's button
    navigates away from the underlyings page to the marketing homepage,
    losing the table. DOM removal has no navigation side-effects: the
    modal is purely a pointer-event-intercepting overlay, and once its
    nodes are gone the underlying table behaves normally for the rest
    of the session.

    Safety net: after the JS evaluation, verify we're still on the
    underlyings URL. If something we didn't anticipate navigated us
    away, log loudly and navigate back."""
    try:
        removed = page.evaluate(
            """
            () => {
                let n = 0;
                document.querySelectorAll('.modal-disclaimer, .ltq-modal').forEach(el => { el.remove(); n++; });
                return n;
            }
            """
        )
        page.wait_for_timeout(300)
    except Exception as e:
        emit(
            f"Disclaimer-modal removal raised {type(e).__name__}: {e}; "
            f"pagination clicks will likely time out.",
            None,
        )
        return

    if removed:
        emit(f"Removed {removed} disclaimer-modal element(s) from the DOM.", None)
    else:
        emit(
            "No disclaimer modal detected (no `.modal-disclaimer` or `.ltq-modal` elements found).",
            None,
        )

    # Safety check — DOM removal shouldn't navigate, but verify in case
    # Leonteq's SPA has a MutationObserver that triggers redirect on
    # modal removal. If we somehow ended up on a different page, go
    # back to the underlyings URL.
    try:
        current_url = page.url
    except Exception:
        current_url = ""
    if current_url and "/services/underlyings" not in current_url:
        emit(
            f"Page URL changed unexpectedly during modal dismissal "
            f"({current_url!r}) — navigating back to {url!r}.",
            None,
        )
        try:
            page.goto(url, timeout=_PAGE_LOAD_TIMEOUT_MS, wait_until="domcontentloaded")
            page.wait_for_timeout(3_000)
            # The modal will re-appear after the re-navigation. Strip it
            # again. (No recursion — the second pass either removes it
            # cleanly or finds nothing.)
            page.evaluate(
                """
                () => {
                    document.querySelectorAll('.modal-disclaimer, .ltq-modal').forEach(el => el.remove());
                }
                """
            )
            page.wait_for_timeout(300)
            emit("Re-navigated and stripped the modal again.", None)
        except Exception as e:
            emit(
                f"Re-navigation to underlyings URL failed: {type(e).__name__}: {e}",
                None,
            )


def _try_click(page: Page, selectors: list[str]) -> bool:
    """Try each selector; click the first match. Returns True if any
    click landed. No-ops silently otherwise."""
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=500):
                loc.click()
                page.wait_for_timeout(500)
                return True
        except (PlaywrightTimeoutError, Exception):  # noqa: PERF203
            continue
    return False


def _try_set_page_size_max(
    page: Page, emit: Callable[[str, int | None], None],
) -> None:
    """Drive Leonteq's Vue rows-per-page dropdown. Their control isn't
    a real <select> — it's `<div class="ltq-dropdown-button">` with a
    `<span class="ltq-dropdown-label">30 rows</span>` child and an
    options panel that's portal-rendered when the button is clicked.
    Largest offered option is "50 rows", which drops the page count
    from ~57 → 34 (≈40% fewer pagination clicks)."""
    # 1) Find the rows-per-page button. We identify it by the literal
    # "rows" text in its label — the page has multiple ltq-dropdown-
    # buttons (sort, filter, etc.), so generic ".ltq-dropdown-button"
    # would match the wrong one.
    button_selectors = [
        ".ltq-dropdown-button:has(.ltq-dropdown-label:has-text('rows'))",
        ".ltq-dropdown-button:has-text('rows')",
    ]
    button_loc = None
    for bsel in button_selectors:
        try:
            cand = page.locator(bsel).first
            if cand.is_visible(timeout=400):
                button_loc = cand
                break
        except Exception:
            continue
    if button_loc is None:
        emit(
            "No rows-per-page dropdown found — pagination will use the "
            "default page size.",
            None,
        )
        return

    # 2) Click via JS to bypass any leftover modal/overlay event
    # interception (same trick used for Next-page clicks).
    try:
        page.evaluate(
            """
            () => {
                const buttons = Array.from(document.querySelectorAll('.ltq-dropdown-button'));
                const target = buttons.find(b => {
                    const label = b.querySelector('.ltq-dropdown-label');
                    return label && /rows/i.test(label.textContent || '');
                });
                if (target) target.click();
            }
            """
        )
        page.wait_for_timeout(500)
    except Exception as e:
        emit(
            f"Rows-per-page dropdown click raised {type(e).__name__}: {e}; "
            f"falling back to default page size.",
            None,
        )
        return

    # 3) Click the "50 rows" option. Leonteq renders options inside a
    # portal layer — we don't assume the option's container class
    # name, just look for any clickable element whose text is exactly
    # "50 rows" (or that contains "50" near a "rows" label).
    option_selectors = [
        "text='50 rows'",
        "text=/^50 rows?$/i",
        ".ltq-dropdown-option:has-text('50 rows')",
        ".ltq-dropdown-option:has-text('50')",
        "[role='option']:has-text('50 rows')",
        "[role='option']:has-text('50')",
        "li:has-text('50 rows')",
    ]
    clicked = False
    for osel in option_selectors:
        try:
            opt = page.locator(osel).first
            if not opt.is_visible(timeout=400):
                continue
            # JS click for the same pointer-event-safety reason.
            handle = opt.element_handle()
            if handle is None:
                continue
            page.evaluate("el => el.click()", handle)
            clicked = True
            emit(f"Set rows-per-page to 50 via option selector {osel!r}", None)
            break
        except Exception:
            continue

    if not clicked:
        emit(
            "Rows-per-page dropdown opened but no '50 rows' option matched "
            "any candidate selector — pagination will use the default 30/page.",
            None,
        )
        return

    # 4) Wait for the table to re-render with the larger page. The DOM
    # row count should jump from 30 → 50; poll for that as a positive
    # signal so we don't proceed before the change lands.
    elapsed = 0
    while elapsed < 5_000:
        try:
            n = page.locator("table tbody tr").count()
            if n >= 50:
                emit(f"Confirmed {n} rows on the new page size.", None)
                return
        except Exception:
            pass
        page.wait_for_timeout(150)
        elapsed += 150
    emit(
        "Rows-per-page change clicked but the table didn't grow to 50 within 5s — "
        "proceeding anyway.",
        None,
    )


def _first_row_text(page: Page, row_sel: str) -> str:
    """Inner text of the first row in the table — used as a cheap "did
    the table actually re-render after the Next click?" signal. Reading
    via Playwright's locator is faster than parsing rows; on an empty
    table returns ''."""
    try:
        return (page.locator(row_sel).first.inner_text(timeout=400) or "").strip()
    except Exception:
        return ""


def _wait_for_table_change(
    page: Page, row_sel: str, prev_first_row_text: str,
    max_wait_ms: int = 5_000, poll_ms: int = 150,
) -> bool:
    """Poll the first row's inner text until it differs from
    `prev_first_row_text`, or until `max_wait_ms` elapses. Returns True
    when the table changed (re-render landed); False on timeout.

    Solves the race between JS-click advancing the pagination indicator
    and the table re-rendering — without this wait, we sometimes
    extract the previous page's rows again. Cheaper than re-counting
    rows or hashing all of them.
    """
    elapsed = 0
    while elapsed < max_wait_ms:
        if _first_row_text(page, row_sel) != prev_first_row_text:
            return True
        page.wait_for_timeout(poll_ms)
        elapsed += poll_ms
    return False


def _current_page_number(page: Page) -> int | None:
    """Read Leonteq's currently-selected page number from the pagination
    DOM. Returns the int if found, else None. Used by `_click_next_page`
    to verify the click actually advanced the page (vs the click landing
    but Vue swallowing the event)."""
    try:
        txt = page.evaluate(
            """
            () => {
                const sel = document.querySelector(
                    '.ltq-pagination-link--selected, .ltq-pagination-link.ltq-pagination-link--selected'
                );
                return sel ? (sel.textContent || '').trim() : null;
            }
            """
        )
    except Exception:
        return None
    if txt is None:
        return None
    try:
        return int(txt)
    except (ValueError, TypeError):
        return None


def _is_disabled(loc) -> bool:
    """Cross-library disabled detection. Honors:
      - `disabled` attr presence (HTML5 boolean), with Vue's literal
        `disabled="false"` treated as NOT disabled.
      - `aria-disabled='true'`.
      - Any class containing the substring "disabled" (catches
        ltq-disabled, mui-disabled, pagination-link--disabled, etc.)."""
    try:
        disabled_attr = loc.get_attribute("disabled")
        if disabled_attr is not None and disabled_attr.strip().lower() not in ("false", "0"):
            return True
        if (loc.get_attribute("aria-disabled") or "").lower() in ("true", "1"):
            return True
        cls = (loc.get_attribute("class") or "").lower()
        if any("disabled" in c for c in cls.split()):
            return True
    except Exception:
        pass
    return False


def _click_next_page(
    page: Page, emit: Callable[[str, int | None], None],
) -> tuple[bool, str | None]:
    """Find Leonteq's "next page" control and click it, verifying the
    page actually advanced. Returns `(advanced, matched_selector)`:

      - `(True, sel)`: page advanced — we can read the new rows.
      - `(False, sel)`: selector matched but disabled (real last page)
        OR all click strategies were tried and the page did NOT advance.
        The latter is a hard failure that should crash the scrape; the
        caller distinguishes the two via the disabled-check log line.
      - `(False, None)`: no selector matched anything visible. Caller
        should dump diagnostics + crash.

    Verification: reads the currently-selected page number from the DOM
    before and after the click. If the number didn't change, the click
    was swallowed (Vue handler not firing, wrong element, etc.) and we
    try the next strategy: `force=True` click, then JS `.click()` via
    `page.evaluate`. If all three fail to advance, returns `(False, sel)`
    with an emit-logged warning so the outer loop bails."""
    before_page = _current_page_number(page)

    for sel in _NEXT_PAGE_SELECTORS:
        try:
            loc = page.locator(sel).first
            if not loc.is_visible(timeout=400):
                continue
            if _is_disabled(loc):
                emit(
                    f"Next control {sel!r} matched but is disabled "
                    f"(current page = {before_page}); treating as last page.",
                    None,
                )
                return False, sel

            # Try three click strategies in order. Each is followed by
            # a wait + page-number re-read. JS native `.click()` goes
            # FIRST because it bypasses any overlaying-modal pointer-
            # event interception (the disclaimer-modal pattern Leonteq
            # uses; even after we dismiss it once, the SPA may re-insert
            # it on rerender). Each Playwright `.click()` carries a tight
            # `_CLICK_TIMEOUT_MS` so a regression doesn't burn 30s per
            # page like it did before.
            click_strategies: list[tuple[str, Callable[[], None]]] = [
                # Vue components dispatch their own click handlers on the
                # `<a>` element. A native `el.click()` from the page's
                # JS context invokes the handler directly with no event-
                # propagation path, so a pointer-events:none overlay
                # can't absorb it. Fast and reliable.
                ("page.evaluate JS click", lambda: page.evaluate(
                    f"() => {{ const el = document.querySelector({sel!r}); "
                    f"if (el) el.click(); }}"
                )),
                ("locator.click(force=True)", lambda: loc.click(force=True, timeout=_CLICK_TIMEOUT_MS)),
                ("locator.click", lambda: loc.click(timeout=_CLICK_TIMEOUT_MS)),
            ]

            advanced = False
            for strategy_name, do_click in click_strategies:
                try:
                    do_click()
                except Exception as e:
                    emit(
                        f"Click strategy {strategy_name!r} on {sel!r} threw "
                        f"{type(e).__name__}: {e}",
                        None,
                    )
                    continue
                page.wait_for_timeout(_PAGE_NAV_PAUSE_MS)
                after_page = _current_page_number(page)
                if after_page is not None and (before_page is None or after_page > before_page):
                    return True, f"{sel} via {strategy_name}"
                emit(
                    f"Click strategy {strategy_name!r} on {sel!r} did not "
                    f"advance the page (before={before_page}, after={after_page}) — "
                    f"trying next strategy.",
                    None,
                )

            if not advanced:
                emit(
                    f"All click strategies on {sel!r} failed to advance the page. "
                    f"Trying next selector candidate.",
                    None,
                )
                continue
        except (PlaywrightTimeoutError, Exception):  # noqa: PERF203
            continue
    # No selector advanced the page.
    return False, None


def _dump_pagination_diagnostic(page: Page, emit: Callable[[str, int | None], None]) -> None:
    """When `_click_next_page` returns `(False, None)`, none of our
    selectors matched anything visible. Dump the outer HTML of every
    element that looks pagination-shaped so we can pattern-match
    Leonteq's actual DOM and pick a working selector. The output goes
    through `emit` so it lands in the schedule run-detail's verbose
    log — same place the user reads scrape errors."""
    try:
        # Heuristic: look for any element whose class or aria-label
        # hints at pagination. Trim each to ~400 chars so we don't
        # flood the log.
        diag_js = """
        () => {
            const out = [];
            const sel = [
                "[class*='paginat' i]",
                "[class*='Paginat']",
                "[aria-label*='paginat' i]",
                "[aria-label*='page' i]",
                "[data-testid*='paginat' i]",
                "[data-testid*='page' i]",
                "nav",
            ];
            const seen = new Set();
            for (const s of sel) {
                document.querySelectorAll(s).forEach(el => {
                    if (seen.has(el)) return;
                    seen.add(el);
                    out.push(el.outerHTML.slice(0, 400));
                });
            }
            return out.slice(0, 20);
        }
        """
        hits = page.evaluate(diag_js) or []
    except Exception as e:
        emit(f"Pagination diagnostic failed: {type(e).__name__}: {e}", None)
        return
    if not hits:
        emit("Pagination diagnostic: NO pagination-shaped elements found on the page.", None)
        return
    emit(f"Pagination diagnostic: {len(hits)} candidate elements (showing first ~400 chars each):", None)
    for i, html in enumerate(hits):
        emit(f"  [{i}] {html}", None)


def _find_rows_locator(page: Page):
    """Locate the table-row container that holds equity entries. Tries
    each candidate selector and returns the first one that finds rows."""
    for sel in _ROW_SELECTORS:
        loc = page.locator(sel)
        try:
            if loc.count() > 0:
                return loc, sel
        except Exception:
            continue
    return None, None


def _scroll_to_load_all(page: Page) -> None:
    """Leonteq's listing might lazy-load on scroll. Repeatedly scroll
    to the bottom of the table container until row count stops
    growing."""
    prev_count = -1
    for _ in range(_MAX_SCROLL_PASSES):
        loc, _ = _find_rows_locator(page)
        if loc is None:
            return
        count = loc.count()
        if count == prev_count:
            return  # No new rows landed; assume fully loaded.
        prev_count = count
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            pass
        page.wait_for_timeout(_SCROLL_PAUSE_MS)


def _extract_row(
    page: Page, row_idx: int, row_sel: str,
) -> dict[str, str | None] | None:
    """Pull (name, ticker, isin, sector, industry) out of a single row.
    Best-effort — picks the cell whose header matches each field, or
    falls back to positional reads when headers are unavailable."""
    try:
        row = page.locator(row_sel).nth(row_idx)
        # Pull every visible cell's text as a fallback row dump.
        cells = row.locator("td, [role='cell']")
        n = cells.count()
        if n == 0:
            return None
        texts = [(cells.nth(i).inner_text() or "").strip() for i in range(n)]
        # When we can read header labels, pair texts to headers; else
        # rely on a positional guess. The header read is unreliable on
        # CSS-grid layouts, so guard it.
        headers: list[str] = []
        try:
            head = page.locator("table thead th, [role='columnheader']")
            for i in range(head.count()):
                headers.append((head.nth(i).inner_text() or "").strip().lower())
        except Exception:
            headers = []
        keyed: dict[str, str | None] = {}
        if len(headers) == len(texts) and headers:
            for h, t in zip(headers, texts):
                keyed[h] = t
        else:
            # Positional fallback: name, ticker, isin, sector, industry
            for h, t in zip(["name", "ticker", "isin", "sector", "industry"], texts):
                keyed[h] = t
        def _pick(*aliases: str) -> str | None:
            for a in aliases:
                for k, v in keyed.items():
                    if a in k:
                        return v or None
            return None
        return {
            "name": _pick("name", "underlying") or texts[0] if texts else None,
            "ticker": _pick("ticker", "symbol"),
            "isin": _pick("isin"),
            "sector": _pick("sector"),
            "industry": _pick("industry", "sub-sector", "subsector"),
        }
    except Exception as e:
        _log.warning("[leonteq.scrape] row=%s extract failed: %s", row_idx, e)
        return None


def scrape_underlyings(
    on_progress: Callable[[str, int | None], None] | None = None,
) -> list[dict[str, str | None]]:
    """Headless-Chromium scrape of the Leonteq underlyings table.

    Walks every paginated page until the "next" control is missing
    or disabled (or until we've extracted a duplicate page — a defensive
    guard against pagination that silently no-ops at the end).
    Deduplicates by `(name, ticker, isin)` so any overlap between pages
    is harmless.

    Returns a list of dict rows (see module docstring). Emits progress
    via `on_progress(message, pct)` if provided.

    Failure modes (each logged + returned as empty/partial list, NOT
    raised):
      * Page navigation timeout — Leonteq's site is down or blocked us.
      * No rows located — our DOM selectors don't match (see comment
        on `_ROW_SELECTORS`). Logs the page title for triage.
    """
    def emit(msg: str, pct: int | None = None) -> None:
        _log.info("[leonteq.scrape] %s", msg)
        if on_progress is not None:
            try:
                on_progress(msg, pct)
            except Exception:
                pass

    rows: list[dict[str, str | None]] = []
    seen: set[tuple[str, str, str]] = set()
    emit(f"Launching headless Chromium → {URL}", 2)
    with sync_playwright() as pw:
        browser: Browser = pw.chromium.launch(headless=True)
        try:
            ctx = browser.new_context(
                viewport={"width": 1600, "height": 1000},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/146.0.0.0 Safari/537.36"
                ),
            )
            page = ctx.new_page()
            try:
                page.goto(URL, timeout=_PAGE_LOAD_TIMEOUT_MS, wait_until="domcontentloaded")
            except PlaywrightTimeoutError:
                emit("Page navigation timed out — aborting", None)
                return []

            emit("Page loaded — waiting for SPA to render the equity table", 6)
            page.wait_for_timeout(5_000)

            emit("Dismissing legal-disclaimer modal (blocks all clicks until cleared)", 7)
            _dismiss_disclaimer_modal(page, emit, URL)

            emit("Trying to filter to equities only", 9)
            _try_click(page, _EQUITY_FILTER_CANDIDATES)
            page.wait_for_timeout(2_000)

            emit("Looking for a 'rows per page' control to maximize page size", 12)
            _try_set_page_size_max(page, emit)

            # First page locator probe — if selectors don't match, fail
            # fast with a clear message.
            loc, row_sel = _find_rows_locator(page)
            if loc is None or row_sel is None:
                # The page might still be loading — give it one more
                # nudge with a scroll + retry.
                _scroll_to_load_all(page)
                loc, row_sel = _find_rows_locator(page)
            if loc is None or row_sel is None:
                title = ""
                try:
                    title = page.title()
                except Exception:
                    pass
                emit(
                    f"No equity rows found — selectors need tuning. Page title: {title!r}",
                    None,
                )
                return []

            emit(f"Found table rows via selector {row_sel!r} — starting pagination", 15)

            # Track consecutive 0-added pages. Leonteq's table sometimes
            # re-renders late (the pagination indicator advances but the
            # rows lag a beat) — when that race hits we mistakenly read
            # the prior page's rows and count 0 new. A single occurrence
            # used to terminate the loop, capping the scrape at the
            # first stale page. Now we allow up to N before bailing,
            # and only bail via the count gate (the actual end-of-
            # pagination signal is the disabled-attr check on Next).
            zero_added_streak = 0
            _ZERO_ADDED_TOLERANCE = 3

            for page_idx in range(_MAX_PAGES):
                # Re-locate after navigation (the DOM is replaced on
                # every page click).
                loc = page.locator(row_sel)
                total = loc.count()
                if total == 0:
                    emit(f"Page {page_idx + 1}: 0 rows; stopping", None)
                    break

                # Track how many NEW rows this page contributes so we
                # can detect "click next did nothing" (same page served
                # twice). Pagination libs sometimes silently no-op on
                # the last page instead of disabling the button.
                added = 0
                for i in range(total):
                    row = _extract_row(page, i, row_sel)
                    if not row or not row.get("name"):
                        continue
                    key = (
                        (row.get("name") or "").strip(),
                        (row.get("ticker") or "").strip(),
                        (row.get("isin") or "").strip(),
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append(row)
                    added += 1

                # Coarse pct: assume up to ~60 pages. The bar climbs
                # 15→95 across the pagination loop.
                pct = 15 + min(80, int((page_idx + 1) * 80 / 60))
                emit(
                    f"Page {page_idx + 1}: +{added} new (total {len(rows)})",
                    pct,
                )

                if added == 0 and page_idx > 0:
                    zero_added_streak += 1
                    if zero_added_streak >= _ZERO_ADDED_TOLERANCE:
                        emit(
                            f"No new rows for {zero_added_streak} consecutive pages — "
                            f"assuming pagination has wrapped or stalled, stopping.",
                            None,
                        )
                        break
                    emit(
                        f"Page {page_idx + 1} contributed 0 new rows (streak={zero_added_streak}/"
                        f"{_ZERO_ADDED_TOLERANCE}) — likely a render race, retrying after Next.",
                        None,
                    )
                else:
                    zero_added_streak = 0

                # Snapshot the table's first-row text BEFORE clicking
                # Next. After clicking we wait for the first row to
                # actually change before extracting — without this wait,
                # the JS click advances the pagination indicator faster
                # than Leonteq's SPA re-renders the rows, and we end up
                # re-reading the previous page's data. _click_next_page
                # also waits _PAGE_NAV_PAUSE_MS internally, but that
                # fixed delay is sometimes insufficient on slow loads.
                pre_click_first_row = _first_row_text(page, row_sel)

                clicked, matched_sel = _click_next_page(page, emit)
                if clicked:
                    # Active wait for the table re-render. If it doesn't
                    # land in 5s, fall through anyway — the next loop's
                    # extraction will see whatever is current and the
                    # 0-added tolerance above handles repeat-pages.
                    if not _wait_for_table_change(page, row_sel, pre_click_first_row):
                        emit(
                            f"Table did not visibly change within 5s after Next click on page "
                            f"{page_idx + 1} — proceeding with whatever is rendered.",
                            None,
                        )
                if not clicked:
                    if matched_sel is None:
                        # No selector matched anything visible. Dump
                        # the page's pagination-shaped elements + raise
                        # so the caller surfaces a hard error instead of
                        # quietly persisting a 30-row partial universe.
                        emit(
                            "No 'next' control matched any of our candidate selectors — "
                            "dumping pagination diagnostics:",
                            None,
                        )
                        _dump_pagination_diagnostic(page, emit)
                        raise RuntimeError(
                            f"Leonteq scrape aborted: no 'next' selector matched on page "
                            f"{page_idx + 1} of {len(rows)} rows. See diagnostic above to "
                            f"add a matching selector."
                        )
                    # Selector matched but didn't advance: either the
                    # real last page (disabled control), OR all three
                    # click strategies failed. _click_next_page already
                    # emitted the distinguishing log lines; downstream
                    # the row-count sanity check catches the silent-fail
                    # case.
                    emit(
                        f"Pagination loop exited at page {page_idx + 1} (matched={matched_sel!r}).",
                        None,
                    )
                    break
        finally:
            browser.close()
    pages_scraped = (page_idx + 1) if rows else 0
    emit(f"Scrape complete: {len(rows)} equities across {pages_scraped} pages", 100)

    # Sanity check. Leonteq's underlyings listing has ~1700 equities
    # today; if we ended with <500, something silently went wrong (most
    # likely pagination didn't actually advance and we read page 1
    # repeatedly, or the filter clicked into an empty asset class).
    # Raise loudly with the row count + page count so the caller surfaces
    # a hard error instead of persisting a broken universe.
    if len(rows) < _MIN_EXPECTED_ROWS:
        raise RuntimeError(
            f"Leonteq scrape returned only {len(rows)} equities across "
            f"{pages_scraped} pages — expected >= {_MIN_EXPECTED_ROWS}. "
            f"Pagination likely failed silently; check the verbose log "
            f"for 'did not advance the page' or 'click strategy ... threw' "
            f"warnings, or page-1 row count being identical across iterations."
        )
    return rows
