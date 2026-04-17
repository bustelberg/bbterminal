"""Parse iShares MSCI ACWI ETF holdings and scrape MSCI announcements."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from html.parser import HTMLParser

import requests
from lxml import etree

log = logging.getLogger(__name__)

_FILE = os.path.join(os.path.dirname(__file__), "iShares-MSCI-ACWI-ETF_fund.xls")
_NS = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}


def _parse_xml_spreadsheet(path: str) -> list[list[str]]:
    """Parse the XML Spreadsheet format into a list of rows (list of cell strings)."""
    with open(path, "rb") as f:
        raw = f.read()
    # Strip BOM(s)
    while raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    tree = etree.fromstring(raw, parser=etree.XMLParser(recover=True))
    # Find the "Holdings" worksheet
    for sheet in tree.findall(".//ss:Worksheet", _NS):
        name = sheet.get("{urn:schemas-microsoft-com:office:spreadsheet}Name")
        if name == "Holdings":
            rows = []
            for row_el in sheet.findall(".//ss:Row", _NS):
                cells = []
                for cell in row_el.findall("ss:Cell", _NS):
                    data = cell.find("ss:Data", _NS)
                    cells.append(data.text.strip() if data is not None and data.text else "")
                rows.append(cells)
            return rows
    raise ValueError("No 'Holdings' worksheet found in the file")


def load_acwi_holdings() -> tuple[list[dict], str]:
    """Load ACWI holdings as a list of dicts plus the as-of date string.

    Returns (holdings, as_of_date). Only equity rows are included.
    """
    rows = _parse_xml_spreadsheet(_FILE)

    # Row 0 contains the as-of date (e.g. "15-Apr-2026")
    as_of = rows[0][0] if rows and rows[0] else ""

    # Find the header row (starts with "Ticker")
    header_idx = None
    for i, row in enumerate(rows):
        if row and row[0] == "Ticker":
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("Could not find header row in Holdings sheet")

    headers = rows[header_idx]
    holdings = []
    for row in rows[header_idx + 1 :]:
        if len(row) < len(headers):
            row += [""] * (len(headers) - len(row))
        record = dict(zip(headers, row))
        # Skip non-equity / empty rows
        if not record.get("Ticker") or record.get("Asset Class") != "Equity":
            continue
        holdings.append(record)

    return holdings, as_of


# ---------------------------------------------------------------------------
# MSCI Announcements scraper
# ---------------------------------------------------------------------------

_MSCI_ANN_URL = (
    "https://app2.msci.com/webapp/index_ann/Announcement"
    "?doc_type=ANNOUNCEMENT&lang=en&prod_type=STANDARD&visibility=public&format=html"
    "&date_range=0"
)
_MSCI_BASE = "https://app2.msci.com/webapp/index_ann/"


class _MsciAnnouncementParser(HTMLParser):
    """Extract rows from the MSCI announcements table."""

    def __init__(self):
        super().__init__()
        self.rows: list[dict] = []
        self.in_results_row = False
        self.in_td = False
        self.in_a = False
        self._cells: list[str] = []
        self._cell_text = ""
        self._href: str | None = None

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "tr" and "results-row" in (attrs_dict.get("class") or ""):
            self.in_results_row = True
            self._cells = []
            self._href = None
        elif self.in_results_row and tag == "td":
            self.in_td = True
            self._cell_text = ""
        elif self.in_results_row and tag == "a":
            self.in_a = True
            href = attrs_dict.get("href", "")
            if href and not href.startswith("http"):
                href = _MSCI_BASE + href
            self._href = href

    def handle_endtag(self, tag):
        if tag == "td" and self.in_td:
            self._cells.append(self._cell_text.strip())
            self.in_td = False
        elif tag == "a":
            self.in_a = False
        elif tag == "tr" and self.in_results_row:
            self.in_results_row = False
            if len(self._cells) >= 2 and self._cells[0] != "Date":
                self.rows.append({
                    "date": self._cells[0],
                    "title": self._cells[1],
                    "href": self._href or "",
                })

    def handle_data(self, data):
        if self.in_td:
            self._cell_text += data


_CACHE_FILE = os.path.join(os.path.dirname(__file__), "msci_announcements_cache.json")
_CACHE_MAX_AGE = 24 * 60 * 60  # 24 hours

# Matches "XX: COMPANY NAME" pattern — constituent changes
_CONSTITUENT_RE = re.compile(r"^[A-Z]{2}: .+")
# Phrases that indicate a non-constituent announcement
_EXCLUDE_PHRASES = ["UPDATE", "TO THE", "IN THE", "INITIAL PUBLIC OFFERING", "INDEX REVIEW"]


def _scrape_msci_announcements() -> list[dict]:
    """Scrape the MSCI index announcements page. Returns list of {date, title, href}."""
    resp = requests.get(_MSCI_ANN_URL, headers={"User-Agent": "bbterminal/1.0"}, timeout=30)
    resp.raise_for_status()
    parser = _MsciAnnouncementParser()
    parser.feed(resp.text)
    return parser.rows


def _load_cache() -> list[dict] | None:
    """Load cached announcements if the cache file exists and is fresh."""
    try:
        if not os.path.exists(_CACHE_FILE):
            return None
        age = time.time() - os.path.getmtime(_CACHE_FILE)
        if age > _CACHE_MAX_AGE:
            return None
        with open(_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_cache(rows: list[dict]) -> None:
    try:
        with open(_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False)
    except Exception as e:
        log.warning("Failed to save MSCI announcements cache: %s", e)


def get_msci_announcements(force_refresh: bool = False) -> list[dict]:
    """Get MSCI announcements, using a local cache (24h TTL).

    Each row has: date, title, href, is_constituent_change, is_other_country_coded.
    Constituent changes also get a `detail` field from the detail cache if available.
    """
    if not force_refresh:
        cached = _load_cache()
        if cached is not None:
            rows = cached
        else:
            rows = None
    else:
        rows = None

    if rows is None:
        rows = _scrape_msci_announcements()
        for row in rows:
            title = row["title"].upper()
            has_country_code = bool(_CONSTITUENT_RE.match(row["title"]))
            has_exclude = any(p in title for p in _EXCLUDE_PHRASES)
            row["is_constituent_change"] = has_country_code and not has_exclude
            row["is_other_country_coded"] = has_country_code and has_exclude
        _save_cache(rows)

    # Attach cached details to constituent changes
    detail_cache = _load_detail_cache()
    for row in rows:
        if row["is_constituent_change"] and row["href"] in detail_cache:
            row["detail"] = detail_cache[row["href"]]
        else:
            row.pop("detail", None)

    return rows


# ---------------------------------------------------------------------------
# Announcement detail parser
# ---------------------------------------------------------------------------

class _AnnouncementContentParser(HTMLParser):
    """Extract text from all <pre class="announcementContent"> tags."""

    def __init__(self):
        super().__init__()
        self.in_pre = False
        self.blocks: list[str] = []
        self._current = ""

    def handle_starttag(self, tag, attrs):
        if tag == "pre" and "announcementContent" in (dict(attrs).get("class") or ""):
            self.in_pre = True
            self._current = ""

    def handle_endtag(self, tag):
        if tag == "pre" and self.in_pre:
            self.blocks.append(self._current)
            self.in_pre = False

    def handle_data(self, data):
        if self.in_pre:
            self._current += data


# New format: structured key-value lines
_STANDARD_RE = re.compile(r"^STANDARD\s+(\S+)", re.MULTILINE)
_EFFECTIVE_DATE_RE = re.compile(r"^EFFECTIVE DATE\s+(.+)", re.MULTILINE)

# Old format: "MSCI STANDARD INDEX" section with Additions/Deletions
_OLD_STANDARD_SECTION_RE = re.compile(
    r"MSCI STANDARD INDEX\s*\n+"
    r"(?:\s*\n)*Additions:\s*\n(.*?)\n"
    r"(?:\s*\n)*Deletions:\s*\n(.*?)(?:\n\s*\n|\Z)",
    re.DOTALL,
)
# Old format: effective date in prose "as of the close of <date>" (may span lines)
_OLD_EFFECTIVE_RE = re.compile(
    r"as of the close\s+of\s+(\w+\s+\d{1,2}\s*,?\s*\d{4})",
    re.IGNORECASE | re.DOTALL,
)


def _parse_old_format(text: str) -> tuple[str | None, str | None]:
    """Parse the old announcement format with MSCI STANDARD INDEX sections."""
    standard = None
    effective_date = None

    m = _OLD_STANDARD_SECTION_RE.search(text)
    if m:
        additions = m.group(1).strip()
        deletions = m.group(2).strip()
        has_additions = additions and additions.lower() != "none"
        has_deletions = deletions and deletions.lower() != "none"
        if has_additions and has_deletions:
            standard = "ADDED+DELETED"
        elif has_additions:
            standard = "ADDED"
        elif has_deletions:
            standard = "DELETED"
        else:
            standard = "-"

    m = _OLD_EFFECTIVE_RE.search(text)
    if m:
        effective_date = m.group(1).strip()

    return standard, effective_date


def fetch_announcement_detail(url: str) -> dict:
    """Fetch an individual MSCI announcement and extract STANDARD action and EFFECTIVE DATE.

    Handles both the new format (structured key-value) and old format
    (MSCI STANDARD INDEX with Additions/Deletions sections).
    """
    resp = requests.get(url, headers={"User-Agent": "bbterminal/1.0"}, timeout=15)
    resp.raise_for_status()
    parser = _AnnouncementContentParser()
    parser.feed(resp.text)

    # Try each <pre> block — use the first one that yields results
    text = "\n".join(parser.blocks)

    standard = None
    effective_date = None

    # Try new format first
    m = _STANDARD_RE.search(text)
    if m:
        standard = m.group(1)

    m = _EFFECTIVE_DATE_RE.search(text)
    if m:
        effective_date = m.group(1).strip()

    # Fall back to old format if new format didn't find anything
    if standard is None and effective_date is None:
        standard, effective_date = _parse_old_format(text)

    # Don't report an effective date if we couldn't determine the action —
    # the date was likely picked up from unrelated prose.
    if standard is None:
        effective_date = None

    return {"standard": standard, "effective_date": effective_date}


# ---------------------------------------------------------------------------
# Detail cache — persists fetched details keyed by URL
# ---------------------------------------------------------------------------

_DETAIL_CACHE_FILE = os.path.join(os.path.dirname(__file__), "msci_details_cache.json")


def _load_detail_cache() -> dict[str, dict]:
    try:
        if os.path.exists(_DETAIL_CACHE_FILE):
            with open(_DETAIL_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_detail_cache(cache: dict[str, dict]) -> None:
    try:
        with open(_DETAIL_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False)
    except Exception as e:
        log.warning("Failed to save MSCI details cache: %s", e)


def fetch_announcement_detail_cached(url: str) -> dict:
    """Like fetch_announcement_detail but reads/writes a local cache."""
    cache = _load_detail_cache()
    if url in cache:
        return cache[url]
    detail = fetch_announcement_detail(url)
    cache[url] = detail
    _save_detail_cache(cache)
    return detail


def fetch_bulk_details(urls: list[str]) -> dict[str, dict]:
    """Fetch details for multiple URLs, using cache where possible.

    Returns {url: {standard, effective_date}} for each URL.
    """
    cache = _load_detail_cache()
    results: dict[str, dict] = {}
    to_fetch: list[str] = []

    for url in urls:
        if url in cache:
            results[url] = cache[url]
        else:
            to_fetch.append(url)

    for url in to_fetch:
        try:
            detail = fetch_announcement_detail(url)
        except Exception as e:
            log.warning("Failed to fetch detail for %s: %s", url, e)
            detail = {"standard": None, "effective_date": None, "error": str(e)}
        cache[url] = detail
        results[url] = detail

    if to_fetch:
        _save_detail_cache(cache)

    return results
