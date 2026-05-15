"""MSCI announcements list scraper + 24h list cache.

Hits the MSCI public announcements page, extracts each results row's
(date, title, href), classifies titles into constituent-change vs
other-coded vs everything else, and stitches in any per-URL detail
data the detail cache already has."""
from __future__ import annotations

import json
import logging
import os
import re
import time
from html.parser import HTMLParser

import requests

from .announcement_detail import _load_detail_cache


log = logging.getLogger(__name__)

# JSON cache files stay in index_universe/ (parent of this package).
_DATA_DIR = os.path.dirname(os.path.dirname(__file__))

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


_CACHE_FILE = os.path.join(_DATA_DIR, "msci_announcements_cache.json")
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
