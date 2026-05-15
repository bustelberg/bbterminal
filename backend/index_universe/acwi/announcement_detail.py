"""Per-URL MSCI announcement detail fetcher.

For each announcement URL the page contains one or more
`<pre class="announcementContent">` blocks. Two formats live in the
archive:

 - "New" format: structured key-value lines, including `STANDARD <action>`
   and `EFFECTIVE DATE <date>`.
 - "Old" format: an `MSCI STANDARD INDEX` section with `Additions:` and
   `Deletions:` lists plus prose "as of the close of <date>".

We try the new format first, fall back to the old, and persist the
parsed result keyed by URL so the next call short-circuits without
re-fetching. Bulk fetch caches all hits and only goes to the network
for misses."""
from __future__ import annotations

import json
import logging
import os
import re
from html.parser import HTMLParser

import requests


log = logging.getLogger(__name__)

# JSON cache files stay in index_universe/ (parent of this package).
_DATA_DIR = os.path.dirname(os.path.dirname(__file__))
_DETAIL_CACHE_FILE = os.path.join(_DATA_DIR, "msci_details_cache.json")


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
