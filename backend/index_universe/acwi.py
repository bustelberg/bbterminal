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
# GuruFocus URL builder
# ---------------------------------------------------------------------------

_US_EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "CBOE BZX"}

# Map iShares exchange names → GuruFocus exchange prefixes
_ISHARES_TO_GF: dict[str, str] = {
    # US — no prefix needed
    "NYSE": "",
    "NASDAQ": "",
    "Cboe BZX": "",
    # Europe
    "London Stock Exchange": "LSE",
    "Xetra": "XTER",
    "Nyse Euronext - Euronext Paris": "XPAR",
    "Nyse Euronext - Euronext Amsterdam": "XAMS",
    "Euronext Amsterdam": "XAMS",
    "Nyse Euronext - Euronext Brussels": "XBRU",
    "Nyse Euronext - Euronext Lisbon": "XLIS",
    "Borsa Italiana": "MIL",
    "Bolsa De Madrid": "XMAD",
    "SIX Swiss Exchange": "XSWX",
    "Nasdaq Omx Nordic": "OSTO",
    "Omx Nordic Exchange Copenhagen A/S": "OCSE",
    "Oslo Bors Asa": "OSL",
    "Nasdaq Omx Helsinki Ltd.": "OHEL",
    "Warsaw Stock Exchange/Equities/Main Market": "WAR",
    "Wiener Boerse Ag": "XPRA",
    "Athens Exchange S.A. Cash Market": "ATH",
    "Irish Stock Exchange - All Market": "ISE",
    "Budapest Stock Exchange": "BDP",
    "Prague Stock Exchange": "PRA",
    "Istanbul Stock Exchange": "IST",
    # Americas
    "Toronto Stock Exchange": "TSX",
    "Bolsa Mexicana De Valores": "MEX",
    "XBSP": "BSP",
    "Santiago Stock Exchange": "SGO",
    "Bolsa De Valores De Colombia": "BOG",
    # Asia-Pacific
    "Tokyo Stock Exchange": "TSE",
    "Hong Kong Exchanges And Clearing Ltd": "HKSE",
    "Shanghai Stock Exchange": "SSE",
    "Shenzhen Stock Exchange": "SZSE",
    "Taiwan Stock Exchange": "TWSE",
    "Gretai Securities Market": "GTSM",
    "Korea Exchange (Stock Market)": "XKRX",
    "Korea Exchange (Kosdaq)": "XKRX",
    "National Stock Exchange Of India": "NSE",
    "Bse Ltd": "BSE",
    "Asx - All Markets": "ASX",
    "New Zealand Exchange Ltd": "NZSE",
    "Singapore Exchange": "SGX",
    "Bursa Malaysia": "KLSE",
    "Indonesia Stock Exchange": "IDX",
    "Stock Exchange Of Thailand": "SET",
    "Philippine Stock Exchange Inc.": "PSE",
    # Middle East / Africa
    "Saudi Stock Exchange": "TADAWUL",
    "Abu Dhabi Securities Exchange": "ADX",
    "Dubai Financial Market": "DFM",
    "Qatar Exchange": "QSE",
    "Kuwait Stock Exchange": "KSE",
    "Tel Aviv Stock Exchange": "TASE",
    "Johannesburg Stock Exchange": "JSE",
    "Egyptian Exchange": "EGX",
    # Russia
    "Standard-Classica-Forts": "MCX",
}


def gurufocus_url(ticker: str, exchange: str) -> str | None:
    """Build a GuruFocus summary URL for a holding.

    Returns None if the exchange is unknown or the ticker is empty.
    """
    if not ticker or ticker == "--":
        return None

    gf_prefix = _ISHARES_TO_GF.get(exchange)
    if gf_prefix is None:
        return None  # unknown exchange

    # HKSE tickers must be zero-padded to 5 digits
    t = ticker
    if gf_prefix == "HKSE" and t.isdigit():
        t = t.zfill(5)

    if gf_prefix == "":
        # US stock — no prefix
        symbol = t
    else:
        symbol = f"{gf_prefix}:{t}"

    return f"https://www.gurufocus.com/stock/{symbol}/summary"


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


# ---------------------------------------------------------------------------
# Net additions with holdings matching
# ---------------------------------------------------------------------------

_CLEAN_RE = re.compile(r"\s*\([^)]*\)")
_SUFFIX_LIST = [
    " INC", " CORP", " LTD", " PLC", " CO", " SA", " AG", " NV", " SE",
    " ASA", " ADR", " PN C", " ORD", " CLASS A", " CLASS B", " CLASS C",
    " RIGHT", " CASH",
]


def _clean_name(name: str) -> str:
    """Normalize a company name for matching."""
    n = _CLEAN_RE.sub("", name).upper().strip()
    for suf in _SUFFIX_LIST:
        if n.endswith(suf):
            n = n[: -len(suf)]
    return n.strip()


_STOP_TOKENS = {
    "GROUP", "HOLDINGS", "HOLDING", "HLDGS", "HLDG",
    "FINANCIAL", "FINL", "BANK", "INSURANCE",
    "CHINA", "CORP", "INTERNATIONAL", "INTL",
    "POWER", "ENERGY", "SECURITIES", "SEC",
    "CO", "THE", "OF", "AND", "DE", "DEL",
    "ENTERTAINMENT", "INVESTMENT", "REIT",
    "ELECTRIC", "RAILWAY", "CONSTRUCTION",
    "OIL", "GAS", "NATURAL", "RESOURCES",
    "PROPERTY", "PROPERTIES", "REAL", "ESTATE",
    "INDUSTRIAL", "INDUSTRIES", "INDUSTRY",
    "CAPITAL", "PARTNERS", "GLOBAL", "NATIONAL",
    "NEW", "FIRST", "COMM", "COMMUNICATIONS",
    "H",  # share class suffix in HK listings
    "INC", "LTD", "SA", "SPA", "BHD", "TBK", "ASA",
    "CLAS", "CLASS", "PN", "ORD", "PREF", "ADR",
}


def _tokenize(name: str) -> set[str]:
    return set(re.findall(r"[A-Z0-9]+", _clean_name(name)))


def _tokenize_significant(name: str) -> set[str]:
    """Tokenize, removing common stop words and single-char tokens."""
    return {t for t in _tokenize(name) - _STOP_TOKENS if len(t) >= 2}


def _fuzzy_token_match(a: str, b: str) -> bool:
    """Check if two tokens are a fuzzy match.

    Matches if one is a prefix of the other, or the shorter one is a
    subsequence of the longer one with ≥80% of its characters matching
    AND the tokens are similar length (catches abbreviations like
    BANCSHS → BANCSHARES but not CITIC ≈ CITIZENS).
    """
    if a.startswith(b) or b.startswith(a):
        return True
    short, long = (a, b) if len(a) <= len(b) else (b, a)
    if len(short) < 4 or len(short) < len(long) * 0.65:
        return False
    # Check if short is a subsequence of long
    it = iter(long)
    matched = sum(1 for ch in short if ch in it)
    return matched >= len(short) * 0.8


_NAME_OVERRIDES_FILE = os.path.join(os.path.dirname(__file__), "name_overrides.json")


def _load_name_overrides() -> dict[str, str]:
    """Load manual name overrides: {ANNOUNCEMENT_NAME: HOLDING_NAME}."""
    try:
        with open(_NAME_OVERRIDES_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        # Normalize keys to uppercase
        return {k.upper(): v for k, v in raw.items()}
    except Exception:
        return {}


def _extract_first_company(title: str) -> str:
    """Extract the first company name from an announcement title like 'US: COMPANY, US: OTHER'."""
    m = re.match(r"^[A-Z]{2}: (.+?)(?:,\s*[A-Z]{2}:|$)", title)
    return m.group(1).strip() if m else re.sub(r"^[A-Z]{2}: ", "", title)


def compute_net_additions() -> list[dict]:
    """Compute net additions (added & not deleted) matched against current holdings.

    Returns list of dicts with: title, country, date, effective_date, href,
    matched (bool), matched_ticker, matched_name, match_method.
    """
    from collections import defaultdict
    from datetime import datetime

    anns = get_msci_announcements()
    cache = _load_detail_cache()
    holdings, _ = load_acwi_holdings()

    # Load manual overrides: maps announcement company name → holding name
    overrides = _load_name_overrides()

    # Build holdings lookup structures
    h_clean_map: dict[str, dict] = {}
    h_name_map: dict[str, dict] = {}  # exact Name → holding for override lookups
    h_token_list: list[tuple[set[str], dict]] = []
    for h in holdings:
        c = _clean_name(h["Name"])
        h_clean_map[c] = h
        h_name_map[h["Name"].upper()] = h
        h_token_list.append((_tokenize_significant(h["Name"]), h))

    def _match(ann_name: str) -> tuple[dict | None, str]:
        # Manual override (highest priority)
        override_target = overrides.get(ann_name.upper())
        if override_target:
            h = h_name_map.get(override_target.upper())
            if h:
                return h, "override"

        c = _clean_name(ann_name)
        # Exact cleaned match
        if c in h_clean_map:
            return h_clean_map[c], "exact"
        # Prefix match (either direction)
        for hc, h in h_clean_map.items():
            if len(c) >= 3 and len(hc) >= 3 and (hc.startswith(c) or c.startswith(hc)):
                return h, "prefix"
        # Token overlap — require at least 2 overlapping significant tokens and 60% score
        # Also counts fuzzy matches: one token is a prefix of or contained in another (min 4 chars)
        ann_tokens = _tokenize_significant(ann_name)
        if len(ann_tokens) < 2:
            return None, ""
        best = None
        best_score = 0.0
        best_overlap = 0
        for ht, h in h_token_list:
            if not ht:
                continue
            overlap = 0
            for at in ann_tokens:
                if at in ht:
                    overlap += 1
                elif len(at) >= 4:
                    for htok in ht:
                        if len(htok) >= 4 and _fuzzy_token_match(at, htok):
                            overlap += 1
                            break
            score = overlap / max(len(ann_tokens), len(ht))
            if score > best_score:
                best_score = score
                best_overlap = overlap
                best = h
        if best_score >= 0.6 and best_overlap >= 2:
            return best, f"token({best_score:.0%})"
        return None, ""

    # Build history per announcement title
    constituent = [a for a in anns if a.get("is_constituent_change") and a.get("href")]
    history: dict[str, list[tuple[datetime, str, dict]]] = defaultdict(list)
    for a in constituent:
        d = cache.get(a["href"])
        if not d:
            continue
        std = d.get("standard")
        if std not in ("ADDED", "DELETED"):
            continue
        try:
            ts = datetime.strptime(a["date"], "%d %b %Y")
        except Exception:
            ts = datetime.min
        history[a["title"]].append((ts, std, {**a, "detail": d}))

    results: list[dict] = []
    for title, events in history.items():
        events.sort(key=lambda x: x[0], reverse=True)
        ts, action, a = events[0]
        if action != "ADDED":
            continue

        country_m = re.match(r"^([A-Z]{2}): ", title)
        country = country_m.group(1) if country_m else ""
        company_name = _extract_first_company(title)

        h, method = _match(company_name)
        results.append({
            "title": title,
            "company_name": company_name,
            "country": country,
            "date": a["date"],
            "effective_date": (a.get("detail") or {}).get("effective_date"),
            "href": a["href"],
            "matched": h is not None,
            "matched_ticker": h["Ticker"] if h else None,
            "matched_name": h["Name"] if h else None,
            "match_method": method,
        })

    results.sort(key=lambda x: x["date"], reverse=False)
    # Sort by parsed date descending
    def _parse_dt(s: str):
        try:
            return datetime.strptime(s, "%d %b %Y")
        except Exception:
            return datetime.min
    results.sort(key=lambda x: _parse_dt(x["date"]), reverse=True)

    return results
