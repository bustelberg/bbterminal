"""Net additions: which MSCI ADDED events are still in the current fund.

For each announcement title we keep the most recent action. If that
action is ADDED, the title becomes a "net addition" candidate. We then
fuzzy-match its first company name to the loaded fund holdings — manual
overrides win, then exact-cleaned, then prefix, then significant-token
overlap with a configurable threshold. The result feeds the
historical-reconstruction loop in `reconstruction.py` (a matched
addition's effective_date is the date the listing entered ACWI)."""
from __future__ import annotations

import json
import os
import re

from .announcement_detail import _load_detail_cache
from .announcements import get_msci_announcements
from .holdings import load_acwi_holdings


# JSON config files stay in index_universe/ (parent of this package).
_DATA_DIR = os.path.dirname(os.path.dirname(__file__))


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


_NAME_OVERRIDES_FILE = os.path.join(_DATA_DIR, "name_overrides.json")


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
