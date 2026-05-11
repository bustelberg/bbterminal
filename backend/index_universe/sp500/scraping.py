"""Scrape the S&P 500 current constituents + change history from Wikipedia.

The Wikipedia article has two `<table class="wikitable">` blocks: the
first lists current members (with sector + headquarters), the second is
a change log dating back to ~1998. We parse both and return:
  - the set of current tickers
  - the change list (newest-first) of `{date, added, removed}` events
  - a `{ticker: {name, sector, country}}` map for current members
    used downstream to enrich existing/missing company rows."""
from __future__ import annotations

from datetime import date, datetime
from html.parser import HTMLParser

import requests


_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


class _WikiTableParser(HTMLParser):
    """Extract rows from all <table class='wikitable'> elements."""

    def __init__(self):
        super().__init__()
        self.tables: list[dict] = []
        self.in_table = False
        self.in_cell = False
        self.in_header = False
        self.current_row: list[str] = []
        self._headers: list[str] = []
        self._rows: list[list[str]] = []
        self._cell_text = ""

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "table" and "wikitable" in attrs_dict.get("class", ""):
            self.in_table = True
            self._headers = []
            self._rows = []
        elif self.in_table:
            if tag == "tr":
                self.current_row = []
            elif tag in ("td", "th"):
                self.in_cell = True
                self.in_header = tag == "th"
                self._cell_text = ""

    def handle_endtag(self, tag):
        if tag == "table" and self.in_table:
            self.tables.append({"headers": self._headers[:], "rows": self._rows[:]})
            self.in_table = False
        elif self.in_table:
            if tag in ("td", "th") and self.in_cell:
                text = self._cell_text.strip()
                if self.in_header:
                    self._headers.append(text)
                else:
                    self.current_row.append(text)
                self.in_cell = False
            elif tag == "tr" and self.current_row:
                self._rows.append(self.current_row)

    def handle_data(self, data):
        if self.in_cell:
            self._cell_text += data


def _parse_change_date(raw: str) -> date | None:
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _month_key(d: date) -> str:
    return d.strftime("%Y-%m")


def scrape_sp500() -> tuple[set[str], list[dict], dict[str, dict]]:
    """Scrape Wikipedia for current constituents and historical changes.

    Returns (current_tickers, changes, company_info) where:
    - changes is a list of {"date": date, "added": str|None, "removed": str|None}
    - company_info is {ticker: {"name": str, "sector": str, "country": str}}
      extracted from the current constituents table (headquarters → country).
    """
    resp = requests.get(_WIKI_URL, headers={"User-Agent": "bbterminal/1.0"})
    resp.raise_for_status()

    parser = _WikiTableParser()
    parser.feed(resp.text)

    if len(parser.tables) < 2:
        raise ValueError("Expected at least 2 wikitable tables on the Wikipedia page")

    # Table 1: current constituents
    # Columns: Symbol, Security, GICS Sector, GICS Sub-Industry, Headquarters Location, ...
    current: set[str] = set()
    company_info: dict[str, dict] = {}
    for row in parser.tables[0]["rows"]:
        if row:
            ticker = row[0].strip()
            current.add(ticker)
            name = row[1].strip() if len(row) > 1 else ""
            sector = row[2].strip() if len(row) > 2 else ""
            hq = row[4].strip() if len(row) > 4 else ""
            # Extract country from headquarters (e.g. "Cupertino, California" → "United States")
            # Wikipedia HQ is typically "City, State" for US companies
            country = "United States" if hq else ""
            company_info[ticker] = {"name": name, "sector": sector, "country": country}

    # Table 2: changes — Date | Added Ticker | Added Name | Removed Ticker | Removed Name | Reason
    changes: list[dict] = []
    for row in parser.tables[1]["rows"]:
        if len(row) < 5:
            continue
        d = _parse_change_date(row[0])
        if not d:
            continue
        added = row[1].strip() or None
        removed = row[3].strip() or None
        changes.append({"date": d, "added": added, "removed": removed})

    changes.sort(key=lambda c: c["date"], reverse=True)
    return current, changes, company_info
