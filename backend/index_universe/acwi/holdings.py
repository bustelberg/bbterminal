"""iShares MSCI ACWI ETF fund XML parser.

The fund publishes its holdings as an XML Spreadsheet 2003 file —
`iShares-MSCI-ACWI-ETF_fund.xls` next to this module. This file is the
sole source of "what's currently in ACWI"; historical reconstruction
combines it with MSCI announcement events.
"""
from __future__ import annotations

import os

from lxml import etree


# Data files live in index_universe/ (the parent of this package) — not
# moved into the acwi/ package so the git history of the JSON / XLS files
# stays intact.
_DATA_DIR = os.path.dirname(os.path.dirname(__file__))
_FILE = os.path.join(_DATA_DIR, "iShares-MSCI-ACWI-ETF_fund.xls")
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
