"""Read the newest committed official iShares export (CSV or legacy XML XLS).

CSV source: https://www.ishares.com/us/products/239600/ishares-msci-acwi-etf/latest-holdings.csv
Membership includes every source equity, independently of local listing/price coverage.
Historical reconstruction combines the current snapshot with MSCI announcement events.
"""
from __future__ import annotations

import os
import csv
import math
from datetime import datetime

from lxml import etree


# Data files live in index_universe/ (the parent of this package) — not
# moved into the acwi/ package so the git history of the JSON / XLS files
# stays intact.
_DATA_DIR = os.path.dirname(os.path.dirname(__file__))
_FILE = os.path.join(_DATA_DIR, "iShares-MSCI-ACWI-ETF_fund.xls")
_CSV_FILE = os.path.join(_DATA_DIR, "iShares-MSCI-ACWI-ETF_holdings.csv")
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
    if os.path.isfile(_CSV_FILE):
        current, current_date = _parse_holdings_csv(_CSV_FILE)
        original_date = datetime.strptime(as_of, '%d-%b-%Y').date().isoformat()
        if current_date >= original_date:
            return current, current_date

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


def _parse_holdings_csv(path: str) -> tuple[list[dict], str]:
    """Keep the original issuer snapshot; derive precision from values, not rounded weights."""
    with open(path, encoding='utf-8-sig', newline='') as stream:
        rows = list(csv.reader(stream))
    stamp = next((row[1].strip() for row in rows if row and row[0] == 'Fund Holdings as of' and len(row) > 1), '')
    as_of = datetime.strptime(stamp, '%b %d, %Y').date().isoformat()
    index = next(i for i, row in enumerate(rows) if row[:2] == ['Ticker', 'Name'])
    records = [dict(zip(rows[index], row)) for row in rows[index + 1:] if row]
    equities = [row for row in records if row.get('Asset Class') == 'Equity' and row.get('Name')]
    values = [float(row['Market Value'].replace(',', '')) for row in equities]
    if len(equities) < 1000 or any(not math.isfinite(value) or value < 0 for value in values) or sum(values) <= 0:
        raise ValueError('Invalid/incomplete official ACWI holdings snapshot')
    total = sum(values)
    for row, value in zip(equities, values):
        # Small constituents round to 0.00% in the CSV but have real, nonzero weights.
        row['Weight (%)'] = str(value / total * 100)
        row['Currency'] = row.get('Market Currency') or row.get('Currency') or ''
    return equities, as_of
