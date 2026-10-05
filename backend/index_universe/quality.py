"""Source loader for the static Quality universe.

The Quality universe is the union of the committed Global Compounders
workbook and the iShares MSCI World Quality Factor ETF holdings.  Both files
are source data, rather than application data: this module normalizes them
into company candidates; the template is responsible for matching candidates
to canonical ``company`` rows.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from lxml import etree


_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "company-lists"
_COMPOUNDERS_FILE = _DATA_DIR / "Long-Equity---December-2025---Global-Compounders-Database.xlsx"
_ISHARES_FILE = _DATA_DIR / "iShares-Edge-MSCI-World-Quality-Factor-UCITS-ETF-USD-Acc_fund.xls"
_NS = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}


@dataclass
class QualityCandidate:
    ticker: str
    name: str
    sector: str | None
    country: str | None
    ishares_exchange: str | None = None
    sources: set[str] = field(default_factory=set)


def _xml_rows(path: Path, sheet_name: str) -> list[list[str]]:
    raw = path.read_bytes()
    while raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    root = etree.fromstring(raw, parser=etree.XMLParser(recover=True))
    for sheet in root.findall(".//ss:Worksheet", _NS):
        if sheet.get("{urn:schemas-microsoft-com:office:spreadsheet}Name") != sheet_name:
            continue
        rows: list[list[str]] = []
        for row in sheet.findall(".//ss:Row", _NS):
            cells: list[str] = []
            for cell in row.findall("ss:Cell", _NS):
                data = cell.find("ss:Data", _NS)
                cells.append(data.text.strip() if data is not None and data.text else "")
            rows.append(cells)
        return rows
    raise ValueError(f"No {sheet_name!r} worksheet in {path.name}")


def load_quality_candidates() -> tuple[list[QualityCandidate], date]:
    """Return the de-duplicated source union and its newest source date.

    Ticker is the only common identifier supplied by both files.  It is safe
    for creating the source union, but not sufficient to select a database
    listing; the template subsequently prefers the ETF's exchange and only
    uses an unambiguous existing database ticker otherwise.
    """
    out: dict[str, QualityCandidate] = {}

    compounders = pd.read_excel(_COMPOUNDERS_FILE, sheet_name="Data", header=1)
    for row in compounders.itertuples(index=False):
        ticker = str(getattr(row, "Ticker", "")).strip().upper()
        if not ticker:
            continue
        out[ticker] = QualityCandidate(
            ticker=ticker,
            name=str(getattr(row, "Company", "")).strip(),
            sector=(str(getattr(row, "Sector", "")).strip() or None),
            country=(str(getattr(row, "Country", "")).strip() or None),
            sources={"compounders"},
        )

    rows = _xml_rows(_ISHARES_FILE, "Posities")
    header_index = next(
        (i for i, row in enumerate(rows) if row and row[0] == "Beurscode emittent"),
        None,
    )
    if header_index is None:
        raise ValueError("Could not find the holdings header in the iShares quality file")
    headers = rows[header_index]
    for row in rows[header_index + 1:]:
        row = row + [""] * max(0, len(headers) - len(row))
        record = dict(zip(headers, row))
        if record.get("Beleggingscategorie") != "Aandelen":
            continue
        ticker = record.get("Beurscode emittent", "").strip().upper()
        if not ticker:
            continue
        existing = out.get(ticker)
        if existing is None:
            existing = QualityCandidate(
                ticker=ticker,
                name=record.get("Naam", "").strip(),
                sector=record.get("Sector", "").strip() or None,
                country=record.get("Locatie", "").strip() or None,
                ishares_exchange=record.get("Beurs", "").strip() or None,
            )
            out[ticker] = existing
        else:
            # The ETF gives us an exact listing venue, so prefer it for
            # database resolution while retaining the Compounders name/sector.
            existing.ishares_exchange = record.get("Beurs", "").strip() or existing.ishares_exchange
        existing.sources.add("ishares_quality")

    # The ETF is dated 30-sep-2026 in this committed source.  Parse the cell
    # rather than using today's date so the snapshot's provenance stays clear.
    as_of_raw = rows[0][0] if rows and rows[0] else ""
    try:
        as_of = datetime.strptime(as_of_raw, "%d-%b-%Y").date()
    except ValueError as exc:
        raise ValueError(f"Could not parse iShares Quality as-of date: {as_of_raw!r}") from exc
    return [out[t] for t in sorted(out)], as_of
