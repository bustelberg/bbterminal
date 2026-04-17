"""S&P 500 index universe: scrape, reconstruct, store, and check coverage."""

from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from datetime import date, datetime
from html.parser import HTMLParser
from typing import Callable

import requests
from supabase import Client


log = logging.getLogger(__name__)

_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_BUCKET = "gurufocus-raw"
_START_MONTH = "2000-01"  # Only store from Jan 2000 onwards


# ---------------------------------------------------------------------------
# HTML parser
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_change_date(raw: str) -> date | None:
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _month_key(d: date) -> str:
    return d.strftime("%Y-%m")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

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


def reconstruct_monthly_holdings(
    current_tickers: set[str],
    changes: list[dict],
    start_month: str = _START_MONTH,
) -> tuple[dict[str, set[str]], list[dict]]:
    """Walk backwards from current holdings through changes to build
    monthly composition: {YYYY-MM: set[ticker]}.

    Only keeps months >= start_month.

    Also returns the changes list filtered to start_month onwards, with
    each change tagged with its YYYY-MM month key.

    Returns (monthly_holdings, filtered_changes).
    """
    # Group changes by month
    changes_by_month: defaultdict[str, list[dict]] = defaultdict(list)
    for c in changes:
        changes_by_month[_month_key(c["date"])].append(c)

    change_months = sorted(changes_by_month.keys(), reverse=True)
    current_month = _month_key(date.today())

    holdings = current_tickers.copy()
    all_holdings: dict[str, set[str]] = {current_month: holdings.copy()}

    for month in change_months:
        for c in changes_by_month[month]:
            if c["added"] and c["added"] in holdings:
                holdings.discard(c["added"])
            if c["removed"]:
                holdings.add(c["removed"])
        all_holdings[month] = holdings.copy()

    # Fill gaps: for every month between earliest and today, carry forward
    all_months = sorted(all_holdings.keys())
    if all_months:
        start = datetime.strptime(all_months[0], "%Y-%m").date()
        end = date.today()
        cursor = start
        prev = all_holdings[all_months[0]]
        while cursor <= end:
            mk = _month_key(cursor)
            if mk in all_holdings:
                prev = all_holdings[mk]
            else:
                all_holdings[mk] = prev.copy()
            if cursor.month == 12:
                cursor = cursor.replace(year=cursor.year + 1, month=1)
            else:
                cursor = cursor.replace(month=cursor.month + 1)

    # Filter to start_month onwards
    result = {m: t for m, t in all_holdings.items() if m >= start_month}

    # Build filtered changes with month key
    filtered_changes = []
    for c in changes:
        mk = _month_key(c["date"])
        if mk >= start_month:
            filtered_changes.append({
                "date": c["date"].isoformat(),
                "month": mk,
                "added": c["added"],
                "removed": c["removed"],
            })
    # Sort oldest first for changelog display
    filtered_changes.sort(key=lambda c: c["date"])

    return result, filtered_changes


def resolve_and_create_companies(
    supabase: Client,
    tickers: set[str],
    on_progress: Callable[[str], None] | None = None,
    company_info: dict[str, dict] | None = None,
) -> dict[str, int]:
    """Resolve S&P 500 tickers via OpenFIGI and create missing company records.

    If company_info is provided ({ticker: {name, sector, country}}), it will be
    used to fill in empty company_name fields — existing values are never overwritten.

    Returns ticker → company_id mapping for all resolved tickers.
    """
    wiki_info = company_info or {}
    emit = on_progress or (lambda _: None)

    # Load exchange_id map for US exchanges
    exch_resp = supabase.table("gurufocus_exchange").select("exchange_id, exchange_code").execute()
    exchange_id_map = {r["exchange_code"]: r["exchange_id"] for r in (exch_resp.data or [])}

    # Load existing US companies
    existing: dict[str, int] = {}
    _company_cache: dict[int, dict] = {}
    for exchange in ("NYSE", "NASDAQ"):
        eid = exchange_id_map.get(exchange)
        if eid is None:
            continue
        resp = supabase.table("company").select(
            "company_id, gurufocus_ticker, company_name"
        ).eq("exchange_id", eid).execute()
        for row in resp.data:
            existing[row["gurufocus_ticker"]] = row["company_id"]
            _company_cache[row["company_id"]] = row

    already_matched = {t for t in tickers if t in existing}
    to_resolve = sorted(tickers - already_matched)

    emit(f"Company lookup: {len(already_matched)} already in DB, {len(to_resolve)} need resolution")

    # Tag already-matched companies with 'sp500' source via company_source table
    tagged = 0
    enriched = 0
    for t in already_matched:
        cid = existing[t]
        cached = _company_cache.get(cid, {})
        # Ensure company_source row exists for sp500
        try:
            supabase.table("company_source").upsert(
                {"company_id": cid, "source_code": "sp500"},
                on_conflict="company_id,source_code",
                ignore_duplicates=True,
            ).execute()
            tagged += 1
        except Exception:
            pass
        # Fill empty company_name from Wikipedia info
        info = wiki_info.get(t, {})
        updates: dict = {}
        if info.get("name") and not cached.get("company_name"):
            updates["company_name"] = info["name"]
        if updates:
            try:
                supabase.table("company").update(updates).eq("company_id", cid).execute()
                enriched += 1
            except Exception:
                pass
    if tagged or enriched:
        emit(f"Updated {tagged} source tags, enriched {enriched} companies with Wikipedia data")

    if not to_resolve:
        return existing

    # Resolve via OpenFIGI in batches with progress
    unknowns = [{"ticker": t, "country": "USA"} for t in to_resolve]
    total = len(unknowns)
    batch_size = 100
    resolved: list[dict] = []
    unresolved: list[str] = []

    import os
    from ingest.resolve_tickers import _exchcode_to_exchange, _best_match, _normalize_ticker_for_gurufocus

    api_key = os.environ.get("OPENFIGI_API_KEY")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    _OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
    _COUNTRY_EXCHCODE = "US"  # All S&P 500 tickers are US

    for i in range(0, total, batch_size):
        batch = unknowns[i : i + batch_size]
        jobs = [{"idType": "TICKER", "idValue": u["ticker"].replace("-", " "), "exchCode": _COUNTRY_EXCHCODE} for u in batch]

        try:
            resp = requests.post(_OPENFIGI_URL, json=jobs, headers=headers, timeout=30)
            resp.raise_for_status()
            items = resp.json()
        except Exception as e:
            emit(f"  OpenFIGI batch error: {e}")
            unresolved.extend(u["ticker"] for u in batch)
            continue

        _US_EXCHCODES = {"US", "UW", "UN", "UA", "UP", "UR", "UQ"}

        for u, item in zip(batch, items):
            if "data" not in item or not item["data"]:
                resolved.append({
                    "ticker": u["ticker"],
                    "gurufocus_ticker": u["ticker"],
                    "gurufocus_exchange": "NYSE",
                })
                continue

            match = _best_match(item["data"])
            if not match:
                resolved.append({
                    "ticker": u["ticker"],
                    "gurufocus_ticker": u["ticker"],
                    "gurufocus_exchange": "NYSE",
                })
                continue

            # Only accept US exchange matches for S&P 500 tickers
            exchcode = match.get("exchCode", "")
            if exchcode not in _US_EXCHCODES:
                resolved.append({
                    "ticker": u["ticker"],
                    "gurufocus_ticker": u["ticker"],
                    "gurufocus_exchange": "NYSE",
                })
                continue

            exchange = _exchcode_to_exchange(exchcode)
            raw_ticker = match.get("ticker") or u["ticker"]
            gf_ticker = _normalize_ticker_for_gurufocus(raw_ticker, exchange)
            resolved.append({
                "ticker": u["ticker"],
                "gurufocus_ticker": gf_ticker,
                "gurufocus_exchange": exchange,
            })

        done = min(i + batch_size, total)
        emit(f"OpenFIGI: {done}/{total} resolved ({len(resolved)} found, {len(unresolved)} missed)")

    # Create company records for resolved tickers not yet in DB
    created = 0
    already_existed = 0
    for j, r in enumerate(resolved):
        gt = r["gurufocus_ticker"]
        ge = r["gurufocus_exchange"]
        eid = exchange_id_map.get(ge)

        # Check if already exists (might match under different raw ticker)
        query = supabase.table("company").select("company_id").eq("gurufocus_ticker", gt)
        if eid is not None:
            query = query.eq("exchange_id", eid)
        check = query.limit(1).execute()

        if check.data:
            cid = check.data[0]["company_id"]
            existing[r["ticker"]] = cid
            existing[gt] = cid
            already_existed += 1
            # Ensure company_source row
            try:
                supabase.table("company_source").upsert(
                    {"company_id": cid, "source_code": "sp500"},
                    on_conflict="company_id,source_code",
                    ignore_duplicates=True,
                ).execute()
            except Exception:
                pass
            continue

        # Create new company record, using Wikipedia info if available
        info = wiki_info.get(r["ticker"], {})
        row = {
            "gurufocus_ticker": gt,
            "exchange_id": eid,
            "company_name": info.get("name") or None,
        }
        try:
            ins = supabase.table("company").insert(row).execute()
            if ins.data:
                cid = ins.data[0]["company_id"]
                existing[r["ticker"]] = cid
                existing[gt] = cid
                created += 1
                # Add company_source
                supabase.table("company_source").upsert(
                    {"company_id": cid, "source_code": "sp500"},
                    on_conflict="company_id,source_code",
                    ignore_duplicates=True,
                ).execute()
        except Exception as e:
            log.warning("Failed to create company %s/%s: %s", gt, ge, e)

        if (j + 1) % 50 == 0:
            emit(f"Creating companies: {j + 1}/{len(resolved)} ({created} new, {already_existed} existing)")

    emit(f"Companies: {created} created, {already_existed} already existed, {len(unresolved)} unresolved")
    return existing


def store_index_membership(
    supabase: Client,
    index_name: str,
    monthly_holdings: dict[str, set[str]],
    changes: list[dict],
    company_lookup: dict[str, int],
    on_progress: Callable[[str], None] | None = None,
) -> dict:
    """Store monthly holdings and changes in the database.

    Uses universe + universe_membership tables. Deletes existing data
    for the universe and batch-inserts rows.
    Returns summary stats.
    """
    emit = on_progress or (lambda _: None)

    # Ensure universe exists
    emit(f"Setting up universe '{index_name}'...")
    u_resp = supabase.table("universe").select("universe_id").eq("label", index_name).limit(1).execute()
    if u_resp.data:
        universe_id = u_resp.data[0]["universe_id"]
        # Clear existing membership rows
        emit(f"Clearing existing {index_name} membership data...")
        # Delete in batches by month to avoid timeout
        months_resp = supabase.table("universe_membership").select("target_month").eq("universe_id", universe_id).limit(100000).execute()
        existing_months = set(r["target_month"] for r in (months_resp.data or []))
        for m in existing_months:
            supabase.table("universe_membership").delete().eq("universe_id", universe_id).eq("target_month", m).execute()
    else:
        resp = supabase.table("universe").insert({"label": index_name, "description": f"{index_name} index"}).execute()
        universe_id = resp.data[0]["universe_id"]

    # Collect all unique tickers for stats
    all_tickers: set[str] = set()
    for tickers in monthly_holdings.values():
        all_tickers |= tickers

    matched = sum(1 for t in all_tickers if t in company_lookup)
    emit(f"Ticker matching: {matched}/{len(all_tickers)} unique tickers have company records")

    # Batch insert membership rows
    months = sorted(monthly_holdings.keys())
    total_rows = 0
    batch: list[dict] = []
    batch_size = 500

    for i, month in enumerate(months):
        for ticker in sorted(monthly_holdings[month]):
            cid = company_lookup.get(ticker)
            if cid is None:
                continue  # Can't store without a company_id (FK constraint)
            batch.append({
                "universe_id": universe_id,
                "target_month": month,
                "company_id": cid,
                "universe_ticker": ticker,
            })
            if len(batch) >= batch_size:
                supabase.table("universe_membership").upsert(
                    batch, on_conflict="universe_id,company_id,target_month"
                ).execute()
                total_rows += len(batch)
                batch = []

        if (i + 1) % 50 == 0 or i == len(months) - 1:
            emit(f"Storing months: {i + 1}/{len(months)} ({total_rows + len(batch)} rows)")

    if batch:
        supabase.table("universe_membership").upsert(
            batch, on_conflict="universe_id,company_id,target_month"
        ).execute()
        total_rows += len(batch)

    # Store changes as a JSON blob in the first row's metadata (or a separate approach)
    # For simplicity, store as a separate storage file
    changes_path = f"index_changes/{index_name}.json"
    changes_json = json.dumps(changes, ensure_ascii=False).encode("utf-8")
    try:
        supabase.storage.from_(_BUCKET).upload(
            changes_path, changes_json,
            file_options={"content-type": "application/json"},
        )
    except Exception:
        try:
            supabase.storage.from_(_BUCKET).update(
                changes_path, changes_json,
                file_options={"content-type": "application/json"},
            )
        except Exception:
            log.warning("Could not store changes file for %s", index_name)

    return {
        "months": len(months),
        "total_rows": total_rows,
        "unique_tickers": len(all_tickers),
        "matched_companies": matched,
        "changes_count": len(changes),
    }


def load_changes(supabase: Client, index_name: str) -> list[dict]:
    """Load stored changes for an index from storage."""
    path = f"index_changes/{index_name}.json"
    try:
        raw = supabase.storage.from_(_BUCKET).download(path)
        return json.loads(raw)
    except Exception:
        return []


def check_gurufocus_availability(
    supabase: Client,
    tickers: set[str],
    on_progress: Callable[[str], None] | None = None,
) -> dict:
    """Check which tickers have cached GuruFocus financials data.

    Uses company table to find the correct exchange, then checks storage.
    Falls back to trying NYSE/NASDAQ/AMEX if ticker isn't in company table.
    """
    emit = on_progress or (lambda _: None)

    # Build ticker → exchange lookup from company table
    ticker_exchange: dict[str, str] = {}
    exch_resp = supabase.table("gurufocus_exchange").select("exchange_id, exchange_code").execute()
    eid_to_code = {r["exchange_id"]: r["exchange_code"] for r in (exch_resp.data or [])}
    for exchange in ("NYSE", "NASDAQ"):
        eid = next((eid for eid, code in eid_to_code.items() if code == exchange), None)
        if eid is None:
            continue
        resp = supabase.table("company").select("gurufocus_ticker, exchange_id").eq("exchange_id", eid).execute()
        for row in resp.data:
            ticker_exchange[row["gurufocus_ticker"]] = exchange

    emit(f"Loaded {len(ticker_exchange)} company exchange mappings")

    available: list[str] = []
    missing: list[str] = []
    sorted_tickers = sorted(tickers)

    for i, ticker in enumerate(sorted_tickers):
        found = False

        # Try known exchange first
        known_ex = ticker_exchange.get(ticker)
        if known_ex:
            path = f"{known_ex}_{ticker}/financials.json"
            try:
                supabase.storage.from_(_BUCKET).download(path)
                available.append(ticker)
                found = True
            except Exception:
                pass

        # Fallback: try all US exchanges
        if not found:
            for exchange in ("NYSE", "NASDAQ", "AMEX"):
                path = f"{exchange}_{ticker}/financials.json"
                try:
                    supabase.storage.from_(_BUCKET).download(path)
                    available.append(ticker)
                    found = True
                    break
                except Exception:
                    continue

        if not found:
            missing.append(ticker)

        if (i + 1) % 25 == 0 or i == len(sorted_tickers) - 1:
            emit(f"Checking GuruFocus coverage: {i + 1}/{len(sorted_tickers)} ({len(available)} found)")

    coverage_pct = (len(available) / len(sorted_tickers) * 100) if sorted_tickers else 0
    return {
        "available": available,
        "missing": missing,
        "total": len(sorted_tickers),
        "available_count": len(available),
        "missing_count": len(missing),
        "coverage_pct": round(coverage_pct, 1),
    }
