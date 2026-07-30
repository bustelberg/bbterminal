"""Leonteq (lynqs) "Verified" universe — membership + per-instrument metadata.

The lynqs CSV (id, ticker, name, productType, ric, isin, currency) defines the
Leonteq-Verified set. Uploading it REPLACES `leonteq_universe` (identifier = the
ISIN) with its name / currency / productType, so the grid can badge any matching
row and surface those columns next to the OpenFIGI + yfinance ones. Full-replace
by design — the set always reflects the current file (see the base migration)."""
from __future__ import annotations

from datetime import datetime, timezone

from deps import supabase

_CHUNK = 500


def replace_universe(rows: list[dict]) -> dict:
    """Full-replace the leonteq_universe set with {identifier, name, currency,
    product_type} rows (dedup by identifier, last wins). Clears the table first
    so a re-upload drops instruments no longer in the list. Returns counts."""
    now = datetime.now(timezone.utc).isoformat()
    by_id: dict[str, dict] = {}
    for r in rows:
        ident = (r.get("identifier") or "").strip().upper()
        if not ident:
            continue
        by_id[ident] = {
            "identifier": ident,
            "name": (r.get("name") or "").strip() or None,
            "currency": (r.get("currency") or "").strip() or None,
            "product_type": (r.get("product_type") or "").strip() or None,
            "added_at": now,
        }
    payload = list(by_id.values())
    # Full replace: clear the whole set (delete needs a filter; every identifier
    # is non-empty so `neq ''` matches all), then re-insert the current file.
    supabase.table("leonteq_universe").delete().neq("identifier", "").execute()
    for i in range(0, len(payload), _CHUNK):
        supabase.table("leonteq_universe").upsert(
            payload[i:i + _CHUNK], on_conflict="identifier"
        ).execute()
    return {"members": len(payload)}


def seed_execution_placeholders(isins: list[str]) -> int:
    """Insert a `status='queued'` asset_execution row for every ISIN not already
    present, so the WHOLE uploaded Leonteq universe shows in the grid immediately
    — badged + carrying its name/currency/productType via the `asset_grid` join —
    instead of trickling in only as the Yahoo-throttled worker resolves each.

    `ignore_duplicates` means an already-resolved (or already-queued) row is never
    clobbered; the background worker later upserts the real yfinance/OpenFIGI data
    onto the placeholder (flipping `queued` → ok/bond/not_found/error). Returns the
    number of distinct ISINs submitted."""
    now = datetime.now(timezone.utc).isoformat()
    seen: set[str] = set()
    rows: list[dict] = []
    for x in isins:
        i = (x or "").strip().upper()
        if i and i not in seen:
            seen.add(i)
            rows.append({"isin": i, "status": "queued", "updated_at": now})
    for i in range(0, len(rows), _CHUNK):
        supabase.table("asset_execution").upsert(
            rows[i:i + _CHUNK], on_conflict="isin", ignore_duplicates=True
        ).execute()
    return len(rows)
