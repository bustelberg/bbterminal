"""Verify each company against OpenFIGI by its ISIN.

OpenFIGI's `/v3/mapping` resolves an ISIN to every listing of that security
(ticker + exchCode + name + securityType). We use it to answer "does the ISIN
we store for this company actually belong to this company?" — the cheap,
authoritative catch for wrong-ISIN traps (e.g. "Hindustan Aeronautics Ltd"
whose stored ISIN BMG455841020 maps to "HAL TRUST", a different security).

Two-tier classification per company (`classify_openfigi`):
  1. LISTING tier — our (gurufocus_ticker, exchange) appears among OpenFIGI's
     listings for the ISIN → `verified` (the listing we price is confirmed).
  2. NAME tier — the OpenFIGI security name matches ours (suffix-tolerant, so
     "NESTLE SA" == "NESTLE SA-REG") → `verified` (right company, maybe a
     different venue). Otherwise → `mismatch` (the ISIN is a different company).
No OpenFIGI data → `not_found`; no ISIN stored → `no_isin`.

Populated on demand by the /companies "Verify OpenFIGI" bulk action +
per-row re-check (see `routers/companies.py`). One batched OpenFIGI call per
100 ISINs; set OPENFIGI_API_KEY for the higher rate limit (already used by the
ISIN backfill).
"""
from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import requests

from ingest.dedupe import canonical_ticker
from ingest.resolve_tickers import _best_match, _exchcode_to_exchange

_OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
_BATCH_SIZE = 100
# Polite spacing between batches. OpenFIGI's keyed limit is generous (25
# req/6s); a small sleep keeps us comfortably under it during a full sweep.
_BATCH_PAUSE_S = 0.3


@dataclass
class VerifyResult:
    scanned: int = 0
    verified: int = 0
    mismatch: int = 0
    not_found: int = 0
    no_isin: int = 0
    errors: int = 0
    mismatches: list[str] = field(default_factory=list)  # "cid name -> OpenFIGI name" samples


def _name_key(s: str | None) -> str:
    """Alphanumeric-only, lowercased — compares names without churning on
    punctuation/spacing ("Apple Inc" == "Apple Inc."). "&" is normalized to
    "and" first so "SMITH AND NEPHEW" == "SMITH & NEPHEW" (GuruFocus spells it
    out, OpenFIGI uses the ampersand)."""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower().replace("&", "and"))


def _names_match(a: str | None, b: str | None) -> bool:
    """Suffix-tolerant name match. Equal keys match; otherwise:
      * the shorter key being a full prefix of the longer counts (OpenFIGI
        appends share-class/listing suffixes: "NESTLE SA" → "NESTLE SA-REG",
        "...-SP ADR", "...-REG"); OR
      * the two share a LONG common root (≥10 chars and ≥60% of the shorter
        key). OpenFIGI both abbreviates AND suffixes issuer names — "Chocolade-
        fabriken Lindt & Spruengli AG" vs "CHOCOLADEFABRIKEN LINDT-PC",
        "Samsung Electronics Co Ltd" vs "SAMSUNG ELECTR-GDR REG S" — so a strict
        prefix isn't enough, but a 10+ char shared root still separates them
        from a genuinely different company ("HAL TRUST" vs "Hindustan
        Aeronautics" share only 1 char; "C3.AI INC" vs "AIR LIQUIDE SA" share 0)."""
    ka, kb = _name_key(a), _name_key(b)
    if not ka or not kb:
        return False
    if ka == kb:
        return True
    shorter, longer = (ka, kb) if len(ka) <= len(kb) else (kb, ka)
    # Require a meaningful shared root so short tickers-as-names don't over-match.
    if len(shorter) >= 4 and longer.startswith(shorter):
        return True
    lcp = 0
    for x, y in zip(shorter, longer):
        if x != y:
            break
        lcp += 1
    return lcp >= 10 and lcp >= 0.6 * len(shorter)


def _openfigi_name(data: list[dict]) -> str | None:
    """The security name OpenFIGI reports — taken from the primary-listing
    pick so it's the issuer's canonical name, not an ADR sub-class."""
    match = _best_match(data)
    name = (match or {}).get("name")
    return name.strip() if isinstance(name, str) and name.strip() else None


def classify_openfigi(
    isin: str | None,
    our_name: str | None,
    our_ticker: str | None,
    our_exchange: str | None,
    data: list[dict] | None,
) -> tuple[str, str | None]:
    """Classify one company against its OpenFIGI `data` array (the records under
    `data` for an ID_ISIN mapping). Returns `(status, openfigi_name)` where
    status ∈ {verified, mismatch, not_found, no_isin}. Pure — no I/O."""
    if not isin or not isin.strip():
        return "no_isin", None
    if not data:
        return "not_found", None

    our_t = canonical_ticker(our_ticker, our_exchange)
    our_e = (our_exchange or "").strip().upper()
    if our_t and our_e:
        # Space-insensitive ticker compare: GuruFocus writes Nordic class shares
        # with a space ("HM B"), OpenFIGI without ("HMB"). Same listing.
        our_t_ns = our_t.replace(" ", "")
        for d in data:
            d_exch = _exchcode_to_exchange(d.get("exchCode")).upper()
            d_tick = canonical_ticker(d.get("ticker"), d_exch)
            if d_tick and d_exch == our_e and (d_tick == our_t or d_tick.replace(" ", "") == our_t_ns):
                return "verified", _openfigi_name(data)

    # NAME tier — compare our name against EVERY listing's name, not just the
    # primary pick: `_best_match` sometimes returns an abbreviated record ("EDP
    # SA") while a later record carries the full name ("EDP-ENERGIAS DE PORTUGAL
    # SA") that matches ours exactly.
    ofg_name = _openfigi_name(data)  # canonical issuer name for display
    if any(_names_match(our_name, d.get("name")) for d in data):
        return "verified", ofg_name
    return "mismatch", ofg_name


_COLS = ("company_id, company_name, isin, gurufocus_ticker, openfigi_status, "
        "gurufocus_exchange:gurufocus_exchange(exchange_code)")


def _load_companies(supabase, company_ids: list[int] | None, only_missing: bool) -> list[dict]:
    # Explicit id list: chunk the `.in_()` (a 1600-id filter overflows the URI →
    # 414 "URI too long"); IN_CHUNK_SIZE-sized chunks, same as elsewhere.
    if company_ids:
        from deps import fetch_in_chunks  # noqa: PLC0415
        return list(fetch_in_chunks(
            company_ids,
            lambda chunk: supabase.table("company").select(_COLS).in_("company_id", chunk).execute(),
        ))
    out: list[dict] = []
    offset = 0
    page = 1000
    for _ in range(50):
        q = supabase.table("company").select(_COLS).order("company_id").range(offset, offset + page - 1)
        if only_missing:
            q = q.is_("openfigi_status", "null")
        batch = q.execute().data or []
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return out


def _fetch_isin_data(isins: list[str], headers: dict) -> dict[str, list[dict] | None]:
    """Batch-resolve ISINs via OpenFIGI. Returns `{isin: data_list_or_None}`.
    None means OpenFIGI had no security for that ISIN (not an error)."""
    out: dict[str, list[dict] | None] = {}
    for i in range(0, len(isins), _BATCH_SIZE):
        batch = isins[i : i + _BATCH_SIZE]
        jobs = [{"idType": "ID_ISIN", "idValue": s} for s in batch]
        resp = requests.post(_OPENFIGI_URL, json=jobs, headers=headers, timeout=30)
        resp.raise_for_status()
        for s, item in zip(batch, resp.json()):
            data = item.get("data") if isinstance(item, dict) else None
            out[s] = data or None
        if i + _BATCH_SIZE < len(isins):
            time.sleep(_BATCH_PAUSE_S)
    return out


def verify_companies_openfigi(
    supabase,
    *,
    company_ids: list[int] | None = None,
    only_missing: bool = False,
    on_progress=None,
) -> VerifyResult:
    """Verify companies against OpenFIGI by ISIN and persist
    `openfigi_status` / `openfigi_name` / `openfigi_checked_at`. Scope: all
    companies, or `company_ids`, or (with `only_missing`) those never checked.
    `on_progress(dict)` is called with `{message, processed, total, ...}` for
    the /companies progress bar."""
    result = VerifyResult()

    def emit(msg: str, processed: int, total: int) -> None:
        if on_progress:
            try:
                on_progress({"message": msg, "processed": processed, "total": total,
                             "verified": result.verified, "mismatch": result.mismatch})
            except Exception:
                pass

    companies = _load_companies(supabase, company_ids, only_missing)
    total = len(companies)
    emit(f"Resolving {total} companies via OpenFIGI…", 0, total)

    api_key = os.environ.get("OPENFIGI_API_KEY")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    # Resolve every distinct ISIN once (one company can't share an ISIN, but
    # batching dedupes defensively).
    isins = sorted({(c.get("isin") or "").strip() for c in companies if (c.get("isin") or "").strip()})
    try:
        isin_data = _fetch_isin_data(isins, headers) if isins else {}
    except Exception as e:  # noqa: BLE001 — surface to the status endpoint
        emit(f"OpenFIGI request failed: {type(e).__name__}: {e}", 0, total)
        raise

    now = datetime.now(timezone.utc).isoformat()
    for idx, c in enumerate(companies, 1):
        cid = int(c["company_id"])
        isin = (c.get("isin") or "").strip()
        exch = ((c.get("gurufocus_exchange") or {}).get("exchange_code")) or ""
        data = isin_data.get(isin) if isin else None
        status, ofg_name = classify_openfigi(isin, c.get("company_name"), c.get("gurufocus_ticker"), exch, data)

        result.scanned += 1
        if status == "verified":
            result.verified += 1
        elif status == "mismatch":
            result.mismatch += 1
            if len(result.mismatches) < 50:
                result.mismatches.append(f"{cid} {c.get('company_name')!r} -> OpenFIGI {ofg_name!r}")
        elif status == "not_found":
            result.not_found += 1
        elif status == "no_isin":
            result.no_isin += 1

        try:
            supabase.table("company").update({
                "openfigi_status": status,
                "openfigi_name": ofg_name,
                "openfigi_checked_at": now,
            }).eq("company_id", cid).execute()
        except Exception:  # noqa: BLE001 — best-effort per row
            result.errors += 1

        if idx % 25 == 0 or idx == total:
            emit(f"Verified {idx}/{total} "
                 f"(✓{result.verified} ⚠{result.mismatch} ?{result.not_found})",
                 idx, total)

    emit(f"Done — {result.verified} verified, {result.mismatch} mismatch, "
         f"{result.not_found} not found, {result.no_isin} no ISIN.", total, total)
    return result
