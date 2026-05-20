"""Find the canonical GuruFocus listing for "no data" companies by name.

Approach (chained search):
  1. Search OpenFIGI `/v3/search` by company name -> up to 100 candidate
     listings (different exchanges, share classes, depositary receipts).
  2. Filter to securityType in {"Common Stock", "Ordinary Shares"} on
     an exchCode we know how to map to a GuruFocus exchange (i.e. in
     `_EXCHCODE_MAP`).
  3. For each surviving candidate, build the (gurufocus_exchange,
     ticker) pair and PROBE GuruFocus for prices. The first probe that
     returns data wins -- that's the canonical listing.
  4. Report findings. Doesn't auto-update the DB; the user reviews and
     applies via /companies (or pass --apply to write the changes).

Why probe rather than trust OpenFIGI's ordering? `_best_match` (the
existing primary-listing heuristic) picks "first Common Stock on a
mapped exchange", which favours US OTC ADRs (NYSE:DRRKF for Dormakaba)
over the primary listing (XSWX:DOKA). Probing eliminates that ambiguity
-- a wrong exchange returns 404 / 403 / empty data and gets skipped.

Usage (from backend/):
    uv run python scripts/discover_canonical_listing.py
        # dry-run: probe and report, no DB writes
    uv run python scripts/discover_canonical_listing.py --apply
        # when exactly ONE unique candidate works, update the
        # company row's exchange_id + gurufocus_ticker
    uv run python scripts/discover_canonical_listing.py --cid 5114
        # single-company probe by cid (useful for testing)
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests
from dotenv import load_dotenv
from supabase import create_client


_UNSUBSCRIBED_EXCHANGES = {
    "LSE", "JSE", "BSE", "NSE", "BMV", "ASX", "NZE", "MOEX",
}


def load_env() -> None:
    backend_dir = Path(__file__).resolve().parents[1]
    load_dotenv(backend_dir / ".env")
    load_dotenv(backend_dir / ".env.local", override=True)


def get_supabase():
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


def openfigi_search(name: str) -> list[dict]:
    """POST to OpenFIGI /v3/search and return the `data` array."""
    headers = {"Content-Type": "application/json"}
    if os.environ.get("OPENFIGI_API_KEY"):
        headers["X-OPENFIGI-APIKEY"] = os.environ["OPENFIGI_API_KEY"]
    body = {"query": name, "securityType2": "Common Stock"}
    try:
        r = requests.post(
            "https://api.openfigi.com/v3/search",
            json=body, headers=headers, timeout=20,
        )
        if r.status_code != 200:
            return []
        return r.json().get("data", []) or []
    except Exception:
        return []


def _candidates_from_figi(results: list[dict]) -> list[tuple[str, str]]:
    """Filter + dedupe OpenFIGI hits to (our_exchange, ticker) pairs we
    can probe. Preserves OpenFIGI's return order (first = best guess)."""
    from ingest.resolve_tickers import _EXCHCODE_MAP  # noqa: PLC0415
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for r in results:
        if r.get("securityType") not in ("Common Stock", "Ordinary Shares"):
            continue
        gf_exch = _EXCHCODE_MAP.get(r.get("exchCode"))
        if not gf_exch:
            continue
        if gf_exch in _UNSUBSCRIBED_EXCHANGES:
            continue
        raw_ticker = (r.get("ticker") or "").strip()
        # OpenFIGI sometimes appends `*` to denote primary listing
        # within a tier; GuruFocus URLs don't accept it.
        ticker = raw_ticker.rstrip("*")
        if not ticker:
            continue
        pair = (gf_exch, ticker)
        if pair in seen:
            continue
        seen.add(pair)
        out.append(pair)
    return out


def probe_gurufocus(ticker: str, exchange: str) -> tuple[bool, str]:
    """Probe GuruFocus for prices on (exchange, ticker). Returns
    (has_data, status_label) where status_label is one of:
    'data' / 'delisted' / 'not_found' / 'forbidden' / 'other'."""
    from ingest.prices import _fetch_indicator_from_api, normalize_gurufocus_ticker  # noqa: PLC0415
    ticker = normalize_gurufocus_ticker(ticker, exchange)
    data, log, status = _fetch_indicator_from_api(ticker, exchange, "price", timeout=15)
    if data:
        return True, "data"
    body = (log or "").lower()
    if "delisted" in body:
        return False, "delisted"
    if "stock not found" in body:
        return False, "not_found"
    if "unsubscribed region" in body:
        return False, "forbidden"
    return False, "other"


def find_canonical_for_company(
    name: str, current_exch: str, current_ticker: str,
) -> dict:
    """Return a verdict dict describing the best-canonical listing for
    `name` (or "no_data" / "delisted" / "search_empty")."""
    results = openfigi_search(name)
    candidates = _candidates_from_figi(results)
    # Exclude the current broken pair from probing — it's already known
    # not to work.
    candidates = [
        (e, t) for (e, t) in candidates
        if not (e == current_exch and t.upper() == current_ticker.upper())
    ]
    if not candidates:
        return {"outcome": "search_empty", "probes": []}

    probes: list[dict] = []
    winning: tuple[str, str] | None = None
    saw_delisted = False
    for exch, ticker in candidates:
        has_data, status = probe_gurufocus(ticker, exch)
        probes.append({"exchange": exch, "ticker": ticker, "status": status})
        if has_data:
            winning = (exch, ticker)
            break
        if status == "delisted":
            saw_delisted = True

    if winning:
        return {
            "outcome": "found",
            "exchange": winning[0],
            "ticker": winning[1],
            "probes": probes,
        }
    if saw_delisted:
        return {"outcome": "delisted", "probes": probes}
    return {"outcome": "no_data", "probes": probes}


def _has_price_data(sb, cid: int) -> bool:
    r = (
        sb.table("metric_data")
        .select("company_id")
        .eq("company_id", cid)
        .eq("metric_code", "close_price")
        .limit(1)
        .execute()
    )
    return bool(r.data)


def load_candidates(sb, only_cid: int | None) -> list[dict]:
    if only_cid is not None:
        r = (
            sb.table("company")
            .select(
                "company_id, company_name, gurufocus_ticker, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            )
            .eq("company_id", only_cid)
            .limit(1)
            .execute()
        )
        if not r.data:
            return []
        row = r.data[0]
        return [{
            "company_id": int(row["company_id"]),
            "company_name": row.get("company_name") or "",
            "ticker": row.get("gurufocus_ticker") or "",
            "exchange": ((row.get("gurufocus_exchange") or {}).get("exchange_code")) or "",
        }]

    # All non-delisted, on subscribed exchange, with zero close_price rows.
    print("Loading non-delisted, subscribed-exchange companies...")
    out: list[dict] = []
    offset = 0
    page = 1000
    while True:
        resp = (
            sb.table("company")
            .select(
                "company_id, company_name, gurufocus_ticker, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            )
            .is_("delisted_at", "null")
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        for r in batch:
            exch = ((r.get("gurufocus_exchange") or {}).get("exchange_code")) or ""
            ticker = r.get("gurufocus_ticker") or ""
            if not ticker or not exch or exch in _UNSUBSCRIBED_EXCHANGES:
                continue
            out.append({
                "company_id": int(r["company_id"]),
                "company_name": r.get("company_name") or "",
                "ticker": ticker,
                "exchange": exch,
            })
        if len(batch) < page:
            break
        offset += page

    print(f"  {len(out)} rows; checking close_price coverage...")
    from concurrent.futures import ThreadPoolExecutor  # noqa: PLC0415

    def _check(cid: int) -> tuple[int, bool]:
        return cid, _has_price_data(sb, cid)

    has_data: set[int] = set()
    with ThreadPoolExecutor(max_workers=16) as pool:
        for cid, ok in pool.map(_check, [c["company_id"] for c in out]):
            if ok:
                has_data.add(cid)
    return [c for c in out if c["company_id"] not in has_data]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true",
        help="Write the resolved exchange + ticker to the company row.",
    )
    parser.add_argument(
        "--cid", type=int, default=None,
        help="Probe a single cid (skip the candidate search).",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Cap candidates to N for testing.",
    )
    args = parser.parse_args()

    load_env()
    sb = get_supabase()
    candidates = load_candidates(sb, args.cid)
    if args.limit is not None:
        candidates = candidates[: args.limit]
    if not candidates:
        print("Nothing to do.")
        return 0
    print(f"Resolving {len(candidates)} companies via OpenFIGI + GuruFocus probe...")
    print()

    # Cache the exchange_code -> exchange_id lookup once.
    ex_resp = sb.table("gurufocus_exchange").select("exchange_id, exchange_code").execute()
    exchange_id_map = {r["exchange_code"]: r["exchange_id"] for r in (ex_resp.data or [])}

    findings: dict[str, list[dict]] = {
        "found": [], "delisted": [], "no_data": [], "search_empty": [],
    }
    for i, c in enumerate(candidates, 1):
        label = f"{c['exchange']}:{c['ticker']}"
        print(f"  [{i}/{len(candidates)}] {label:24} ({c['company_name'][:40]})...", flush=True)
        verdict = find_canonical_for_company(c["company_name"], c["exchange"], c["ticker"])
        verdict["company"] = c
        findings[verdict["outcome"]].append(verdict)
        if verdict["outcome"] == "found":
            print(f"      -> {verdict['exchange']}:{verdict['ticker']}  (probes: {len(verdict['probes'])})")
        elif verdict["outcome"] == "delisted":
            print(f"      -> delisted (saw 'Delisted stocks' on {len(verdict['probes'])} candidate(s))")
        elif verdict["outcome"] == "no_data":
            print(f"      -> no data (probed {len(verdict['probes'])} candidate(s), none worked)")
        else:
            print("      -> OpenFIGI returned no usable candidates")

    print()
    print("=== Summary ===")
    for k in ("found", "delisted", "no_data", "search_empty"):
        print(f"  {k}: {len(findings[k])}")

    if findings["found"]:
        print()
        print("=== Suggested DB updates (--apply to write) ===")
        for v in findings["found"][:50]:
            c = v["company"]
            print(
                f"  cid={c['company_id']:>5}  {c['exchange']}:{c['ticker']:<10} "
                f"-> {v['exchange']}:{v['ticker']:<10}  ({c['company_name'][:40]})"
            )

    if not args.apply:
        if findings["found"]:
            print()
            print("Dry run only. Re-run with --apply to write the changes.")
        return 0

    if not findings["found"]:
        return 0

    print()
    print(f"Applying {len(findings['found'])} updates...")
    ok = 0
    fail: list[tuple[int, str]] = []
    for v in findings["found"]:
        c = v["company"]
        new_exch = v["exchange"]
        new_ticker = v["ticker"]
        eid = exchange_id_map.get(new_exch)
        if eid is None:
            fail.append((c["company_id"], f"no exchange_id for {new_exch}"))
            continue
        try:
            sb.table("company").update({
                "exchange_id": eid,
                "gurufocus_ticker": new_ticker,
            }).eq("company_id", c["company_id"]).execute()
            ok += 1
        except Exception as e:
            fail.append((c["company_id"], f"{type(e).__name__}: {e}"))

    print(f"Done: {ok} updated, {len(fail)} failed")
    for cid, err in fail:
        print(f"  cid={cid}: {err}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
