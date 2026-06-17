"""Validate every company's GuruFocus link and flag the bad ones.

For each company we ask GuruFocus for `stock/<SYMBOL>/summary` (the same
endpoint + Cloudflare-bypass ladder the price/ISIN/market-cap backfills use)
and classify the response. The GuruFocus API gives an unambiguous signal for
a wrong ticker/exchange — exactly the "Results similar to" case on the
website surfaces here as a clean **404 "Stock not found"**:

  * 404 "Stock not found"            -> NOT_FOUND  (the link is broken — wrong
                                        ticker/exchange; e.g. XTER:1071 when the
                                        listing is really HKSE:01071). FLAGGED.
  * 200 + a company name that does    -> MISMATCH   (the symbol resolves to a
    NOT match our stored name           DIFFERENT company — mislabeled row).
                                        FLAGGED (for review — name diffs can be
                                        benign, e.g. ADR/translation).
  * 200 + matching name               -> OK.
  * 403 "unsubscribed region" / other -> RESTRICTED (UK/India/… out-of-GF-scope,
    403                                 or delisted-stocks body). NOT a wrong
                                        ticker — reported, never flagged.
  * network / Cloudflare / 5xx / circuit-breaker open -> ERROR (transient).
                                        Reported so you can re-run; never flagged.

Name comparison uses `_name_key` (alphanumeric-only, case-insensitive) so
"Apple Inc" == "Apple Inc." but "TSMC" != "Forside Co Ltd".

HKSE tickers are zero-padded to 5 digits before the lookup (`HKSE:1071` 404s;
`HKSE:01071` resolves) via the shared `pad_hkse_ticker` — the one place that
rule lives.

Usage (from backend/):
    uv run python scripts/check_gurufocus_links.py                  # full report (no writes)
    uv run python scripts/check_gurufocus_links.py --limit 30       # sample
    uv run python scripts/check_gurufocus_links.py --ids 1071,5021  # specific companies
    uv run python scripts/check_gurufocus_links.py --include-delisted
    uv run python scripts/check_gurufocus_links.py --apply          # set gurufocus_lookup_failed_at on NOT_FOUND

Default scope: active companies only (NOT delisted, NOT out_of_scope) — those
are the ones that can land in a tradeable universe, where a wrong link matters.
`--apply` flags only the unambiguous NOT_FOUND (404) rows; MISMATCH rows are
always report-only (they need a human eyeball). Writes a CSV report regardless.

~1.5s/company (the GuruFocus client's built-in rate limit) and one API call
each (well within the 20k/region monthly cap).
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # loads SUPABASE_* + GURUFOCUS_* from .env / .env.local  # noqa: E402
from index_universe.backfill_isin import _gf_symbol  # noqa: E402
from index_universe.backfill_market_cap import _name_from_company_data, _name_key  # noqa: E402
from ingest.earnings._api_client import _api_request, _build_api_url  # noqa: E402
from ingest.gurufocus_url import gurufocus_url, pad_hkse_ticker  # noqa: E402

sb = deps.supabase


@dataclass
class Row:
    company_id: int
    company_name: str
    ticker: str
    exchange: str
    symbol: str | None
    status: str          # OK | NOT_FOUND | MISMATCH | RESTRICTED | ERROR | NO_SYMBOL
    gf_name: str | None  # name GuruFocus returned (for MISMATCH)
    detail: str          # short human note
    url: str | None      # the human gurufocus.com link, for eyeballing


FLAGGABLE = {"NOT_FOUND"}            # safe to auto-flag (unambiguous 404)
BAD = {"NOT_FOUND", "MISMATCH"}      # "doesn't come back good" — reported as flagged


def _load_companies(*, include_delisted: bool, ids: list[int] | None) -> list[dict]:
    out: list[dict] = []
    offset = 0
    page = 1000
    while True:
        q = (
            sb.table("company")
            .select("company_id, company_name, gurufocus_ticker, gurufocus_lookup_failed_at, "
                    "delisted_at, out_of_scope_at, "
                    "gurufocus_exchange:gurufocus_exchange(exchange_code)")
            .order("company_id")
            .range(offset, offset + page - 1)
        )
        if ids:
            q = q.in_("company_id", ids)
        elif not include_delisted:
            q = q.is_("delisted_at", "null").is_("out_of_scope_at", "null")
        batch = q.execute().data or []
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return out


def check_one(c: dict) -> Row:
    cid = int(c["company_id"])
    name = c.get("company_name") or ""
    raw_ticker = c.get("gurufocus_ticker") or ""
    exch = ((c.get("gurufocus_exchange") or {}).get("exchange_code")) or ""
    # Pad HKSE so the API symbol resolves (HKSE:1071 404s, HKSE:01071 works).
    ticker = pad_hkse_ticker(raw_ticker, exch)
    symbol = _gf_symbol(ticker, exch)
    url = gurufocus_url(ticker, exch)
    if not symbol:
        return Row(cid, name, raw_ticker, exch, None, "NO_SYMBOL", None, "no GuruFocus symbol (missing ticker)", url)

    res = _api_request(_build_api_url(f"stock/{symbol}/summary"))
    if res.data is None:
        sc = res.status_code
        if sc == 404:
            return Row(cid, name, ticker, exch, symbol, "NOT_FOUND", None, "404 stock not found", url)
        if sc == 403 or res.is_forbidden:
            return Row(cid, name, ticker, exch, symbol, "RESTRICTED", None, "403 unsubscribed/restricted region", url)
        return Row(cid, name, ticker, exch, symbol, "ERROR", None, (res.log or "no response")[:120], url)

    cd = ((res.data.get("summary") or {}).get("company_data") or {}) if isinstance(res.data, dict) else {}
    gf_name = _name_from_company_data(cd)
    if not gf_name:
        return Row(cid, name, ticker, exch, symbol, "NOT_FOUND", None, "200 but no company name in payload", url)
    if _name_key(gf_name) == _name_key(name):
        return Row(cid, name, ticker, exch, symbol, "OK", gf_name, "name matches", url)
    return Row(cid, name, ticker, exch, symbol, "MISMATCH", gf_name, f"GF says '{gf_name}'", url)


def main() -> int:
    ap = argparse.ArgumentParser(description="GuruFocus link check across companies.")
    ap.add_argument("--limit", type=int, default=None, help="cap number of companies checked")
    ap.add_argument("--ids", type=str, default=None, help="comma-separated company_ids to check (overrides scope)")
    ap.add_argument("--include-delisted", action="store_true", help="also check delisted / out-of-scope companies")
    ap.add_argument("--apply", action="store_true", help="set gurufocus_lookup_failed_at on NOT_FOUND rows")
    ap.add_argument("--out", type=str, default="gurufocus_link_check.csv", help="CSV report path")
    ap.add_argument("--progress-every", type=int, default=25, help="emit a progress line every N companies")
    args = ap.parse_args()

    ids = [int(x) for x in args.ids.split(",") if x.strip()] if args.ids else None
    companies = _load_companies(include_delisted=args.include_delisted, ids=ids)
    if args.limit:
        companies = companies[: args.limit]

    total = len(companies)
    print(f"[check] {total} companies to check "
          f"({'including delisted/oos' if args.include_delisted else 'active only'})"
          f"{' [ids filter]' if ids else ''}. ~{total * 1.5 / 60:.0f} min.\n", flush=True)

    tally: dict[str, int] = {}
    rows: list[Row] = []
    t0 = time.time()
    for i, c in enumerate(companies, 1):
        r = check_one(c)
        rows.append(r)
        tally[r.status] = tally.get(r.status, 0) + 1
        if r.status in BAD:
            # Surface every bad link the moment we find it.
            print(f"  ! {r.status:<9} cid={r.company_id:<5} {r.exchange}:{r.ticker:<10} "
                  f"stored='{r.company_name}'"
                  + (f"  ->  GF='{r.gf_name}'" if r.gf_name else "")
                  + f"   {r.url}", flush=True)
        if i % args.progress_every == 0 or i == total:
            rate = i / max(1e-9, time.time() - t0)
            eta = (total - i) / max(1e-9, rate)
            summary = " ".join(f"{k}={v}" for k, v in sorted(tally.items()))
            print(f"  [{i}/{total}] {summary}  | {rate:.1f}/s ETA {eta/60:.0f}m", flush=True)

    # Write CSV report.
    out_path = Path(args.out)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company_id", "status", "exchange", "ticker", "symbol",
                    "stored_name", "gurufocus_name", "detail", "gurufocus_url"])
        for r in rows:
            w.writerow([r.company_id, r.status, r.exchange, r.ticker, r.symbol or "",
                        r.company_name, r.gf_name or "", r.detail, r.url or ""])

    bad = [r for r in rows if r.status in BAD]
    not_found = [r for r in rows if r.status == "NOT_FOUND"]
    print("\n==================== SUMMARY ====================")
    for k in ("OK", "NOT_FOUND", "MISMATCH", "RESTRICTED", "ERROR", "NO_SYMBOL"):
        if k in tally:
            print(f"  {k:<11} {tally[k]}")
    print(f"\n  flagged (NOT_FOUND + MISMATCH): {len(bad)}")
    print(f"  CSV report: {out_path.resolve()}")

    if args.apply and not_found:
        now = datetime.now(timezone.utc).isoformat()
        flagged = 0
        for r in not_found:
            try:
                sb.table("company").update({"gurufocus_lookup_failed_at": now}).eq(
                    "company_id", r.company_id
                ).execute()
                flagged += 1
            except Exception as e:  # noqa: BLE001
                print(f"  [apply] cid={r.company_id} failed: {type(e).__name__}: {e}", flush=True)
        print(f"\n  [apply] set gurufocus_lookup_failed_at on {flagged} NOT_FOUND companies.")
        print("  (MISMATCH rows left untouched — review the CSV; they may be benign name diffs.)")
    elif not_found:
        print("\n  Re-run with --apply to set gurufocus_lookup_failed_at on the NOT_FOUND rows.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
