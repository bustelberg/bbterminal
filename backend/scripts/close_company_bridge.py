"""Fill `company.isin` from the asset grid, so a company row can be reached from the asset world.

WHY THIS EXISTS
    `company` (GuruFocus) and `asset_execution` (Yahoo) are disjoint id spaces joined only by
    ISIN. Measured 2026-08-06: 296 of 2,790 company rows cannot make that hop, and every step of
    the asset-spine migration maps `company -> analysis_id`, so each one would silently drop them.
    They are not junk rows — they include Assurant, CSL, Alimentation Couche-Tard, and 159 Indian
    ACWI constituents.

WHAT IT CAN AND CANNOT CLOSE — MEASURED, NOT ESTIMATED

        A  direct ticker+name match against the asset grid          16   <- this script
        B  OpenFIGI compositeFIGI -> asset row -> its ISIN          15   <- this script
        C  OpenFIGI resolves the name, but NO asset row exists     105   x  needs a new asset row
        D  OpenFIGI cannot resolve the ticker at all               114   x  needs another source

    So this closes 31 of 250. It is deliberately not "the fix" — C and D are 219 rows, 158 of
    them Indian, and both need `asset_execution.isin` (`text NOT NULL UNIQUE`) to stop being the
    table's premise before a row can exist for them. That is a schema decision, not a backfill.

⚠⚠ OPENFIGI DOES NOT RETURN AN ISIN. Its `/v3/mapping` response carries figi, compositeFIGI,
    shareClassFIGI, ticker, name, exchCode — and no ISIN, at all. So the FIGI is only ever the
    MATCHING KEY here: OpenFIGI turns a ticker into a compositeFIGI, that FIGI finds the asset
    row, and the ISIN is read off THAT ROW. Anyone reading this expecting OpenFIGI to be the
    source of the identifier will look for a field that does not exist.

⚠ THE COMPOSITE FIGI IS THE RIGHT KEY, THE LISTING FIGI IS NOT. A FIGI is per-listing, so CSL on
    the ASX and the same company's Munich line have different ones — matching on `figi` would miss
    exactly the cross-listed rows this is for. `asset_execution.openfigi_figi` stores the COMPOSITE
    (verified: CSL's Munich row `CSJ.MU` carries `BBG000BKBN81`, which is what OpenFIGI returns as
    `compositeFIGI` for `CSL/AU`), and a composite is stable across venues.

⚠ NAME MATCHING IS A GATE, NEVER A SOURCE. Every candidate passes `same_company` before it is
    used, and it is load-bearing: `company` holds "Ashtead Group" while the asset grid holds
    `AT.L` "Ashtead Technology" — a different business with a near-identical name. Nothing here
    accepts a match on name alone.

⚠⚠ AND THE HARDEST GUARD: AN ISIN THAT ANOTHER COMPANY ALREADY HAS IS REFUSED. In this codebase
    two companies sharing an ISIN ARE the same security — `ingest/dedupe.py::dedupe_by_isin` MERGES
    them on the next ingest, moving memberships and dropping one row's prices. So a wrong ISIN
    written here does not show up as a wrong field; it shows up weeks later as a company that
    vanished. Every write is checked against every ISIN already in `company` first.

Dry run by default. Nothing is written without `--apply`.
"""
from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402, F401  — loads .env before the Supabase client is built
from asset_pipeline.resolve import same_company  # noqa: E402
from deps import supabase  # noqa: E402

_OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
# OpenFIGI's unkeyed limit is 25 requests/minute in batches of 10. The pause keeps a full sweep
# (~24 batches for 234 rows) comfortably inside it; with a key it could go much faster, but this
# runs once.
_BATCH = 10
_PAUSE_S = 2.6

# GuruFocus exchange code -> OpenFIGI (Bloomberg) exchCode.
#
# ⚠ NSE/BOM ARE HERE AND THEY DO NOT WORK. `ASIANPAINT` returns "No identifier found" under IS,
# IB, IN and under no exchCode at all — those tickers are not in OpenFIGI's free TICKER index.
# They are kept so the failure is attributable to the exchange rather than looking like a missing
# mapping, and so a keyed account can retest them by changing nothing but the credentials.
_EXCH = {
    "LSE": "LN", "ASX": "AU", "TSX": "CT", "TSXV": "CV", "NZSE": "NZ",
    "DUB": "ID", "NSE": "IS", "BOM": "IB", "NYSE": "US", "NAS": "US",
}


def _page(table: str, select: str, order: str, **filters) -> list[dict]:
    """Every row, paged and ORDERED ON A UNIQUE KEY.

    ⚠ PostgREST truncates SILENTLY at 1,000 rows on cloud, so the paging is mandatory.

    ⚠⚠ AND `order` IS LOAD-BEARING — WITHOUT IT THE PAGING SILENTLY SKIPS AND REPEATS ROWS.
    Postgres makes no promise about row order across separate LIMIT/OFFSET queries, so an unordered
    page boundary lands wherever it likes and lands somewhere different each run. This shipped
    without it: two runs of the (since-removed) membership backfill script minutes apart reported
    the S&P at 411 and 28 members against a truth of 503.

    ⚠ HERE THE CONSEQUENCE WAS WORSE THAN A WRONG COUNT. `taken` — every ISIN already in use — is
    what stops this script handing one ISIN to two companies, and two companies sharing an ISIN
    ARE one security (`dedupe_by_isin` merges them). A short read makes that guard incomplete
    without making it look incomplete. Verified after the fact on the 27 rows already written:
    2,563 distinct ISINs, zero duplicates — the under-read cost candidates, not correctness. That
    was luck, and this argument is what replaces it.
    """
    out: list[dict] = []
    off = 0
    while True:
        q = supabase.table(table).select(select).order(order)
        for k, v in filters.items():
            q = q.is_(k, v) if v == "null" else q.eq(k, v)
        rows = q.range(off, off + 999).execute().data or []
        if not rows:
            return out
        out += rows
        off += len(rows)


def _load() -> tuple[list[dict], list[dict], set[str]]:
    companies = _page(
        "company",
        "company_id,company_name,gurufocus_ticker,isin,"
        "gurufocus_exchange:gurufocus_exchange(exchange_code)",
        "company_id",
    )
    assets = _page("asset_execution",
                   "analysis_id,isin,yahoo_symbol,name,openfigi_name,openfigi_figi",
                   "isin")
    # Every ISIN already spoken for, so a proposal can never collide with one.
    taken = {(c["isin"] or "").strip().upper() for c in companies if c.get("isin")}
    return companies, assets, taken


def _asset_name(a: dict) -> str:
    return a.get("openfigi_name") or a.get("name") or ""


def _home_country(exch_by_code: dict[str, str], c: dict) -> str | None:
    code = ((c.get("gurufocus_exchange") or {}) or {}).get("exchange_code")
    return exch_by_code.get((code or "").upper())


def _country_ok(home: str | None, isin: str) -> bool:
    """⚠⚠ THE ADR GUARD, AND IT IS THE DIFFERENCE BETWEEN A BRIDGE AND A WRONG SECURITY.

    A ticker+name match is not enough, because an ADR carries its issuer's name and often its
    ticker while being a DIFFERENT security with a DIFFERENT ISIN. Measured on the first dry run:
    `HALEON PLC` (LSE) and `RENTOKIL INITIAL PLC` (LSE) both matched a NYSE asset row and would
    have been given `US4055521003` / `US7601251041` — the US ADRs, not the London ordinaries.
    Writing those makes the company row claim to be a security it is not, and since two rows
    sharing an ISIN ARE one security here, `dedupe_by_isin` would then merge it into whatever
    else holds that ADR.

    ⚠ VENUE EQUALITY IS THE WRONG TEST — it refuses the cases that are fine. Canadian Pacific is
    `TSX` for us and `NYSE` in the asset grid, one interlisted security under one `CA…` ISIN; and
    EchoStar/Bio-Techne/News Corp are `NYSE` for us and `NasdaqGS` there, which is our exchange
    field being wrong rather than a different instrument. All five are correct matches. The test
    that separates them from the ADRs is the ISIN's COUNTRY against the company's home market.

    ⚠ IT ERRS SAFE AND THAT IS DELIBERATE. A US-listed, foreign-domiciled issuer (Accenture, `IE…`
    on NYSE) is refused here even though the match is good. A false refusal costs nothing — the
    row stays exactly as it is today — while a false accept silently merges two companies weeks
    later. Unknown home country is refused for the same reason.
    """
    return bool(home) and isin[:2].upper() == home.upper()


def _match_direct(todo: list[dict], assets: list[dict]) -> dict[int, tuple[dict, str]]:
    """A: the company's GuruFocus ticker IS an asset row's Yahoo symbol, and the names agree."""
    by_tick: dict[str, list[dict]] = defaultdict(list)
    for a in assets:
        if a.get("yahoo_symbol"):
            by_tick[a["yahoo_symbol"].strip().upper()].append(a)
    hits: dict[int, tuple[dict, str]] = {}
    for c in todo:
        cands = [a for a in by_tick.get((c.get("gurufocus_ticker") or "").strip().upper(), [])
                 if same_company(c.get("company_name") or "", _asset_name(a))]
        # ⚠ EXACTLY ONE, OR NONE. Two asset rows passing the name gate means the ticker is reused
        # across venues by related instruments; picking either is a guess.
        if len(cands) == 1:
            hits[c["company_id"]] = (cands[0], "ticker")
    return hits


def _match_figi(todo: list[dict], assets: list[dict], *, verbose: bool) -> dict[int, tuple[dict, str]]:
    """B: ticker+exchCode -> OpenFIGI compositeFIGI -> the asset row carrying it."""
    by_figi = {a["openfigi_figi"]: a for a in assets if a.get("openfigi_figi")}
    hits: dict[int, tuple[dict, str]] = {}
    for i in range(0, len(todo), _BATCH):
        batch = todo[i:i + _BATCH]
        jobs = []
        for c in batch:
            code = ((c.get("gurufocus_exchange") or {}) or {}).get("exchange_code") or ""
            job = {"idType": "TICKER",
                   "idValue": (c.get("gurufocus_ticker") or "").replace("-", " ")}
            if _EXCH.get(code):
                job["exchCode"] = _EXCH[code]
            jobs.append(job)
        try:
            resp = requests.post(_OPENFIGI_URL, json=jobs,
                                 headers={"Content-Type": "application/json"}, timeout=30)
            resp.raise_for_status()
            items = resp.json()
        except Exception as e:  # noqa: BLE001
            # ⚠ REPORTED, NOT SWALLOWED. A silent batch failure would look identical to "these
            # companies are unresolvable", which is the wrong conclusion to draw from a 429.
            print(f"  ! OpenFIGI batch {i // _BATCH + 1} failed: {type(e).__name__}: {e}",
                  file=sys.stderr)
            time.sleep(_PAUSE_S)
            continue
        for c, item in zip(batch, items):
            data = [d for d in (item.get("data") or [])
                    if same_company(c.get("company_name") or "", d.get("name") or "")]
            hit = next((by_figi[d["compositeFIGI"]] for d in data
                        if d.get("compositeFIGI") in by_figi), None)
            if hit:
                hits[c["company_id"]] = (hit, "figi")
        if verbose:
            print(f"  … OpenFIGI {min(i + _BATCH, len(todo))}/{len(todo)} probed, "
                  f"{len(hits)} matched")
        time.sleep(_PAUSE_S)
    return hits


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--apply", action="store_true", help="persist the fills (default: dry run)")
    ap.add_argument("--no-openfigi", action="store_true",
                    help="skip route B (no network); only the direct ticker matches")
    ap.add_argument("--limit", type=int, default=0, help="stop after N fills (0 = all)")
    ap.add_argument("--quiet", action="store_true", help="suppress per-batch progress")
    args = ap.parse_args()

    print("Loading company + asset rows…")
    companies, assets, taken = _load()
    live = [c for c in companies
            if not c.get("isin")
            and c.get("gurufocus_ticker")]
    print(f"  company rows {len(companies):,} · asset rows {len(assets):,} · "
          f"ISINs already in use {len(taken):,}")
    print(f"  candidates (live, no ISIN, has a ticker): {len(live)}")

    hits = _match_direct(live, assets)
    print(f"  A. direct ticker+name match: {len(hits)}")

    if not args.no_openfigi:
        rest = [c for c in live if c["company_id"] not in hits]
        print(f"  B. probing OpenFIGI for the remaining {len(rest)} "
              f"(~{-(-len(rest) // _BATCH)} batches, ~{_PAUSE_S}s apart)…")
        hits.update(_match_figi(rest, assets, verbose=not args.quiet))
        print(f"  B. via compositeFIGI: {sum(1 for v in hits.values() if v[1] == 'figi')}")

    exch_by_code = {e["exchange_code"].upper(): e.get("country_code")
                    for e in (supabase.table("gurufocus_exchange")
                              .select("exchange_code,country_code").execute().data or [])
                    if e.get("exchange_code")}
    by_id = {c["company_id"]: c for c in companies}
    proposals: list[tuple[dict, str, str]] = []
    refused: list[tuple[str, str]] = []
    claimed: dict[str, str] = {}

    for cid, (asset, how) in sorted(hits.items()):
        c = by_id[cid]
        isin = (asset.get("isin") or "").strip().upper()
        name = c.get("company_name") or str(cid)
        if not isin:
            refused.append((name, "the matched asset row has no ISIN"))
            continue
        home = _home_country(exch_by_code, c)
        if not _country_ok(home, isin):
            refused.append((name, f"{isin} is not a {home or '?'} security — "
                                  f"likely an ADR or cross-listing, not this row"))
            continue
        # ⚠⚠ THE GUARD THAT MATTERS. Two companies with one ISIN are ONE security here, and
        # `dedupe_by_isin` will merge them on the next ingest — the damage surfaces later, as a
        # disappeared company, not as a bad field.
        if isin in taken:
            refused.append((name, f"{isin} already belongs to another company row"))
            continue
        if isin in claimed:
            refused.append((name, f"{isin} also proposed for {claimed[isin]} — ambiguous"))
            continue
        claimed[isin] = name
        proposals.append((c, isin, how))
        if args.limit and len(proposals) >= args.limit:
            break

    print()
    print(f"{'PROPOSED' if not args.apply else 'APPLYING'} {len(proposals)} fills:")
    for c, isin, how in proposals:
        print(f"  {c['company_id']:>6}  {(c.get('company_name') or '')[:34]:36} "
              f"{(c.get('gurufocus_ticker') or ''):12} -> {isin}  ({how})")
    if refused:
        print()
        print(f"REFUSED {len(refused)} (never silently — each names its reason):")
        for name, why in refused:
            print(f"  {name[:34]:36} {why}")

    if not args.apply:
        print()
        print("Dry run — nothing written. Re-run with --apply to persist.")
        return 0

    written = 0
    for c, isin, _how in proposals:
        try:
            (supabase.table("company").update({"isin": isin})
             .eq("company_id", c["company_id"])
             # ⚠ ONLY IF IT IS STILL NULL. The read happened minutes ago; another ingest may have
             # filled it since, and overwriting a fresher value with a stale proposal is the one
             # way this script could destroy information rather than add it.
             .is_("isin", "null").execute())
            written += 1
        except Exception as e:  # noqa: BLE001
            print(f"  ! {c.get('company_name')}: {type(e).__name__}: {e}", file=sys.stderr)
    print()
    print(f"Wrote {written} of {len(proposals)}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
