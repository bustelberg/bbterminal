"""Remove GuruFocus rows for a company whose vendor data our own price history contradicts.

⚠⚠ THE MOTIVATING CASE. Diploma plc trades on the LSE, which our GuruFocus subscription does not
cover. GuruFocus did not refuse the request — it returned a complete statements payload whose price
column is **0 from 1998 to 2013** and then **frozen at 11.1 from 2016-09 to 2023-03** while the real
share price went £8.79 to £28.10, before stepping 3.81x in one period. `refuse_unsubscribed` now
stops this being fetched again; it cannot remove what is already stored, and the Fundamental modal
draws these rows today.

⚠⚠ THE SCOPE IS `source_code = 'gurufocus'`, AND THE COLUMN IS WHY THIS IS SAFE. Diploma's 21,411
`metric_data` rows are NOT all from the vendor: 20,898 carry `source_code='gurufocus'` (the
`annuals__`/`quarterly__` financial line items) and **513 carry `source_code='longequity'`** — the
research scores, ranks and qualitative fields that the LongEquity reports produce and that nothing
here is questioning. Deleting by company, or by an `annuals__`/`quarterly__` code prefix, would
have taken the flat-coded rows too or relied on a naming convention; the source column says
outright where a row came from. Verified: no other LSE company carries a single `quarterly__` row.

⚠⚠ IT PURGES ONLY WHAT THE EVIDENCE CONDEMNS, NOT EVERY UNSUBSCRIBED COMPANY. Two companies sit on
an exchange outside the subscription while holding vendor rows — Diploma and VERBUND AG (Vienna,
`WBO`). VERBUND's series AGREES with ours, which is the expected result if `WBO` is simply missing
from `FEASIBLE_GF_EXCHANGES` rather than genuinely unsubscribed. So every candidate is put through
the same cross-source check the audit uses (`ingest.earnings.price_sanity.compare`) and only a
FAILING one is offered for deletion. A coverage-map gap must not cost a company its data.

⚠ DRY RUN BY DEFAULT. Nothing is deleted without `--apply`, and `--apply` writes every row it is
about to remove to a JSON backup first (`--no-backup` to skip, which you should not).

Usage:
    uv run python scripts/purge_unsubscribed_vendor_data.py                 # show what would go
    uv run python scripts/purge_unsubscribed_vendor_data.py --apply         # do it, with a backup
    uv run python scripts/purge_unsubscribed_vendor_data.py --company 188   # one company
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402
from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: E402
from ingest.earnings.price_sanity import compare  # noqa: E402

VENDOR = "gurufocus"
PRICE_CODE = "quarterly__Valuation and Quality__Month End Stock Price"


def _paged(table: str, select: str, build, order: str) -> list[dict]:
    """⚠ PAGED. PostgREST truncates at 1,000 rows on cloud, and a purge that reads a truncated list
    would report a partial backup as complete."""
    out: list[dict] = []
    off = 0
    while True:
        rows = build(deps.supabase.table(table).select(select)).order(order) \
            .range(off, off + 999).execute().data or []
        if not rows:
            return out
        out += rows
        off += len(rows)


def _exchange_of(c: dict) -> str:
    return ((c.get("gurufocus_exchange") or {}) or {}).get("exchange_code") or ""


def candidates(only: int) -> list[dict]:
    """Companies on an exchange outside the subscription that nevertheless hold vendor rows."""
    comps = _paged("company", "company_id,company_name,gurufocus_ticker,isin,"
                              "gurufocus_exchange:gurufocus_exchange(exchange_code)",
                   lambda q: q, "company_id")
    if only:
        return [c for c in comps if c["company_id"] == only]
    return [c for c in comps if not is_gf_subscribed_exchange(_exchange_of(c))]


def vendor_rows(cid: int, select: str = "*") -> list[dict]:
    return _paged("metric_data", select,
                  lambda q: q.eq("company_id", cid).eq("source_code", VENDOR), "metric_code")


def our_closes(isin: str | None) -> list[tuple[str, float]]:
    """Our own yfinance series for this ISIN — the independent second opinion."""
    if not isin:
        return []
    ae = (deps.supabase.table("asset_execution").select("analysis_id")
          .eq("isin", isin).eq("status", "ok").limit(1).execute().data or [])
    if not ae:
        return []
    return [(str(r["target_date"]), float(r["close"]))
            for r in _paged("asset_price", "target_date,close",
                            lambda q: q.eq("analysis_id", ae[0]["analysis_id"]), "target_date")
            if r.get("close") is not None]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually delete (default: dry run)")
    ap.add_argument("--company", type=int, default=0, help="restrict to one company_id")
    ap.add_argument("--no-backup", action="store_true", help="skip the JSON backup (not advised)")
    ap.add_argument("--backup-dir", default=".", help="where to write the backup")
    args = ap.parse_args()

    print("DRY RUN - nothing will be deleted. Add --apply to act.\n" if not args.apply
          else "APPLY - rows will be DELETED.\n", flush=True)

    cands = candidates(args.company)
    print(f"{len(cands)} candidate compan{'y' if len(cands) == 1 else 'ies'} to examine\n",
          flush=True)

    condemned: list[tuple[dict, list[dict], str]] = []
    for c in cands:
        cid = c["company_id"]
        rows = vendor_rows(cid, "metric_code,target_date,numeric_value")
        if not rows:
            continue
        name = f"{c['company_name']} ({c['gurufocus_ticker']}, {_exchange_of(c)})"
        vendor_price = sorted((r["target_date"], r["numeric_value"] or 0.0)
                              for r in rows if r["metric_code"] == PRICE_CODE)
        verdict = compare(vendor_price, our_closes(c.get("isin")))
        if verdict.ok:
            # ⚠ A PASS IS LEFT ALONE, LOUDLY. This is the VERBUND case: an exchange missing from
            # the coverage map looks identical to an unsubscribed one from here, and its data is
            # fine. Saying so is what stops the next reader "tidying up" the whole list.
            print(f"  KEEP {name}: {len(rows):,} vendor rows - {verdict.reason}", flush=True)
            continue
        condemned.append((c, rows, verdict.reason))
        print(f"  PURGE {name}: {len(rows):,} vendor rows", flush=True)
        print(f"        {verdict.reason}", flush=True)
        for d in verdict.detail:
            print(f"        {d}", flush=True)

    if not condemned:
        print("\nNothing to purge.")
        return 0

    print()
    for c, rows, _ in condemned:
        # ⚠ WHAT SURVIVES IS NAMED, because a purge that silently took the research scores as well
        # would be discovered weeks later on a page nobody connected to this run.
        kept = _paged("metric_data", "source_code",
                      lambda q, i=c["company_id"]: q.eq("company_id", i)
                      .neq("source_code", VENDOR), "source_code")
        by_src: dict[str, int] = defaultdict(int)
        for r in kept:
            by_src[r["source_code"]] += 1
        print(f"company_id={c['company_id']} {c['company_name']}: "
              f"deleting {len(rows):,} '{VENDOR}' rows; "
              f"keeping {sum(by_src.values()):,} "
              f"({', '.join(f'{v:,} {k}' for k, v in sorted(by_src.items())) or 'none'})")

    if not args.apply:
        print("\nDry run complete. Re-run with --apply to delete.")
        return 0

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for c, _rows, reason in condemned:
        cid = c["company_id"]
        # ⚠⚠ THE BACKUP IS READ FRESH AND IN FULL, not reused from the summary above, which selected
        # three columns. A backup that cannot restore the row it deleted is not a backup.
        full = vendor_rows(cid, "*")
        if not args.no_backup:
            path = Path(args.backup_dir) / f"purge_{VENDOR}_{cid}_{stamp}.json"
            path.write_text(json.dumps(
                {"company_id": cid, "company_name": c["company_name"],
                 "exchange": _exchange_of(c), "reason": reason,
                 "source_code": VENDOR, "rows": full}, indent=1, default=str), encoding="utf-8")
            print(f"\nbacked up {len(full):,} rows -> {path}", flush=True)
        # ⚠⚠ ONE STATEMENT, BOTH COLUMNS. `company_id` alone would take the LongEquity research rows
        # with it — 513 of them on Diploma, from a source nothing here is questioning.
        #
        # ⚠⚠ AND IT IS DELIBERATELY **NOT** CHUNKED. A chunked version shipped first and was wrong
        # twice over. It chunked `metric_code` into `IN_CHUNK_SIZE` (200) groups, which PostgREST
        # encodes into the URL — and these codes are ~50 characters each
        # (`quarterly__Valuation and Quality__Month End Stock Price`), so the very first chunk came
        # back **414 URI too long**. `IN_CHUNK_SIZE` is calibrated for integer ids, not for long
        # strings; the guard it exists for is payload size, and a URI limit is a different limit.
        #
        # ⚠⚠ THE DEEPER ERROR WAS THE REASONING, WHICH WAS BACKWARDS. The chunking was justified by
        # the 8s `statement_timeout` on `authenticator` — "a timeout mid-way leaves a company
        # half-purged". A single DELETE is one transaction, so a timeout rolls ALL of it back and
        # leaves zero deleted; it is FOUR statements that create four independent transactions and
        # therefore the partial purge. Chunking manufactured the hazard it claimed to prevent.
        #
        # ⚠ This filter is also two short predicates, so there is no URI to overflow, and it is the
        # same shape `routers/companies.py` uses to drop a whole company's `metric_data`.
        deps.supabase.table("metric_data").delete() \
            .eq("company_id", cid).eq("source_code", VENDOR).execute()
        left = (deps.supabase.table("metric_data").select("company_id", count="exact")
                .eq("company_id", cid).eq("source_code", VENDOR).limit(1).execute().count)
        other = (deps.supabase.table("metric_data").select("company_id", count="exact")
                 .eq("company_id", cid).neq("source_code", VENDOR).limit(1).execute().count)
        print(f"deleted; {left} '{VENDOR}' rows remain, {other:,} other rows untouched", flush=True)

    # ⚠ ASCII ONLY IN PRINTED TEXT. This runs in PowerShell, whose console is cp1252 — a marker
    # glyph here raises UnicodeEncodeError and kills the run AFTER the delete but BEFORE the
    # summary, which is the worst moment for it to fail. (The audit script hit exactly that.)
    print("\nNOTE: the Fundamental modal will now show no vendor data for these companies,")
    print("      which is the intended outcome - it showed wrong data before.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
