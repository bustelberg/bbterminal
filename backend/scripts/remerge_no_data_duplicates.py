"""Remediate "no data" companies that are actually duplicates of an
existing-with-data row.

The pattern:
  - Leonteq's `_match_company` looked up its scraped ticker (e.g. `2628`
    for China Life H-share) against `company.gurufocus_ticker` without
    HKSE 5-digit padding, missed the existing `HKSE:02628` row, fell
    through to OpenFIGI auto-resolution, and created a German
    depositary stub (`XTER:CHL`) that has no GuruFocus data.

  - Result: TWO `company` rows for the same issuer. The correct one
    has data, the duplicate has none. `universe_membership` on the
    duplicate row stores the source ticker (`2628`) — which is exactly
    the ticker we need to look up the canonical row.

Algorithm:
  For each no-data candidate:
    1. Pull its `universe_membership` rows; collect their
       `universe_ticker` values.
    2. For each candidate ticker, look up the company table for a
       different cid with the same ticker (or its HKSE-padded form).
    3. If exactly ONE such cid is found AND it has price data, treat
       it as the canonical row.
    4. With `--apply`: move every FK (universe_membership, metric_data,
       portfolio_weight, ...) from the broken cid to the canonical cid,
       then delete the broken cid. Same primitives `ingest.dedupe`
       uses for merge_existing_duplicates.

Reuses the dedupe primitives so we don't reimplement FK reassignment.
Dry-run by default.

Usage (from backend/):
    uv run python scripts/remerge_no_data_duplicates.py            # dry run
    uv run python scripts/remerge_no_data_duplicates.py --apply    # write
    uv run python scripts/remerge_no_data_duplicates.py --limit 5  # test slice
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from supabase import create_client


def load_env() -> None:
    backend_dir = Path(__file__).resolve().parents[1]
    load_dotenv(backend_dir / ".env")
    load_dotenv(backend_dir / ".env.local", override=True)


def get_supabase():
    import os
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


# Exchanges outside the GuruFocus subscription -- their "no data" is
# expected, not a duplicate-row symptom. Keep in sync with
# `_KNOWN_UNSUBSCRIBED_EXCHANGES` in
# `routers/momentum/backtest_stream/audit.py`.
_UNSUBSCRIBED_EXCHANGES = {
    "LSE", "JSE", "BSE", "NSE", "BMV", "ASX", "NZE", "MOEX",
}


def _candidate_forms(ticker: str) -> list[str]:
    """Forms to probe the canonical company by. Adds the HKSE-padded
    variant for short numeric tickers."""
    t = (ticker or "").strip().upper()
    out = [t] if t else []
    if t and t.isdigit() and len(t) < 5:
        out.append(t.zfill(5))
    return out


# Corporate suffixes / boilerplate to strip before comparing names.
# Tickers like "BN" cross-collide (Brookfield in Canada, Danone in
# France); without a name-similarity check the merge would mis-merge
# unrelated issuers. This list intentionally over-includes common
# suffixes — false matches would cost more than false rejections.
_NAME_NOISE = {
    "ltd", "inc", "corp", "corporation", "company", "co", "plc", "sa",
    "se", "ag", "nv", "bv", "kgaa", "spa", "spa.", "asa", "ab", "as",
    "ord", "ordinary", "shares", "share", "class", "the", "of", "and",
    "&", "group", "holdings", "holding",
    # Share-class qualifiers seen in iShares names: "LTD H", "LTD A",
    # "LTD B", etc. Single letters get dropped along with these.
    "a", "b", "c", "h",
}


def _name_tokens(name: str) -> set[str]:
    """Tokenize for fuzzy-match purposes. Lowercase, strip punctuation,
    drop noise/suffix words."""
    if not name:
        return set()
    import re  # noqa: PLC0415
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", name.lower())
    return {
        w for w in cleaned.split()
        if w and w not in _NAME_NOISE
    }


def _names_match(a: str, b: str) -> bool:
    """Loose name-similarity. Accept when the two names share at least
    two significant tokens AND at least one token of >= 4 characters.
    Catches "China Life Insurance Co Ltd" vs "CHINA LIFE INSURANCE LTD
    H" (3+ tokens overlap), rejects "Brookfield Corp" vs "DANONE SA"
    (0 overlap)."""
    ta = _name_tokens(a)
    tb = _name_tokens(b)
    if not ta or not tb:
        return False
    overlap = ta & tb
    if len(overlap) < 2:
        return False
    # Require at least one meaningful word (>= 4 chars). "BN" sharing
    # "the" + "group" with another row shouldn't be enough.
    return any(len(w) >= 4 for w in overlap)


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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually merge the duplicates. Default: dry-run report only.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Cap candidates to N for testing. Default: all.",
    )
    args = parser.parse_args()

    load_env()
    sb = get_supabase()

    # Step 1: find no-data candidates (same logic as
    # probe_no_data_companies). Per-cid HEAD checks rather than the
    # company_latest_close_price_dates RPC, which times out at scale.
    print("Loading non-delisted companies...")
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
            if not ticker or not exch:
                continue
            if exch in _UNSUBSCRIBED_EXCHANGES:
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

    print(f"  {len(out)} non-delisted companies in DB")

    print("Checking close_price coverage...")
    from concurrent.futures import ThreadPoolExecutor  # noqa: PLC0415

    def _check(cid: int) -> tuple[int, bool]:
        return cid, _has_price_data(sb, cid)

    has_data: set[int] = set()
    with ThreadPoolExecutor(max_workers=16) as pool:
        for cid, ok in pool.map(_check, [c["company_id"] for c in out]):
            if ok:
                has_data.add(cid)

    no_data = [c for c in out if c["company_id"] not in has_data]
    print(f"  {len(no_data)} have zero close_price rows")
    if args.limit is not None:
        no_data = no_data[: args.limit]
        print(f"  (limited to {len(no_data)} for this run)")

    if not no_data:
        print("Nothing to do.")
        return 0

    # Build a fast `ticker -> cid` index over WITH-data companies.
    by_ticker_with_data: dict[str, dict] = {}
    for c in out:
        if c["company_id"] not in has_data:
            continue
        by_ticker_with_data[c["ticker"].strip().upper()] = c

    print()
    print("Resolving duplicates via universe_membership.universe_ticker...")

    merges: list[tuple[dict, dict, set[str]]] = []  # (broken, canonical, source_tickers)
    skipped: list[tuple[dict, str]] = []

    for broken in no_data:
        cid = broken["company_id"]
        um_resp = (
            sb.table("universe_membership")
            .select("universe_ticker")
            .eq("company_id", cid)
            .execute()
        )
        source_tickers = {
            ((r.get("universe_ticker") or "").strip().upper())
            for r in (um_resp.data or [])
            if r.get("universe_ticker")
        }
        if not source_tickers:
            skipped.append((broken, "no universe_membership rows"))
            continue

        candidates: dict[int, dict] = {}
        for src in source_tickers:
            for form in _candidate_forms(src):
                m = by_ticker_with_data.get(form)
                if m and m["company_id"] != cid:
                    candidates[m["company_id"]] = m

        if not candidates:
            skipped.append((broken, f"no canonical match for tickers {sorted(source_tickers)}"))
            continue

        # Filter to candidates whose name actually resembles the broken
        # row's name. Same-ticker-on-different-exchange collisions
        # (e.g. TSX:BN Brookfield vs XPAR:BN Danone) get rejected here.
        name_matched: dict[int, dict] = {
            cid_other: row for cid_other, row in candidates.items()
            if _names_match(broken["company_name"], row["company_name"])
        }
        if not name_matched:
            cids_str = sorted(candidates.keys())
            skipped.append((
                broken,
                f"ticker match(es) {cids_str} but no name overlap "
                f"({broken['company_name'][:30]!r} vs "
                f"{[(c['company_id'], c['company_name'][:30]) for c in candidates.values()]})",
            ))
            continue
        if len(name_matched) > 1:
            cids_str = sorted(name_matched.keys())
            skipped.append((broken, f"ambiguous: {len(name_matched)} name-matched candidates {cids_str}"))
            continue

        canonical = next(iter(name_matched.values()))
        merges.append((broken, canonical, source_tickers))

    print()
    print(f"Plan: {len(merges)} unambiguous merges, {len(skipped)} skipped")
    print()
    if merges:
        print(f"=== Merges ({len(merges)}) ===")
        for broken, canonical, srcs in merges[:50]:
            srcs_str = ",".join(sorted(srcs))
            print(
                f"  loser  cid={broken['company_id']:>5} {broken['exchange']}:{broken['ticker']:<10} "
                f"({broken['company_name'][:40]})"
            )
            print(
                f"  winner cid={canonical['company_id']:>5} {canonical['exchange']}:{canonical['ticker']:<10} "
                f"({canonical['company_name'][:40]})  via universe_ticker={srcs_str}"
            )
            print()
        if len(merges) > 50:
            print(f"  ... and {len(merges) - 50} more")
            print()
    if skipped:
        print(f"=== Skipped ({len(skipped)}) ===")
        for broken, reason in skipped[:30]:
            print(
                f"  cid={broken['company_id']:>5} {broken['exchange']}:{broken['ticker']:<10} "
                f"({broken['company_name'][:40]}) -- {reason}"
            )
        if len(skipped) > 30:
            print(f"  ... and {len(skipped) - 30} more")

    if not args.apply:
        print()
        print("Dry run only. Re-run with --apply to merge.")
        return 0

    if not merges:
        print("Nothing to merge.")
        return 0

    print()
    print(f"Applying {len(merges)} merges via ingest.dedupe primitives...")
    from ingest.dedupe import (  # noqa: PLC0415
        _move_metric_data,
        _move_simple_fk,
    )

    merged_ok = 0
    merged_err: list[tuple[int, str]] = []
    for broken, canonical, _srcs in merges:
        from_cid = broken["company_id"]
        to_cid = canonical["company_id"]
        try:
            _move_metric_data(sb, from_cid, to_cid)
            # FK tables that follow the same shape as
            # merge_existing_duplicates. Each dedup_keys list defines a
            # uniqueness constraint we should respect during the move
            # (rows that collide are dropped, not duplicated).
            _move_simple_fk(
                sb, "universe_membership", from_cid, to_cid,
                dedup_keys=["universe_id", "target_month"],
            )
            _move_simple_fk(
                sb, "portfolio_weight", from_cid, to_cid,
                dedup_keys=["portfolio_id", "target_date"],
            )
            _move_simple_fk(
                sb, "company_source", from_cid, to_cid,
                dedup_keys=["source_code"],
            )
            _move_simple_fk(
                sb, "leonteq_equity", from_cid, to_cid, dedup_keys=["isin"],
            )
            sb.table("company").delete().eq("company_id", from_cid).execute()
            merged_ok += 1
            print(
                f"  OK   cid={from_cid} -> {to_cid} "
                f"({broken['exchange']}:{broken['ticker']} -> "
                f"{canonical['exchange']}:{canonical['ticker']})"
            )
        except Exception as e:
            merged_err.append((from_cid, f"{type(e).__name__}: {e}"))
            print(f"  FAIL cid={from_cid}: {type(e).__name__}: {e}")

    print()
    print(f"Done: {merged_ok} merged, {len(merged_err)} failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
