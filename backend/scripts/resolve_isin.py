"""Resolve ONE named equity ISIN that never resolved — the targeted counterpart to the
bulk sweep, for when a specific company you care about is sitting `not_found`.

WHY THIS EXISTS
    `asset_resolve_missing.py` sweeps everything QUEUED-FIRST, so a single `not_found` retry
    you actually care about is at the back of a ~10,000-row queue. The `--isin` repointers
    can't stand in: `repoint_primary_listing.py` detects on ADV / market cap and needs an
    incumbent listing to compare against, and `repoint_etf_listing.py` gates with `_same_fund`
    + `_consensus_anchor`, which are right for a share class and wrong for an operating
    company. Nothing resolved a NAMED, NEVER-RESOLVED equity. This does.

⚠ `not_found` IS OFTEN A LIE, AND THAT IS THE WHOLE POINT
    Yahoo answers an overloaded caller with an EMPTY RESULT, not a 429 — so a bulk sweep that
    hits the soft throttle writes `not_found` over perfectly good companies, and the marker is
    indistinguishable from a real absence. Measured: SMIC (`KYG8020E1199`) sat `not_found`
    while `0981.HK` does EUR 1.06bn/day with 22.4 years of history. A stuck `not_found` on a
    liquid name is a retry candidate, not a verdict.

⚠ IT WILL NOT TOUCH AN ALREADY-RESOLVED ROW WITHOUT `--force`
    Re-resolving is DESTRUCTIVE and silently so — under load Yahoo's empty candidate set hands
    the win to whatever thin foreign line survives, which is how Alphabet went from GOOGL
    (EUR 8.79bn/day) to GOOA.VI Vienna (EUR 76,634/day), a 75,000x thinner listing, with no
    error anywhere. Same rule as the `/store` endpoint's 409. `--force` additionally refuses
    to DOWNGRADE: a replacement must be more liquid than the incumbent, so a throttled miss
    on the primary can never strand a row worse than it already was.

SAFETY IS STRUCTURAL, NOT HEURISTIC
    Candidates are enumerated from OpenFIGI's listings OF THIS ONE ISIN, so they are by
    construction the venues this exact security trades on — a different company cannot enter
    the pool. The name gate that remains guards the other failure: `build_candidates`
    CONSTRUCTS `ticker + venue suffix`, and tickers are reused across venues, so a constructed
    symbol can land on an unrelated instrument. That check is `same_company` (which strips
    corporate forms), never a raw score floor — "NVIDIA Corporation" vs "NVIDIA CORP" scores
    75.9 against an 80 floor, and that false reject is what once put NVDA on Stuttgart.

    uv run python scripts/resolve_isin.py --isin KYG8020E1199              # dry run
    uv run python scripts/resolve_isin.py --isin KYG8020E1199 --apply
    uv run python scripts/resolve_isin.py --isin A --isin B --apply
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402,F401  # loads env + Supabase before the pipeline imports
from deps import supabase  # noqa: E402

from asset_pipeline import openfigi, store  # noqa: E402
from asset_pipeline.fast_resolve import _score_retry, build_candidates, fast_resolve  # noqa: E402
from asset_pipeline.yahoo import YahooThrottled  # noqa: E402


def _existing(isin: str) -> dict | None:
    rows = (supabase.table("asset_execution")
            .select("execution_id,isin,yahoo_symbol,status,analysis_id,med_adv_eur,name")
            .eq("isin", isin).execute().data or [])
    if not rows:
        return None
    # Prefer a resolved row if several executions share the ISIN — that is the incumbent the
    # overwrite guard has to protect.
    rows.sort(key=lambda r: (r.get("status") == "ok", r.get("med_adv_eur") or 0), reverse=True)
    return rows[0]


def _resolve_one(isin: str, *, apply: bool, force: bool, yahoo_isin: bool) -> str:
    """Returns a one-word outcome; prints its own detail."""
    isin = isin.strip().upper()
    print(f"\n=== {isin} ===", flush=True)

    cur = _existing(isin)
    if cur is None:
        print("   no asset_execution row — this ISIN is not in the grid at all.")
        print("   Add it first (/asset-pipeline ISIN box, or scripts/ingest_universe_assets.py).")
        return "absent"

    old_adv = float(cur.get("med_adv_eur") or 0)
    print(f"   current: status={cur.get('status')!r} symbol={cur.get('yahoo_symbol')!r} "
          f"analysis_id={cur.get('analysis_id')} adv_eur={old_adv:,.0f}")

    if cur.get("status") == "ok" and cur.get("analysis_id") is not None and not force:
        print("   REFUSED — already resolved. Re-resolving is destructive (see module docstring);")
        print("   pass --force if you genuinely intend to repoint it.")
        return "refused"

    # ── candidates, from OpenFIGI's listings of THIS ISIN ──────────────────────────────
    try:
        figi_rows = openfigi.lookup_isin(isin) or []
    except Exception as e:  # noqa: BLE001
        print(f"   ERROR OpenFIGI lookup failed: {type(e).__name__}: {e}")
        return "error"
    # `extract_columns` collapses the listings into the 5 `openfigi_*` columns — both the name
    # anchor and the shape `upsert_asset(figi=...)` expects. Passing the raw list instead is an
    # AttributeError at write time, i.e. AFTER the Yahoo calls have been spent.
    fig = openfigi.extract_columns(figi_rows)
    figi_name = fig.get("openfigi_name")
    print(f"   OpenFIGI listings: {len(figi_rows)}   name={figi_name!r}")
    if not figi_rows:
        print("   No OpenFIGI listings — nothing to build a candidate from.")
        return "no-candidates"

    cands = build_candidates(isin, figi_rows, None, yahoo_isin=yahoo_isin)
    print(f"   candidates: {cands or '(none)'}")
    for sym in cands:
        try:
            sc = _score_retry(sym)
        except YahooThrottled:
            print("   Yahoo HARD rate-limit — stopping.")
            return "throttled"
        if sc:
            print(f"      {sym:14} adv_eur={(sc.get('med_adv_eur') or 0):>16,.0f}  "
                  f"name={sc.get('name')}")
        else:
            print(f"      {sym:14} -- empty --")

    try:
        res = fast_resolve(isin, figi_rows, figi_name)
    except YahooThrottled:
        print("   Yahoo HARD rate-limit — stopping.")
        return "throttled"

    if not res or not res.get("analysis"):
        print("   UNRESOLVED — no candidate validated.")
        print("   An empty result under load is the throttle signature, not proof of absence;")
        print("   re-run later before concluding the listing does not exist.")
        return "unresolved"

    an, ex = res["analysis"], (res.get("execution") or {})
    new_adv = float(ex.get("med_adv_eur") or 0)
    print(f"   RESOLVED -> {an.get('symbol')}  ({ex.get('currency') or '?'}, "
          f"{ex.get('exchange') or '?'})  adv_eur={new_adv:,.0f}  from {ex.get('first_date')}")

    # ⚠ Upgrade-only. A forced repoint must never leave the row on a THINNER listing than it
    #   already had — that is precisely the damage a throttled miss on the primary causes.
    if force and old_adv > 0 and new_adv <= old_adv:
        print(f"   KEPT — incumbent is more liquid (EUR {old_adv:,.0f} >= EUR {new_adv:,.0f}); "
              "not downgrading.")
        return "kept"

    if not apply:
        print("   dry run — re-run with --apply to persist")
        return "would-write"

    ids = store.upsert_asset(res, figi=fig)
    bars = store.store_series(ids["analysis_id"], an["symbol"], an.get("first_ts"))
    print(f"   WROTE analysis_id={ids['analysis_id']}  {bars:,} bars")
    return "written"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--isin", action="append", required=True,
                    help="ISIN to resolve (repeatable)")
    ap.add_argument("--apply", action="store_true", help="persist (default: dry run)")
    ap.add_argument("--force", action="store_true",
                    help="also repoint an already-resolved row (upgrade-only; see docstring)")
    ap.add_argument("--no-yahoo-isin", action="store_true",
                    help="skip Yahoo's own ISIN resolution as a candidate source")
    args = ap.parse_args()

    outcomes: dict[str, int] = {}
    wrote = False
    for isin in args.isin:
        try:
            out = _resolve_one(isin, apply=args.apply, force=args.force,
                               yahoo_isin=not args.no_yahoo_isin)
        except Exception as e:  # noqa: BLE001
            print(f"   ERROR {type(e).__name__}: {e}")
            out = "error"
        outcomes[out] = outcomes.get(out, 0) + 1
        wrote = wrote or out == "written"
        if out == "throttled":
            break

    if wrote:
        # A new execution can change which listing of an analysis asset is the default.
        store.set_default_executions()
        print("\nrefreshed default executions")

    print("\n" + "  ".join(f"{k}={v}" for k, v in sorted(outcomes.items())))
    if outcomes.get("written"):
        # `universe_asset_membership` is a VIEW over the ISIN bridge (migration 20260806060000),
        # so a newly-resolved row joins its universes the moment it exists - nothing to re-run.
        print("\nThe row now bridges to the company world, so it appears in its benchmark "
              "universes automatically.")


if __name__ == "__main__":
    main()
