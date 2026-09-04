"""Un-map ONE ISIN whose stored listing is a DIFFERENT instrument and has no right one to move to.

WHY THIS EXISTS BESIDE THE THREE REPOINTERS
    `repoint_primary_listing` / `repoint_etf_listing` / `repoint_to_symbol` all end by STORING a
    better symbol. They have nothing to offer when the honest answer is that no listing exists:

        AEN000101016  First Abu Dhabi Bank PJSC (ADX)
            stored     FAB     -> "First Trust Multi Cap Value AlphaDEX Fund", NasdaqGM, USD,
                                  4,822 bars from 2007 — a US ETF that happens to share the
                                  ticker. `identity_status` was already 'mismatch'.
            OpenFIGI   `FAB, FABAED` on Bloomberg composite codes (UH, DH, XB, XH, …) — no venue
                       suffix any candidate builder can turn into a Yahoo symbol.
            Yahoo      no ADX coverage at all. Measured with a working control (AAPL 23 bars,
                       ASML.AS 24, EMAAR.AE 25 on Dubai): FAB.AE / FAB.AD / FAB.DU / FAB.AB /
                       NBAD.AE / FGB.AE all return ZERO bars, as do ADCB.AE, IHC.AE and
                       ALDAR.AE — every Abu Dhabi name tried. `.AE` is Dubai, and Yahoo does not
                       carry ADX.

    So the choice is between a wrong series and no series, and no series is the honest one: an
    unpriceable holding reads as unpriceable everywhere, while a wrong one reads as a price.

⚠ IT REFUSES A ROW THAT IS NOT ALREADY FLAGGED. `identity_status='mismatch'` is the pipeline's own
  verdict, recorded at resolve time; requiring it means this script cannot be pointed at a healthy
  row by a typo. `--force` is there for a mismatch the flag missed, and says so in the output.

⚠ IT DOES NOT DELETE THE PRICE ROWS. They are real bars for a real instrument, they are simply
  filed under the wrong ISIN, and once the execution row stops pointing at them nothing reads them.
  Deleting is a separate, irreversible decision — `--drop-prices` takes it deliberately, and only
  when no OTHER execution shares the analysis.

⚠⚠ IT IS NOT DURABLE ON ITS OWN, AND THAT IS NOT THIS SCRIPT'S TO FIX. `queue.requeue_unmapped()`
  re-queues any `not_found` row OpenFIGI identified, and `requeue_suspects()` re-queues every
  `mismatch` — so the next `asset_fix_mismaps.py` run hands this ISIN back to the worker, which can
  resolve it onto the same ticker again. The durable fix is a gate at the STORE (refuse to file a
  `Common Stock` resolution whose name test fails), which is a change to the resolver and wants its
  own measurement: 110 rows carry `identity_status='mismatch'` today and most are funds whose
  OpenFIGI names are merely vowel-crushed ("VANG FTSE JPN USDA"), so a blanket gate would unmap
  correct rows.

    cd backend && uv run python scripts/unmap_asset_row.py --isin AEN000101016
    cd backend && uv run python scripts/unmap_asset_row.py --isin AEN000101016 --apply
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402, F401  — loads SUPABASE_* before anything reads them

from asset_pipeline import store  # noqa: E402
from asset_pipeline.store import MANUAL_UNMAP_PREFIX  # noqa: E402

DEFAULT_REASON = (f"{MANUAL_UNMAP_PREFIX}: the stored listing is a different instrument and no "
                  "Yahoo listing exists for this ISIN")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--isin", required=True)
    ap.add_argument("--reason", default=DEFAULT_REASON)
    ap.add_argument("--force", action="store_true",
                    help="proceed even if identity_status is not 'mismatch'")
    ap.add_argument("--drop-prices", action="store_true",
                    help="also delete the orphaned asset_price rows (only if unshared)")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    sb = deps.supabase
    # ⚠ `*`, SO THE PATCH BELOW CAN BE FILTERED TO COLUMNS THAT REALLY EXIST. `sector` looks like
    # an `asset_execution` column because `asset_grid` has one — the view reads it off the ANALYSIS
    # — and PostgREST answers an update naming a column that is not there with a 42703, at the one
    # moment this script is halfway through a write.
    rows = (sb.table("asset_execution").select("*").eq("isin", a.isin).execute().data or [])
    if not rows:
        print(f"  {a.isin}: no asset_execution row")
        return 1
    if len(rows) > 1:
        print(f"  {a.isin}: {len(rows)} execution rows — refusing, this script handles one")
        return 1
    r = rows[0]

    print(f"\n  {r['isin']}")
    print(f"    stored     {str(r['yahoo_symbol']):<12} {str(r['currency']):<5} "
          f"{str(r['exchange']):<12} EUR {float(r.get('med_adv_eur') or 0):>13,.0f}/day  "
          f"{r['name']}")
    print(f"    OpenFIGI   {str(r['openfigi_ticker']):<12} {' ' * 31}{r['openfigi_name']}")
    print(f"    status     {r['status']}  is_default={r['is_default']}  "
          f"identity_status={r['identity_status']}  analysis_id={r['analysis_id']}")

    if (r.get("identity_status") or "") != "mismatch" and not a.force:
        print("\n    !! identity_status is not 'mismatch' — refusing. This row is not flagged as a "
              "wrong mapping; pass --force if you have checked it by hand.\n")
        return 1

    # ⚠⚠ ALREADY UNMAPPED IS A SUCCESS, NOT AN ERROR — and it used to be a CRASH. A second run (or
    # a run after the one-shot repair) finds `analysis_id` NULL, and passing that to `.eq()` makes
    # PostgREST answer `invalid input syntax for type bigint: "None"` — a stack trace where the
    # right answer is "nothing to do". These scripts get re-run precisely because nobody is sure
    # what already landed, so being safe to re-run is most of their value.
    if r.get("analysis_id") is None:
        print("    prices     none — this row is already unmapped, nothing to do\n")
        return 0

    # ⚠ THE PRICE ROWS ARE ONLY ORPHANED IF NOBODY ELSE POINTS AT THE ANALYSIS. A share class or an
    #   ADR legitimately mapped to the same series must not lose its prices to this.
    others = [x for x in (sb.table("asset_execution").select("execution_id,isin")
                          .eq("analysis_id", r["analysis_id"]).execute().data or [])
              if x["execution_id"] != r["execution_id"]]
    n_prices = (sb.table("asset_price").select("analysis_id", count="exact")
                .eq("analysis_id", r["analysis_id"]).limit(1).execute()).count or 0
    print(f"    prices     {n_prices:,} row(s) under analysis {r['analysis_id']}; "
          f"{len(others)} other execution(s) share it"
          + (f" ({', '.join(x['isin'] for x in others[:5])})" if others else ""))

    if not a.apply:
        print("\n  dry run — re-run with --apply to persist\n")
        return 0

    # ⚠⚠ THE WHOLE RESOLVED IDENTITY GOES, NOT JUST THE STATUS. `asset_grid` is a VIEW that joins
    # the analysis and reads `asset_class` / `name` / `sector` off THIS row, so clearing the status
    # alone leaves every screen still calling an Abu Dhabi bank a First Trust ETF — and
    # `classify_holding` still reads `asset_class='etf'` and still says `fund`. Checked against
    # what a genuine `not_found` row looks like (`analysis_id`, `asset_class`, `sector`, venue and
    # liquidity all NULL, `is_default` false) rather than guessed field by field.
    #
    # ⚠ `name` BECOMES THE OPENFIGI NAME, which is the instrument this ISIN actually is. Leaving
    # the ETF's name would keep the wrong answer on screen with nothing pointing at it.
    # ⚠ THE WRITE ITSELF LIVES IN `store.unmap_execution`, NOT HERE. The one-shot repair script
    # (`fix_mismapped_assets.py`) does the same thing to fifteen rows, and two copies of "which
    # columns make a row unmapped" is two places for `asset_class` — the field that keeps
    # `classify_holding` answering `fund` — to be forgotten. See that function for why the whole
    # resolved identity goes, and why the reason carries `MANUAL_UNMAP_PREFIX` whatever the
    # operator typed.
    store.unmap_execution(a.isin, a.reason)
    print("    -> unmapped: status=not_found, is_default=False, analysis/venue/liquidity cleared, "
          f"name={(r.get('openfigi_name') or r.get('name'))!r}")

    if a.drop_prices:
        if others:
            print("    -> NOT dropping prices: another execution shares this analysis")
        else:
            sb.table("asset_price").delete().eq("analysis_id", r["analysis_id"]).execute()
            print(f"    -> dropped {n_prices:,} price row(s)")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
