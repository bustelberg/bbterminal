"""Repoint ONE ISIN to a NAMED Yahoo symbol. The deliberate, no-ranking repointer.

WHY THIS EXISTS BESIDE THE OTHER TWO
    `repoint_primary_listing.py` finds a better listing by ADV ÷ market cap, and
    `repoint_etf_listing.py` enumerates an ISIN's venues from OpenFIGI and picks the most liquid.
    Both RANK, and both therefore refuse when they cannot trust the candidate set. Measured on
    US8740391003 (the TSMC ADR, sitting on TSMN.MX at EUR 473k/day):

        repoint_primary_listing  -> only surfaced TSFA.VI (EUR 5,949/day); its name-search
                                    candidate set never reached TSM at all.
        repoint_etf_listing      -> FOUND TSM at EUR 4.96bn/day and still refused:
                                    "TSMN.MX is not among this ISIN's listings, so the candidate
                                    set is incomplete. NOT judged."

    That refusal is correct and it is also a dead end: the incumbent is wrong BY CONSTRUCTION
    (TSMN.MX is not a listing of that ISIN), which is exactly when a ranker has nothing to rank
    against. So this script does not rank. A human names the symbol; the script verifies it has a
    real price series and stores it.

⚠ IT STILL REFUSES A ZERO-BAR SYMBOL. Naming a symbol by hand does not make it a listing — the
    GODE.DE incident wrote ten structured products onto one empty series with `status='ok'`. A
    target that probes to no bars is refused here too, exactly as the automatic paths refuse it.

⚠ AN ADR AND ITS ORDINARY ARE NOT INTERCHANGEABLE, AND THIS SCRIPT WILL HAPPILY DO EITHER.
    TSMC is 1 ADR = 5 ordinary shares. Pointing both ISINs at TSM would price the ordinary 5x too
    high, silently. The pairing that is correct here is:

        US8740391003 (ADR)      -> TSM       NYSE, USD
        TW0002330008 (ordinary) -> 2330.TW   Taiwan, TWD

    The script prints the incumbent and the target side by side so that decision is visible before
    `--apply`.

    cd backend && uv run python scripts/repoint_to_symbol.py --isin US8740391003 --symbol TSM
    cd backend && uv run python scripts/repoint_to_symbol.py --isin US8740391003 --symbol TSM --apply
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402, F401  — loads SUPABASE_* / OPENFIGI_API_KEY before anything else
from deps import supabase  # noqa: E402
from asset_pipeline import openfigi, store  # noqa: E402
from asset_pipeline.resolve import resolve_analysis_instrument  # noqa: E402
from asset_pipeline.fast_resolve import _score_retry  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--isin", required=True)
    ap.add_argument("--symbol", required=True, help="the Yahoo symbol to point this ISIN at")
    ap.add_argument("--apply", action="store_true", help="persist (default: dry run)")
    a = ap.parse_args()

    isin, target = a.isin.strip().upper(), a.symbol.strip()

    row = (supabase.table("asset_grid").select("isin,name,analysis_symbol,currency,med_adv_eur,"
                                               "asset_class,bars,price_from,price_to")
           .eq("isin", isin).limit(1).execute().data or [])
    if not row:
        print(f"!! {isin} is not in the grid.")
        return 1
    r = row[0]
    print(f"\n  {isin}  {r.get('name') or ''}")
    print(f"    incumbent  {str(r.get('analysis_symbol')):<12} {str(r.get('currency')):<5} "
          f"EUR {float(r.get('med_adv_eur') or 0):>15,.0f}/day  "
          f"{r.get('bars')} bars {r.get('price_from')}..{r.get('price_to')}")

    # ⚠ PROBE BEFORE STORING. A named symbol is a claim, not a listing.
    sc = _score_retry(target)
    if not sc or not float(sc.get("med_adv_eur") or 0):
        print(f"    target     {target:<12} !! no price series — refusing. "
              f"A symbol with no bars is not a listing (the GODE.DE incident).")
        return 1
    print(f"    target     {target:<12} {str(sc.get('currency')):<5} "
          f"EUR {float(sc.get('med_adv_eur') or 0):>15,.0f}/day  {sc.get('name')}")

    old_adv = float(r.get("med_adv_eur") or 0)
    new_adv = float(sc.get("med_adv_eur") or 0)
    if old_adv:
        print(f"    -> {new_adv / old_adv:,.1f}x the incumbent's liquidity")

    if not a.apply:
        print("\n  dry run — re-run with --apply to persist\n")
        return 0

    figi_rows = openfigi.lookup_isin(isin)
    fig = {}
    if figi_rows:
        f0 = figi_rows[0]
        fig = {"openfigi_figi": f0.get("figi"), "openfigi_name": f0.get("name"),
               "openfigi_ticker": f0.get("ticker"), "openfigi_type": f0.get("securityType2")}
    sc["eligible"] = True
    ai = resolve_analysis_instrument(sc, r.get("asset_class") or "equity")
    res = {
        "input": isin, "id_type": "isin",
        "asset_class": ai["analysis_asset_class"], "wrapper": ai["wrapper"],
        "is_leveraged": ai["is_leveraged"], "candidates": [sc],
        "execution": ai["execution"], "analysis": ai["analysis"],
        "chosen": ai["analysis"], "underlying": None,
        "reason": f"Repointed to {target} by hand — named target, not a ranked pick.",
        "analysis_note": ai["analysis_note"], "sector": ai["analysis_asset_class"],
        "candles": None, "ibkr": None,
    }
    ids = store.upsert_asset(res, figi=fig)
    rows = store.store_series(ids["analysis_id"], ai["analysis"]["symbol"],
                              ai["analysis"].get("first_ts"))
    if not rows:
        print(f"    !! {target} stored 0 bars — NOT a usable listing. Nothing was repointed.")
        return 1
    store.set_default_executions()
    # ⚠ A repoint writes `asset_execution` per ISIN and would hand an ALIASED or OVERRIDDEN row a
    # listing of its own again. Put both back before returning, or the override lasts until the
    # next run. (A repoint of the overridden ISIN itself is a no-op here — it already names the
    # pinned symbol — so this cannot fight the thing the user just asked for.)
    from asset_pipeline.isin_alias import apply_aliases  # noqa: PLC0415
    from asset_pipeline.symbol_override import apply_symbol_overrides  # noqa: PLC0415

    n = apply_aliases()
    m = apply_symbol_overrides()
    print(f"    stored {rows:,} bars on {target}."
          + (f"  ({n} alias row(s) re-applied.)" if n else "")
          + (f"  ({m} symbol override(s) re-applied.)" if m else "") + "\n")

    # ⚠ A HAND REPOINT IS NOT DURABLE ON ITS OWN. Nothing here records the decision, so the next
    # by-name resolution can undo it and no other database learns of it. Say so, every time.
    pinned = (supabase.table("asset_symbol_override").select("isin")
              .eq("isin", isin).limit(1).execute().data or [])
    if not pinned:
        print(f"  NOTE  this repoint is not recorded. To make it survive a re-resolve and to\n"
              f"        carry it to another database, store it:\n"
              f"          insert into asset_symbol_override (isin, yahoo_symbol, note)\n"
              f"          values ('{isin}', '{target}', '<why>');\n"
              f"        then: uv run python scripts/apply_symbol_overrides.py\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
