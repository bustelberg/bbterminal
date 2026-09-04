"""ONE COMMAND that puts every hand-checked asset mapping right, in whichever database you name.

Fifteen ISINs were pointing at a DIFFERENT company's price series (2026-09-04). Seven have a real
listing and are repointed to it; eight have none and are unmapped. This applies both, verifies the
result, and is safe to run twice — a decision already in force is skipped, not redone.

    cd backend
    uv run python scripts/fix_mismapped_assets.py                       # local, dry run
    uv run python scripts/fix_mismapped_assets.py --apply               # local
    uv run python scripts/fix_mismapped_assets.py --url https://<ref>.supabase.co \
        --key <service_role key> --apply                                # prod

⚠⚠ `--url` / `--key` EXIST BECAUSE ENV VARS CANNOT REACH PROD FROM HERE. `deps` loads `.env` and
    then `.env.local` with `override=True`, so a prod URL exported in the shell is overwritten by
    the local one and a "prod" run silently rewrites the LOCAL database — the same contamination
    recorded for the CI repros. Passing them as arguments is the only form that cannot be
    overridden, and the script prints the host it is about to write to before doing anything.

⚠ DEPLOY THE CODE FIRST. `NBK.KW` quotes in Kuwaiti FILS (`KWF`), 1/1000 of a dinar, and the
    divisor lives in `asset_pipeline.fx.SUBUNIT`. Until that ships, a backend reading this repoint
    finds no `fx_rate` row for `KWF`, treats the holding as unpriceable, and drops it from every
    portfolio and index silently. This script refuses to repoint it if the running code lacks the
    entry, so the order cannot be got wrong by accident.

⚠ NO MIGRATION IS NEEDED. Nothing here changes the schema; `asset_symbol_override` already exists.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

#: (isin, yahoo_symbol, why) — the seven with a real listing, pinned so a re-resolve cannot undo it.
REPOINT = [
    ("AU000000MFG4", "MFG.AX",
     "was MFG = Mizuho Financial Group (NYSE); ASX home line of Magellan Financial Group"),
    ("KW0EQ0100010", "NBK.KW",
     "was NBKCF = National Bank of CANADA; Kuwait home line, quotes in KWF (fils, /1000)"),
    ("MYL1295OO004", "1295.KL",
     "was PBK.RO = Patria Bank (Romania); Kuala Lumpur home line of Public Bank Berhad"),
    ("SE0000108847", "LUND-B.ST",
     "was LDBA.SG = H. Lundbeck; Stockholm home line of L E Lundbergforetagen B"),
    ("HK0087000532", "0087.HK",
     "was 878.F = PT Chandra Asri; HKEX home line of Swire Pacific B"),
    ("ZAE000179438", "RCL.JO",
     "was RCL.AX = ReadCloud (Australia); Johannesburg home line of RCL Foods, ZAc (/100)"),
    ("MYL5296OO008", "5296.KL",
     "was MRDIYT-R.BK = the THAI listing; Kuala Lumpur home line of MR DIY Group (M)"),
]

#: (isin, why) — no tradeable listing exists, so a wrong series is replaced by no series.
UNMAP = [
    ("AEN000101016", "First Abu Dhabi Bank: Yahoo has no Abu Dhabi coverage (FAB.AE/.AD/.DU, "
                     "NBAD.AE, ADCB.AE, IHC.AE, ALDAR.AE all probe to zero bars); was the US "
                     "First Trust AlphaDEX ETF that shares the ticker FAB"),
    ("FR0004180537", "AKKA Technologies was acquired by Adecco in 2022 and delisted; AKA.PA and "
                     "TKKAF both probe to zero bars; was AKAM = Akamai"),
    ("DK0060495240", "SimCorp was acquired by Deutsche Boerse in 2023 and delisted; SIM.CO probes "
                     "to zero bars; was SIM = Grupo Simec"),
    ("BMG0684D1074", "Athene Holding was taken private by Apollo in 2022; ATH probes to zero bars; "
                     "was 9O1.F = Athens International Airport"),
    ("PLPGNIG00014", "PGNiG merged into Orlen in 2022; the only constructible candidate 7GG.SW "
                     "probes to zero bars; was PGN.F = paragon GmbH"),
    ("ZAE000191979", "RFG Holdings: OpenFIGI offers only Johannesburg composite codes and RFG.JO "
                     "probes to zero bars; was RFG.AX = Retail Food Group"),
    ("AU000000JRV4", "Jervois Global: no ASX code among the ISIN's OpenFIGI listings and JRV.AX "
                     "probes to zero bars; was 400590.KS = a Shinhan SOL ETF"),
    ("KYG210891001", "China Rare Earth 0769.HK is SUSPENDED - 64 closes and zero volume on every "
                     "one of them; was 7692.T = Earth Infinity"),
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", help="Supabase URL. Overrides .env/.env.local — see the docstring.")
    ap.add_argument("--key", help="service_role key for --url")
    ap.add_argument("--apply", action="store_true", help="write; otherwise report only")
    a = ap.parse_args()

    # ⚠⚠ AFTER THE IMPORT, NOT BEFORE, AND THE ORDER IS THE WHOLE TRICK. Importing `deps` runs
    # `load_dotenv(.env)` then `load_dotenv(.env.local, override=True)`, which OVERWRITES anything
    # already in the environment — so values exported by the shell, or set here before the import,
    # lose to the local file and the run silently rewrites the LOCAL database. The client is LAZY
    # (`_LazySupabase._build` reads `os.environ` on first use), so setting them after the import
    # and before the first call is the one window where they win.
    import deps  # noqa: PLC0415

    if a.url:
        os.environ["SUPABASE_URL"] = a.url
    if a.key:
        os.environ["SUPABASE_SERVICE_KEY"] = a.key
    from asset_pipeline import store  # noqa: PLC0415
    from asset_pipeline.fx import SUBUNIT  # noqa: PLC0415
    from asset_pipeline.symbol_override import apply_symbol_overrides  # noqa: PLC0415

    sb = deps.supabase
    host = os.environ.get("SUPABASE_URL") or "?"
    print(f"\n  database: {host}")
    print(f"  mode:     {'APPLY — this writes' if a.apply else 'dry run'}\n")

    if "KWF" not in SUBUNIT:
        print("  !! the running code has no KWF entry in fx.SUBUNIT, so NBK.KW would price at "
              "nothing.\n     Deploy asset_pipeline/fx.py first. Refusing.\n")
        return 1

    ids = [i for i, _s, _w in REPOINT] + [i for i, _w in UNMAP]
    ex = {r["isin"]: r for r in (sb.table("asset_execution")
                                 .select("isin,yahoo_symbol,status,name,reason")
                                 .in_("isin", ids).execute().data or [])}

    todo_pin = [(i, s, w) for i, s, w in REPOINT if (ex.get(i) or {}).get("yahoo_symbol") != s]
    todo_unmap = [(i, w) for i, w in UNMAP
                  if not (((ex.get(i) or {}).get("reason") or "")
                          .startswith(store.MANUAL_UNMAP_PREFIX))]

    print(f"  repoints: {len(REPOINT) - len(todo_pin)} already in force, {len(todo_pin)} to do")
    for i, s, _w in todo_pin:
        print(f"      {i}  {str((ex.get(i) or {}).get('yahoo_symbol')):<12} -> {s}")
    print(f"  unmaps:   {len(UNMAP) - len(todo_unmap)} already in force, {len(todo_unmap)} to do")
    for i, _w in todo_unmap:
        print(f"      {i}  {str((ex.get(i) or {}).get('name'))[:44]}")

    if not a.apply:
        print("\n  dry run — re-run with --apply to persist\n")
        return 0
    if not todo_pin and not todo_unmap:
        print("\n  nothing to do\n")
        return 0

    if todo_pin:
        # ⚠ THE PIN IS WRITTEN FIRST AND THE REPOINT IS DONE BY `apply_symbol_overrides`, WHICH IS
        # THE ONE IMPLEMENTATION OF "POINT AN ISIN AT A SYMBOL" — it probes, refuses a zero-bar
        # target (the GODE.DE guard) and stores the series. A second copy here is a second place
        # for that guard to be forgotten.
        sb.table("asset_symbol_override").upsert(
            [{"isin": i, "yahoo_symbol": s, "note": w} for i, s, w in REPOINT],
            on_conflict="isin").execute()
        print(f"\n  pinned {len(REPOINT)} override(s); applying…")
        changed = apply_symbol_overrides()
        print(f"  repointed {changed} row(s)")

    for i, w in todo_unmap:
        before = store.unmap_execution(i, w)
        print(f"  unmapped {i} (was {str((before or {}).get('yahoo_symbol'))!r})")

    print("\n  done — verify with: uv run python scripts/verify_asset_mappings.py\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
