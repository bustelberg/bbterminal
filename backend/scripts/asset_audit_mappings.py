"""Audit the OpenFIGI -> yfinance mapping of EVERY asset row.

READ-ONLY. Walks every `asset_grid` row and checks whether the resolved yfinance analysis is the
SAME company OpenFIGI identified for the ISIN (rapidfuzz name match — the same test
`resolve.identity_status` stamps on the row). Prints each mismatch + a summary.

    uv run python scripts/asset_audit_mappings.py

⚠⚠ IT NO LONGER ACTS, AND THE COUNT IS WHY. `--fix` re-queued every mismatch for re-resolution;
measured 2026-09-04 on the live grid, 110 rows fail this test and only ~15 are genuinely the wrong
company. The rest are OpenFIGI's own spelling — `MUENCHENER RUECKVER AG-REG` for Münchener
Rückversicherungs-Gesellschaft, `IND & COMM BK OF CHINA-H` for ICBC, `SAMSUNG ELECTRO-REGS GDR
PFD`, `DHL GROUP` for Deutsche Post (a rename), `VANG FTSE JPN USDA` for a Vanguard ETF — i.e.
CORRECT mappings on liquid names. Re-resolving a correct row can only make it worse: Yahoo answers
an overloaded caller with an EMPTY list rather than a 429, so the ranker picks from a candidate set
missing the real listing (Alphabet -> GOOA.VI, 75,000x thinner). The action lives in
`scripts/asset_fix_mismaps.py`, which takes the ISINs a human has checked.

⚠ THE TEST IS RE-RUN RATHER THAN READ off `identity_status`, deliberately: this is the checker, and
a row stamped before a change to `same_company` carries a verdict nobody has re-asked. The two
agreed exactly when last compared (110 = 110).

Caveat: this catches WRONG-COMPANY mappings (yfinance resolved to a different
company than the ISIN). It does NOT catch same-company-wrong-listing (a name that
matches but on a thin/wrong exchange) — the names match there by definition.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402,F401  (loads env + Supabase client)
from asset_pipeline.resolve import same_company  # noqa: E402

APPLY = "--fix" in sys.argv


def _load_all() -> list[dict]:
    rows: list[dict] = []
    off = 0
    while True:
        r = (
            deps.supabase.table("asset_grid")
            .select("isin, name, analysis_symbol, status, openfigi_name, openfigi_type")
            .range(off, off + 999).execute().data
        ) or []
        rows += r
        if len(r) < 1000:
            break
        off += 1000
    return rows


def main() -> None:
    rows = _load_all()
    ok = no_figi = unmapped = 0
    bad: list[dict] = []
    for r in rows:
        if r.get("status") != "ok":
            unmapped += 1
            continue
        figi_name = r.get("openfigi_name")
        if not figi_name:
            no_figi += 1
            continue
        if same_company(r.get("name"), figi_name):
            ok += 1
        else:
            bad.append(r)

    for r in sorted(bad, key=lambda x: x["isin"]):
        print(f"  MISMATCH {r['isin']}: yfinance {r.get('analysis_symbol') or '?'} "
              f"({(r.get('name') or '?')[:26]})  !=  OpenFIGI {r.get('openfigi_name')}", flush=True)

    print(f"\n{len(rows)} rows · {ok} verified-correct · {len(bad)} MISMATCH · "
          f"{no_figi} no-OpenFIGI-name · {unmapped} unmapped", flush=True)

    if bad and APPLY:
        # ⚠⚠ `--fix` IS REFUSED, AND IT IS REFUSED HERE BECAUSE THIS WAS THE SECOND DOOR TO THE
        # SAME BLANKET RE-RESOLVE. `asset_fix_mismaps.py` was made review-first on 2026-09-04 —
        # measured, only ~15 of these 110 are genuinely the wrong company, the rest are OpenFIGI's
        # own spelling (`MUENCHENER RUECKVER AG-REG`, `IND & COMM BK OF CHINA-H`, `DHL GROUP`) —
        # and this line went straight to `queue.enqueue` on ALL of them, bypassing that entirely.
        # Re-resolving a CORRECT row can only move it to a thinner listing: Yahoo answers an
        # overloaded caller with an empty list, not a 429.
        print("\n--fix is no longer supported here: it re-queued every mismatch, and most of "
              "these\nare OpenFIGI spelling rather than wrong mappings. Review the list above, "
              "then:\n"
              "    uv run python scripts/asset_fix_mismaps.py --isin <ISIN> [--isin <ISIN> …]\n"
              "which takes the ones you have actually checked.", flush=True)
    elif bad:
        print("Review these, then re-queue the ones that are really wrong with "
              "scripts/asset_fix_mismaps.py --isin <ISIN>.", flush=True)


if __name__ == "__main__":
    main()
