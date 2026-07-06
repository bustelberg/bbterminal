"""Filter a lynqs universe CSV to the tradable, resolvable classes and emit a
single `identifier` column the asset pipeline can resolve — ISIN for equities/
ETFs, Yahoo symbol for crypto (derived from `ric`, e.g. BTC=BTSP -> BTC-USD).

Different product types carry different identity: equities/ETFs have real ISINs
(-> yfinance), crypto has no/fake ISINs but IS on Yahoo by ticker, and bonds/FX/
futures/options aren't price-resolvable here. So we keep EQUITY/ETF/CRYPTO by
default, compute the right identifier per row, and dedupe by it. The upload's
identifier column then feeds the resolver (which handles ISIN *or* symbol).

    uv run python scripts/lynqs_filter.py lynqs_universe_all.csv
    uv run python scripts/lynqs_filter.py lynqs_universe_all.csv --types EQUITY,ETF   # no crypto
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

from asset_pipeline.isin_util import is_valid_isin  # noqa: E402

IN_FIELDS = ["id", "ticker", "name", "productType", "ric", "isin", "currency"]
OUT_FIELDS = ["identifier", *IN_FIELDS]


def _identifier(r: dict) -> str | None:
    """Best price-resolvable identifier for a row: a valid ISIN, else (for crypto)
    the Yahoo symbol derived from `ric` (BTC=BTSP -> BTC-USD)."""
    isin = (r.get("isin") or "").strip().upper()
    if is_valid_isin(isin):
        return isin
    if r.get("productType") == "CRYPTO_CURRENCY":
        # ric is clean when it has '=' (BTC=BTSP -> BTC); otherwise it's a fake
        # ISIN-ish blob (AXSUSD / XLMCOIN00USD) and the ticker is the clean code.
        ric = (r.get("ric") or "").strip().upper()
        ticker = (r.get("ticker") or "").strip().upper()
        code = ric.split("=")[0] if "=" in ric else ticker
        code = code.replace("-USD", "").strip()
        if code == "XBT":  # lynqs code for Bitcoin
            code = "BTC"
        return f"{code}-USD" if code else None
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Filter a lynqs universe CSV to resolvable instruments + identifier.")
    ap.add_argument("input")
    ap.add_argument("--out", default=None)
    ap.add_argument("--types", default="EQUITY,ETF,CRYPTO_CURRENCY", help="comma-separated productTypes to keep")
    ap.add_argument("--keep-invalid", action="store_true", help="keep rows even without a resolvable identifier")
    args = ap.parse_args()

    keep = {t.strip() for t in args.types.split(",") if t.strip()}
    src = Path(args.input)
    out = Path(args.out) if args.out else src.with_name(f"{src.stem}_pipeline.csv")

    rows_in = list(csv.DictReader(src.open(encoding="utf-8")))
    kept: list[dict] = []
    seen: set[str] = set()
    dropped_type = dropped_noid = dupes = 0
    for r in rows_in:
        if r.get("productType") not in keep:
            dropped_type += 1
            continue
        ident = _identifier(r)
        if not ident:
            if args.keep_invalid:
                ident = ""
            else:
                dropped_noid += 1
                continue
        if ident and ident in seen:
            dupes += 1
            continue
        if ident:
            seen.add(ident)
        kept.append({"identifier": ident, **{k: (r.get(k) or "") for k in IN_FIELDS}})

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUT_FIELDS)
        w.writeheader()
        w.writerows(kept)

    by_type = dict(Counter(r["productType"] for r in kept))
    crypto_syms = [r["identifier"] for r in kept if r["productType"] == "CRYPTO_CURRENCY"]
    print(f"in: {len(rows_in):,} rows  ->  kept {len(kept):,} -> {out}")
    print(f"  by type: {by_type}")
    print(f"  dropped: {dropped_type:,} wrong-type, {dropped_noid:,} no identifier, {dupes:,} dupe")
    if crypto_syms:
        print(f"  crypto symbols ({len(crypto_syms)}): {', '.join(crypto_syms[:15])}"
              + (" ..." if len(crypto_syms) > 15 else ""))


if __name__ == "__main__":
    main()
