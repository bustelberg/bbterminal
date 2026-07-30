"""Re-resolve executions stuck on a THIN CROSS-LISTING instead of the primary one.

A DIFFERENT DEFECT FROM `reresolve_asset_mismaps.py`
    That script fixes the WRONG COMPANY (SkyWater stored as Micron). This one
    fixes the RIGHT company on the WRONG LISTING: NVIDIA stored as `NVD.SG`
    (Stuttgart, EUR, €1.6M median daily traded value) rather than `NVDA`
    (NasdaqGS, USD, €28,076M). Same issuer, same ISIN — a near-dead German line
    standing in for the real market.

WHERE IT COMES FROM — TWO CAUSES
    1. The rows were written by `fast_resolve`, which derives a Yahoo symbol from
       the Leonteq ticker/RIC in one call. That is what makes the bulk ingest
       affordable — but Leonteq's RIC names the line THEY trade, which for many US
       and French mega-caps is a German regional venue.

    2. `resolve()` could not repair them, because its OpenFIGI name anchor used a
       raw `rapidfuzz.token_set_ratio >= 80` floor. "NVIDIA Corporation" (Yahoo)
       vs "NVIDIA CORP" (OpenFIGI) scores 75.9, so the anchor concluded the
       most-liquid NVDA was a DIFFERENT company and deliberately swapped in the
       Stuttgart line. Same for "Intel Corporation"/"INTEL CORP" (74.1) and
       "Eli Lilly and Company"/"LILLY(ELI) & CO" (50.0). Fixed 2026-07-10: the
       anchor now uses `same_company()`, which strips corporate forms first — and
       which `identity_status()` already used, so the two can no longer disagree.
       Without that fix this script finds nothing to do.

DETECTION, WITHOUT A MAPPING TABLE
    Not "ISIN country vs listing country": that flags every ADR mapped to its
    foreign ordinary (`US7595091023` -> `RELIANCE.NS`), which is deliberate.
    Not an exchange-code map either — that is the Vienna≠Prague trap.

    Instead, a measured ratio: median daily traded value / market cap. Real
    primary listings sit around 4.6e-03 (median across our 6,088 priced
    equities). NVIDIA-on-Stuttgart sits at 3.8e-07, four orders of magnitude
    below. The 5th percentile is 1.07e-05, so `--max-ratio 1e-5` is a natural cut.
    It is source-agnostic: it also catches L'Oreal on Stuttgart and ICBC on Vienna.

SAFETY
    A row is only rewritten when the re-resolution
      * returns a DIFFERENT symbol,
      * whose name still matches this ISIN's OpenFIGI identity (so we can never
        swap in a different company), and
      * is at least `--min-gain`x more liquid than the incumbent.
    Dry-run by default. Re-resolution hits Yahoo (~1 search + 1 chart per
    candidate), so don't run it while a big ingest is competing for the throttle.

    cd backend && PYTHONPATH=. uv run python scripts/repoint_primary_listing.py
    cd backend && PYTHONPATH=. uv run python scripts/repoint_primary_listing.py --isin US67066G1040 --apply
    cd backend && PYTHONPATH=. uv run python scripts/repoint_primary_listing.py --apply
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402, F401  — loads .env
from asset_pipeline import store  # noqa: E402
from asset_pipeline.resolve import resolve  # noqa: E402

_FIGI_COLS = ("openfigi_figi", "openfigi_name", "openfigi_ticker", "openfigi_exch", "openfigi_type")

# ⚠ THIS SCRIPT RESOLVES BY NAME, SO IT MAY ONLY TOUCH AN OPERATING COMPANY.
#
# `resolve()` searches Yahoo by NAME and gates identity with `same_company()`. That is right for
# a company and DANGEROUS for a fund, whose siblings share almost every word of their names.
# Measured, on a real candidate this script produced:
#
#     JE00BN7KB557  WisdomTree Coffee (an ETC on coffee futures, OpenFIGI "WT COFFEE")
#                -> LKNCY             (Luckin Coffee Inc, a Chinese coffee RETAILER)
#
#     same_company("Luckin Coffee Inc", "WT COFFEE") -> True   (token_set_ratio 80.0)
#
# The word "coffee" carried it over the floor, and the identity gate waved through a swap from a
# futures ETC to an operating company. Nothing downstream would have caught it: the new listing
# is real, liquid, and priced.
#
# Until now a market cap was required, and funds do not report one (all 29 ETP rows have none) —
# so the cap check was, by accident, the fence keeping funds away from a name-based resolver.
# Once `--no-cap-max-adv` removed that accident, the hole was immediate. So the fence is now
# EXPLICIT and structural: an allowlist of types where a name IS an identity.
#
# A fund is repointed by ISIN instead — `scripts/repoint_etf_listing.py` enumerates the venues
# OpenFIGI lists for THAT ISIN, so the candidate set cannot contain another instrument at all.
# (Closed-End Funds mostly DO have a cap, so they were already reachable by the ratio path — the
# hole predates the sweep.)
_NAME_RESOLVABLE_TYPES = {
    "Common Stock", "ADR", "GDR", "REIT", "Preference", "Preferred Stock",
    "Stapled Security", "CDI", "NY Reg Shrs", "Dutch Cert", "Receipt",
}


def _candidates(sb, max_ratio: float, isin: str | None,
                no_cap_max_adv: float = 0.0) -> list[dict]:
    """Equity executions whose listing is implausibly illiquid for its market cap.

    `wrapper` rows are excluded: a Bitcoin ETF legitimately analyses as `BTC-USD`,
    and its ADV/market-cap ratio means nothing.

    ⚠ A MISSING MARKET CAP IS A SIGNAL, NOT A REASON TO SKIP (`no_cap_max_adv`).
        The ratio needs a cap, so a row without one cannot be scored — and for years that meant
        it was passed over. But a DEAD listing is exactly what fails to report a cap: Brown &
        Brown sat on BTW.DE (Xetra, €7,836/day, no cap, NO PRICE BARS AT ALL) while its real
        market, NYSE `BRO`, does €154m/day — 19,698x. The detector could not see it *because*
        it was broken enough to have no cap.

        221 equity rows are in that blind spot and 112 of them trade under €1m/day (Pool Corp
        on Milan at €1,042/day; the Warsaw Stock Exchange on Düsseldorf; Remgro on Munich). So
        `--no-cap-max-adv` sweeps them in on ADV alone. It over-includes on purpose — plenty of
        genuine German micro-caps are down there too (publity AG really does trade like that) —
        and that is safe: a wrong candidate costs a Yahoo call and is then rejected by the
        identity + liquidity gates below, never a rewrite.
    """
    cols = ("isin,yahoo_symbol,analysis_symbol,name,med_adv_eur,market_cap_eur,"
            "asset_class,status,wrapper," + ",".join(_FIGI_COLS))
    rows, off = [], 0
    while True:
        q = sb.table("asset_grid").select(cols).eq("status", "ok").eq("asset_class", "equity")
        if isin:
            q = q.eq("isin", isin)
        batch = q.range(off, off + 999).execute().data or []
        rows += batch
        if len(batch) < 1000:
            break
        off += 1000

    out = []
    skipped_funds = 0
    for r in rows:
        if r.get("wrapper"):
            continue

        # A NAME is only an identity for an operating company. Everything else — an ETP, a
        # closed-end fund, an unknown type, or a row with no OpenFIGI name to check against at
        # all (the gate below is `if figi_name and ...`, so no name = NO gate) — is off limits
        # to a by-name resolver. See `_NAME_RESOLVABLE_TYPES`: this is the WisdomTree-Coffee-to-
        # Luckin-Coffee fence, and it holds even when `--isin` is passed. Repoint a fund with
        # `repoint_etf_listing.py`, which enumerates venues from the ISIN itself.
        if r.get("openfigi_type") not in _NAME_RESOLVABLE_TYPES or not r.get("openfigi_name"):
            skipped_funds += 1
            continue

        adv, cap = r.get("med_adv_eur"), r.get("market_cap_eur")
        priced = bool(adv) and bool(cap) and float(cap) > 0 and float(adv) > 0

        # An EXPLICIT `--isin` bypasses the ratio ENTIRELY — including the market cap the ratio
        # is computed from. That guard exists to *detect* a bad listing; when the caller has
        # already named one, requiring a cap only hides the worst rows there are.
        #
        # Brown & Brown (`US1152361010`) is exactly that: mapped to BTW.DE (Xetra, EUR, €8k
        # median daily traded value) instead of BRO (NYSE, USD), with NO market cap and NO price
        # bars at all. A dead listing yields no cap, so the row that most needs repointing was
        # the one row the detector could not see — and `--isin`, documented as "bypassing the
        # ratio filter", silently returned "0 candidates" for it.
        if isin:
            r["_ratio"] = (float(adv) / float(cap)) if priced else None
            out.append(r)
            continue

        if not priced:
            # No cap = unscoreable, not innocent. Sweep it in on ADV alone when asked.
            if no_cap_max_adv and adv and float(adv) < no_cap_max_adv:
                r["_ratio"] = None
                out.append(r)
            continue
        r["_ratio"] = float(adv) / float(cap)
        if r["_ratio"] < max_ratio:
            out.append(r)
    if skipped_funds:
        # Never silent. A row this script refuses is not a row that is fine — it is one that has
        # to be repointed by ISIN instead, and saying nothing would read as "nothing to do".
        print(f"  ({skipped_funds} row(s) skipped: not name-resolvable — a fund/ETP, or no "
              f"OpenFIGI name to verify against. Use scripts/repoint_etf_listing.py)", flush=True)
    out.sort(key=lambda r: -(float(r.get("market_cap_eur") or 0)))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="persist the fixes (default: dry run)")
    ap.add_argument("--max-ratio", type=float, default=1e-5,
                    help="flag rows whose ADV/market-cap is below this (p5 = 1.07e-5)")
    ap.add_argument("--min-gain", type=float, default=2.0,
                    help="require the new listing to be this many times more liquid")
    ap.add_argument("--isin", help="fix a single ISIN, bypassing the ratio filter entirely "
                                   "(including the market cap that filter needs)")
    ap.add_argument("--no-cap-max-adv", type=float, default=0.0,
                    help="ALSO sweep equity rows with NO market cap — invisible to the ratio — "
                         "whose median daily traded value is under this (try 1e6). A dead "
                         "listing is precisely what reports no cap: Brown & Brown hid here.")
    ap.add_argument("--limit", type=int, default=0, help="stop after N candidates (0 = all)")
    a = ap.parse_args()

    from asset_pipeline.resolve import same_company  # noqa: PLC0415

    sb = deps.supabase
    cands = _candidates(sb, a.max_ratio, a.isin, a.no_cap_max_adv)
    if a.limit:
        cands = cands[: a.limit]

    print(f"{len(cands)} candidate(s){' — APPLYING' if a.apply else ' — DRY RUN'}\n", flush=True)
    fixed = kept = failed = 0

    for r in cands:
        isin, old = r["isin"], r.get("analysis_symbol")
        figi_name = r.get("openfigi_name")
        fig = {k: r.get(k) for k in _FIGI_COLS}
        old_adv = float(r.get("med_adv_eur") or 0)

        try:
            res = resolve(isin, with_candles=False, figi_hint=fig)
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  FAIL  {isin} {old}: {type(e).__name__}: {e}", flush=True)
            continue

        an = res.get("analysis") or {}
        new, new_name = an.get("symbol"), an.get("name")
        new_adv = float(an.get("med_adv_eur") or 0)

        if not new or new == old:
            kept += 1
            print(f"  keep  {isin} {old:12} — resolver returned the same listing", flush=True)
            continue
        # Identity gate: the new listing must still be THIS company.
        # `same_company` (corporate-form-stripping), not a raw fuzz floor — see
        # the comment on the anchor in `asset_pipeline/resolve.py`.
        if figi_name and not same_company(new_name, figi_name):
            kept += 1
            print(f"  keep  {isin} {old:12} — {new} is a different name "
                  f"({new_name!r} vs OpenFIGI {figi_name!r})", flush=True)
            continue
        # Liquidity gate: only swap for a materially better market.
        if new_adv < old_adv * a.min_gain:
            kept += 1
            print(f"  keep  {isin} {old:12} — {new} is not {a.min_gain}x more liquid "
                  f"({new_adv:,.0f} vs {old_adv:,.0f})", flush=True)
            continue

        fixed += 1
        gain = (new_adv / old_adv) if old_adv else float("inf")
        print(f"  FIX   {isin} {old:12} -> {new:12} "
              f"adv {old_adv:>13,.0f} -> {new_adv:>15,.0f}  ({gain:,.0f}x)  {(r.get('name') or '')[:26]}",
              flush=True)
        if a.apply:
            ids = store.upsert_asset(res, figi=fig)
            store.store_series(ids["analysis_id"], new, an.get("first_ts"))

    print(f"\n  fixed={fixed}  kept={kept}  failed={failed}")
    if a.apply and fixed:
        try:
            store.set_default_executions()
            print("  refreshed is_default flags")
        except Exception as e:  # noqa: BLE001
            print(f"  set_default_executions failed: {e}")
    if not a.apply and fixed:
        print("\n  dry run — re-run with --apply to persist")
    return 0


if __name__ == "__main__":
    sys.exit(main())
