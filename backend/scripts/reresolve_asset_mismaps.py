"""Re-resolve asset-pipeline executions that may be mis-mapped to the WRONG
Yahoo analysis instrument, using the OpenFIGI-anchored resolver.

Background: Yahoo's ISIN search sometimes false-matches a more-liquid but
DIFFERENT company — e.g. SkyWater `US83089J1088` -> Micron/MU, or several Spanish
ISINs -> GGAL. The resolver now anchors to the ISIN's OpenFIGI ticker + name
(with a rapidfuzz name check so an ambiguous ticker can't anchor to the wrong
company). This script re-runs it over already-stored `ok` rows whose stored
symbol matches NEITHER the execution nor the analysis OpenFIGI ticker (the
candidates for a mis-map) and re-stores the ones whose company actually changes.

Dry-run by default (prints old -> new, no writes). Pass --apply to persist.
NOTE: re-resolution hits Yahoo — run it when a big ingest isn't competing for
the throttle.

    uv run python scripts/reresolve_asset_mismaps.py            # dry-run
    uv run python scripts/reresolve_asset_mismaps.py --apply    # fix them
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402
from asset_pipeline import store  # noqa: E402
from asset_pipeline.resolve import resolve  # noqa: E402

APPLY = "--apply" in sys.argv
_FIGI_COLS = ("openfigi_figi", "openfigi_name", "openfigi_ticker", "openfigi_exch", "openfigi_type")


def _load_ok_rows(sb) -> list[dict]:
    cols = "isin,yahoo_symbol,analysis_symbol,name," + ",".join(_FIGI_COLS)
    rows, off = [], 0
    while True:
        batch = (
            sb.table("asset_grid").select(cols).eq("status", "ok")
            .range(off, off + 999).execute().data
        ) or []
        rows += batch
        if len(batch) < 1000:
            break
        off += 1000
    return rows


def main() -> None:
    from asset_pipeline.resolve import _NAME_MATCH, _name_score  # noqa: PLC0415

    sb = deps.supabase
    rows = _load_ok_rows(sb)
    # Suspect = the STORED analysis name is a DIFFERENT company than the ISIN's
    # OpenFIGI name (a true wrong-company mis-map, e.g. Cytokinetics stored as
    # QCOM). Deliberately name-based, NOT ticker-based: same-company relistings
    # (A-share↔H-share, cross-exchange) share the name and must be left alone —
    # they trade at different prices and aren't errors.
    suspects = [
        r for r in rows
        if r.get("openfigi_name") and _name_score(r.get("name"), r.get("openfigi_name")) < _NAME_MATCH
    ]

    print(f"{len(rows)} ok rows · {len(suspects)} wrong-company suspects"
          f"{' — APPLYING' if APPLY else ' — DRY RUN'}", flush=True)
    changed = unchanged = failed = 0
    for r in suspects:
        isin, old = r["isin"], r.get("analysis_symbol")
        figi_name = r.get("openfigi_name")
        fig = {k: r.get(k) for k in _FIGI_COLS}
        try:
            res = resolve(isin, with_candles=False, figi_hint=fig)
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  FAIL {isin}: {type(e).__name__}: {e}", flush=True)
            continue
        an = res.get("analysis") or {}
        new, new_name = an.get("symbol"), an.get("name")
        # Only replace when the NEW resolution IS the right company (its name
        # matches OpenFIGI) and the symbol actually changed.
        if new and new != old and _name_score(new_name, figi_name) >= _NAME_MATCH:
            changed += 1
            print(f"  FIXED {isin}: {old} ({(r.get('name') or '?')[:22]}) -> {new} ({(new_name or '?')[:22]})  "
                  f"[OpenFIGI: {figi_name}]", flush=True)
            if APPLY:
                ids = store.upsert_asset(res, figi=fig)
                store.store_series(ids["analysis_id"], new, an.get("first_ts"))
        else:
            unchanged += 1
            why = "resolver found no better-named match" if not new else f"best it found was {new} ({(new_name or '?')[:22]})"
            print(f"  keep  {isin}: {old} ({(r.get('name') or '?')[:22]}) — {why}  [OpenFIGI: {figi_name}]", flush=True)

    if APPLY:
        try:
            store.set_default_executions()
        except Exception:  # noqa: BLE001
            pass
    print(f"done: {changed} changed, {unchanged} unchanged, {failed} failed"
          f"{'' if APPLY else ' — re-run with --apply to persist'}", flush=True)


if __name__ == "__main__":
    main()
