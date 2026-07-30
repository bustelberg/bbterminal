"""Manual symbol overrides: the Yahoo listing an ISIN MUST resolve to.

`asset_symbol_override` says "IE00BJSFQW37 is 36B7.DE". Applying it repoints that ISIN's execution
row onto the named symbol, exactly as `scripts/repoint_to_symbol.py` does by hand — same probe,
same store path, same refusal on a symbol with no bars.

⚠ RE-APPLIED AFTER EVERY RESOLUTION, OR IT IS NOT AN OVERRIDE. `fast_resolve`, the queue worker,
    both repointers and the per-row Resolve action all write `asset_execution.yahoo_symbol`, and
    each of them would hand an overridden ISIN a listing of its own again — by NAME, which is how
    the wrong one was chosen in the first place. `apply_symbol_overrides()` runs after them and
    puts it back. Idempotent: a no-op once the row already names the right symbol, and in that
    case it costs one query and NOT a Yahoo call.

⚠ IT IS NOT `asset_isin_alias`. An alias means "this ISIN is deliberately served by ANOTHER
    ISIN's instrument" — an ADR priced off its ordinary, two securities sharing one series on
    purpose, which is why the alias inherits the canonical's `analysis_id`. This says the
    opposite: the ISIN has its own listing and the automatic path picked the wrong one. Recording
    a wrong-listing fix as an alias would assert a relationship between two securities that does
    not exist, and would then quietly price one off the other for ever.

⚠ THE TWO MUST NOT BOTH CLAIM ONE ISIN. An alias points the row at another ISIN's instrument and
    this points it at a named symbol; applied in either order they fight, and which one wins would
    depend on call order rather than on intent. `apply_symbol_overrides` refuses an ISIN that is
    also aliased and says so, rather than picking a winner.

⚠ A ZERO-BAR SYMBOL IS REFUSED, EVEN THOUGH A HUMAN NAMED IT. Naming a symbol by hand does not
    make it a listing — the GODE.DE incident wrote ten structured products onto one empty series
    with `status='ok'`. The override is stored; it is simply not applied, and the reason is logged.
"""
from __future__ import annotations

import logging

from deps import supabase

_log = logging.getLogger(__name__)


def load_symbol_overrides() -> dict[str, str]:
    """{isin: the Yahoo symbol it must resolve to}."""
    rows = (supabase.table("asset_symbol_override").select("isin,yahoo_symbol")
            .limit(2000).execute().data or [])
    return {r["isin"]: r["yahoo_symbol"] for r in rows if r.get("isin") and r.get("yahoo_symbol")}


def _needs_repoint(isin: str, symbol: str) -> bool:
    """True when the execution row does not already name `symbol`.

    ⚠ CHECKED BEFORE ANY NETWORK CALL. This runs after every resolution, and the overwhelmingly
    common case is that nothing changed — probing Yahoo to discover that would put a call per
    override on every pipeline tick, which is how Yahoo starts answering with empty results.
    """
    cur = (supabase.table("asset_execution").select("isin,yahoo_symbol")
           .eq("isin", isin).limit(1).execute().data or [])
    if not cur:
        _log.warning("[symbol_override] %s has no execution row to repoint; skipped", isin)
        return False
    return (cur[0].get("yahoo_symbol") or "") != symbol


def apply_symbol_overrides(only_isin: str | None = None) -> int:
    """Repoint every overridden ISIN onto its named symbol. Returns rows changed.

    Deliberately mirrors `scripts/repoint_to_symbol.py`: probe the symbol, refuse it if it has no
    price series, then `upsert_asset` + `store_series`. A second implementation of "point an ISIN
    at a symbol" is a second place for the GODE.DE guard to be forgotten.
    """
    overrides = load_symbol_overrides()
    if only_isin:
        overrides = {k: v for k, v in overrides.items() if k == only_isin}
    if not overrides:
        return 0

    # ⚠ An ISIN cannot be both aliased and symbol-overridden — see the module docstring.
    from .isin_alias import load_aliases  # noqa: PLC0415

    aliased = load_aliases()
    changed = 0
    for isin, symbol in sorted(overrides.items()):
        if isin in aliased:
            _log.error(
                "[symbol_override] %s is BOTH symbol-overridden (-> %s) and aliased (-> %s). "
                "Refusing to apply either from here: delete whichever is wrong. An alias shares "
                "another ISIN's price series; a symbol override gives this ISIN its own.",
                isin, symbol, aliased[isin])
            continue
        if not _needs_repoint(isin, symbol):
            continue                                   # already correct — idempotent no-op
        try:
            if _repoint(isin, symbol):
                changed += 1
        except Exception as e:  # noqa: BLE001 — one bad override must not stop the rest
            _log.warning("[symbol_override] %s -> %s failed: %s: %s",
                         isin, symbol, type(e).__name__, e)
    return changed


def _repoint(isin: str, symbol: str) -> bool:
    """Point one ISIN at one symbol. False when the symbol proves not to be a listing."""
    from . import openfigi, store  # noqa: PLC0415
    from .fast_resolve import _score_retry  # noqa: PLC0415
    from .resolve import resolve_analysis_instrument, sector_for  # noqa: PLC0415

    row = (supabase.table("asset_grid").select("isin,asset_class,sector")
           .eq("isin", isin).limit(1).execute().data or [])
    if not row:
        _log.warning("[symbol_override] %s is not in the grid; skipped", isin)
        return False

    # ⚠ PROBE BEFORE STORING. A named symbol is a claim, not a listing.
    sc = _score_retry(symbol)
    if not sc or not float(sc.get("med_adv_eur") or 0):
        _log.warning("[symbol_override] %s -> %s: no price series. NOT applied — a symbol with "
                     "no bars is not a listing (the GODE.DE incident).", isin, symbol)
        return False

    figi_rows = openfigi.lookup_isin(isin)
    fig = {}
    if figi_rows:
        f0 = figi_rows[0]
        fig = {"openfigi_figi": f0.get("figi"), "openfigi_name": f0.get("name"),
               "openfigi_ticker": f0.get("ticker"), "openfigi_type": f0.get("securityType2")}
    sc["eligible"] = True
    ai = resolve_analysis_instrument(sc, row[0].get("asset_class") or "equity")
    res = {
        "input": isin, "id_type": "isin",
        "asset_class": ai["analysis_asset_class"], "wrapper": ai["wrapper"],
        "is_leveraged": ai["is_leveraged"], "candidates": [sc],
        "execution": ai["execution"], "analysis": ai["analysis"],
        "chosen": ai["analysis"], "underlying": None,
        "reason": f"asset_symbol_override: pinned to {symbol} by hand, not a ranked pick.",
        "analysis_note": ai["analysis_note"],
        # ⚠ NOT `analysis_asset_class` — see `sector_for`.
        "sector": sector_for(symbol, ai["analysis_asset_class"], row[0].get("sector")),
        "candles": None, "ibkr": None,
    }
    ids = store.upsert_asset(res, figi=fig)
    bars = store.store_series(ids["analysis_id"], ai["analysis"]["symbol"],
                              ai["analysis"].get("first_ts"))
    if not bars:
        _log.warning("[symbol_override] %s -> %s stored 0 bars; nothing repointed.", isin, symbol)
        return False
    store.set_default_executions()
    _log.info("[symbol_override] %s now pinned to %s (%s bars)", isin, symbol, f"{bars:,}")
    return True
