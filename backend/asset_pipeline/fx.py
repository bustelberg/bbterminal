"""EUR conversion for the asset-pipeline price series.

Reuses the OLD system's `fx_rate` table (ECB daily rates, forward-filled) via
`momentum.data.load_fx_rates`. `rate` there is units-of-currency per 1 EUR, so
`eur = native / rate`. Best-effort: a date with no available FX rate (e.g.
pre-1999, or a currency the fx sync never covered) yields close_eur=None. GBp
(London pence) is handled as GBP/100."""
from __future__ import annotations

import bisect
from datetime import date as _date

# Minor-unit quotes → (major currency, divisor). Normalise the minor unit into
# its major unit before FX.
#
# ⚠ `GBp` IS NOT A CURRENCY CODE — it is PENCE, and `fx_rate` has no row for it.
#     Yahoo quotes every London listing in pence, so `asset_execution.currency`
#     says "GBp" for 343 of our rows. Any FX lookup that passes that string
#     straight to `fx_rate` gets NOTHING back, and a caller that then treats a
#     missing rate as "unpriceable" silently drops the holding — with all of its
#     bars sitting right there. Judges Scientific (`GB0032398678`) has 5,930 bars
#     from 2003 and read as unpriced in every AIRS portfolio holding it.
#
#     The failure is quiet in BOTH directions, which is why the map has to be
#     shared rather than re-derived: forget to normalise the CODE and the
#     holding vanishes; forget to divide by the DIVISOR and a £46.75 share
#     prices at £4,675 — a hundredfold error that still looks like a number.
#
# ⚠⚠ AND THE DIVISOR IS NOT ALWAYS 100. `KWF` is Kuwaiti FILS, of which there are **1,000** to the
#     dinar — the one entry here that would be wrong if it were copied from the line above it.
#     Measured on the live quote before it was added: NBK.KW closed at **861.0 KWF** against a
#     share that trades at ~0.861 KWD. At a divisor of 100 that share would price at 8.61 KWD,
#     ten times its value, and still look like an ordinary number.
SUBUNIT: dict[str, tuple[str, float]] = {
    "GBp": ("GBP", 100.0),   # London pence
    "GBX": ("GBP", 100.0),
    "ZAc": ("ZAR", 100.0),   # SA cents
    "ILA": ("ILS", 100.0),   # Tel-Aviv agorot
    "KWF": ("KWD", 1000.0),  # Kuwaiti fils — ⚠ 1,000, not 100
}

_SUBUNIT = SUBUNIT          # legacy private alias — this module's own callers

# Asset classes where Yahoo reports `volume` as a quote-currency NOTIONAL amount
# (already money) rather than a share COUNT — so EUR volume is volume×fx, NOT
# price×volume×fx. Matches the reference `is_notional_volume`. Crypto
# (e.g. BTC-USD) is the case that matters here.
_NOTIONAL_CLASSES = {"crypto"}


def _fx_series(base: str, dates: list[_date]) -> tuple[list[str], list[float]] | None:
    """Sorted (iso-date, units-per-EUR) rate series for `base` over the span of
    `dates`, or None if unavailable. Best-effort."""
    try:
        from deps import supabase  # noqa: PLC0415
        from momentum.data import load_fx_rates  # noqa: PLC0415
        fx = load_fx_rates(supabase, [base], min(dates), max(dates))
    except Exception:  # noqa: BLE001
        return None
    ser = fx.get(base)
    if ser is None or len(ser) == 0:
        return None
    ser = ser.sort_index()
    keys = [(d.date().isoformat() if hasattr(d, "date") else str(d)[:10]) for d in ser.index]
    vals = [float(v) for v in ser.values]
    return keys, vals


def to_eur_series(rows: list[dict], currency: str | None, asset_class: str | None = None) -> list[dict]:
    """Return `rows` with `close_eur` + `volume_eur` added, using the
    reference conversion methodology:

      * price:  close_eur = (close / subunit_divisor) / rate     (rate = units/EUR)
      * volume: NOTIONAL (crypto) → volume_eur = volume / rate    (already money)
                share COUNT (else) → volume_eur = native_close * volume / rate
                                     (= daily traded value / turnover in EUR)

    EUR / unknown currency passes close through and computes turnover at
    close×volume (fx = 1). A date with no FX rate leaves both *_eur None."""
    out = [{**r, "close_eur": None, "volume_eur": None} for r in rows]
    if not rows:
        return out
    notional = (asset_class or "") in _NOTIONAL_CLASSES
    ccy = (currency or "").strip()

    if not ccy or ccy.upper() == "EUR":
        for r, o in zip(rows, out):
            close = r.get("close")
            vol = r.get("volume") or 0
            o["close_eur"] = close
            o["volume_eur"] = vol if notional else ((close or 0.0) * vol)
        return out

    base, div = _SUBUNIT.get(ccy, (ccy, 1.0))
    dates = [_date.fromisoformat(str(r["date"])[:10]) for r in rows]
    look = _fx_series(base, dates)
    if look is None:
        return out
    keys, vals = look
    for r, o in zip(rows, out):
        d = str(r["date"])[:10]
        i = bisect.bisect_right(keys, d) - 1  # last rate on/before this date
        if i < 0:
            continue
        rate = vals[i]
        if not rate:
            continue
        close = r.get("close")
        vol = r.get("volume") or 0
        native_close = None if close is None else close / div
        if native_close is not None:
            o["close_eur"] = native_close / rate
        if notional:
            o["volume_eur"] = vol / rate                       # notional already in quote ccy
        elif native_close is not None:
            o["volume_eur"] = native_close * vol / rate        # turnover in EUR
    return out


