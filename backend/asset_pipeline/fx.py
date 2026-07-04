"""EUR conversion for the asset-pipeline price series.

Reuses the OLD system's `fx_rate` table (ECB daily rates, forward-filled) via
`momentum.data.load_fx_rates`. `rate` there is units-of-currency per 1 EUR, so
`eur = native / rate`. Best-effort: a date with no available FX rate (e.g.
pre-1999, or a currency the fx sync never covered) yields close_eur=None. GBp
(London pence) is handled as GBP/100."""
from __future__ import annotations

import bisect
from datetime import date as _date


def to_eur(rows: list[dict], currency: str | None) -> list[dict]:
    """Return `rows` with a `close_eur` field added. EUR / no currency → close_eur
    = close. Otherwise divide the native close by the units-per-EUR rate on that
    date (as-of the last available rate). Rows with no FX get close_eur=None."""
    out = [{**r, "close_eur": None} for r in rows]
    if not rows:
        return out
    ccy = (currency or "").strip()
    if not ccy or ccy.upper() == "EUR":
        for o in out:
            o["close_eur"] = o.get("close")
        return out

    gbp_pence = ccy == "GBp"  # London pence → GBP/100
    base = "GBP" if gbp_pence else ccy

    dates = [_date.fromisoformat(str(r["date"])[:10]) for r in rows]
    try:
        from deps import supabase  # noqa: PLC0415
        from momentum.data import load_fx_rates  # noqa: PLC0415
        fx = load_fx_rates(supabase, [base], min(dates), max(dates))
    except Exception:  # noqa: BLE001 — best-effort; leave close_eur None
        return out
    ser = fx.get(base)
    if ser is None or len(ser) == 0:
        return out

    ser = ser.sort_index()
    keys = [
        (d.date().isoformat() if hasattr(d, "date") else str(d)[:10]) for d in ser.index
    ]
    vals = [float(v) for v in ser.values]

    for r, o in zip(rows, out):
        d = str(r["date"])[:10]
        i = bisect.bisect_right(keys, d) - 1  # last rate on/before this date
        if i < 0:
            continue
        rate = vals[i]
        close = r.get("close")
        if not rate or close is None:
            continue
        native = close / 100.0 if gbp_pence else close
        o["close_eur"] = native / rate
    return out
