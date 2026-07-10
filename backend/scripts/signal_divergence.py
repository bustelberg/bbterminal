"""Measure how far apart two same-named signals in different cadences really are.

This is what produces the numbers in `signal_engine.registry.PARITY`. Re-run it
whenever a signal is added whose `name` collides with one in the other cadence —
`tests/test_signal_registry.py::test_exactly_the_two_known_collisions_exist`
fails until the relationship has been measured and recorded.

    cd backend && PYTHONPATH=. uv run python scripts/signal_divergence.py

Both engines are fed the SAME daily close/volume series (real GuruFocus data,
loaded through the Phase-1 `timeseries` facade), evaluated at the SAME month-end
decision points, and their outputs are compared cell by cell.

Alignment:
  * alphalab's `m` at month-end M is the last close IN month M.
    `mom_12_1 = m.shift(1)/m.shift(12) - 1` -> last close of M-1 over last close of M-12.
  * momentum's cutoff `c` anchors on the last bar STRICTLY BEFORE c.
    Setting c = (month-end M) + 1 day makes the anchor the month-end M bar, so the
    two look at the same decision point.

Known nominal differences to confirm, not discover:
  * units      : momentum returns percent (x100), alphalab a raw fraction
  * rounding   : momentum rounds to 2dp, alphalab does not
  * anchoring  : momentum walks back by pd.DateOffset(months=n) from a CALENDAR day;
                 alphalab counts month-end BARS. These disagree around holidays and
                 short months.
  * staleness  : momentum drops names whose last bar is >30d before the cutoff
"""
from __future__ import annotations

import warnings
from datetime import date

import numpy as np
import pandas as pd

import deps  # noqa: F401 — loads .env before anything touches Supabase
from deps import supabase
from timeseries import load_series, to_panel

# numpy emits divide-by-zero warnings on the all-NaN cross-sections; they're
# expected and drown the report.
warnings.filterwarnings("ignore")

N_COMPANIES = 300
START, END = date(2021, 1, 1), date(2026, 6, 30)


def _company_ids(n: int) -> list[int]:
    # ORDER BY before LIMIT: an unordered .limit() returns an arbitrary subset,
    # so two runs would compare different companies.
    r = (supabase.table("universe_membership")
         .select("company_id, universe!inner(label)")
         .eq("universe.label", "LEONTEQ (as of 2026-06-17)")
         .order("company_id").limit(n).execute())
    return sorted({x["company_id"] for x in (r.data or [])})[:n]


def month_end_cutoffs(idx: pd.DatetimeIndex) -> tuple[list[date], pd.DatetimeIndex]:
    """(momentum cutoffs, alphalab month-end labels) for the same decision points."""
    month_ends = pd.DatetimeIndex(sorted({d for d in idx.to_period("M").to_timestamp("M")}))
    month_ends = month_ends[(month_ends >= idx.min()) & (month_ends <= idx.max())]
    cutoffs = [(me + pd.Timedelta(days=1)).date() for me in month_ends]
    return cutoffs, month_ends


def main() -> None:
    cids = _company_ids(N_COMPANIES)
    print(f"companies: {len(cids)}   window: {START} .. {END}")

    px = load_series(cids, "gf.close", START, END).rename(columns={"close": "price"})
    vl = load_series(cids, "gf.volume", START, END)
    print(f"close rows: {len(px):,}   volume rows: {len(vl):,}")

    # ---- momentum engine (daily, strict-< as-of, pct, 2dp) -----------------
    from momentum.backtest.indices import _build_price_index, _build_volume_index
    from momentum.signals import compute_signals_panel

    price_index = _build_price_index(px.rename(columns={"entity_id": "company_id", "date": "target_date"}))
    volume_index = _build_volume_index(vl.rename(columns={"entity_id": "company_id", "date": "target_date"}))

    all_dates = pd.DatetimeIndex(sorted(px["date"].unique()))
    cutoffs, month_ends = month_end_cutoffs(all_dates)
    print(f"decision points: {len(cutoffs)}  ({cutoffs[0]} .. {cutoffs[-1]})")

    universe_df = pd.DataFrame({
        "company_id": cids, "sector": "X", "company_name": "n", "gurufocus_ticker": "t",
    })
    mom = compute_signals_panel(universe_df, cutoffs, price_index=price_index, volume_index=volume_index)

    # {(month_end, cid): value}
    mom_flat: dict[str, dict[tuple[pd.Timestamp, int], float]] = {"mom_12_1": {}, "vol_trend_3m": {}}
    for c, me in zip(cutoffs, month_ends):
        df = mom.get(c)
        if df is None or df.empty:
            continue
        for sig in mom_flat:
            if sig not in df.columns:
                continue
            for cid, v in zip(df["company_id"], df[sig]):
                if v is not None and not pd.isna(v):
                    mom_flat[sig][(me, int(cid))] = float(v)

    # ---- alphalab engine (month-end bars, .shift(), fraction) --------------
    from asset_pipeline.signals import build_signals, monthly_panels

    close_panel = to_panel(px, "price")
    vol_panel = to_panel(vl, "volume")
    m, ret1, vol = monthly_panels(close_panel, vol_panel)
    ap = build_signals(m, ret1, vol)

    # ---- compare -----------------------------------------------------------
    for sig in ("mom_12_1", "vol_trend_3m"):
        print()
        print("=" * 78)
        print(f"{sig}")
        print("=" * 78)
        a_panel = ap[sig]
        a = a_panel.stack()
        a.index.names = ["month_end", "company_id"]
        a_map = {(d, int(c)): float(v) for (d, c), v in a.items() if np.isfinite(v)}
        b_map = mom_flat[sig]

        # momentum is percent; alphalab is a fraction
        b_map = {k: v / 100.0 for k, v in b_map.items()}

        only_a = len(set(a_map) - set(b_map))
        only_b = len(set(b_map) - set(a_map))
        both = sorted(set(a_map) & set(b_map))
        print(f"  cells: alphalab={len(a_map):,}  momentum={len(b_map):,}  "
              f"both={len(both):,}  only-alphalab={only_a:,}  only-momentum={only_b:,}")
        if not both:
            print("  no overlap")
            continue

        av = np.array([a_map[k] for k in both])
        bv = np.array([b_map[k] for k in both])
        diff = bv - av

        exact = int((np.abs(diff) < 1e-12).sum())
        within_round = int((np.abs(diff) <= 0.5e-2 / 100 + 1e-12).sum())  # 2dp of a percent
        print(f"  identical (<1e-12)          : {exact:,} / {len(both):,}  ({100*exact/len(both):.1f}%)")
        print(f"  within momentum's 2dp rounding: {within_round:,}  ({100*within_round/len(both):.1f}%)")
        print(f"  |diff|  median={np.median(np.abs(diff)):.6f}  p90={np.quantile(np.abs(diff),0.9):.6f}  max={np.abs(diff).max():.6f}")
        print(f"  pearson r  = {np.corrcoef(av, bv)[0,1]:.6f}")
        rs = pd.Series(av).rank().corr(pd.Series(bv).rank())
        print(f"  spearman r = {rs:.6f}   <- this is what selection actually depends on")
        sign_flips = int(((av > 0) != (bv > 0)).sum())
        print(f"  sign disagreements: {sign_flips:,}  ({100*sign_flips/len(both):.2f}%)")

        # Where does the cross-sectional RANK differ? That's what changes holdings.
        dfc = pd.DataFrame({"k": both, "a": av, "b": bv})
        dfc["month"] = [k[0] for k in both]
        rank_delta = []
        for _, g in dfc.groupby("month"):
            if len(g) < 20:
                continue
            ra, rb = g["a"].rank(ascending=False), g["b"].rank(ascending=False)
            rank_delta.append((ra - rb).abs().mean())
        if rank_delta:
            print(f"  mean |rank shift| within a month: {np.mean(rank_delta):.2f} places "
                  f"(of ~{int(dfc.groupby('month').size().mean())} names)")


if __name__ == "__main__":
    main()
