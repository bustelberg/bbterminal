"""Relative 12-1 momentum: where a company's momentum ranks against the universe it competes with.

WHAT THIS IS FOR
    A public-facing indicator. The question it answers is NOT "did this stock go up" — it is
    "how does this stock's 12-1 month momentum compare to everything else you could have owned".
    Those are different questions and they routinely disagree: a +8% name in a market whose median
    is +27% has positive absolute momentum and weak RELATIVE momentum.

⚠⚠ SO EVERY SURFACE BUILT ON THIS OWES THE READER THE RAW NUMBER TOO. A red orb on its own reads as
    "this stock fell", which for that +8% name is simply false. `raw_return_pct` travels with the
    rank for exactly this reason — the hover says "+8.0% return, 14th percentile" and the reader can
    see which of the two is being ranked. Titling the surface "Relative Momentum" is the other half.

⚠⚠ THE SEVEN STATES HAVE FIXED POPULATIONS, BY CONSTRUCTION AND ON PURPOSE. The cut points below
    are percentiles, so exactly 10% of the universe is `+++` on any given day and exactly 10% is
    `---`. This indicator therefore CANNOT say "the whole market is strong" — it does not measure
    that, and a reader who thinks it does will misread every bull and bear market. Measured on ACWI:
    113 / 171 / 170 / 227 / 171 / 170 / 114 of 1,136 names.

⚠⚠ THE MOMENTUM IS NOT COMPUTED HERE. `signal_engine.daily` owns it, via
    `momentum.signals.compute_signals_panel`, and it owns three rules that are easy to not know
    about: the cutoff is a STRICT `<` (never train on the bar you trade), `MIN_BARS = 20`, and
    `MAX_STALENESS_DAYS = 30` (a name whose last close is older than that is dropped rather than
    ranked on a stale price). A hand-rolled `price[t-1m] / price[t-12m] - 1` over a panel is the
    obvious version and it is wrong in a way that does not look wrong: written that way during this
    module's design it returned ZERO names, because on a global universe the last date on or before
    a target is often a date only one exchange traded, and every other column on that row is NaN.

⚠ WHY BOTH SCALES ARE PERSISTED. A percentile is insensitive to distribution shape and needs no
    explanation, which is what a public page wants. A robust z-score says how UNUSUALLY far from the
    pack something is, which a percentile structurally cannot: the 99th and 100th percentile are one
    rank apart whether the gap between them is 1% or 1000%. Neither is derivable from the other, so
    both are stored — percentile for display, z for anything modelling this.

⚠ ROBUST, NOT PLAIN, z: `(x - median) / (1.4826 * MAD)`, clipped to +/-2. Mean and standard
    deviation are both dominated by the tail this cross-section always has — the largest ACWI 12-1
    return measured during design was +1638% (Kioxia), with SK Hynix, Micron, Western Digital and
    SK Square behind it. That is a real memory/AI supercycle, not a data error, so it cannot be
    cleaned away; it has to be handled. Same reasoning, and the same constants, as
    `momentum.scoring._normalize`.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd

from deps import supabase as _default_supabase
from momentum.backtest.indices import _build_price_index
from momentum.data.fx import convert_prices_to_eur, load_fx_rates
from momentum.data.prices import load_all_prices
from momentum.data.universe import load_company_currency, load_universe
from momentum.scoring import _MAD_TO_SIGMA, _ROBUST_Z_CAP
from momentum.signals import compute_signals_panel

log = logging.getLogger(__name__)

TABLE = "relative_momentum"

# ⚠⚠ THE CUT POINTS LIVE HERE AND NOWHERE ELSE. `state` is persisted rather than derived on read so
#   that every surface — the orb, the tooltip, an API consumer — shows the same seven states. A
#   second copy of these numbers in a component is how two screens come to disagree about whether a
#   company is `+` or `++`.
#
# ⚠ Upper bounds, walked in order. A percentile at exactly a boundary falls in the LOWER bucket,
#   which is arbitrary but must be decided once: `bisect` semantics, not "closest".
_CUTS = ((0.10, -3), (0.25, -2), (0.40, -1), (0.60, 0), (0.75, 1), (0.90, 2), (1.01, 3))

STATE_LABELS = {-3: "---", -2: "--", -1: "-", 0: "*", 1: "+", 2: "++", 3: "+++"}

# 13 months of history is what a 12-1 return spans; the rest is slack for closed markets, holidays
# and the staleness window. Deliberately generous — the load is one COPY either way and a name that
# falls a few bars short of `MIN_BARS` is silently absent, which is the failure that looks like data.
_LOOKBACK_DAYS = 500


@dataclass(frozen=True)
class RelativeMomentum:
    """One universe, one date."""
    universe_label: str
    as_of: date
    rows: pd.DataFrame          # company_id, raw_return_pct, pct_rank, robust_z, state
    universe_n: int             # companies that carried a 12-1 return — the rank's denominator
    members_total: int          # universe members before coverage; `universe_n` is a subset

    @property
    def coverage_pct(self) -> float:
        return 0.0 if not self.members_total else 100.0 * self.universe_n / self.members_total


def _bucket(pct: pd.Series) -> pd.Series:
    """Percentile (0-1) -> one of seven states."""
    out = pd.Series(3, index=pct.index, dtype="int64")
    for hi, state in reversed(_CUTS):
        out = out.mask(pct <= hi, state)
    return out


def _robust_z(values: pd.Series) -> pd.Series:
    """(x - median) / (1.4826 * MAD), clipped. NaN when the cross-section has no dispersion.

    ⚠ NaN, NOT 0.0, on a zero MAD. "Every company is identical" and "this company sits exactly at
    the median" are different facts, and a column that renders them the same removes the reader's
    ability to tell a degenerate universe from an ordinary one.
    """
    med = values.median()
    mad = (values - med).abs().median()
    if not mad or pd.isna(mad):
        return pd.Series(np.nan, index=values.index)
    return ((values - med) / (_MAD_TO_SIGMA * mad)).clip(-_ROBUST_Z_CAP, _ROBUST_Z_CAP)


def compute(
    universe_label: str,
    as_of: date,
    *,
    supabase=None,
    on_step=None,
) -> RelativeMomentum:
    """Rank one universe's 12-1 momentum as of one date.

    `on_step(str)` reports progress — this takes seconds and a silent multi-second wait is
    indistinguishable from a hang.
    """
    sb = supabase if supabase is not None else _default_supabase
    step = on_step or (lambda _m: None)

    step(f"loading {universe_label} membership")
    universe_df = load_universe(sb, universe_label=universe_label)
    if universe_df.empty:
        raise ValueError(f"universe {universe_label!r} has no members")
    members_total = len(universe_df)
    company_ids = [int(c) for c in universe_df["company_id"].unique()]
    step(f"{members_total} members; loading {_LOOKBACK_DAYS} days of closes")

    start = as_of - timedelta(days=_LOOKBACK_DAYS)
    prices = load_all_prices(sb, company_ids, start, as_of)
    if prices.empty:
        raise ValueError(f"no prices for {universe_label!r} as of {as_of}")

    # ⚠⚠ EUR FIRST, AND THIS IS NOT OPTIONAL — IT IS THE WHOLE CROSS-SECTION'S BASIS. `gf.close` is
    #   the raw close in each security's OWN trading currency, so ranking it directly compares a
    #   Japanese stock's JPY return against an American one's USD return and calls the difference
    #   momentum. A name up 20% in JPY while JPY fell 10% against the euro is up 8% to the reader
    #   this page is for. EUR is the return basis everywhere in this codebase, and the backtest
    #   pipeline converts before computing signals for exactly this reason; a precompute that did
    #   not would produce a plausible ranking that no euro investor could act on.
    #
    # ⚠ It is also what makes this comparable to the Analyse modal's per-holding momentum, which is
    #   computed from the daily EUR close. Two momentum numbers on two currency bases, one bucketed
    #   against the other's distribution, would disagree near every bucket boundary.
    #
    # ⚠ A currency with no FX series drops its rows here rather than converting at a wrong rate —
    #   `convert_prices_to_eur` reports it, and the name then fails the bar minimum and is absent.
    currencies = load_company_currency(sb, company_ids)
    fx = load_fx_rates(sb, sorted({c for c in currencies.values() if c}), start, as_of)
    prices, fx_stats = convert_prices_to_eur(prices, currencies, fx)
    if fx_stats.get("missing_currencies"):
        step(f"⚠ no FX for {', '.join(fx_stats['missing_currencies'])} — those names are dropped")
    step(f"{len(prices):,} price rows in EUR "
         f"({fx_stats.get('converted_rows', 0):,} converted); computing signals")

    # ⚠ THE SIGNAL ENGINE, NOT AN INLINE FORMULA — see the module docstring. It owns the strict `<`
    #   cutoff, MIN_BARS and the 30-day staleness drop, and those three are the difference between
    #   this number and a plausible wrong one.
    panel = compute_signals_panel(
        universe_df, [as_of], price_index=_build_price_index(prices),
    )[as_of]

    if panel.empty or "mom_12_1" not in panel.columns:
        raise ValueError(f"no 12-1 momentum computable for {universe_label!r} as of {as_of}")

    df = panel[["company_id", "mom_12_1"]].dropna(subset=["mom_12_1"]).copy()
    df = df.rename(columns={"mom_12_1": "raw_return_pct"})
    # ⚠ A company that cannot carry a 12-1 return is ABSENT, never present with a hole: a reader of
    #   this table should never have to tell "no data" apart from "zero momentum".
    if df.empty:
        raise ValueError(f"every {universe_label!r} member was dropped as of {as_of}")

    df["pct_rank"] = df["raw_return_pct"].rank(pct=True, method="average")
    df["robust_z"] = _robust_z(df["raw_return_pct"])
    df["state"] = _bucket(df["pct_rank"])
    n = len(df)
    step(f"ranked {n} of {members_total} members ({100.0 * n / members_total:.0f}% coverage)")

    return RelativeMomentum(universe_label, as_of, df.reset_index(drop=True), n, members_total)


def load_distribution(
    universe_label: str = "ACWI", *, supabase=None,
) -> tuple[np.ndarray, date, int] | None:
    """The universe's sorted 12-1 returns for its newest computed date: (values, as_of, n).

    `None` when nothing has been precomputed yet — a caller must render the absence, never a
    default distribution.

    ⚠⚠ THIS IS HOW A HOLDING GETS A STATE WITHOUT BEING A UNIVERSE MEMBER, AND IT IS THE WHOLE
    REASON THE MODAL CAN USE THIS AT ALL. The obvious integration — join each holding's ISIN to a
    `company_id` and read its stored rank — covers only **154 of 254** held ISINs (61%), because a
    book holds ETFs, bonds, funds and plenty of names that are not ACWI constituents. Measured per
    book it ranges from 56% (BUS_Neutraal_Dyn, 324 of 574) to 95%. Replacing a column that has a
    number for every priced holding with one that is blank two rows in five is a regression however
    good the new number is.

    Bucketing the holding's OWN momentum against the universe's DISTRIBUTION has no such gap: any
    instrument with a 12-1 return gets a state, member or not, and the statement it makes ("this
    moved like the 82nd percentile of ACWI") is exactly the intended one.

    ⚠⚠ IT IS ONLY LEGITIMATE BECAUSE BOTH SIDES ARE IN EUR. `_holding_risk` computes momentum from
    the daily EUR close; this distribution is built from EUR-converted closes (see `compute`). Had
    either stayed in native currency, a number from one basis would be bucketed against a
    distribution from the other and every reading near a boundary would be arbitrary. The two are
    still different VENDORS (Yahoo for the holding, GuruFocus for the universe), which is fine for a
    bucket boundary and would not be fine for a subtraction — so nothing here subtracts them.

    ⚠ The whole array, not six cut points: `percentile_of` then answers exactly rather than by
    interpolating between breaks, and 1,745 floats is 14 KB.
    """
    sb = supabase if supabase is not None else _default_supabase
    newest = (sb.table(TABLE).select("as_of_date").eq("universe_label", universe_label)
              .order("as_of_date", desc=True).limit(1).execute().data or [])
    if not newest:
        return None
    as_of = date.fromisoformat(str(newest[0]["as_of_date"])[:10])

    vals: list[float] = []
    off = 0
    while True:
        # ⚠ Paged: a universe is ~1,750 rows and PostgREST caps a page at 1,000 on the cloud, so an
        #   unpaged read would silently rank against the first 1,000 of them.
        page = (sb.table(TABLE).select("raw_return_pct")
                .eq("universe_label", universe_label).eq("as_of_date", as_of.isoformat())
                .order("raw_return_pct").range(off, off + 999).execute().data or [])
        if not page:
            break
        vals.extend(float(r["raw_return_pct"]) for r in page)
        off += len(page)
    if not vals:
        return None
    return np.sort(np.asarray(vals, dtype="float64")), as_of, len(vals)


def percentile_of(value: float, dist: np.ndarray) -> float:
    """Where `value` falls in `dist` (0-1), by the same midpoint convention `rank(pct=True)` uses.

    ⚠⚠ IT MUST REPRODUCE `rank(pct=True, method="average")` EXACTLY, or a company that IS a
    constituent gets one percentile from this path and a different one from its stored row — two
    answers to one question, one click apart. The ranks pandas averages are ONE-BASED, so the
    +1 below is not an off-by-one guard, it is the convention: with no ties `searchsorted` gives
    (i, i+1) and the average rank is i+1, not i+0.5. Dropping it shifts every percentile by 0.5/n —
    small enough to look right, and enough to move a value across a bucket boundary. Pinned by
    `tests/test_relative_momentum.py`, which round-trips every stored constituent.
    """
    n = len(dist)
    if not n:
        return 0.5
    lo = float(np.searchsorted(dist, value, side="left"))
    hi = float(np.searchsorted(dist, value, side="right"))
    return ((lo + hi + 1.0) / 2.0) / n


def state_of(value: float, dist: np.ndarray) -> tuple[int, float]:
    """(state, percentile) for one instrument's 12-1 return against a universe's distribution."""
    pct = percentile_of(value, dist)
    return int(_bucket(pd.Series([pct])).iloc[0]), pct


def persist(result: RelativeMomentum, *, supabase=None, chunk: int = 500) -> int:
    """Upsert one (universe, date) slice. Returns rows written.

    ⚠ UPSERT ON THE FULL KEY, so re-running a date REPLACES it rather than accumulating. The 05:00
    price tick can land late closes under an earlier `target_date`, so the same as-of date can
    legitimately produce different numbers on a later run — the newest run wins.
    """
    sb = supabase if supabase is not None else _default_supabase
    payload = [{
        "universe_label": result.universe_label,
        "as_of_date": result.as_of.isoformat(),
        "company_id": int(r.company_id),
        "raw_return_pct": float(r.raw_return_pct),
        "pct_rank": float(r.pct_rank),
        # ⚠ `robust_z` is the one nullable column — None, never 0.0. See `_robust_z`.
        "robust_z": None if pd.isna(r.robust_z) else float(r.robust_z),
        "state": int(r.state),
        "universe_n": result.universe_n,
    } for r in result.rows.itertuples(index=False)]

    written = 0
    for i in range(0, len(payload), chunk):
        sb.table(TABLE).upsert(
            payload[i:i + chunk],
            on_conflict="universe_label,as_of_date,company_id",
        ).execute()
        written += len(payload[i:i + chunk])
    return written
