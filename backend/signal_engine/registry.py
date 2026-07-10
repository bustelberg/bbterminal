"""Every signal in the codebase, declared once.

Phase 2 of the engine unification. Two engines grew two signal batteries that
overlap by NAME and disagree by DEFINITION. This file is the single place those
definitions are stated, so the disagreements are data instead of folklore.

KEY vs NAME
    `key` is globally unique and namespaced by cadence (`daily.mom_12_1`,
    `me.mom_12_1`). `name` is what the rest of the system already calls it — the
    string in `PRICE_SIGNAL_DEFS`, in a saved `scheduled_strategy.config`'s
    `signal_weights`, and in the `/signal-lab` API response. Names are NOT unique
    across cadences, and that is the point: two specs sharing a name is the
    collision made visible rather than resolved by whoever imported last.

CADENCE
    `daily_asof`  — momentum's engine. Signals are evaluated at a cutoff date
        against a company's daily series, using only bars STRICTLY BEFORE the
        cutoff, dropping names whose last bar is >30 days stale. Values are
        percent (x100) and rounded to 2dp.
    `month_end`   — the asset pipeline's engine. Signals are `.shift()`s over a
        month-end panel. Values are raw fractions, unrounded. No cutoff guard and
        no staleness guard: lookahead is avoided structurally, not enforced.

THE TWO COLLISIONS, MEASURED (300 companies, 66 month-ends, identical inputs)
    mom_12_1      spearman 0.9961, mean intra-month rank shift 3.2 of ~298 names.
        The SAME signal. Differences are units, 2dp rounding, and calendar-offset
        vs month-end-bar anchoring. Safe to unify behind one definition.
    vol_trend_3m  spearman 0.5848, 29.0% sign disagreements, mean rank shift 59.6
        of ~298 names. NOT the same signal:
          daily.vol_trend_3m — recent 21-day avg volume vs the 21-day window 3
              months ago (asymmetric window boundaries).
          me.vol_trend_3m    — this month's avg daily volume vs 3 month-ends ago.
        Merging them under one name would silently change either /schedule's
        holdings or AlphaLab's research numbers. `PARITY` records this, and
        `tests/test_signal_registry.py` asserts nobody quietly "fixes" it.

Ownership today: this file owns every signal's IDENTITY (key, name, cadence,
units, rounding, group, default weight, description) and the `month_end`
BUILDERS. The `daily_asof` builders still live in `momentum/signals.py`'s
vectorized panel functions; porting them is the next step, and the golden master
(`tests/test_golden_rebalance.py`) is what proves that port.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

from .context import MonthEndCtx

Cadence = Literal["daily_asof", "month_end"]
Group = Literal["price", "volume", "trend"]
# pct    — already multiplied by 100 (a "12.34" means 12.34%)
# ratio  — a dimensionless quotient, NOT centered on zero (1.0 = unchanged)
# binary — 0 or 1
# fraction — a return-like quantity centered on zero (0.1234 means +12.34%)
Units = Literal["pct", "ratio", "binary", "fraction"]


@dataclass(frozen=True)
class SignalSpec:
    key: str                    # globally unique, cadence-namespaced
    name: str                   # legacy/public name; NOT unique across cadences
    cadence: Cadence
    group: Group
    label: str
    units: Units
    round_dp: int | None = None
    description: str = ""
    default_weight: float = 0.0
    # Only the month_end cadence has builders here yet (see module docstring).
    build: Callable[[MonthEndCtx], pd.DataFrame] | None = field(default=None, compare=False)


# ---------------------------------------------------------------------------
# daily_asof — momentum's battery. Order is load-bearing: `PRICE_SIGNAL_DEFS`
# and `TREND_SIGNAL_DEFS` are derived from this list in order, and the frontend
# renders the weight sliders in that order.
# ---------------------------------------------------------------------------
_DAILY: list[SignalSpec] = [
    SignalSpec(
        key="daily.mom_12_1", name="mom_12_1", cadence="daily_asof", group="price",
        label="12-1M Return", units="pct", round_dp=2, default_weight=3,
        description="Price return from 12 months ago to 1 month ago, skipping the most recent month. The classic Jegadeesh-Titman momentum factor — avoids short-term mean reversion.",
    ),
    SignalSpec(
        key="daily.mom_6m", name="mom_6m", cadence="daily_asof", group="price",
        label="6M Return", units="pct", round_dp=2, default_weight=2,
        description="Total price return over the last 6 months. Captures medium-term trend strength.",
    ),
    SignalSpec(
        key="daily.volatility_adjusted_return_6m", name="volatility_adjusted_return_6m",
        cadence="daily_asof", group="price",
        label="Vol-Adj Return", units="ratio", round_dp=4, default_weight=2,
        description="6-month return divided by annualized 6-month volatility. Rewards consistent uptrends over volatile spikes. Similar to a Sharpe ratio per stock.",
    ),
    SignalSpec(
        key="daily.drawdown_from_recent_high_pct", name="drawdown_from_recent_high_pct",
        cadence="daily_asof", group="price",
        label="Drawdown", units="pct", round_dp=2, default_weight=1,
        description="Current price vs. 52-week high, expressed as a negative %. Closer to 0% = near highs (stronger). Favors stocks holding up well.",
    ),
    SignalSpec(
        key="daily.above_200ma", name="above_200ma", cadence="daily_asof", group="price",
        label="Above 200 MA", units="binary", round_dp=None, default_weight=1,
        description="Binary: 1 if current price is above the 200-day moving average, 0 otherwise. Classic long-term trend filter — stocks below 200 MA are in a downtrend.",
    ),
    SignalSpec(
        key="daily.vol_20d_vs_60d", name="vol_20d_vs_60d", cadence="daily_asof", group="volume",
        label="Volume Surge", units="ratio", round_dp=4, default_weight=1,
        description="Ratio of 20-day average volume to 60-day average volume. Values above 1.0 indicate rising interest and conviction behind price moves. Confirms momentum rather than low-volume drift.",
    ),
    SignalSpec(
        key="daily.vol_trend_3m", name="vol_trend_3m", cadence="daily_asof", group="volume",
        label="Volume Trend 3M", units="pct", round_dp=2, default_weight=1,
        description="Percentage change in average daily volume: current month vs 3 months ago. Positive = growing institutional attention. Stocks with rising volume alongside price momentum tend to sustain their trends.",
    ),
    # Trend-quality pillar — only surfaced when a caller passes EXTRA_SIGNAL_DEFS
    # (the MomentumExtra strategy), so the classic Momentum strategy is untouched.
    SignalSpec(
        key="daily.trend_continuity", name="trend_continuity", cadence="daily_asof", group="trend",
        label="Trend Continuity", units="fraction", round_dp=4, default_weight=2,
        description="'Frog-in-the-pan' information discreteness: sign(6M return) × (% up-days − % down-days) over the last ~6 months. Higher = a winner grinding higher on many small up-days (continuous information → momentum tends to persist) rather than a few big jumps.",
    ),
    SignalSpec(
        key="daily.pct_up_days_6m", name="pct_up_days_6m", cadence="daily_asof", group="trend",
        label="Up-Day Consistency", units="pct", round_dp=2, default_weight=2,
        description="Fraction of up-days over the last ~6 months (×100). A smooth, steady climb scores higher than a jumpy one — a magnitude-agnostic measure of trend quality that complements raw return.",
    ),
    SignalSpec(
        key="daily.rsi_headroom", name="rsi_headroom", cadence="daily_asof", group="trend",
        label="RSI Headroom", units="fraction", round_dp=2, default_weight=1,
        description="Overbought guard: −max(0, RSI(14) − 50). Names with RSI above 50 are penalized (more so the more extended); RSI ≤ 50 is neutral. Keeps the strategy from chasing the most-stretched names near a blow-off top.",
    ),
]


# ---------------------------------------------------------------------------
# month_end — the asset pipeline's battery. Builders live here; `Ctx` panels are
# month-end close (`m`), 1-month return (`ret1`) and monthly avg daily volume
# (`vol`). Skip-a-month on the 6/12m signals avoids short-term-reversal
# contamination. Order matches the /signal-lab scoreboard.
# ---------------------------------------------------------------------------
_MONTH_END: list[SignalSpec] = [
    SignalSpec(
        key="me.mom_12_1", name="mom_12_1", cadence="month_end", group="price",
        label="12-1m momentum", units="fraction",
        build=lambda c: c.m.shift(1) / c.m.shift(12) - 1,
    ),
    SignalSpec(
        key="me.mom_6_1", name="mom_6_1", cadence="month_end", group="price",
        label="6-1m momentum", units="fraction",
        build=lambda c: c.m.shift(1) / c.m.shift(7) - 1,
    ),
    SignalSpec(
        key="me.mom_3m", name="mom_3m", cadence="month_end", group="price",
        label="3m momentum", units="fraction",
        build=lambda c: c.m / c.m.shift(3) - 1,
    ),
    SignalSpec(
        key="me.reversal_1m", name="reversal_1m", cadence="month_end", group="price",
        label="1m reversal", units="fraction",
        build=lambda c: c.ret1,
    ),
    SignalSpec(
        key="me.vol_adj_12_1", name="vol_adj_12_1", cadence="month_end", group="price",
        label="vol-adj 12-1m", units="fraction",
        build=lambda c: (c.m.shift(1) / c.m.shift(12) - 1) / c.ret1.rolling(12).std(),
    ),
    SignalSpec(
        key="me.dist_from_high_12m", name="dist_from_high_12m", cadence="month_end", group="price",
        label="dist from 12m high", units="fraction",
        build=lambda c: c.m / c.m.rolling(12).max() - 1,
    ),
    SignalSpec(
        key="me.dollar_vol_mom_6_1", name="dollar_vol_mom_6_1", cadence="month_end", group="volume",
        label="$-volume 6-1m", units="fraction",
        build=lambda c: c.dv.shift(1) / c.dv.shift(7) - 1,
    ),
    SignalSpec(
        key="me.vol_trend_3m", name="vol_trend_3m", cadence="month_end", group="volume",
        label="volume trend 3m", units="fraction",
        build=lambda c: c.vol / c.vol.shift(3) - 1,
    ),
    SignalSpec(
        key="me.vol_vs_6m_avg", name="vol_vs_6m_avg", cadence="month_end", group="volume",
        label="volume vs 6m avg", units="fraction",
        build=lambda c: c.vol / c.vol.rolling(6).mean() - 1,
    ),
]


SIGNALS: dict[str, SignalSpec] = {s.key: s for s in (*_DAILY, *_MONTH_END)}


@dataclass(frozen=True)
class Parity:
    """A measured relationship between two same-named specs in different cadences.

    Numbers from `scripts/signal_divergence.py` over 300 Leonteq companies and 66
    month-end decision points fed identical daily series. `equivalent` means the
    cross-sectional RANKING agrees closely enough that one definition could
    replace the other; it does NOT mean the values are equal (units and rounding
    differ by construction).
    """

    a: str
    b: str
    equivalent: bool
    spearman: float
    mean_rank_shift: float
    note: str


PARITY: tuple[Parity, ...] = (
    Parity(
        a="daily.mom_12_1", b="me.mom_12_1", equivalent=True,
        spearman=0.9961, mean_rank_shift=3.17,
        note="Same signal. Differs by units (pct vs fraction), 2dp rounding, and "
             "calendar-offset vs month-end-bar anchoring. Safe to unify.",
    ),
    Parity(
        a="daily.vol_trend_3m", b="me.vol_trend_3m", equivalent=False,
        spearman=0.5848, mean_rank_shift=59.62,
        note="DIFFERENT signals sharing a name. daily = recent 21-day avg volume vs "
             "the 21-day window 3 months ago; me = this month's avg daily volume vs "
             "3 month-ends ago. 29.0% sign disagreements. Do NOT merge: /schedule "
             "depends on the daily definition (golden master pins it) and AlphaLab "
             "reports the month-end one.",
    ),
)


def by_cadence(cadence: Cadence) -> list[SignalSpec]:
    """Specs for one cadence, in declaration order."""
    return [s for s in SIGNALS.values() if s.cadence == cadence]


def by_name(name: str, cadence: Cadence) -> SignalSpec:
    for s in SIGNALS.values():
        if s.name == name and s.cadence == cadence:
            return s
    raise KeyError(f"no {cadence!r} signal named {name!r}")


def legacy_defs(cadence: Cadence, groups: tuple[Group, ...]) -> list[dict]:
    """The `{key, label, description, default_weight, group}` dicts the momentum
    scoring engine and the frontend weight sliders consume. `key` here is the
    legacy NAME, not the registry key — a saved `signal_weights` config uses it.
    """
    return [
        {
            "key": s.name,
            "label": s.label,
            "description": s.description,
            "default_weight": s.default_weight,
            "group": s.group,
        }
        for s in by_cadence(cadence)
        if s.group in groups
    ]


def colliding_names() -> dict[str, list[str]]:
    """`{name: [keys]}` for names claimed by more than one cadence."""
    seen: dict[str, list[str]] = {}
    for s in SIGNALS.values():
        seen.setdefault(s.name, []).append(s.key)
    return {n: ks for n, ks in seen.items() if len(ks) > 1}
