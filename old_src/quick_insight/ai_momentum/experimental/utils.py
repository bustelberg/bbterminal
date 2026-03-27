# src\quick_insight\ai_momentum\experimental\utils.py
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from quick_insight.ingest.gurufocus.stock_indicator.data_lake import (
    ensure_indicator_in_data_lake,
)


# ---------------------------------------------------------------------------
# Generic timeseries helpers
# ---------------------------------------------------------------------------

def signed_log(s: pd.Series) -> pd.Series:
    """sign(x) * log(|x| + 1) — compress scale while preserving sign and zero."""
    name = s.name or "series"
    return (np.sign(s) * np.log1p(np.abs(s))).rename(f"{name}_signed_log")


def load_timeseries(path: str | Path) -> pd.Series:
    """
    Load a [[date_str, value], ...] JSON file into a date-indexed Series.
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    pairs = [
        (pd.Timestamp(row[0]), float(row[1]))
        for row in data
        if row[1] is not None
    ]

    if not pairs:
        return pd.Series(dtype=float, name=Path(path).stem)

    dates, values = zip(*pairs)
    return (
        pd.Series(values, index=pd.DatetimeIndex(dates), name=Path(path).stem)
        .sort_index()
    )


def log_timeseries(series: pd.Series) -> pd.Series:
    """Apply a natural log to a strictly positive timeseries."""
    name = series.name or "series"
    return np.log(series).rename(f"{name}_log")


def _needs_signed_log(series: pd.Series) -> bool:
    """
    Use signed_log whenever the visible series contains zero or negative values.
    """
    s = series.dropna()
    if s.empty:
        return False
    return bool((s <= 0).any())


def _safe_display_name(name: str) -> str:
    return str(name).strip() or "series"


# ---------------------------------------------------------------------------
# GuruFocus indicator loader
# ---------------------------------------------------------------------------

def load_gurufocus_indicator_timeseries(
    primary_ticker: str,
    primary_exchange: str,
    key: str,
    name: str,
    *,
    use_cache: bool = True,
    type_: str | None = None,
    years_back: int | None = None,
) -> pd.Series:
    """
    Ensure/load a GuruFocus stock-indicator series and return it as a Series.

    Parameters
    ----------
    years_back : int | None
        Number of years back from the most recent observation to keep.
        If None, return the full series.
    """
    actual_path = ensure_indicator_in_data_lake(
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
        indicator_key=key,
        type_=type_,
        use_cache=use_cache,
    )

    series = load_timeseries(actual_path).rename(_safe_display_name(name))

    if years_back is None or series.empty:
        return series

    cutoff = series.index.max() - pd.DateOffset(years=years_back)
    return series[series.index >= cutoff]


def plot_gurufocus_indicator_timeseries(
    primary_ticker: str,
    primary_exchange: str,
    key: str,
    cumulative: bool,
    *,
    use_cache: bool = True,
    type_: str | None = None,
    name: str | None = None,
) -> None:
    name = name or key
    """
    Plot one GuruFocus indicator as:
    1) raw (or cumulative raw)
    2) transformed version below:
       - log if strictly positive
       - signed_log otherwise

    Parameters
    ----------
    primary_ticker : str
    primary_exchange : str
    key : str
        GuruFocus indicator key, e.g. "price", "fcf_yield", "insider_buy".
    name : str
        Friendly display name for the chart.
    cumulative : bool
        If True, plot cumsum() of the series as the top panel.
    use_cache : bool
        Present for API compatibility. Freshness handling is delegated to the
        data-lake ensure function.
    type_ : str | None
        Optional explicit GuruFocus type override. Default omits it.
    """
    raw = load_gurufocus_indicator_timeseries(
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
        key=key,
        name=name,
        use_cache=use_cache,
        type_=type_,
    )

    top = raw.cumsum() if cumulative else raw
    top_name = _safe_display_name(name)

    if cumulative:
        top = top.rename(f"{top_name} (cumulative)")
    else:
        top = top.rename(top_name)

    bottom = signed_log(top) if _needs_signed_log(top) else log_timeseries(top)

    plot_timeseries(
        top,
        bottom,
        title=f"{primary_exchange}:{primary_ticker} | {top_name}",
    )


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def plot_timeseries(
    *series: pd.Series | tuple[pd.Series, str],
    title: str | None = None,
    bar_pct: float = 0.005,
) -> None:
    try:
        import matplotlib
        matplotlib.use("TkAgg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise ImportError("pip install matplotlib to use plot_timeseries()") from exc

    resolved: list[tuple[pd.Series, str]] = []
    for s in series:
        if isinstance(s, tuple):
            resolved.append((s[0], s[1] if len(s) > 1 else "line"))
        else:
            resolved.append((s, "line"))

    n = len(resolved)
    if n == 0:
        return

    fig, axes = plt.subplots(n, 1, figsize=(12, 3 * n), sharex=True)
    if n == 1:
        axes = [axes]

    def _draw_bars(ax, s: pd.Series, bar_width: float) -> None:
        colors = ["#2ecc71" if v >= 0 else "#e74c3c" for v in s.values]
        ax.bar(s.index, s.values, color=colors, width=bar_width, alpha=0.8)
        ax.axhline(0, color="black", linewidth=0.8)

    def _current_bar_width() -> float:
        xmin, xmax = axes[0].get_xlim()
        return (xmax - xmin) * bar_pct

    for ax, (s, style) in zip(axes, resolved):
        if style == "bar":
            _draw_bars(ax, s, _current_bar_width())
        else:
            ax.plot(s.index, s.values, linewidth=1.5)
        ax.set_ylabel(s.name or "value")
        ax.grid(True, alpha=0.3)

    axes[0].set_title(title or "timeseries")
    axes[-1].set_xlabel("date")
    fig.tight_layout()

    def _autoscale_y() -> None:
        for ax, (s, _) in zip(axes, resolved):
            xmin, xmax = ax.get_xlim()
            lo = pd.Timestamp(matplotlib.dates.num2date(xmin)).tz_localize(None)
            hi = pd.Timestamp(matplotlib.dates.num2date(xmax)).tz_localize(None)
            visible = s[(s.index >= lo) & (s.index <= hi)].dropna()
            if visible.empty:
                continue
            margin = (
                (visible.max() - visible.min()) * 0.05
                or abs(visible.max()) * 0.05
                or 1.0
            )
            ax.set_ylim(visible.min() - margin, visible.max() + margin)

    def _redraw_bars() -> None:
        xmin, xmax = axes[0].get_xlim()
        bar_width = (xmax - xmin) * bar_pct
        lo = pd.Timestamp(matplotlib.dates.num2date(xmin)).tz_localize(None)
        hi = pd.Timestamp(matplotlib.dates.num2date(xmax)).tz_localize(None)

        for ax, (s, style) in zip(axes, resolved):
            if style != "bar":
                continue
            ax.cla()
            visible = s[(s.index >= lo) & (s.index <= hi)]
            if not visible.empty:
                _draw_bars(ax, visible, bar_width)
            ax.set_ylabel(s.name or "value")
            ax.grid(True, alpha=0.3)

    def _on_scroll(event) -> None:
        if event.inaxes is None:
            return
        factor = 0.8 if event.button == "up" else 1.2
        for ax in axes:
            xmin, xmax = ax.get_xlim()
            mid = event.xdata if event.xdata else (xmin + xmax) / 2
            ax.set_xlim(mid - (mid - xmin) * factor, mid + (xmax - mid) * factor)
        _autoscale_y()
        _redraw_bars()
        fig.canvas.draw_idle()

    _drag_state: dict = {"active": False, "x0": None, "xlims": None}

    def _on_press(event) -> None:
        if event.inaxes is None:
            return
        _drag_state["active"] = True
        _drag_state["x0"] = event.x
        _drag_state["xlims"] = [ax.get_xlim() for ax in axes]

    def _on_release(event) -> None:
        _drag_state["active"] = False

    def _on_motion(event) -> None:
        if not _drag_state["active"] or event.x is None:
            return
        dx_px = event.x - _drag_state["x0"]
        x0, x1 = _drag_state["xlims"][0]
        data_per_px = (x1 - x0) / axes[0].get_window_extent().width
        dx = dx_px * data_per_px

        for ax, (xmin, xmax) in zip(axes, _drag_state["xlims"]):
            ax.set_xlim(xmin - dx, xmax - dx)
        _autoscale_y()
        _redraw_bars()
        fig.canvas.draw_idle()

    fig.canvas.mpl_connect("scroll_event", _on_scroll)
    fig.canvas.mpl_connect("button_press_event", _on_press)
    fig.canvas.mpl_connect("button_release_event", _on_release)
    fig.canvas.mpl_connect("motion_notify_event", _on_motion)
    plt.show()


if __name__ == "__main__":
    plot_gurufocus_indicator_timeseries(
        primary_ticker="ASML",
        primary_exchange="XAMS",
        key="price",
        name="price",
        cumulative=False,
        use_cache=True,
    )

    # Example:
    # plot_gurufocus_indicator_timeseries(
    #     primary_ticker="NVDA",
    #     primary_exchange="NASDAQ",
    #     key="insider_buy",
    #     name="Number of insider Buys",
    #     cumulative=True,
    #     use_cache=True,
    # )