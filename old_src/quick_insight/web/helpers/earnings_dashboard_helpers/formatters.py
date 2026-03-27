# src/quick_insight/web/pages/earnings_dashboard_helpers/formatters.py
from __future__ import annotations

import numpy as np


def fmt_pct(x: float | None, *, digits: int = 2) -> str:
    """x is a ratio (0.12 -> 12%)."""
    if x is None or not np.isfinite(x):
        return "—"
    return f"{x * 100:,.{digits}f}%"


def fmt_pct_points(x: float | None, *, digits: int = 2) -> str:
    """x is already in percent points (12.0 -> 12%)."""
    if x is None or not np.isfinite(x):
        return "—"
    return f"{x:,.{digits}f}%"


def fmt_num(x: float | None, *, digits: int = 2) -> str:
    if x is None or not np.isfinite(x):
        return "—"
    return f"{x:,.{digits}f}"


def format_value_by_unit(x: float | None, unit: str | None, *, digits: int = 2) -> str:
    if unit == "pct_points":
        return fmt_pct_points(x, digits=digits)
    if unit == "pct_ratio":
        return fmt_pct(x, digits=digits)
    # fallback
    return fmt_num(x, digits=digits)
