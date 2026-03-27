from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional

import pandas as pd
import streamlit as st

# project root = .../quick-insight
LOGO = Path(__file__).resolve().parents[3] / "assets" / "logo.png"

NAV_ITEMS: list[tuple[str, str, str]] = [
    ("app.py", "Home", "🏠"),
    ("pages/Earnings_Dashboard.py", "Earnings Dashboard", "💰"),
    ("pages/Upload_Portfolio.py", "Upload Portfolio", "⬆️"),
    ("pages/Company_Manager.py", "Company Manager", "⬆️"),
    ("pages/GuruFocus_Data.py", "GuruFocus Data", "📡"),
    ("pages/Company_Metric.py", "Company Metric", "🏢"),
    ("pages/Metric_X_vs_Y.py", "Metric X vs Y", "📉"),
    ("pages/5_new_companies.py", "New Companies", "🆕"),
]


def setup_page(*, controls: Optional[Callable[[], None]] = None) -> None:
    """
    Call once at the top of every page.
    - sets consistent tab title/icon + expanded sidebar
    - renders fixed sidebar header + nav
    - optionally renders page-specific controls below nav
    """
    st.set_page_config(
        page_title="Bustelberg Terminal",
        page_icon=str(LOGO) if LOGO.exists() else "🟢",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    with st.sidebar:
        if LOGO.exists():
            st.image(str(LOGO), width=110)
        st.markdown("## Bustelberg Terminal")
        st.divider()

        for path, label, icon in NAV_ITEMS:
            st.page_link(path, label=label, icon=icon)

        if controls is not None:
            st.divider()
            controls()


# ============================================================
# Shared UI helpers
# ============================================================
def company_label_map(companies: pd.DataFrame) -> tuple[list[str], dict[str, str], dict[str, dict]]:
    """
    Build consistent company labels + lookup maps.

    Returns:
      labels: list[str]
      label_to_ticker: dict[label -> primary_ticker]
      label_to_meta: dict[label -> company row dict]
    """
    if companies is None or companies.empty:
        return [], {}, {}

    df = companies.copy().fillna("")
    for c in ["primary_ticker", "primary_exchange", "company_name", "sector", "country"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    labels = (
        df["company_name"]
        + " (Exchange: "
        + df["primary_exchange"]
        + ") (Sector: "
        + df["sector"]
        + ") (Country: "
        + df["country"]
        + ")"
    )

    label_list = labels.tolist()
    label_to_ticker = dict(zip(label_list, df["primary_ticker"].tolist()))
    label_to_meta = dict(zip(label_list, df.to_dict(orient="records")))
    return label_list, label_to_ticker, label_to_meta


def sidebar_company_selectbox(
    *,
    companies: pd.DataFrame,
    key: str,
    label: str = "Company",
) -> tuple[str, dict]:
    """
    Renders the standard Company dropdown used across pages.
    Returns (ticker, meta).

    NOTE: must be called inside your controls() function (i.e. inside st.sidebar).
    """
    labels, label_to_ticker, label_to_meta = company_label_map(companies)

    if not labels:
        # still render something so sidebar doesn't feel broken
        st.selectbox(label, options=["(no companies)"], index=0, disabled=True, key=key)
        return "", {}

    picked = st.selectbox(label, options=labels, index=0, key=key)
    return label_to_ticker[picked], label_to_meta[picked]
