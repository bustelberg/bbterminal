from __future__ import annotations

import pandas as pd
import streamlit as st

from quick_insight.web.ui import setup_page
from quick_insight.web.helpers.upload_portfolio_helpers.repo import PortfolioRepo
from quick_insight.web.helpers.upload_portfolio_helpers.ui_blocks import (
    render_existing_portfolios,
    render_manual_builder,
    render_upload_section,
)
from quick_insight.web.helpers.portfolio_cache import portfolio_cache

setup_page()


# ============================================================
# Repo + caching
# ============================================================
_repo = PortfolioRepo()


@portfolio_cache.register
@st.cache_data(show_spinner=False)
def cached_company_lookup() -> pd.DataFrame:
    return _repo.load_company_lookup()


@portfolio_cache.register
@st.cache_data(show_spinner=False)
def cached_list_portfolios() -> pd.DataFrame:
    return _repo.list_portfolios()


@portfolio_cache.register
@st.cache_data(show_spinner=False)
def cached_load_portfolio_weights(portfolio_id: int) -> pd.DataFrame:
    return _repo.load_portfolio_weights(portfolio_id)


def clear_portfolio_caches() -> None:
    portfolio_cache.clear_all()


# ============================================================
# Page
# ============================================================
st.title("Portfolio Manager")

company_lu = cached_company_lookup()
if company_lu.empty:
    st.warning("No companies found in DB. Add companies first before creating portfolios.")
    st.stop()

company_labels = company_lu["label"].tolist()
label_to_company_id = dict(zip(company_lu["label"], company_lu["company_id"].astype(int)))

_shared = dict(
    company_labels=company_labels,
    repo=_repo,
    clear_caches_fn=clear_portfolio_caches,
)

render_existing_portfolios(
    ports=cached_list_portfolios(),
    cached_load_portfolio_weights=cached_load_portfolio_weights,
    **_shared,
)

st.divider()

render_manual_builder(label_to_company_id=label_to_company_id, **_shared)

st.divider()

render_upload_section(company_lu=company_lu, label_to_company_id=label_to_company_id, **_shared)