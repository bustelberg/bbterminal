from __future__ import annotations

import pandas as pd
import streamlit as st

from quick_insight.web.ui import setup_page
from quick_insight.web.helpers.company_manager_helpers.repo import CompanyRepo
from quick_insight.web.helpers.company_manager_helpers.ui_blocks import (
    render_add_company,
    render_company_list,
    render_delete_companies,
)
from quick_insight.web.helpers.portfolio_cache import portfolio_cache

setup_page()

# ── Repo + caching ────────────────────────────────────────────────────────────
_repo = CompanyRepo()


@portfolio_cache.register
@st.cache_data(show_spinner=False)
def cached_list_companies() -> pd.DataFrame:
    return _repo.list_companies()


def clear_company_caches() -> None:
    # clears ALL portfolio_cache-registered caches (companies + portfolios)
    # so that downstream pages (Earnings, Upload_Portfolio) also see fresh data
    portfolio_cache.clear_all()


# ── Page ──────────────────────────────────────────────────────────────────────
st.title("Company Manager")

render_add_company(
    repo=_repo,
    clear_caches_fn=clear_company_caches,
)

st.divider()

render_company_list(
    repo=_repo,
    clear_caches_fn=clear_company_caches,
    cached_list_companies=cached_list_companies,
)

st.divider()

render_delete_companies(
    repo=_repo,
    clear_caches_fn=clear_company_caches,
    cached_list_companies=cached_list_companies,
)
