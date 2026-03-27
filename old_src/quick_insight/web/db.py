# src/quick_insight/web/db.py
from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from quick_insight.config.config import settings


def db_path() -> str:
    p = getattr(settings, "db_path", None) or getattr(settings, "duckdb_path", None)
    if not p:
        raise RuntimeError("No DuckDB path configured. Expected settings.db_path or settings.duckdb_path.")
    return str(p)


def q(sql: str, params: list | None = None) -> pd.DataFrame:
    with duckdb.connect(db_path(), read_only=True) as con:
        return con.execute(sql, params or []).df()


@st.cache_data(show_spinner=False)
def load_companies() -> pd.DataFrame:
    df = q(
        """
        SELECT company_id, primary_ticker, primary_exchange, company_name, sector, country
        FROM company
        ORDER BY company_name
        """
    )
    for c in ["primary_ticker", "primary_exchange", "company_name", "sector", "country"]:
        if c in df.columns:
            df[c] = df[c].astype("string")
    return df
