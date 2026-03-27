from __future__ import annotations
import streamlit as st
from quick_insight.web.ui import setup_page, LOGO

setup_page()

col1, col2 = st.columns([1, 2], vertical_alignment="center")
with col1:
    if LOGO.exists():
        st.image(str(LOGO), width=220)

with col2:
    st.title("Bustelberg Terminal")
    st.markdown(
        """
        Welcome to the Bustelberg Terminal.

        Use the navigation on the left to open dashboards.
        """
    )
