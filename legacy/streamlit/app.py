import streamlit as st

from config.settings import PAGE_ICON

st.set_page_config(
    page_title="ICP Intelligence Platform",
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)

st.switch_page("pages/1_Dashboard.py")