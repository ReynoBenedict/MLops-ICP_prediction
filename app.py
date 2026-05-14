import streamlit as st

from components.layouts import render_footer
from components.styles import apply_custom_styles
from config.settings import PAGE_ICON

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ICP Intelligence Platform", page_icon=PAGE_ICON, layout="wide", initial_sidebar_state="expanded"
)

apply_custom_styles()

st.title("ICP Intelligence Platform")
st.caption("Peramalan eksekutif dan analisis pasar untuk penetapan harga minyak mentah Indonesia")

st.divider()

st.subheader("Platform Overview")
st.info(
    "Peramalan terintegrasi, analisis pasar, dan tata kelola model untuk prediksi ICP. Gunakan sidebar untuk menjelajahi dashboard, prediksi, tren, dan metrik model"
)

st.divider()

st.subheader("Modules")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("**Dashboard**")
    st.caption("Ringkasan prediksi real-time dan metrik kunci")

with col2:
    st.markdown("**Forecasting**")
    st.caption("Proyeksi bulan depan dengan rentang kepercayaan")

with col3:
    st.markdown("**Market Analysis**")
    st.caption("Tren, volatilitas, dan hubungan ICP-WTI")

st.divider()

st.subheader("Quick Start")
st.markdown("1. Dashboard | 2. Forecasting | 3. Market Trends | 4. MLOps")

render_footer()
