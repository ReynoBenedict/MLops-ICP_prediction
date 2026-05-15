import logging

import streamlit as st

from components.layouts import render_footer
from components.styles import apply_custom_styles
from config.settings import PAGE_ICON

# Page Configuration
st.set_page_config(
    page_title="ICP Intelligence Platform",
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    apply_custom_styles()

    st.title("ICP Intelligence Platform")
    st.caption("Peramalan eksekutif dan analisis pasar untuk penetapan harga minyak mentah Indonesia")

    st.divider()

    # Hero Section
    st.subheader("Platform Overview")
    st.info(
        """
        Selamat datang di platform intelijen peramalan ICP. Platform ini mengintegrasikan
        pipeline MLOps untuk prediksi harga minyak mentah Indonesia (ICP) dengan
        analisis pasar real-time dan manajemen model otomatis.
        """
    )

    st.divider()

    # Feature Grid
    st.subheader("Modul Platform")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### 📊 Dashboard")
        st.write("Ringkasan prediksi real-time, metrik performa model, dan indikator pasar utama.")
        if st.button("Buka Dashboard", key="btn_dash"):
            st.switch_page("pages/1_Dashboard.py")

    with col2:
        st.markdown("### 📈 Forecasting")
        st.write("Proyeksi harga ICP periode berikutnya lengkap dengan rentang kepercayaan dan sinyal model.")
        if st.button("Lihat Forecast", key="btn_fore"):
            st.switch_page("pages/2_Forecasting.py")

    with col3:
        st.markdown("### 🔍 Market Analysis")
        st.write("Analisis korelasi ICP-WTI, tren volatilitas, dan dinamika penggerak pasar.")
        if st.button("Analisis Pasar", key="btn_market"):
            st.switch_page("pages/3_ICP_vs_WTI.py")

    st.divider()

    # Secondary Features
    col4, col5 = st.columns(2)
    with col4:
        st.markdown("### 🧪 Simulation")
        st.caption("Uji skenario pasar kustom dan lihat dampaknya terhadap harga.")
        if st.button("Mulai Simulasi", key="btn_sim"):
            st.switch_page("pages/5_Predict_Price.py")
    with col5:
        st.markdown("### ⚙️ MLOps Hub")
        st.caption("Status pipeline, audit model, dan manajemen registry MLflow.")
        if st.button("Sistem MLOps", key="btn_mlops"):
            st.switch_page("pages/6_MLOps_Center.py")

    st.divider()

    # System Status
    with st.expander("ℹ️ Tentang Platform", expanded=False):
        st.write("""
        Platform ini dikembangkan untuk memberikan wawasan berbasis data kepada pengambil kebijakan.
        Data diperbarui secara otomatis melalui pipeline data yang terhubung dengan benchmark global.
        """)
        st.caption("MLOps-ICP Pipeline v1.0.0 | Engine: Scikit-learn + MLflow")

    render_footer()

if __name__ == "__main__":
    main()
