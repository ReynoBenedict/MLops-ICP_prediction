import streamlit as st

from components.diagnostics import render_diagnostics_panel
from components.layouts import render_footer
from components.styles import apply_custom_styles
from utils.data_loader import load_pipeline_metrics

apply_custom_styles()

metrics = load_pipeline_metrics()

# PAGE CONTENT: SYSTEM GOVERNANCE
st.title("MLOps & System Governance")
st.markdown("Transparansi model, audit registry, dan metrik integritas sistem.")

# 1. High-level Health
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("**Champion Model**")
    st.code(metrics.get("best_model", "LinearRegression"))
with col2:
    st.markdown("**Accuracy (RMSE)**")
    st.code(f"{metrics.get('rmse', 0.0):.4f}")
with col3:
    st.markdown("**Registry Stage**")
    st.success("Production")

# 2. Registry Deep-Dive
st.markdown("---")
st.markdown("### Model Registry Inspection")
st.write("Detail teknis mengenai model yang sedang aktif di lingkungan produksi.")
render_diagnostics_panel("Registry Audit")

# 3. Model Lineage Narrative
st.markdown("---")
st.markdown("### Model Governance Insights")
st.write(f"""
Model yang aktif saat ini (**{metrics.get("best_model", "LinearRegression")}**) dipilih berdasarkan kriteria RMSE terendah
melalui pipeline audit otomatis. Versi ini mendukung skema multi-fitur yang mencakup korelasi WTI dan rolling statistics.
Seluruh log eksperimen tersedia di MLflow Tracking server untuk keperluan audit kepatuhan.
""")

render_footer()

