import streamlit as st

def render_hero_prediction(pred_val: float):
    """Native Streamlit hero prediction card."""
    st.metric(label="📊 Forecast", value=f"${pred_val:.2f}", help="USD / Barrel")

def render_kpi_card(label: str, value: str, detail: str = ""):
    """Compact native Streamlit KPI metric card."""
    st.metric(label=label, value=value, help=detail if detail else None)
