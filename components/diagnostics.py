import streamlit as st
from pathlib import Path

from services.prediction_service import get_prediction_service

def render_diagnostics_panel(label="Engineering Diagnostics"):
    """Reusable diagnostics expander for model observability."""
    with st.expander(f"🔍 {label}"):
        st.markdown("**Model Contract & Inference Validation**")
        service = get_prediction_service()
        debug_info = service.get_debug_info()
        st.json(debug_info)
        
        if "critical_error" in debug_info:
            st.error(f"System Alert: {debug_info['critical_error']}")
