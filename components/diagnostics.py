import streamlit as st

from services.prediction_service import get_prediction_service


def render_diagnostics_panel(label="System Diagnostics"):
    with st.expander(label, expanded=False):

        st.caption(
            "Observability panel for model validation, registry state, and inference diagnostics."
        )

        service = get_prediction_service()
        debug_info = service.get_debug_info()

        if "critical_error" in debug_info:
            st.error(
                f"Critical system issue detected: {debug_info['critical_error']}"
            )
        else:
            st.success("Inference service operational.")

        st.markdown("### Runtime Metadata")

        st.json(debug_info)