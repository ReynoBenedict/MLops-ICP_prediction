import streamlit as st


def render_sidebar():
    """Empty function - native Streamlit multipage navigation handles sidebar automatically."""
    pass


def render_footer():
    """Compact footer using native Streamlit components."""
    st.divider()
    col1, col2 = st.columns([3, 1])
    with col1:
        st.caption("© 2026 MLOps ICP Intelligence Platform | v2.5.1")
    with col2:
        st.caption("🔒 Secured")
