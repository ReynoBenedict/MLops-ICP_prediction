import streamlit as st


def render_sidebar():
    pass


def render_footer():

    st.markdown(
        """
        <div style="
            margin-top: 0.5rem;
            padding-top: 0.5rem;
            border-top: 1px solid rgba(255,255,255,0.06);
        ">
        </div>
        """,
        unsafe_allow_html=True,
    )

    left, right = st.columns([6, 1])

    with left:
        st.caption(
            "ICP Intelligence Platform • Institutional Market Analytics & Forecasting Engine"
        )

    with right:
        st.caption("Operational")
