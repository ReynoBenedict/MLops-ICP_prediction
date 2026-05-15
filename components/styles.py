import streamlit as st


def apply_custom_styles():
    st.markdown(
        """
        <style>
        /* Base layout refinement */
        .stApp {
            background-color: #f1f5f9;
        }

        .main .block-container {
            max-width: 1300px;
            padding-top: 1rem;
            padding-bottom: 0rem;
        }

        /* Sidebar styling */
        section[data-testid="stSidebar"] {
            background-color: #ffffff;
            border-right: 1px solid #e2e8f0;
        }

        /* Typography & Hierarchy */
        h1 {
            font-size: 2rem !important;
            font-weight: 800 !important;
            color: #0f172a;
            letter-spacing: -0.02em;
            margin-bottom: 0.25rem !important;
        }

        h2 {
            font-size: 1.25rem !important;
            font-weight: 700 !important;
            color: #1e293b;
            margin-top: 0.75rem !important;
            margin-bottom: 0.75rem !important;
        }

        h3 {
            font-size: 1rem !important;
            font-weight: 600 !important;
            color: #475569;
            margin-bottom: 0.4rem !important;
        }

        p, label, .stCaption {
            color: #64748b;
            line-height: 1.4;
        }


        /* Chart Area */
        .stPlotlyChart {
            background: #ffffff;
            border-radius: 8px;
            padding: 0.25rem;
        }

        /* Buttons & Inputs */
        .stButton button {
            border-radius: 6px;
            padding: 0.4rem 1rem;
            font-weight: 600;
            background-color: #0f172a;
            color: #ffffff;
            border: none;
            transition: all 0.15s ease;
        }

        .stButton button:hover {
            background-color: #1e293b;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }

        .stTextInput input, .stNumberInput input, .stSelectbox div[data-baseweb="select"] {
            border-radius: 6px !important;
            border: 1px solid #e2e8f0 !important;
            background-color: #ffffff !important;
        }

        /* Scrollbar styling */
        ::-webkit-scrollbar {
            width: 5px;
            height: 5px;
        }

        ::-webkit-scrollbar-thumb {
            background: #cbd5e1;
            border-radius: 10px;
        }

        /* Native metric enhancement - simplified */
        div[data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #e2e8f0;
            padding: 0.5rem;
            border-radius: 8px;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )
