import streamlit as st


def apply_custom_styles():
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Manrope:wght@600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap');

        :root {
            --bg: #f7f9fb;
            --panel: #ffffff;
            --panel-soft: #f2f4f6;
            --border: #c6c6cd;
            --border-soft: #e0e3e5;
            --text: #191c1e;
            --muted: #45464d;
            --blue: #0058be;
            --green: #16a34a;
            --amber: #f59e0b;
            --red: #ba1a1a;
            --black: #000000;
        }

        .stApp {
            background: var(--bg) !important;
            color: var(--text) !important;
            font-family: Inter, sans-serif !important;
        }

        .main .block-container {
            max-width: 1440px !important;
            padding: 1.35rem 1.7rem 0.8rem 1.7rem !important;
        }

        /* SIDEBAR */
        section[data-testid="stSidebar"] {
            background: #f2f4f6 !important;
            border-right: 1px solid var(--border) !important;
            min-width: 260px !important;
        }

        section[data-testid="stSidebar"] * {
            color: var(--text) !important;
        }

        section[data-testid="stSidebar"] p,
        section[data-testid="stSidebar"] label,
        section[data-testid="stSidebar"] div[data-testid="stMarkdownContainer"] {
            font-family: Inter, sans-serif !important;
        }

        span[data-testid="stIconMaterial"],
        [data-testid="stIconMaterial"],
        .material-symbols-rounded,
        .material-symbols-outlined {
            font-family: "Material Symbols Rounded" !important;
        }

        section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
            font-size: 0.9rem !important;
            font-weight: 600 !important;
        }

        /* TYPOGRAPHY */
        h1 {
            font-family: Manrope, Inter, sans-serif !important;
            font-size: 1.9rem !important;
            font-weight: 800 !important;
            letter-spacing: -0.035em !important;
            color: var(--text) !important;
            margin-bottom: 0.15rem !important;
        }

        h2 {
            font-family: Manrope, Inter, sans-serif !important;
            font-size: 1.35rem !important;
            font-weight: 800 !important;
            letter-spacing: -0.025em !important;
            color: var(--text) !important;
            margin-top: 1.15rem !important;
            margin-bottom: 0.45rem !important;
        }

        h3 {
            font-family: Inter, sans-serif !important;
            font-size: 1rem !important;
            font-weight: 700 !important;
            color: var(--text) !important;
            margin-bottom: 0.35rem !important;
        }

        p, label, .stCaption, .stMarkdown, .stText {
            color: var(--muted) !important;
            font-size: 0.9rem !important;
            line-height: 1.55 !important;
        }

        /* CARD */
        .analytics-card,
        .narrative-card,
        div[data-testid="stMetric"],
        [data-testid="stVerticalBlockBorderWrapper"] {
            background: var(--panel) !important;
            border: 1px solid var(--border-soft) !important;
            border-radius: 0.25rem !important;
            box-shadow: none !important;
            position: relative !important;
            overflow: hidden !important;
        }

        .analytics-card,
        .narrative-card,
        div[data-testid="stMetric"] {
            padding: 1.05rem 1.15rem !important;
            min-height: 112px !important;
        }

        .analytics-card::before,
        .narrative-card::before,
        div[data-testid="stMetric"]::before {
            content: "" !important;
            position: absolute !important;
            top: 0 !important;
            left: 0 !important;
            width: 100% !important;
            height: 3px !important;
            background: var(--blue) !important;
        }

        .analytics-card.accent-blue::before,
        .narrative-card.accent-blue::before { background: var(--blue) !important; }

        .analytics-card.accent-amber::before,
        .narrative-card.accent-amber::before { background: var(--amber) !important; }

        .analytics-card.accent-emerald::before,
        .narrative-card.accent-emerald::before { background: var(--green) !important; }

        .analytics-card.accent-slate::before,
        .narrative-card.accent-slate::before { background: #76777d !important; }

        .analytics-card.accent-red::before,
        .narrative-card.accent-red::before { background: var(--red) !important; }

        /* KPI TEXT */
        .metric-label,
        div[data-testid="stMetricLabel"] > div,
        .narrative-title {
            font-family: "JetBrains Mono", monospace !important;
            font-size: 0.76rem !important;
            font-weight: 700 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.15em !important;
            color: var(--text) !important;
            margin-bottom: 0.42rem !important;
        }

        .metric-value,
        .analytics-main,
        div[data-testid="stMetricValue"] {
            font-family: Manrope, Inter, sans-serif !important;
            font-size: 1.65rem !important;
            font-weight: 800 !important;
            color: var(--black) !important;
            letter-spacing: -0.035em !important;
            line-height: 1.1 !important;
            margin-bottom: 0.35rem !important;
        }

        .metric-subtitle {
            color: var(--muted) !important;
            font-size: 0.9rem !important;
            line-height: 1.5 !important;
            margin-top: 0.25rem !important;
        }

        .metric-delta {
            font-family: "JetBrains Mono", monospace !important;
            font-size: 0.78rem !important;
            font-weight: 700 !important;
            margin-top: 0.25rem !important;
            margin-bottom: 0.25rem !important;
        }

        /* NARRATIVE */
        .narrative-card {
            min-height: auto !important;
            padding: 1.25rem 1.35rem !important;
        }

        .narrative-paragraph {
            color: var(--text) !important;
            font-size: 0.93rem !important;
            line-height: 1.7 !important;
            margin: 0 0 0.75rem 0 !important;
        }

        .narrative-paragraph:last-child {
            margin-bottom: 0 !important;
        }

        /* CHART */
        .stPlotlyChart {
            background: var(--panel) !important;
            border: 1px solid var(--border) !important;
            border-radius: 0.25rem !important;
            padding: 0.8rem !important;
            box-shadow: none !important;
        }

        /* ALERTS */
        div[data-testid="stAlert"] {
            border-radius: 0.25rem !important;
            border: 1px solid var(--border-soft) !important;
            box-shadow: none !important;
        }

        /* BUTTON */
        .stButton button {
            width: 100% !important;
            border-radius: 0.2rem !important;
            border: 1px solid var(--black) !important;
            background: var(--black) !important;
            color: #ffffff !important;
            font-family: Inter, sans-serif !important;
            font-size: 0.85rem !important;
            font-weight: 700 !important;
            padding: 0.65rem 1rem !important;
            box-shadow: none !important;
        }

        .stButton button:hover {
            background: var(--blue) !important;
            border-color: var(--blue) !important;
            color: #ffffff !important;
        }

        /* INPUTS */
        .stTextInput input,
        .stNumberInput input,
        .stSelectbox div[data-baseweb="select"],
        textarea {
            background: var(--panel-soft) !important;
            border: 1px solid var(--border) !important;
            border-radius: 0.2rem !important;
            font-family: "JetBrains Mono", monospace !important;
            color: var(--text) !important;
        }

        hr {
            border: none !important;
            height: 1px !important;
            background: var(--border) !important;
            margin: 1rem 0 !important;
        }

        .element-container {
            margin-bottom: 0.5rem !important;
        }

        [data-testid="column"] {
            padding-top: 0 !important;
        }

        .section-subtext {
            color: var(--muted) !important;
            font-size: 0.9rem !important;
            margin-top: -0.35rem !important;
            margin-bottom: 1.1rem !important;
            font-weight: 400 !important;
        }

        details {
            background: var(--panel) !important;
            border: 1px solid var(--border) !important;
            border-radius: 0.25rem !important;
            box-shadow: none !important;
        }

        details summary {
            font-family: "JetBrains Mono", monospace !important;
            font-size: 0.78rem !important;
            text-transform: uppercase !important;
            letter-spacing: 0.12em !important;
        }

        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}

        ::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }

        ::-webkit-scrollbar-thumb {
            background: #b8bcc2;
            border-radius: 999px;
        }

        ::-webkit-scrollbar-track {
            background: transparent;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )