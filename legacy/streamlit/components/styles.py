import streamlit as st


def apply_custom_styles():
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Manrope:wght@600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap');

        :root {
            --bg: #f6f8fb;
            --panel: #ffffff;
            --panel-soft: #f8fafc;
            --border: #e2e8f0;
            --border-strong: #cbd5e1;
            --text: #0f172a;
            --muted: #475569;
            --muted-soft: #64748b;
            --blue: #2563eb;
            --blue-dark: #1d4ed8;
            --green: #16a34a;
            --amber: #f59e0b;
            --red: #dc2626;
            --slate: #64748b;
            --black: #020617;
            --shadow: 0 10px 30px rgba(15, 23, 42, 0.06);
        }

        .stApp {
            background:
                radial-gradient(circle at top left, rgba(37, 99, 235, 0.08), transparent 30%),
                linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%) !important;
            color: var(--text) !important;
            font-family: Inter, sans-serif !important;
        }

        .main .block-container {
            max-width: 1360px !important;
            padding: 1.4rem 2rem 1rem 2rem !important;
        }

        section[data-testid="stSidebar"] {
            background: #f8fafc !important;
            border-right: 1px solid var(--border) !important;
        }

        section[data-testid="stSidebar"] * {
            color: var(--text) !important;
            font-family: Inter, sans-serif !important;
        }

        h1 {
            font-family: Manrope, Inter, sans-serif !important;
            font-size: 2rem !important;
            font-weight: 800 !important;
            letter-spacing: -0.04em !important;
            color: var(--text) !important;
            margin-bottom: 0.2rem !important;
        }

        h2 {
            font-family: Manrope, Inter, sans-serif !important;
            font-size: 1.35rem !important;
            font-weight: 800 !important;
            letter-spacing: -0.03em !important;
            color: var(--text) !important;
            margin-top: 1.4rem !important;
            margin-bottom: 0.4rem !important;
        }

        h3 {
            font-family: Manrope, Inter, sans-serif !important;
            font-size: 1.05rem !important;
            font-weight: 800 !important;
            color: var(--text) !important;
        }

        p, label, .stCaption, .stMarkdown, .stText {
            color: var(--muted) !important;
            font-size: 0.9rem !important;
            line-height: 1.6 !important;
        }

        .section-subtext {
            color: var(--muted-soft) !important;
            font-size: 0.9rem !important;
            margin-top: -0.25rem !important;
            margin-bottom: 1rem !important;
        }

        .analytics-card,
        .narrative-card,
        div[data-testid="stMetric"] {
            background: rgba(255, 255, 255, 0.92) !important;
            border: 1px solid var(--border) !important;
            border-radius: 16px !important;
            box-shadow: var(--shadow) !important;
            position: relative !important;
            overflow: hidden !important;
        }

        .analytics-card,
        .narrative-card,
        div[data-testid="stMetric"] {
            padding: 1.15rem 1.25rem !important;
            min-height: 118px !important;
        }

        .analytics-card::before,
        .narrative-card::before,
        div[data-testid="stMetric"]::before {
            content: "" !important;
            position: absolute !important;
            top: 0 !important;
            left: 0 !important;
            width: 100% !important;
            height: 4px !important;
            background: var(--blue) !important;
        }

        .analytics-card.accent-blue::before,
        .narrative-card.accent-blue::before { background: var(--blue) !important; }

        .analytics-card.accent-amber::before,
        .narrative-card.accent-amber::before { background: var(--amber) !important; }

        .analytics-card.accent-emerald::before,
        .narrative-card.accent-emerald::before { background: var(--green) !important; }

        .analytics-card.accent-slate::before,
        .narrative-card.accent-slate::before { background: var(--slate) !important; }

        .analytics-card.accent-red::before,
        .narrative-card.accent-red::before { background: var(--red) !important; }

        .metric-label,
        div[data-testid="stMetricLabel"] > div,
        .narrative-title {
            font-family: "JetBrains Mono", monospace !important;
            font-size: 0.72rem !important;
            font-weight: 700 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.16em !important;
            color: var(--muted) !important;
            margin-bottom: 0.55rem !important;
        }

        .metric-value,
        .analytics-main,
        div[data-testid="stMetricValue"] {
            font-family: Manrope, Inter, sans-serif !important;
            font-size: 1.85rem !important;
            font-weight: 800 !important;
            color: var(--black) !important;
            letter-spacing: -0.04em !important;
            line-height: 1.05 !important;
            margin-bottom: 0.35rem !important;
        }

        .metric-subtitle {
            color: var(--muted) !important;
            font-size: 0.88rem !important;
            line-height: 1.5 !important;
            margin-top: 0.35rem !important;
        }

        .metric-delta {
            font-family: "JetBrains Mono", monospace !important;
            font-size: 0.78rem !important;
            font-weight: 700 !important;
            color: var(--green) !important;
            margin-top: 0.25rem !important;
            margin-bottom: 0.25rem !important;
        }

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

        .stPlotlyChart {
            background: rgba(255, 255, 255, 0.95) !important;
            border: 1px solid var(--border) !important;
            border-radius: 16px !important;
            padding: 0.9rem !important;
            box-shadow: var(--shadow) !important;
        }

        div[data-testid="stAlert"] {
            border-radius: 12px !important;
            border: 1px solid var(--border) !important;
            box-shadow: none !important;
        }

        .stButton button {
            width: 100% !important;
            border-radius: 12px !important;
            border: 1px solid var(--blue) !important;
            background: var(--blue) !important;
            color: #ffffff !important;
            font-family: Inter, sans-serif !important;
            font-size: 0.86rem !important;
            font-weight: 700 !important;
            padding: 0.7rem 1rem !important;
            box-shadow: none !important;
        }

        .stButton button:hover {
            background: var(--blue-dark) !important;
            border-color: var(--blue-dark) !important;
            color: #ffffff !important;
        }

        .stTextInput input,
        .stNumberInput input,
        .stSelectbox div[data-baseweb="select"],
        textarea {
            background: var(--panel-soft) !important;
            border: 1px solid var(--border-strong) !important;
            border-radius: 10px !important;
            font-family: Inter, sans-serif !important;
            color: var(--text) !important;
        }

        hr {
            border: none !important;
            height: 1px !important;
            background: var(--border) !important;
            margin: 1rem 0 !important;
        }

        .element-container {
            margin-bottom: 0.55rem !important;
        }

        [data-testid="column"] {
            padding-top: 0 !important;
        }

        details {
            background: var(--panel) !important;
            border: 1px solid var(--border) !important;
            border-radius: 12px !important;
            box-shadow: none !important;
        }

        details summary {
            font-family: "JetBrains Mono", monospace !important;
            font-size: 0.78rem !important;
            text-transform: uppercase !important;
            letter-spacing: 0.12em !important;
        }

        /* =========================
           PREMIUM SIDEBAR
        ========================= */

        section[data-testid="stSidebar"] {
            background: #ffffff !important;
            border-right: 1px solid #e2e8f0 !important;
            box-shadow: 6px 0 24px rgba(15,23,42,0.05) !important;
        }

        section[data-testid="stSidebar"] > div {
            padding-top: 1rem !important;
        }

        section[data-testid="stSidebarNav"] {
            padding-top: 0.75rem !important;
        }

        section[data-testid="stSidebarNav"]::before {
            content: "ICP Intelligence Platform";
            display: block;
            margin-bottom: 1rem;
            padding-left: 0.85rem;

            font-family: Manrope, Inter, sans-serif;
            font-size: 1.15rem;
            font-weight: 800;
            letter-spacing: -0.03em;
            color: #0f172a;
        }

        section[data-testid="stSidebarNav"] ul {
            gap: 0.25rem !important;
        }

        section[data-testid="stSidebarNav"] li {
            margin-bottom: 0.15rem !important;
        }

        section[data-testid="stSidebarNav"] a {
            border-radius: 12px !important;
            margin: 0 0.35rem !important;
            transition: all .18s ease !important;
        }

        section[data-testid="stSidebarNav"] a:hover {
            background: rgba(37,99,235,0.08) !important;
        }

        section[data-testid="stSidebarNav"] a p {
            font-size: 0.92rem !important;
            font-weight: 600 !important;
            color: #0f172a !important;
        }

        section[data-testid="stSidebarNav"] a[aria-current="page"] {
            background: rgba(37,99,235,0.12) !important;
            border: 1px solid rgba(37,99,235,0.20) !important;
        }

        section[data-testid="stSidebarNav"] a[aria-current="page"] p {
            color: #2563eb !important;
            font-weight: 700 !important;
        }

        .hero-section {
            background: linear-gradient(
                135deg,
                rgba(255, 255, 255, 0.95),
                rgba(241, 245, 249, 0.95)
            );
            border: 1px solid #e2e8f0;
            border-radius: 24px;
            padding: 28px 30px;
            margin-bottom: 24px;
            box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06);
        }

        .hero-label {
            font-family: "JetBrains Mono", monospace;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: .18em;
            text-transform: uppercase;
            color: #2563eb;
            margin-bottom: 10px;
        }

        .hero-title {
            font-family: Manrope, sans-serif;
            font-size: 2.25rem;
            font-weight: 800;
            color: #0f172a;
            letter-spacing: -0.04em;
            margin-bottom: 10px;
        }

        .hero-description {
            font-size: 1rem;
            line-height: 1.7;
            color: #475569;
            max-width: 760px;
        }

        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}

        ::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }

        ::-webkit-scrollbar-thumb {
            background: #cbd5e1;
            border-radius: 999px;
        }

        ::-webkit-scrollbar-track {
            background: transparent;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )