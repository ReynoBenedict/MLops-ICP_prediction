import streamlit as st


def apply_custom_styles():
    st.markdown(
        """
        <style>

        .stApp {
            background:
                radial-gradient(circle at top left,
                rgba(37,99,235,0.04),
                transparent 24%),

                radial-gradient(circle at bottom right,
                rgba(14,165,233,0.03),
                transparent 20%),

                #f8fafc;

            color: #0f172a;
        }

        .main .block-container {
            max-width: 1720px !important;
            width: 100% !important;

            padding-top: 0.9rem !important;
            padding-bottom: 0.7rem !important;

            padding-left: 1.4rem !important;
            padding-right: 1.4rem !important;

            margin: 0 auto !important;
        }

        /* SIDEBAR */

        section[data-testid="stSidebar"] {
            background:
                linear-gradient(
                    180deg,
                    #ffffff,
                    #f8fafc
                );

            border-right: 1px solid rgba(148,163,184,0.14);

            min-width: 230px !important;
        }

        section[data-testid="stSidebar"] * {
            color: #1e293b !important;
        }

        /* TYPOGRAPHY */

        h1 {
            font-size: 2.3rem !important;
            font-weight: 850 !important;

            color: #0f172a !important;

            letter-spacing: -0.05em;

            margin-bottom: 0.15rem !important;
        }

        h2 {
            font-size: 1.02rem !important;
            font-weight: 700 !important;

            color: #0f172a !important;

            margin-top: 0.2rem !important;
            margin-bottom: 0.5rem !important;
        }

        h3 {
            font-size: 0.95rem !important;
            font-weight: 650 !important;

            color: #334155 !important;

            margin-bottom: 0.25rem !important;
        }

        p,
        label,
        .stCaption,
        .stMarkdown,
        .stText {
            color: #64748b !important;
            line-height: 1.6;
            font-size: 0.92rem;
        }

        /* KPI CARDS */

        .analytics-card,
        div[data-testid="stMetric"] {

            background:
                linear-gradient(
                    180deg,
                    rgba(255,255,255,0.96),
                    rgba(248,250,252,0.98)
                );

            border: 1px solid rgba(148,163,184,0.14);

            border-radius: 18px;

            padding: 1rem 1.1rem;

            box-shadow:
                0 1px 2px rgba(15,23,42,0.03),
                0 10px 30px rgba(15,23,42,0.03);

            transition: all 0.22s ease;

            position: relative;

            overflow: hidden;
        }

        .analytics-card:hover,
        div[data-testid="stMetric"]:hover {

            transform: translateY(-2px);

            border-color: rgba(59,130,246,0.18);

            box-shadow:
                0 12px 35px rgba(15,23,42,0.06);
        }

        /* TOP ACCENT */

        .analytics-card::before,
        div[data-testid="stMetric"]::before {

            content: "";

            position: absolute;

            top: 0;
            left: 0;

            width: 100%;
            height: 3px;

            background:
                linear-gradient(
                    90deg,
                    #2563eb,
                    #38bdf8
                );
        }

        /* METRIC */

        .metric-label,
        div[data-testid="stMetricLabel"] > div {

            color: #64748b !important;

            font-size: 0.72rem !important;

            font-weight: 700 !important;

            text-transform: uppercase;

            letter-spacing: 0.07em;

            margin-bottom: 0.55rem;
        }

        .metric-value,
        div[data-testid="stMetricValue"] {

            color: #0f172a !important;

            font-size: 1.9rem !important;

            font-weight: 850 !important;

            line-height: 1;

            letter-spacing: -0.04em;
        }

        .metric-subtitle {
            color: #94a3b8;
            font-size: 0.72rem;
            margin-top: 0.45rem;
        }

        /* RANGE */

        .confidence-range {

            font-size: 1.45rem;

            font-weight: 800;

            letter-spacing: -0.03em;

            color: #0f172a;

            line-height: 1.1;
        }

        /* CHART */

        .stPlotlyChart {

            background:
                linear-gradient(
                    180deg,
                    rgba(255,255,255,0.94),
                    rgba(248,250,252,0.98)
                );

            border: 1px solid rgba(148,163,184,0.14);

            border-radius: 22px;

            padding: 0.6rem;

            box-shadow:
                0 6px 22px rgba(15,23,42,0.04);
        }

        /* CONTAINERS */

        details,
        [data-testid="stVerticalBlockBorderWrapper"] {

            background:
                linear-gradient(
                    180deg,
                    rgba(255,255,255,0.85),
                    rgba(248,250,252,0.95)
                );

            border: 1px solid rgba(148,163,184,0.12);

            border-radius: 20px;

            box-shadow:
                0 4px 20px rgba(15,23,42,0.03);
        }

        /* BUTTON */

        .stButton button {

            width: 100%;

            border-radius: 12px;

            padding: 0.58rem 1rem;

            background:
                linear-gradient(
                    135deg,
                    #0f172a,
                    #1e293b
                );

            color: white;

            border: none;

            font-weight: 600;

            transition: all 0.18s ease;
        }

        .stButton button:hover {

            transform: translateY(-1px);

            box-shadow:
                0 10px 25px rgba(15,23,42,0.12);
        }

        /* INPUT */

        .stTextInput input,
        .stNumberInput input,
        .stSelectbox div[data-baseweb="select"] {

            background: rgba(255,255,255,0.92) !important;

            border: 1px solid rgba(148,163,184,0.22) !important;

            border-radius: 12px !important;
        }

        /* SPACING */

        .element-container {
            margin-bottom: 0.35rem !important;
        }

        .stMarkdown {
            margin-bottom: 0.15rem !important;
        }

        [data-testid="column"] {
            padding-top: 0rem !important;
        }

        hr {
            border: none;
            height: 1px;

            background:
                rgba(148,163,184,0.16);

            margin-top: 0.9rem;
            margin-bottom: 0.9rem;
        }

        /* SUBTEXT */

        .section-subtext {
            color: #64748b !important;
            font-size: 0.88rem !important;
            margin-top: -0.5rem !important;
            margin-bottom: 1.2rem !important;
            font-weight: 500;
        }

        /* ACCENTS */

        .analytics-card.accent-blue::before,
        .narrative-card.accent-blue::before { background: linear-gradient(90deg, #2563eb, #38bdf8); }

        .analytics-card.accent-amber::before,
        .narrative-card.accent-amber::before { background: linear-gradient(90deg, #f59e0b, #fbbf24); }

        .analytics-card.accent-emerald::before,
        .narrative-card.accent-emerald::before { background: linear-gradient(90deg, #10b981, #34d399); }

        .analytics-card.accent-slate::before,
        .narrative-card.accent-slate::before { background: linear-gradient(90deg, #475569, #94a3b8); }

        /* NARRATIVE CARD */

        .narrative-card {
            background:
                linear-gradient(
                    180deg,
                    rgba(255,255,255,0.96),
                    rgba(248,250,252,0.98)
                );

            border: 1px solid rgba(148,163,184,0.14);
            border-radius: 18px;
            padding: 1.4rem 1.5rem;

            box-shadow:
                0 1px 2px rgba(15,23,42,0.03),
                0 10px 30px rgba(15,23,42,0.03);

            transition: all 0.22s ease;
            position: relative;
            overflow: hidden;
        }

        .narrative-card::before {
            content: "";
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 3px;
        }

        .narrative-card:hover {
            transform: translateY(-2px);
            border-color: rgba(59,130,246,0.18);
            box-shadow: 0 12px 35px rgba(15,23,42,0.06);
        }

        .narrative-title {
            color: #64748b;
            font-size: 0.72rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.07em;
            margin-bottom: 0.85rem;
        }

        .narrative-body {
            color: #334155;
        }

        .narrative-paragraph {
            color: #475569;
            font-size: 0.88rem;
            line-height: 1.8;
            margin: 0 0 0.75rem 0;
        }

        .narrative-paragraph:last-child {
            margin-bottom: 0;
        }

        /* SCROLLBAR */

        ::-webkit-scrollbar {
            width: 5px;
            height: 5px;
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