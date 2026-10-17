import logging
import traceback

import plotly.graph_objects as go
import streamlit as st

from components.layouts import render_footer
from components.metrics import (
    render_analytics_card,
    render_kpi_card,
    render_narrative_card,
)
from components.styles import apply_custom_styles
from config.settings import PAGE_ICON
from services.insight_service import InsightService
from services.prediction_service import get_prediction_service
from utils.data_loader import (
    get_latest_context,
    load_pipeline_metrics,
    load_processed_data,
)

logger = logging.getLogger("dashboard")

st.set_page_config(
    page_title="Dashboard | ICP Intelligence",
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)


def safe_predict(service, features, fallback):
    if features is None:
        return fallback, "Fallback"

    try:
        pred = service.predict(features)
        if pred is None:
            return fallback, "Fallback"
        return float(pred), "Live"
    except Exception as e:
        logger.warning(e)
        return fallback, "Fallback"


def build_price_chart(df, pred_val):
    chart_df = df.tail(24).copy()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=chart_df.index,
            y=chart_df["icp_price"],
            mode="lines",
            name="ICP Price",
            line=dict(color="#2563eb", width=3),
            fill="tozeroy",
            fillcolor="rgba(37,99,235,0.08)",
        )
    )

    if "wti_price" in chart_df.columns:
        fig.add_trace(
            go.Scatter(
                x=chart_df.index,
                y=chart_df["wti_price"],
                mode="lines",
                name="WTI Reference",
                line=dict(color="#64748b", width=1.8, dash="dot"),
            )
        )

    if pred_val is not None:
        fig.add_trace(
            go.Scatter(
                x=[chart_df.index[-1]],
                y=[pred_val],
                mode="markers",
                name="Forecast",
                marker=dict(
                    size=12,
                    color="#f59e0b",
                    line=dict(color="white", width=2),
                ),
            )
        )

    fig.update_layout(
        template="plotly_white",
        height=430,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="white",
        margin=dict(l=10, r=10, t=28, b=10),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            y=1.08,
            x=1,
            xanchor="right",
            font=dict(family="Inter", size=11),
        ),
        xaxis=dict(
            showgrid=True,
            gridcolor="rgba(148,163,184,0.18)",
            tickfont=dict(size=10, color="#64748b"),
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(148,163,184,0.22)",
            zeroline=False,
            title=dict(text="USD / BBL", font=dict(size=11, family="Inter")),
            tickfont=dict(size=10, color="#64748b"),
        ),
    )

    return fig


def render_terminal():
    st.markdown(
        """
        <div style="
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:12px;
            padding:10px 14px;
            margin:0 0 18px 0;
            border:1px solid #e2e8f0;
            border-radius:14px;
            background:rgba(255,255,255,0.82);
            box-shadow:0 8px 24px rgba(15,23,42,0.04);
            font-family:'JetBrains Mono', monospace;
            font-size:0.72rem;
            font-weight:700;
            letter-spacing:0.12em;
            text-transform:uppercase;
            color:#475569;
        ">
            <span>● Market Status: Operational</span>
            <span>Sync: 12ms</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_dashboard_header():
    st.markdown(
        """
        <div style="
            padding:22px 24px;
            margin-bottom:18px;
            border:1px solid #e2e8f0;
            border-radius:20px;
            background:
                radial-gradient(circle at top right, rgba(37,99,235,0.12), transparent 30%),
                linear-gradient(135deg, #ffffff 0%, #f8fafc 100%);
            box-shadow:0 12px 34px rgba(15,23,42,0.06);
        ">
            <div style="
                font-family:'JetBrains Mono', monospace;
                font-size:0.72rem;
                font-weight:800;
                letter-spacing:0.16em;
                color:#2563eb;
                text-transform:uppercase;
                margin-bottom:8px;
            ">
                ICP Intelligence Platform
            </div>
            <div style="
                font-family:Manrope, Inter, sans-serif;
                font-size:2.1rem;
                line-height:1.1;
                font-weight:800;
                letter-spacing:-0.045em;
                color:#0f172a;
                margin-bottom:8px;
            ">
                ICP Market Intelligence
            </div>
            <div style="
                font-family:Inter, sans-serif;
                font-size:0.95rem;
                color:#475569;
                max-width:760px;
                line-height:1.6;
            ">
                Executive overview untuk memantau harga ICP, benchmark WTI,
                proyeksi settlement, dan kondisi pasar berbasis model prediksi.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def run_dashboard():
    apply_custom_styles()

    try:
        df = load_processed_data()
        metrics = load_pipeline_metrics()

        if df.empty:
            st.error("Data market kosong")
            return

        service = get_prediction_service()

        latest_icp = float(df["icp_price"].iloc[-1])
        latest_wti = float(df["wti_price"].iloc[-1])
        features = get_latest_context(df)

        pred_val, pred_status = safe_predict(service, features, latest_icp)
        rmse = float(metrics.get("rmse", 3.62))
        corr = df["icp_price"].corr(df["wti_price"])

        render_terminal()
        render_dashboard_header()

        c1, c2, c3, c4 = st.columns(4)

        with c1:
            render_kpi_card(
                "Forecast Price",
                f"${pred_val:.2f}",
                delta="+0.00%",
                detail=(
                    "Proyeksi harga ICP periode settlement mendatang."
                    if pred_status == "Live"
                    else "Fallback dari settlement terakhir."
                ),
                accent="amber",
            )

        with c2:
            render_kpi_card(
                "Latest ICP",
                f"${latest_icp:.2f}",
                detail="Harga settlement resmi ICP terakhir.",
                accent="blue",
            )

        with c3:
            render_kpi_card(
                "WTI Reference",
                f"${latest_wti:.2f}",
                detail="Benchmark global ICP.",
                accent="blue",
            )

        with c4:
            render_analytics_card(
                "Market Bias",
                "Neutral",
                subtitle="Pasar dalam kondisi netral.",
                accent="slate",
            )

        st.markdown("## Price Trend Analysis")
        st.caption("Visualisasi pergerakan historis dan proyeksi nilai ICP.")

        fig = build_price_chart(df, pred_val)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        left, right = st.columns([2, 1])

        with left:
            render_narrative_card(
                "Market Intelligence Summary",
                InsightService.generate_intelligence_summary(df, pred_val, corr),
                accent="blue",
            )

        with right:
            render_analytics_card(
                "ICP-WTI Correlation",
                f"{corr:.2f}",
                subtitle="Korelasi benchmark global",
                accent="emerald",
            )

        st.markdown("## Market Dynamics")

        positioning = InsightService.generate_positioning_narrative(
            df,
            pred_val,
            "Neutral",
            corr,
        )

        if positioning is None or positioning == "" or "Data belum" in positioning:
            positioning = f"""
Posisi pasar saat ini berada pada area ICP ${latest_icp:.2f}/bbl dengan benchmark WTI ${latest_wti:.2f}/bbl.

Korelasi ICP-WTI berada di level {corr:.2f} sehingga benchmark global masih dominan.

Model historis memiliki RMSE {rmse:.2f}.
"""

        render_narrative_card(
            "Market Positioning",
            positioning,
            accent="slate",
        )

        render_footer()

    except Exception as e:
        traceback.print_exc()
        st.error(f"Dashboard gagal: {e}")


run_dashboard()