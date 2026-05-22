import logging
import traceback

import plotly.graph_objects as go
import streamlit as st

from components.layouts import render_footer
from components.styles import apply_custom_styles
from config.settings import PAGE_ICON

logger = logging.getLogger("dashboard")

try:
    from components.metrics import (
        render_analytics_card,
        render_kpi_card,
        render_narrative_card,
    )
except ImportError as e:
    logger.error(str(e))
    def render_kpi_card(label, value, delta=None, **kwargs):
        st.metric(label, value, delta)
    def render_analytics_card(label, value, subtitle="", **kwargs):
        st.metric(label, value)
    def render_narrative_card(title, content, **kwargs):
        st.markdown(f"**{title}**\n\n{content}")

from services.insight_service import InsightService
from services.prediction_service import get_prediction_service
from utils.data_loader import (
    get_latest_context,
    load_pipeline_metrics,
    load_processed_data,
)

# ---------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------

st.set_page_config(
    page_title="Dashboard | ICP Intelligence",
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------------------------------------------------
# CHART BUILDER
# ---------------------------------------------------

def build_price_chart(df, pred_val):
    chart_df = df.tail(24).copy()
    fig = go.Figure()

    # ICP Primary Trace
    fig.add_trace(
        go.Scatter(
            x=chart_df.index,
            y=chart_df["icp_price"],
            mode="lines",
            name="ICP Price",
            line=dict(color="#2563eb", width=3),
            fill="tozeroy",
            fillcolor="rgba(37,99,235,0.08)",
            hovertemplate="ICP: $%{y:.2f}<extra></extra>",
        )
    )

    # WTI Reference Trace
    if "wti_price" in chart_df.columns:
        fig.add_trace(
            go.Scatter(
                x=chart_df.index,
                y=chart_df["wti_price"],
                mode="lines",
                name="WTI Reference",
                line=dict(color="#94a3b8", width=1.5, dash="dot"),
                hovertemplate="WTI: $%{y:.2f}<extra></extra>",
            )
        )

    # Forecast Marker
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
                    line=dict(width=2, color="#ffffff"),
                ),
                hovertemplate="Forecast: $%{y:.2f}<extra></extra>",
            )
        )

    fig.update_layout(
        template="plotly_white",
        height=380,
        margin=dict(l=10, r=10, t=10, b=10),
        hovermode="x unified",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(
            orientation="h",
            y=1.1,
            x=1,
            xanchor="right",
        ),
        xaxis=dict(showgrid=False, zeroline=False),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(148,163,184,0.12)",
            zeroline=False,
            title="USD / BBL",
        ),
    )
    return fig

# ---------------------------------------------------
# DASHBOARD EXECUTION
# ---------------------------------------------------

def run_dashboard():
    apply_custom_styles()

    try:
        with st.spinner("Loading intelligence..."):
            df = load_processed_data()
            metrics = load_pipeline_metrics()

        service = get_prediction_service()
        latest_icp = df["icp_price"].iloc[-1] if not df.empty else 0.0
        latest_wti = df["wti_price"].iloc[-1] if not df.empty else 0.0
        features = get_latest_context(df)
        
        pred_val = None
        if features:
            try:
                pred_val = service.predict(features)
            except Exception as e:
                logger.error(f"Prediction failed: {e}")

        rmse = metrics.get("rmse", 3.6)
        delta_pct = ((pred_val / latest_icp) - 1) * 100 if pred_val and latest_icp > 0 else 0
        relative_delta = f"{delta_pct:+.2f}%" if pred_val else None

        # Market Bias Logic
        if pred_val:
            if pred_val > latest_icp * 1.01:
                trend_label, bias_accent = "Bullish", "emerald"
            elif pred_val < latest_icp * 0.99:
                trend_label, bias_accent = "Bearish", "amber"
            else:
                trend_label, bias_accent = "Neutral", "slate"
        else:
            trend_label, bias_accent = "Unavailable", "slate"

        corr_val = df["icp_price"].corr(df["wti_price"]) if not df.empty else 0.0
        condition_label, condition_narrative = InsightService.generate_condition_narrative(df, rmse)
        condition_accent = "amber" if condition_label == "Volatile" else "emerald"

        # SECTION 1: EXECUTIVE HERO
        st.title("ICP Market Intelligence")
        st.caption("Executive overview: kondisi pasar, posisi harga, dan proyeksi ICP")
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # KPI GRID
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            render_kpi_card(
                "Forecast Price",
                f"${pred_val:.2f}" if pred_val else "N/A",
                delta=relative_delta,
                detail="Proyeksi harga ICP untuk periode settlement mendatang.",
                accent="blue"
            )
        with k2:
            render_kpi_card(
                "Latest ICP",
                f"${latest_icp:.2f}",
                detail="Harga settlement resmi ICP terakhir sebagai acuan transaksi.",
                accent="blue"
            )
        with k3:
            render_kpi_card(
                "WTI Reference",
                f"${latest_wti:.2f}",
                detail="Benchmark global utama yang mempengaruhi pergerakan ICP.",
                accent="blue"
            )
        with k4:
            render_analytics_card(
                "Market Bias",
                trend_label,
                subtitle=InsightService.generate_bias_subtitle(pred_val, latest_icp),
                accent=bias_accent
            )

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # SECTION 2: PRICE TREND ANALYSIS
        st.markdown("## Price Trend Analysis")
        st.caption("Visualisasi pergerakan historis dan proyeksi nilai ICP")
        
        fig = build_price_chart(df, pred_val)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        render_narrative_card(
            "Market Intelligence Summary",
            InsightService.generate_intelligence_summary(df, pred_val, corr_val),
            accent="blue"
        )

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        col_corr, col_cond = st.columns(2)
        with col_corr:
            render_analytics_card(
                "ICP-WTI Correlation",
                f"{corr_val:.2f}",
                subtitle=InsightService.generate_correlation_narrative(corr_val),
                accent="emerald"
            )
        with col_cond:
            render_analytics_card(
                "Market Condition",
                condition_label,
                subtitle=condition_narrative,
                accent=condition_accent
            )

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # SECTION 3: MARKET DYNAMICS
        st.markdown("## Market Dynamics")
        st.caption("Analisis kualitatif positioning pasar dan outlook jangka pendek")
        
        render_narrative_card(
            "Market Positioning",
            InsightService.generate_positioning_narrative(df, pred_val, trend_label, corr_val),
            accent="slate"
        )

        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
        render_footer()

    except Exception as e:
        traceback.print_exc()
        st.error(f"Dashboard failed to load: {e}")

if __name__ == "__main__":
    run_dashboard()
else:
    run_dashboard()