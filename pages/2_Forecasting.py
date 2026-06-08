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

logger = logging.getLogger("forecasting")

# ---------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------

st.set_page_config(
    page_title="Forecasting | ICP Intelligence",
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded"
)

apply_custom_styles()

# ---------------------------------------------------
# FORECAST CHART
# ---------------------------------------------------

def build_forecast_chart(df, pred_val, rmse):
    """Historical trend with forecast point and confidence band."""
    chart_df = df.tail(24).copy()
    fig = go.Figure()

    # Historical ICP
    fig.add_trace(
        go.Scatter(
            x=chart_df.index,
            y=chart_df["icp_price"],
            mode="lines",
            name="ICP Historis",
            line=dict(color="#2563eb", width=2.5),
            hovertemplate="ICP: $%{y:.2f}<extra></extra>",
        )
    )

    if pred_val is not None:
        conf_low = max(0, pred_val - rmse)
        conf_high = pred_val + rmse
        last_idx = chart_df.index[-1]

        # Confidence band
        fig.add_trace(
            go.Scatter(
                x=[last_idx, last_idx],
                y=[conf_high, conf_low],
                mode="lines",
                name="Confidence Band",
                line=dict(color="rgba(245,158,11,0.3)", width=0),
                fill="toself",
                fillcolor="rgba(245,158,11,0.12)",
                showlegend=True,
                hovertemplate="Range: $%{y:.2f}<extra></extra>",
            )
        )

        # Forecast point
        fig.add_trace(
            go.Scatter(
                x=[last_idx],
                y=[pred_val],
                mode="markers",
                name="Forecast",
                marker=dict(
                    size=14,
                    color="#f59e0b",
                    line=dict(width=2, color="#ffffff"),
                ),
                hovertemplate="Forecast: $%{y:.2f}<extra></extra>",
            )
        )

    fig.update_layout(
        template="plotly_white",
        height=400,
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
# EXECUTION
# ---------------------------------------------------

def run_forecasting():
    try:
        with st.spinner("Analyzing forecast data..."):
            df = load_processed_data()
            metrics = load_pipeline_metrics()

        latest_icp = df["icp_price"].iloc[-1] if not df.empty else 0.0
        features = get_latest_context(df)
        service = get_prediction_service()
        rmse = metrics.get("rmse", 3.6)

        pred_val = None
        prediction_error = None
        if features:
            try:
                pred_val = service.predict(features)
            except Exception as e:
                prediction_error = str(e)

        # Metrics calc
        direction = "Unknown"
        direction_desc = "Estimasi tidak tersedia"
        delta_pct = 0.0
        confidence_range = "N/A"

        if pred_val and latest_icp > 0:
            conf_low = max(0, pred_val - rmse)
            conf_high = pred_val + rmse
            confidence_range = f"${conf_low:.2f} - ${conf_high:.2f}"
            delta_pct = ((pred_val / latest_icp) - 1) * 100

            if pred_val > latest_icp * 1.01:
                direction, direction_desc = "Bullish", "Potensi kenaikan harga"
            elif pred_val < latest_icp * 0.99:
                direction, direction_desc = "Bearish", "Potensi pelemahan harga"
            else:
                direction, direction_desc = "Stabil", "Relatif netral"

        st.title("Forecast Harga ICP")
        st.caption("Proyeksi harga dan analisis prediktif untuk periode settlement berikutnya")

        if prediction_error:
            st.error(f"Prediction Error: {prediction_error}")

        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # SECTION 1: SNAPSHOT
        s1, s2, s3, s4 = st.columns(4)
        with s1:
            render_kpi_card(
                "Predicted ICP",
                f"${pred_val:.2f}" if pred_val else "N/A",
                delta=f"{delta_pct:+.2f}%" if pred_val else None,
                detail="Estimasi harga ICP periode berikutnya.",
                accent="blue"
            )
        with s2:
            render_analytics_card(
                "Forecast Direction",
                direction,
                subtitle=direction_desc,
                accent="emerald" if direction == "Bullish" else "amber" if direction == "Bearish" else "slate"
            )
        with s3:
            render_analytics_card(
                "Confidence Range",
                confidence_range,
                subtitle=f"Rentang harga berdasarkan error historis (RMSE: {rmse:.2f}).",
                accent="amber"
            )
        with s4:
            render_analytics_card(
                "Model Accuracy",
                f"RMSE {rmse:.2f}",
                subtitle="Rata-rata deviasi prediksi terhadap harga aktual.",
                accent="blue"
            )

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # SECTION 2: VISUALIZATION
        st.markdown("## Forecast Visualization")
        st.caption("Tren harga historis dengan proyeksi dan confidence band")
        fig = build_forecast_chart(df, pred_val, rmse)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # SECTION 3: INTERPRETATION
        render_narrative_card(
            "Forecast Interpretation",
            InsightService.generate_forecast_interpretation(df, pred_val, rmse),
            accent="blue"
        )

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # SECTION 4: SCENARIOS
        st.markdown("## Scenario Analysis")
        st.caption("Analisis variasi kondisi pasar")
        if pred_val:
            scenarios = InsightService.generate_scenario_cards(pred_val, rmse)
            sc1, sc2, sc3 = st.columns(3)
            with sc1:
                render_analytics_card(scenarios["optimistic"]["label"], f"${scenarios['optimistic']['price']:.2f}", subtitle=scenarios["optimistic"]["desc"], accent="emerald")
            with sc2:
                render_analytics_card(scenarios["base"]["label"], f"${scenarios['base']['price']:.2f}", subtitle=scenarios["base"]["desc"], accent="blue")
            with sc3:
                render_analytics_card(scenarios["pessimistic"]["label"], f"${scenarios['pessimistic']['price']:.2f}", subtitle=scenarios["pessimistic"]["desc"], accent="amber")

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # SECTION 5: OUTLOOK
        st.markdown("## Directional Outlook")
        st.caption("Analisis arah harga dan risiko jangka pendek")
        render_narrative_card(
            "Outlook Jangka Pendek",
            InsightService.generate_directional_outlook(df, pred_val, direction if direction != "Stabil" else "Neutral"),
            accent="slate"
        )

        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
        render_footer()

    except Exception as e:
        traceback.print_exc()
        st.error(f"Forecasting page failed: {e}")

if __name__ == "__main__":
    run_forecasting()
else:
    run_forecasting()
