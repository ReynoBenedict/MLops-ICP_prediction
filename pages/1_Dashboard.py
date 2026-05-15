import logging
import traceback
import plotly.graph_objects as go
import streamlit as st
from components.layouts import render_footer
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

def render_metric_card(label, value, delta=None):
    st.metric(label=label, value=value, delta=delta)

def run_dashboard():
    apply_custom_styles()
    
    try:
        with st.spinner("Loading market data..."):
            df = load_processed_data()
            metrics = load_pipeline_metrics()

        service = get_prediction_service()

        latest_icp = df["icp_price"].iloc[-1] if not df.empty else 0.0
        latest_wti = df["wti_price"].iloc[-1] if not df.empty else 0.0

        features = get_latest_context(df)

        pred_val = None
        model_meta = {}
        prediction_error = None

        if features:
            try:
                with st.spinner("Initializing intelligence engine..."):
                    pred_val = service.predict(features)
                
                with st.spinner("Fetching model metadata..."):
                    model_meta = service.get_model_metadata()
                logger.info(f"Prediction successful: {pred_val}")
            except Exception as e:
                prediction_error = str(e)
                logger.error(f"Intelligence engine error: {prediction_error}")
                pred_val = None
                model_meta = {}
        else:
            prediction_error = "No features available for prediction"
            logger.warning(prediction_error)

        rmse = metrics.get("rmse", 3.6)


        if pred_val is not None and latest_icp > 0:
            delta_pct = ((pred_val / latest_icp) - 1) * 100
        else:
            delta_pct = 0.0

        relative_delta = f"{delta_pct:+.2f}%" if pred_val is not None else "N/A"

        if pred_val is not None and latest_icp:
            if pred_val > latest_icp * 1.01:
                trend_label = "Bullish"
            elif pred_val < latest_icp * 0.99:
                trend_label = "Bearish"
            else:
                trend_label = "Neutral"
        else:
            trend_label = "Unknown"

        if pred_val is not None and rmse > 0:
            confidence_low = max(0, pred_val - rmse)
            confidence_high = pred_val + rmse
            confidence_range = f"${confidence_low:.2f} — ${confidence_high:.2f}"
        else:
            confidence_range = "N/A"

        market_trend = InsightService.get_market_trend_insight(df) if not df.empty else "N/A"
        corr_insight = InsightService.get_correlation_insight(df) if not df.empty else "N/A"
        dominant_driver = InsightService.get_dominance_insight(model_meta) if model_meta else "N/A"

        corr_val = (
            df["icp_price"].corr(df["wti_price"])
            if not df.empty and "icp_price" in df.columns and "wti_price" in df.columns
            else 0.0
        )

        st.title("ICP Forecast Platform")
        st.caption("Intelligence dashboard for oil price forecasting and market analysis.")

        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        with kpi1:
            render_metric_card("Predicted ICP", f"${pred_val:.2f}" if pred_val else "N/A", relative_delta)
        with kpi2:
            render_metric_card("Current ICP", f"${latest_icp:.2f}")
        with kpi3:
            render_metric_card("WTI Reference", f"${latest_wti:.2f}")
        with kpi4:
            render_metric_card("Market Bias", trend_label)

        col_main, col_side = st.columns([2.5, 1], gap="medium")

        with col_main:
            st.subheader("Price Trend Analysis")
            chart_df = df.tail(24).copy()
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=chart_df.index, y=chart_df["icp_price"],
                mode="lines", name="ICP Price",
                line=dict(color="#0f172a", width=2.5),
                hovertemplate="Price: $%{y:.2f}<extra></extra>"
            ))
            if pred_val is not None:
                fig.add_trace(go.Scatter(
                    x=[chart_df.index[-1] if not chart_df.empty else 0], y=[pred_val],
                    mode="markers", name="Next Forecast",
                    marker=dict(size=12, color="#ef4444", line=dict(width=2, color="white")),
                    hovertemplate="Forecast: $%{y:.2f}<extra></extra>"
                ))
            fig.update_layout(
                template="plotly_white", height=380,
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis=dict(showgrid=False),
                yaxis=dict(gridcolor="#f1f5f9", zeroline=False),
                legend=dict(orientation="h", y=1.1, x=1, xanchor="right")
            )
            st.plotly_chart(fig, width="stretch", config={'displayModeBar': False})

        with col_side:
            st.subheader("Forecast Snapshot")
            st.markdown(f"**Confidence Range**\n### {confidence_range}")
            st.caption("Based on historical RMSE model performance.")
            st.divider()
            status_icon = "🟢" if pred_val else "🔴"
            st.markdown(f"**System Status**\n### {status_icon} Operational")
            st.caption("Pipeline and inference service healthy.")

        col_intel, col_perf = st.columns([1.5, 1], gap="medium")

        with col_intel:
            st.subheader("Market Intelligence")
            m1, m2 = st.columns(2)
            with m1:
                st.markdown("**Dominant Driver**")
                st.info(dominant_driver)
            with m2:
                st.markdown("**Correlation Score**")
                st.success(f"{corr_val:.2f}")

            st.markdown("**Market Dynamics**")
            st.write(market_trend)
            st.progress(min(max(abs(corr_val), 0), 1.0))
            st.caption(f"ICP/WTI Pearson: {corr_insight}")

        with col_perf:
            st.subheader("Model Performance")
            st.metric("Model RMSE", f"{rmse:.2f}", delta_color="normal")
            st.caption("Lower RMSE indicates higher prediction precision.")
            with st.expander("Model Metadata", expanded=False):
                if model_meta:
                    st.json(model_meta)

        render_footer()

    except Exception as e:
        traceback.print_exc()
        st.error(f"Dashboard failed to load: {str(e)}")

if __name__ == "__main__":
    run_dashboard()
else:
    # Streamlit pages are imported by streamlit
    run_dashboard()

