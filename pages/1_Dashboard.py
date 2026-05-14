import logging

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

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="ICP Dashboard",
    page_icon=PAGE_ICON,
    layout="wide",
)

apply_custom_styles()

# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════
df = load_processed_data()
metrics = load_pipeline_metrics()

service = get_prediction_service()

latest_icp = df["icp_price"].iloc[-1] if not df.empty else 0.0
latest_wti = df["wti_price"].iloc[-1] if not df.empty else 0.0

features = get_latest_context(df)

# Safe prediction loading with error handling
pred_val = None
model_meta = {}
prediction_error = None

try:
    if features:
        pred_val = service.predict(features)
        model_meta = service.get_model_metadata()
        logger.info(f"Prediction successful: {pred_val}")
    else:
        prediction_error = "No features available for prediction"
        logger.warning(prediction_error)
except Exception as e:
    prediction_error = str(e)
    logger.error(f"Prediction failed: {prediction_error}")
    pred_val = None
    model_meta = {}

rmse = metrics.get("rmse", 3.6)

# ═══════════════════════════════════════════════════════════════════════════════
# DERIVED METRICS - Safe formatting with None checks
# ═══════════════════════════════════════════════════════════════════════════════
# Calculate delta percentage safely
if pred_val is not None and latest_icp and latest_icp > 0:
    delta_pct = ((pred_val / latest_icp) - 1) * 100
else:
    delta_pct = 0.0

# Format relative delta safely
relative_delta = f"{delta_pct:+.2f}%" if pred_val is not None else "N/A"

# Determine trend label safely
if pred_val is not None and latest_icp:
    if pred_val > latest_icp * 1.01:
        trend_label = "Bullish"
    elif pred_val < latest_icp * 0.99:
        trend_label = "Bearish"
    else:
        trend_label = "Neutral"
else:
    trend_label = "Unknown"

# Calculate confidence range safely
if pred_val is not None and rmse > 0:
    confidence_low = max(0, pred_val - rmse)
    confidence_high = pred_val + rmse
    confidence_range = f"${confidence_low:.2f} — ${confidence_high:.2f}"
else:
    confidence_range = "N/A"

# Get market insights safely
market_trend = InsightService.get_market_trend_insight(df) if not df.empty else "Data unavailable"
corr_insight = InsightService.get_correlation_insight(df) if not df.empty else "Data unavailable"
dominant_driver = InsightService.get_dominance_insight(model_meta) if model_meta else "Model metadata unavailable"

# Calculate correlation safely
corr_val = (
    df["icp_price"].corr(df["wti_price"])
    if not df.empty and "icp_price" in df.columns and "wti_price" in df.columns
    else 0.0
)

# ═══════════════════════════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════════════════════════
st.title("ICP Intelligence Dashboard")
st.caption("Executive overview kondisi pasar minyak, forecasting ICP, dan sinyal model prediktif.")
st.markdown("")

# ═══════════════════════════════════════════════════════════════════════════════
# EXECUTIVE STATUS STRIP - Safe rendering with error handling
# ═══════════════════════════════════════════════════════════════════════════════
if prediction_error:
    st.error(f"⚠️ Prediction Service Error: {prediction_error}")
    st.info("Dashboard will display available data. Some metrics may show N/A.")
elif pred_val is not None:
    if trend_label == "Bullish":
        st.success(
            f"""
Pasar saat ini menunjukkan kecenderungan penguatan harga.
Model memproyeksikan ICP berada di sekitar ${pred_val:.2f}
dengan potensi perubahan {relative_delta} dibanding kondisi saat ini.
"""
        )
    elif trend_label == "Bearish":
        st.warning(
            f"""
Model mendeteksi potensi pelemahan harga ICP ke sekitar ${pred_val:.2f} dalam periode berikutnya.
Pergerakan pasar masih dipengaruhi volatilitas global dan dinamika WTI.
"""
        )
    else:
        st.info("Pasar berada dalam kondisi relatif stabil dengan pergerakan yang belum menunjukkan arah dominan.")
else:
    st.warning("Model Prediction Service tidak tersedia. Beberapa metrik mungkin menunjukkan N/A.")

st.markdown("")

# ═══════════════════════════════════════════════════════════════════════════════
# HERO METRICS
# ═══════════════════════════════════════════════════════════════════════════════
hero1, hero2, hero3, hero4 = st.columns(4, gap="medium")

with hero1:
    st.metric(
        label="Predicted ICP",
        value=f"${pred_val:.2f}" if pred_val is not None else "N/A",
        delta=relative_delta if pred_val is not None else None,
    )

with hero2:
    st.metric(label="Current ICP", value=f"${latest_icp:.2f}")

with hero3:
    st.metric(label="WTI Benchmark", value=f"${latest_wti:.2f}")

with hero4:
    st.metric(label="Market Direction", value=trend_label)

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN SECTION
# ═══════════════════════════════════════════════════════════════════════════════
main_left, main_right = st.columns([3.2, 1], gap="large")

with main_left:
    st.markdown("### ICP Historical Trend")
    st.caption("Pergerakan historis ICP dalam 24 periode terakhir beserta titik prediksi model.")

    chart_df = df.tail(24).copy()
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=chart_df.index,
            y=chart_df["icp_price"],
            mode="lines+markers",
            name="ICP Price",
            line=dict(color="#002b5c", width=3),
            marker=dict(size=6),
            hovertemplate="ICP: $%{y:.2f}<extra></extra>",
        )
    )

    if pred_val is not None:
        fig.add_trace(
            go.Scatter(
                x=[chart_df.index[-1]],
                y=[pred_val],
                mode="markers",
                name="Forecast",
                marker=dict(size=14, color="#c0392b", line=dict(width=2, color="white")),
                hovertemplate="Forecast: $%{y:.2f}<extra></extra>",
            )
        )

    fig.update_layout(
        template="plotly_white",
        height=420,
        margin=dict(l=10, r=10, t=20, b=10),
        hovermode="x unified",
        xaxis=dict(showgrid=False),
        yaxis=dict(title="USD / BBL", gridcolor="#f0f0f0"),
    )
    st.plotly_chart(fig, use_container_width=True)

with main_right:
    st.markdown("### Forecast Snapshot")
    st.markdown(
        f"""
<div style="padding: 1rem; border-radius: 14px; background: linear-gradient(135deg,#f8fafc,#eef4ff); border: 1px solid #dbe6ff; margin-bottom: 1rem;">
<div style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.4rem;">Forecast Range</div>
<div style="font-size: 1.4rem; font-weight: 700; color: #002b5c;">{confidence_range}</div>
<div style="font-size: 0.82rem; margin-top: 0.5rem; color: #64748b;">Estimasi rentang prediksi berdasarkan RMSE model</div>
</div>
""",
        unsafe_allow_html=True,
    )

    status_color = "#166534" if pred_val is not None else "#991b1b"
    status_text = "Healthy" if pred_val is not None else "Error"
    st.markdown(
        f"""
<div style="padding: 1rem; border-radius: 14px; background: #f0fdf4; border: 1px solid #bbf7d0; margin-bottom: 1rem;">
<div style="font-size: 0.85rem; color: {status_color}; margin-bottom: 0.4rem;">Model Status</div>
<div style="font-size: 1.5rem; font-weight: 700; color: {status_color};">{status_text}</div>
<div style="font-size: 0.82rem; margin-top: 0.5rem; color: {status_color};">Status model registry dan inference service</div>
</div>
""",
        unsafe_allow_html=True,
    )

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# MARKET INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("### Market Intelligence")
intel_left, intel_right = st.columns([1.3, 1], gap="large")

with intel_left:
    st.markdown("#### Market Trend")
    st.write(market_trend)
    st.markdown("")
    st.markdown("#### Dominant Drivers")
    st.write(dominant_driver)

with intel_right:
    st.markdown("#### ICP vs WTI Correlation")
    st.write(corr_insight)
    st.markdown("")
    st.progress(min(max(abs(corr_val), 0), 1.0))
    st.caption(f"Kekuatan hubungan ICP dan WTI berada di level {corr_val:.2f}")

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# MODEL PERFORMANCE
# ═══════════════════════════════════════════════════════════════════════════════
perf1, perf2, perf3 = st.columns(3, gap="medium")

with perf1:
    st.info(f"### {trend_label}\n\nArah pasar berdasarkan hasil forecasting terbaru.")

with perf2:
    st.success(f"### {relative_delta}\n\nEstimasi perubahan dibanding harga ICP saat ini.")

with perf3:
    st.warning(f"### RMSE {rmse:.2f}\n\nEstimasi rata-rata error historis model.")

st.divider()
render_footer()
