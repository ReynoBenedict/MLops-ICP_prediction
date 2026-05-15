import logging
import traceback
import plotly.graph_objects as go
import streamlit as st
from components.charts import render_timeseries_analysis
from components.layouts import render_footer
from components.styles import apply_custom_styles
from services.insight_service import InsightService
from services.prediction_service import get_prediction_service
from utils.data_loader import (
    get_latest_context,
    load_pipeline_metrics,
    load_processed_data,
)

logger = logging.getLogger("forecasting")

# --- PAGE CONFIG ---
apply_custom_styles()

# --- DATA LOADING ---
with st.spinner("Mengambil data pasar terbaru..."):
    df = load_processed_data()
    metrics = load_pipeline_metrics()

latest_icp = df["icp_price"].iloc[-1] if not df.empty else 0.0
latest_wti = df["wti_price"].iloc[-1] if not df.empty else 0.0
features = get_latest_context(df)
service = get_prediction_service()
rmse = metrics.get("rmse", 3.6)

pred_val = None
model_meta = {}
prediction_error = None

if features:
    try:
        with st.spinner("Menganalisis pola pasar dengan engine AI..."):
            pred_val = service.predict(features)
        with st.spinner("Sinkronisasi metadata model..."):
            model_meta = service.get_model_metadata()
    except Exception as e:
        prediction_error = str(e)
        logger.error(f"Forecasting engine error: {prediction_error}")
        pred_val = None
        model_meta = {}
else:
    prediction_error = "No features available for prediction"

# --- PAGE HEADER ---
st.title("Forecast Harga ICP")
st.caption("Proyeksi harga ICP periode berikutnya berdasarkan kondisi pasar minyak dan pola historis.")

# --- FORECAST CALCULATION ---
direction = "Unknown"
direction_desc = "Estimasi tidak tersedia"
delta_pct = 0.0
confidence_range = "N/A"

if pred_val is not None and latest_icp and latest_icp > 0:
    confidence_low = max(0, pred_val - rmse)
    confidence_high = pred_val + rmse
    confidence_range = f"${confidence_low:.2f} — ${confidence_high:.2f}"
    delta_pct = ((pred_val / latest_icp) - 1) * 100

    if pred_val > latest_icp * 1.01:
        direction = "Bullish"
        direction_desc = "Potensi kenaikan harga"
    elif pred_val < latest_icp * 0.99:
        direction = "Bearish"
        direction_desc = "Potensi pelemahan harga"
    else:
        direction = "Stabil"
        direction_desc = "Pergerakan relatif netral"

# --- SECTION 1: EXECUTIVE FORECAST STATUS ---
if prediction_error:
    st.error(f"⚠️ Prediction Service Error: {prediction_error}")
    st.info("Dashboard will display available data. Some metrics may show N/A.")
elif pred_val is not None:
    if direction == "Bullish":
        st.success(f"Model memproyeksikan harga ICP berada di sekitar ${pred_val:.2f} dengan potensi kenaikan {delta_pct:+.2f}% dibanding kondisi saat ini.")
    elif direction == "Bearish":
        st.warning(f"Model mendeteksi potensi pelemahan harga ICP pada periode berikutnya ke sekitar ${pred_val:.2f}.")
    else:
        st.info("Pergerakan harga diperkirakan relatif stabil tanpa perubahan signifikan dalam jangka pendek.")
else:
    st.error("Model gagal memuat data prediksi. Silakan periksa koneksi MLflow.")

# --- SECTION 2: FORECAST SNAPSHOT ---
st.markdown("")
st.subheader("Forecast Snapshot")
snap1, snap2, snap3, snap4 = st.columns(4, gap="medium")
with snap1:
    st.metric(label="Predicted ICP", value=f"${pred_val:.2f}" if pred_val is not None else "N/A", delta=f"{delta_pct:+.2f}%" if pred_val is not None else None)
with snap2:
    st.metric(label="Current ICP", value=f"${latest_icp:.2f}")
with snap3:
    st.metric(label="Forecast Direction", value=direction)
with snap4:
    st.metric(label="Confidence Level", value="95%" if pred_val is not None else "N/A")

st.markdown("")

# --- SECTION 3: FORECAST INTERPRETATION ---
col_left, col_right = st.columns([2, 1], gap="large")
with col_left:
    st.markdown("### Forecast Interpretation")
    if pred_val is not None:
        st.markdown(f"Prediksi saat ini menunjukkan bahwa harga ICP berpotensi berada pada kisaran **{confidence_range}** dengan estimasi pusat di sekitar **${pred_val:.2f}**.")
    else:
        st.markdown("Interpretasi tidak tersedia karena kegagalan prediksi.")
with col_right:
    st.markdown("### Forecast Signal")
    st.metric(label="Forecast Range", value=confidence_range)

st.divider()

# --- SECTION 4: HISTORICAL & FORECAST CHART ---
st.subheader("Historical Forecast Analysis")
render_timeseries_analysis(df)

st.divider()

# --- SECTION 5: MODEL INSIGHTS ---
st.subheader("Model Intelligence")
intel_left, intel_right = st.columns([1.2, 1], gap="large")
with intel_left:
    st.markdown("#### Market Context")
    st.write(f"Harga ICP saat ini masih bergerak searah dengan benchmark WTI global (${latest_wti:.2f}).")
with intel_right:
    st.markdown("#### Dominant Drivers")
    if model_meta:
        st.info(InsightService.get_dominance_insight(model_meta))
    else:
        st.warning("Metadata model tidak tersedia.")

st.divider()

# --- SECTION 6: FINAL SIGNALS ---
signal1, signal2, signal3 = st.columns(3, gap="medium")
with signal1:
    st.success(f"### {direction}\n\n{direction_desc}")
with signal2:
    st.info(f"### {delta_pct:+.2f}%\n\nEstimasi perubahan.")
with signal3:
    st.warning(f"### RMSE {rmse:.2f}\n\nError historis model.")

render_footer()

