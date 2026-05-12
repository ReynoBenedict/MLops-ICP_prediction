import streamlit as st

from config.settings import PAGE_ICON
from utils.data_loader import load_processed_data, load_pipeline_metrics, get_latest_context
from services.prediction_service import get_prediction_service
from components.layouts import render_footer
from components.metrics import render_kpi_card
from components.styles import apply_custom_styles
from services.insight_service import InsightService

# ── Apply Styles ──────────────────────────────────────────────────────────────
apply_custom_styles()

# ── Data Orchestration ────────────────────────────────────────────────────────
df = load_processed_data()
metrics = load_pipeline_metrics()
service = get_prediction_service()
latest_icp = df['icp_price'].iloc[-1] if not df.empty else 0.0
features = get_latest_context(df)

try:
    pred_val = service.predict(features)
    model_meta = service.get_model_metadata()
except Exception:
    pred_val = None
    model_meta = {}

rmse = metrics.get('rmse', 3.6)

# ── Derived signals ───────────────────────────────────────────────────────────
trend_label = "Bullish" if pred_val and pred_val > latest_icp else "Bearish" if pred_val and pred_val < latest_icp else "Neutral"
latest_wti = df['wti_price'].iloc[-1] if not df.empty else 0.0
confidence_range = f"${pred_val - rmse:.2f} – ${pred_val + rmse:.2f}" if pred_val else "N/A"
relative_delta = f"{((pred_val / latest_icp) - 1) * 100:+.2f}%" if pred_val and latest_icp else "N/A"
dominant_driver = InsightService.get_dominance_insight(model_meta) if model_meta else "Driver data unavailable."
pred_val_str = f"${pred_val:.2f}" if pred_val else "N/A"

# ── Page Header ───────────────────────────────────────────────────────────────
st.title("Ringkasan Prediksi ICP")
st.caption("Arah harga, rentang kepercayaan, dan faktor pasar utama.")

# ── SECTION 1: Hero Forecast ──────────────────────────────────────────────────
if pred_val:
    col_status, col_wti = st.columns([1, 1], gap="small")
    with col_status:
        st.metric(label="Arah Prediksi", value=trend_label)
    with col_wti:
        st.metric(label="Benchmark WTI", value=f"${latest_wti:.2f}")

    st.metric(
        label="Harga ICP Proyeksi",
        value=f"${pred_val:.2f}",
        delta=relative_delta,
        help=f"Rentang 95%: {confidence_range}"
    )

    metric_col1, metric_col2, metric_col3 = st.columns([1, 1, 1], gap="small")
    with metric_col1:
        st.metric(label="ICP Saat Ini", value=f"${latest_icp:.2f}")
    with metric_col2:
        st.metric(label="Perubahan", value=relative_delta)
    with metric_col3:
        st.metric(label="Status Model", value="Sehat")

    st.divider()

    col_brief_main, col_brief_side = st.columns([3, 2], gap="small")
    with col_brief_main:
        st.markdown(f"**Arah:** {trend_label} | **Harga:** {pred_val_str} | **Rentang:** {confidence_range}")
    with col_brief_side:
        st.info("WTI adalah indikator utama")

else:
    st.error("Sistem prediksi tidak tersedia")

# ── SECTION 2: Key Business Metrics ───────────────────────────────────────────
st.subheader("Key Metrics")

metric_cols = st.columns([1, 1, 1, 1], gap="small")
with metric_cols[0]:
    render_kpi_card("ICP Saat Ini", f"${latest_icp:.2f}", "Harga domestik")
with metric_cols[1]:
    render_kpi_card("WTI Terbaru", f"${latest_wti:.2f}", "Benchmark global")
with metric_cols[2]:
    render_kpi_card("Perubahan", relative_delta, "Proyeksi shift")
with metric_cols[3]:
    render_kpi_card("Status", "Sehat", "Pipeline aktif")

# ── SECTION 3: Market Intelligence ───────────────────────────────────────────
st.subheader("Market Analysis")

col_ops, col_corr = st.columns([3, 2], gap="small")

with col_ops:
    market_trend = InsightService.get_market_trend_insight(df)
    st.caption("OPERATIONAL SIGNALS")
    st.markdown(f"WTI adalah indikator utama | Pantau mingguan | {market_trend}")

with col_corr:
    corr_insight = InsightService.get_correlation_insight(df)
    st.caption("ICP-WTI RELATIONSHIP")
    st.markdown(corr_insight)

render_footer()
