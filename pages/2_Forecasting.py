import streamlit as st

from config.settings import PAGE_ICON
from services.insight_service import InsightService
from services.prediction_service import get_prediction_service
from utils.data_loader import load_processed_data, get_latest_context, load_pipeline_metrics
from components.layouts import render_footer
from components.charts import render_timeseries_analysis
from components.styles import apply_custom_styles

# ── Page Config ───────────────────────────────────────────────────────────────
apply_custom_styles()

# ── Data Orchestration ────────────────────────────────────────────────────────
df = load_processed_data()
metrics = load_pipeline_metrics()
latest_icp = df['icp_price'].iloc[-1] if not df.empty else 0.0
features = get_latest_context(df)
service = get_prediction_service()
rmse = metrics.get('rmse', 3.6)

try:
    pred_val = service.predict(features)
    model_meta = service.get_model_metadata()
except Exception:
    pred_val = None
    model_meta = {}

# ── Page Header ───────────────────────────────────────────────────────────────
st.title("Forecast Harga ICP")
st.caption("Proyeksi bulan depan dengan rentang kepercayaan dan faktor pasar.")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Historical & Forecast Chart
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("Historical & Forecast")

st.info("Grafik menunjukkan harga ICP historis dengan prediksi bulan depan dan rentang kepercayaan 95%")

render_timeseries_analysis(df)

st.caption("Garis utama = pusat prediksi | Area berbayang = rentang ketidakpastian")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Forecast Snapshot
# ═══════════════════════════════════════════════════════════════════════════════
st.divider()

st.subheader("Forecast Snapshot")

if pred_val:
    confidence_range = f"${pred_val - rmse:.2f} – ${pred_val + rmse:.2f}"
    direction = "Higher" if pred_val > latest_icp else "Lower" if pred_val < latest_icp else "Stable"
    change_pct = abs((pred_val - latest_icp) / latest_icp * 100) if latest_icp else 0

    # Primary forecast card - full width
    st.metric(
        label="Projected ICP Price",
        value=f"${pred_val:.2f}",
        delta=f"{direction}",
        help=f"Expected range: {confidence_range}"
    )
    st.caption(f"Range: {confidence_range}")

    st.divider()

    metric_col1, metric_col2, metric_col3 = st.columns([1, 1, 1], gap="small")

    with metric_col1:
        st.metric(label="Arah", value=direction, help="vs. level saat ini")

    with metric_col2:
        st.metric(label="Perubahan", value=f"{change_pct:+.1f}%", help=f"dari ${latest_icp:.2f}")

    with metric_col3:
        st.metric(label="Kepercayaan", value="95%", help="rentang statistik")

    # ═══════════════════════════════════════════════════════════════════════════════
    # SECTION 3 — Executive Interpretation
    # ═══════════════════════════════════════════════════════════════════════════════
    st.divider()

    st.subheader("Forecast Interpretation")

    col_main, col_side = st.columns([3, 2], gap="small")

    with col_main:
        st.markdown(f"Target: ${pred_val:.2f} ({direction.lower()}) | Rentang: {confidence_range} | Gunakan untuk: Penetapan harga, hedging, perencanaan")

    with col_side:
        st.info("WTI adalah driver utama. Model menggabungkan momentum dan pola musiman")

    # ═══════════════════════════════════════════════════════════════════════════════
    # SECTION 4 — Model Insights
    # ═══════════════════════════════════════════════════════════════════════════════
    st.divider()

    st.subheader("Model Insights")

    if model_meta:
        dominance_text = InsightService.get_dominance_insight(model_meta)
        confidence_text = InsightService.get_confidence_insight(rmse, pred_val)

        st.markdown(f"Driver: {dominance_text} | Keandalan: {confidence_text}")
    else:
        st.warning("Metadata model tidak tersedia")

else:
    st.error("Prediksi tidak tersedia. Periksa pipeline data dan layanan prediksi.")

render_footer()
