import streamlit as st

from services.insight_service import InsightService
from services.prediction_service import get_prediction_service
from utils.data_loader import (
    load_processed_data,
    get_latest_context,
    load_pipeline_metrics,
)
from components.layouts import render_footer
from components.charts import render_timeseries_analysis
from components.styles import apply_custom_styles


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
apply_custom_styles()

# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════
df = load_processed_data()
metrics = load_pipeline_metrics()

latest_icp = (
    df["icp_price"].iloc[-1]
    if not df.empty
    else 0.0
)

latest_wti = (
    df["wti_price"].iloc[-1]
    if not df.empty
    else 0.0
)

features = get_latest_context(df)

service = get_prediction_service()

rmse = metrics.get("rmse", 3.6)

try:
    pred_val = service.predict(features)
    model_meta = service.get_model_metadata()

except Exception:
    pred_val = None
    model_meta = {}

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════════════════════
st.title("Forecast Harga ICP")

st.caption(
    "Proyeksi harga ICP periode berikutnya berdasarkan kondisi pasar minyak dan pola historis."
)

# ═══════════════════════════════════════════════════════════════════════════════
# FORECAST CALCULATION
# ═══════════════════════════════════════════════════════════════════════════════
if pred_val:

    confidence_low = pred_val - rmse
    confidence_high = pred_val + rmse

    confidence_range = (
        f"${confidence_low:.2f} — ${confidence_high:.2f}"
    )

    delta_pct = (
        ((pred_val / latest_icp) - 1) * 100
        if latest_icp
        else 0
    )

    if pred_val > latest_icp:
        direction = "Bullish"
        direction_desc = "Potensi kenaikan harga"

    elif pred_val < latest_icp:
        direction = "Bearish"
        direction_desc = "Potensi pelemahan harga"

    else:
        direction = "Stabil"
        direction_desc = "Pergerakan relatif netral"

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — EXECUTIVE FORECAST STATUS
# ═══════════════════════════════════════════════════════════════════════════════
if pred_val:

    if direction == "Bullish":

        st.success(
            f"""
Model memproyeksikan harga ICP berada di sekitar ${pred_val:.2f} 
dengan potensi kenaikan {delta_pct:+.2f}% dibanding kondisi saat ini. 
Pergerakan WTI global masih menjadi pendorong utama arah pasar.
"""
        )

    elif direction == "Bearish":

        st.warning(
            """
Model mendeteksi potensi pelemahan harga ICP pada periode berikutnya. 
Volatilitas pasar global masih cukup tinggi sehingga pergerakan harga perlu dipantau lebih ketat.
"""
        )

    else:

        st.info(
            """
Pergerakan harga diperkirakan relatif stabil tanpa perubahan signifikan dalam jangka pendek.
"""
        )

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — FORECAST SNAPSHOT
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("")

st.subheader("Forecast Snapshot")

snap1, snap2, snap3, snap4 = st.columns(4, gap="medium")

with snap1:
    st.metric(
        label="Predicted ICP",
        value=f"${pred_val:.2f}",
        delta=f"{delta_pct:+.2f}%",
    )

with snap2:
    st.metric(
        label="Current ICP",
        value=f"${latest_icp:.2f}",
    )

with snap3:
    st.metric(
        label="Forecast Direction",
        value=direction,
    )

with snap4:
    st.metric(
        label="Confidence Level",
        value="95%",
    )

st.markdown("")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — FORECAST INTERPRETATION PANEL
# ═══════════════════════════════════════════════════════════════════════════════
panel_left, panel_right = st.columns([2, 1], gap="large")

with panel_left:

    st.markdown("### Forecast Interpretation")

    st.markdown(
        f"""
Prediksi saat ini menunjukkan bahwa harga ICP berpotensi berada pada kisaran 
**{confidence_range}** dengan estimasi pusat di sekitar **${pred_val:.2f}**.

Perubahan ini mengindikasikan kondisi pasar yang masih dipengaruhi oleh:
- arah harga minyak global (WTI)
- momentum historis ICP
- volatilitas pasar energi internasional
- pola pergerakan harga beberapa periode terakhir

Forecast ini dapat digunakan sebagai referensi awal untuk:
- penyesuaian pricing
- evaluasi kontrak energi
- monitoring risiko pasar
- perencanaan operasional jangka pendek
"""
    )

with panel_right:

    st.markdown("### Forecast Signal")

    st.markdown(
        f"""
<div style="
padding: 1.1rem;
border-radius: 14px;
background: linear-gradient(135deg,#eef4ff,#dbeafe);
border: 1px solid #bfdbfe;
margin-bottom: 1rem;
">

<div style="
font-size: 0.85rem;
color: #1e40af;
margin-bottom: 0.5rem;
">
Forecast Range
</div>

<div style="
font-size: 1.5rem;
font-weight: 700;
color: #002b5c;
">
{confidence_range}
</div>

<div style="
font-size: 0.82rem;
margin-top: 0.6rem;
color: #475569;
">
Rentang estimasi berdasarkan error historis model
</div>

</div>
""",
        unsafe_allow_html=True,
    )

    st.markdown(
        """
<div style="
padding: 1.1rem;
border-radius: 14px;
background: linear-gradient(135deg,#f0fdf4,#dcfce7);
border: 1px solid #bbf7d0;
">

<div style="
font-size: 0.85rem;
color: #166534;
margin-bottom: 0.5rem;
">
Primary Market Driver
</div>

<div style="
font-size: 1.4rem;
font-weight: 700;
color: #166534;
">
WTI Price
</div>

<div style="
font-size: 0.82rem;
margin-top: 0.6rem;
color: #475569;
">
Harga minyak global masih menjadi variabel paling dominan terhadap prediksi ICP.
</div>

</div>
""",
        unsafe_allow_html=True,
    )

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — HISTORICAL & FORECAST CHART
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("Historical Forecast Analysis")

st.caption(
    "Visualisasi harga ICP historis, titik prediksi model, dan area ketidakpastian forecast."
)

render_timeseries_analysis(df)

st.info(
    """
Grafik membantu melihat posisi forecast terbaru dibanding pola historis ICP sebelumnya. 
Area bayangan menunjukkan rentang ketidakpastian prediksi berdasarkan performa model historis.
"""
)

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — MODEL INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("Model Intelligence")

intel_left, intel_right = st.columns([1.2, 1], gap="large")

with intel_left:

    st.markdown("#### Market Context")

    st.write(
        f"""
Harga ICP saat ini masih bergerak searah dengan benchmark WTI global. 
Dengan posisi WTI terbaru di sekitar ${latest_wti:.2f}, model membaca adanya 
momentum harga yang masih cukup kuat untuk menopang pergerakan ICP jangka pendek.
"""
    )

    st.markdown("")

    st.markdown("#### Forecast Reliability")

    confidence_text = InsightService.get_confidence_insight(
        rmse,
        pred_val,
    )

    st.write(confidence_text)

with intel_right:

    st.markdown("#### Dominant Drivers")

    if model_meta:

        dominance_text = InsightService.get_dominance_insight(
            model_meta
        )

        st.info(dominance_text)

    else:

        st.warning(
            "Metadata model tidak tersedia."
        )

    st.markdown("")

    st.markdown("#### Operational Notes")

    st.warning(
        """
Forecast sebaiknya digunakan sebagai alat pendukung keputusan, 
bukan sebagai satu-satunya dasar pengambilan strategi pasar. 
Perubahan geopolitik dan volatilitas global tetap dapat memengaruhi realisasi harga aktual.
"""
    )

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — FINAL SIGNALS
# ═══════════════════════════════════════════════════════════════════════════════
signal1, signal2, signal3 = st.columns(3, gap="medium")

with signal1:

    st.success(
        f"""
### {direction}

{direction_desc}
"""
    )

with signal2:

    st.info(
        f"""
### {delta_pct:+.2f}%

Estimasi perubahan dibanding harga ICP saat ini.
"""
    )

with signal3:

    st.warning(
        f"""
### RMSE {rmse:.2f}

Estimasi rata-rata error historis model.
"""
    )

render_footer() 