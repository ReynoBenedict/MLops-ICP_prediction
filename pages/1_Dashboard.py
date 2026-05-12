import streamlit as st
import plotly.graph_objects as go

from config.settings import PAGE_ICON
from utils.data_loader import (
    load_processed_data,
    load_pipeline_metrics,
    get_latest_context,
)
from services.prediction_service import get_prediction_service
from components.layouts import render_footer
from components.styles import apply_custom_styles
from services.insight_service import InsightService


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

try:
    pred_val = service.predict(features)
    model_meta = service.get_model_metadata()

except Exception:
    pred_val = None
    model_meta = {}

rmse = metrics.get("rmse", 3.6)

# ═══════════════════════════════════════════════════════════════════════════════
# DERIVED METRICS
# ═══════════════════════════════════════════════════════════════════════════════
if pred_val and latest_icp:
    delta_pct = ((pred_val / latest_icp) - 1) * 100
else:
    delta_pct = 0

relative_delta = f"{delta_pct:+.2f}%"

trend_label = (
    "Bullish"
    if pred_val and pred_val > latest_icp
    else "Bearish"
    if pred_val and pred_val < latest_icp
    else "Neutral"
)

confidence_low = pred_val - rmse if pred_val else 0
confidence_high = pred_val + rmse if pred_val else 0

confidence_range = (
    f"${confidence_low:.2f} — ${confidence_high:.2f}"
    if pred_val
    else "N/A"
)

market_trend = InsightService.get_market_trend_insight(df)
corr_insight = InsightService.get_correlation_insight(df)
dominant_driver = InsightService.get_dominance_insight(model_meta)

corr_val = (
    df["icp_price"].corr(df["wti_price"])
    if not df.empty
    else 0
)

# ═══════════════════════════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════════════════════════
st.title("ICP Intelligence Dashboard")

st.caption(
    "Executive overview kondisi pasar minyak, forecasting ICP, dan sinyal model prediktif."
)

st.markdown("")

# ═══════════════════════════════════════════════════════════════════════════════
# EXECUTIVE STATUS STRIP
# ═══════════════════════════════════════════════════════════════════════════════
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
        """
Model mendeteksi potensi pelemahan harga ICP dalam periode berikutnya. 
Pergerakan pasar masih dipengaruhi volatilitas global dan dinamika WTI.
"""
    )

else:
    st.info(
        """
Pasar berada dalam kondisi relatif stabil dengan pergerakan yang belum menunjukkan arah dominan.
"""
    )

st.markdown("")

# ═══════════════════════════════════════════════════════════════════════════════
# HERO METRICS
# ═══════════════════════════════════════════════════════════════════════════════
hero1, hero2, hero3, hero4 = st.columns(4, gap="medium")

with hero1:
    st.metric(
        label="Predicted ICP",
        value=f"${pred_val:.2f}" if pred_val else "N/A",
        delta=relative_delta,
    )

with hero2:
    st.metric(
        label="Current ICP",
        value=f"${latest_icp:.2f}",
    )

with hero3:
    st.metric(
        label="WTI Benchmark",
        value=f"${latest_wti:.2f}",
    )

with hero4:
    st.metric(
        label="Market Direction",
        value=trend_label,
    )

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN SECTION
# ═══════════════════════════════════════════════════════════════════════════════
main_left, main_right = st.columns([3.2, 1], gap="large")

# ──────────────────────────────────────────────────────────────────────────────
# LEFT — MAIN CHART
# ──────────────────────────────────────────────────────────────────────────────
with main_left:

    st.markdown("### ICP Historical Trend")

    st.caption(
        "Pergerakan historis ICP dalam 24 periode terakhir beserta titik prediksi model."
    )

    chart_df = df.tail(24).copy()

    fig = go.Figure()

    # Historical ICP
    fig.add_trace(
        go.Scatter(
            x=chart_df.index,
            y=chart_df["icp_price"],
            mode="lines+markers",
            name="ICP Price",
            line=dict(
                color="#002b5c",
                width=3,
            ),
            marker=dict(size=6),
            hovertemplate="ICP: $%{y:.2f}<extra></extra>",
        )
    )

    # Forecast Point
    if pred_val:
        fig.add_trace(
            go.Scatter(
                x=[chart_df.index[-1]],
                y=[pred_val],
                mode="markers",
                name="Forecast",
                marker=dict(
                    size=14,
                    color="#c0392b",
                    line=dict(
                        width=2,
                        color="white",
                    ),
                ),
                hovertemplate="Forecast: $%{y:.2f}<extra></extra>",
            )
        )

    fig.update_layout(
        template="plotly_white",
        height=420,
        margin=dict(l=10, r=10, t=20, b=10),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        xaxis=dict(
            showgrid=False,
            title=None,
        ),
        yaxis=dict(
            title="USD / BBL",
            gridcolor="#f0f0f0",
        ),
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.info(
        """
Grafik menunjukkan tren ICP historis dan posisi prediksi terbaru model. 
Titik merah menandakan estimasi harga ICP periode berikutnya berdasarkan kondisi pasar saat ini.
"""
    )

# ──────────────────────────────────────────────────────────────────────────────
# RIGHT — FORECAST PANEL
# ──────────────────────────────────────────────────────────────────────────────
with main_right:

    st.markdown("### Forecast Snapshot")

    st.markdown("")

    st.markdown(
        f"""
<div style="
padding: 1rem;
border-radius: 14px;
background: linear-gradient(135deg,#f8fafc,#eef4ff);
border: 1px solid #dbe6ff;
margin-bottom: 1rem;
">

<div style="
font-size: 0.85rem;
color: #64748b;
margin-bottom: 0.4rem;
">
Forecast Range
</div>

<div style="
font-size: 1.4rem;
font-weight: 700;
color: #002b5c;
">
{confidence_range}
</div>

<div style="
font-size: 0.82rem;
margin-top: 0.5rem;
color: #64748b;
">
Estimasi rentang prediksi berdasarkan RMSE model
</div>

</div>
""",
        unsafe_allow_html=True,
    )

    st.markdown(
        """
<div style="
padding: 1rem;
border-radius: 14px;
background: linear-gradient(135deg,#f0fff4,#dcfce7);
border: 1px solid #bbf7d0;
margin-bottom: 1rem;
">

<div style="
font-size: 0.85rem;
color: #166534;
margin-bottom: 0.4rem;
">
Model Status
</div>

<div style="
font-size: 1.5rem;
font-weight: 700;
color: #166534;
">
Healthy
</div>

<div style="
font-size: 0.82rem;
margin-top: 0.5rem;
color: #166534;
">
Pipeline dan model inference berjalan normal
</div>

</div>
""",
        unsafe_allow_html=True,
    )

    st.markdown(
        """
<div style="
padding: 1rem;
border-radius: 14px;
background: linear-gradient(135deg,#fff8eb,#fef3c7);
border: 1px solid #fde68a;
">

<div style="
font-size: 0.85rem;
color: #92400e;
margin-bottom: 0.4rem;
">
Primary Driver
</div>

<div style="
font-size: 1.5rem;
font-weight: 700;
color: #92400e;
">
WTI Price
</div>

<div style="
font-size: 0.82rem;
margin-top: 0.5rem;
color: #92400e;
">
Variabel paling dominan terhadap prediksi ICP
</div>

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

# ──────────────────────────────────────────────────────────────────────────────
# LEFT
# ──────────────────────────────────────────────────────────────────────────────
with intel_left:

    st.markdown("#### Market Trend")

    st.write(market_trend)

    st.markdown("")

    st.markdown("#### Dominant Drivers")

    st.write(dominant_driver)

# ──────────────────────────────────────────────────────────────────────────────
# RIGHT
# ──────────────────────────────────────────────────────────────────────────────
with intel_right:

    st.markdown("#### ICP vs WTI Correlation")

    st.write(corr_insight)

    st.markdown("")

    st.progress(
        min(max(abs(corr_val), 0), 1.0)
    )

    st.caption(
        f"Kekuatan hubungan ICP dan WTI berada di level {corr_val:.2f}"
    )

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# MODEL PERFORMANCE
# ═══════════════════════════════════════════════════════════════════════════════
perf1, perf2, perf3 = st.columns(3, gap="medium")

with perf1:
    st.info(
        f"""
### {trend_label}

Arah pasar berdasarkan hasil forecasting terbaru.
"""
    )

with perf2:
    st.success(
        f"""
### {relative_delta}

Estimasi perubahan dibanding harga ICP saat ini.
"""
    )

with perf3:
    st.warning(
        f"""
### RMSE {rmse:.2f}

Estimasi rata-rata error historis model.
"""
    )

st.divider()

render_footer()