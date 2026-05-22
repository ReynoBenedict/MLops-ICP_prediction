import plotly.graph_objects as go
import streamlit as st

from components.layouts import render_footer
from components.styles import apply_custom_styles
from config.settings import PAGE_ICON
from services.insight_service import InsightService
from utils.data_loader import load_processed_data

# Page Configuration
st.set_page_config(
    page_title="Market Trends | ICP Intelligence",
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded"
)

apply_custom_styles()

df = load_processed_data()

# ── Derived Metrics ───────────────────────────────────────────────────────────
df["ma3"] = df["icp_price"].rolling(window=3).mean()
df["vol"] = df["icp_price"].rolling(window=3).std()

# ── Page Header ───────────────────────────────────────────────────────────────
st.title("Market Trends")

st.caption("Ringkasan kondisi pasar minyak, arah pergerakan harga, dan tingkat risiko pasar.")

# ── Empty Guard ───────────────────────────────────────────────────────────────
if df.empty:
    st.error("Data tidak tersedia.")
    render_footer()
    st.stop()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — MARKET OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("### Market Overview")

st.info(
    """
Halaman ini digunakan untuk membaca kondisi pasar minyak secara umum,
mulai dari arah tren harga, perubahan momentum, hingga tingkat risiko pasar
berdasarkan volatilitas historis ICP dan WTI.
"""
)

# ── Main History Chart ────────────────────────────────────────────────────────
fig_history = go.Figure()

fig_history.add_trace(
    go.Scatter(
        x=df["date"],
        y=df["icp_price"],
        name="ICP Price",
        line=dict(
            color="#002b5c",
            width=2.8,
        ),
        mode="lines",
        hovertemplate="ICP: $%{y:.2f}<extra></extra>",
    )
)

fig_history.add_trace(
    go.Scatter(
        x=df["date"],
        y=df["wti_price"],
        name="WTI Benchmark",
        line=dict(
            color="#c7a96b",
            width=2,
            dash="dot",
        ),
        mode="lines",
        hovertemplate="WTI: $%{y:.2f}<extra></extra>",
    )
)

fig_history.update_layout(
    template="plotly_white",
    hovermode="x unified",
    margin=dict(l=10, r=10, t=10, b=10),
    height=380,
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
        showgrid=True,
        gridcolor="#f0f0f0",
    ),
)

st.plotly_chart(
    fig_history,
    width="stretch",
)

# ── Context Explanation ───────────────────────────────────────────────────────
exp1, exp2, exp3 = st.columns(3, gap="medium")

with exp1:
    st.success(
        """
ICP dan WTI masih bergerak cukup searah.
Kenaikan WTI global biasanya ikut mendorong penyesuaian ICP domestik.
"""
    )

with exp2:
    st.warning(
        """
Perbedaan jarak antar garis menunjukkan adanya tekanan pasar,
lag penyesuaian harga, atau perubahan kondisi global.
"""
    )

with exp3:
    st.info(
        """
Lonjakan besar seperti 2020 dan 2022 mencerminkan periode
ketidakpastian pasar dan tekanan makro global.
"""
    )

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — MOMENTUM & MARKET RISK
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("### Momentum & Risk Analysis")

st.caption("Membaca arah tren jangka pendek dan tingkat ketidakpastian pasar.")

col1, col2 = st.columns([1, 1], gap="large")

# ──────────────────────────────────────────────────────────────────────────────
# LEFT — MOMENTUM
# ──────────────────────────────────────────────────────────────────────────────
with col1:
    st.markdown("#### Price Momentum")

    st.caption("Garis biru tua menunjukkan tren rata-rata 3 bulan untuk menyaring noise jangka pendek.")

    fig_ma = go.Figure()

    fig_ma.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["icp_price"],
            name="ICP Price",
            line=dict(
                color="#d5dbe5",
                width=1.5,
            ),
            mode="lines",
            hovertemplate="ICP: $%{y:.2f}<extra></extra>",
        )
    )

    fig_ma.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["ma3"],
            name="3-Month Trend",
            line=dict(
                color="#002b5c",
                width=2.8,
            ),
            mode="lines",
            hovertemplate="Trend: $%{y:.2f}<extra></extra>",
        )
    )

    fig_ma.update_layout(
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=10, r=10, t=10, b=10),
        height=300,
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
            showgrid=True,
            gridcolor="#f0f0f0",
        ),
    )

    st.plotly_chart(
        fig_ma,
        width="stretch",
    )

    st.info(
        """
Jika garis tren terus naik, pasar sedang berada dalam fase penguatan harga.
Sebaliknya, tren yang mulai datar atau turun biasanya menandakan pelemahan momentum pasar.
"""
    )

# ──────────────────────────────────────────────────────────────────────────────
# RIGHT — VOLATILITY
# ──────────────────────────────────────────────────────────────────────────────
with col2:
    st.markdown("#### Market Risk")

    st.caption("Semakin tinggi volatilitas, semakin tinggi ketidakpastian pasar.")

    fig_vol = go.Figure()

    fig_vol.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["vol"],
            name="Risk Level",
            fill="tozeroy",
            line=dict(
                color="#c7a96b",
                width=2.5,
            ),
            fillcolor="rgba(199,169,107,0.18)",
            hovertemplate="Risk: %{y:.2f}<extra></extra>",
        )
    )

    fig_vol.update_layout(
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=10, r=10, t=10, b=10),
        height=300,
        showlegend=False,
        xaxis=dict(
            showgrid=False,
            title=None,
        ),
        yaxis=dict(
            title="Volatility",
            showgrid=True,
            gridcolor="#f0f0f0",
        ),
    )

    st.plotly_chart(
        fig_vol,
        width="stretch",
    )

    st.warning(
        """
Lonjakan volatilitas biasanya muncul saat pasar mengalami tekanan besar,
misalnya akibat konflik geopolitik, gangguan supply, atau perubahan kebijakan energi global.
"""
    )

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — MARKET SIGNALS
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("### Market Signals")

# ── Signal Calculation ────────────────────────────────────────────────────────
recent_icp = df["icp_price"].iloc[-1]
prev_icp = df["icp_price"].iloc[-2]

ma3_latest = df["ma3"].iloc[-1]

vol_latest = df["vol"].iloc[-1]
vol_avg = df["vol"].mean()

mom_pct = ((recent_icp - prev_icp) / prev_icp) * 100

# ── Direction ─────────────────────────────────────────────────────────────────
if recent_icp > ma3_latest * 1.02:
    direction = "Bullish"
    direction_color = "#0a7c42"

elif recent_icp < ma3_latest * 0.98:
    direction = "Bearish"
    direction_color = "#c0392b"

else:
    direction = "Stabil"
    direction_color = "#7a8290"

# ── Risk ──────────────────────────────────────────────────────────────────────
if vol_latest > vol_avg * 1.2:
    risk_label = "Tinggi"
    risk_color = "#c0392b"

elif vol_latest < vol_avg * 0.8:
    risk_label = "Rendah"
    risk_color = "#0a7c42"

else:
    risk_label = "Normal"
    risk_color = "#b38b59"

# ── Cycle ─────────────────────────────────────────────────────────────────────
if vol_latest > vol_avg * 1.2:
    cycle_phase = "Pasar Ekspansif"

elif vol_latest < vol_avg * 0.8:
    cycle_phase = "Konsolidasi"

else:
    cycle_phase = "Pergerakan Normal"

# ── Cards ─────────────────────────────────────────────────────────────────────
card1, card2, card3, card4 = st.columns(4, gap="medium")

with card1:
    st.metric(
        label="Arah Pasar",
        value=direction,
        help="Posisi ICP terhadap tren 3 bulan",
    )

with card2:
    st.metric(
        label="Risiko Pasar",
        value=risk_label,
        help="Berdasarkan volatilitas historis",
    )

with card3:
    st.metric(
        label="Momentum",
        value=f"{mom_pct:+.1f}%",
        help="Perubahan dibanding periode sebelumnya",
    )

with card4:
    st.metric(
        label="Fase Pasar",
        value=cycle_phase,
        help="Kondisi pasar berdasarkan volatilitas",
    )

st.markdown("")

st.success(
    f"""
{InsightService.get_market_trend_insight(df)}
"""
)

st.divider()

render_footer()

