import streamlit as st
import plotly.graph_objects as go

from config.settings import PAGE_ICON
from utils.data_loader import load_processed_data
from components.layouts import render_footer
from components.styles import apply_custom_styles
from services.insight_service import InsightService

# ── Page Config ───────────────────────────────────────────────────────────────
apply_custom_styles()

df = load_processed_data()

# ── Pre-compute derived series (no logic change) ──────────────────────────────
df['ma3'] = df['icp_price'].rolling(window=3).mean()
df['vol'] = df['icp_price'].rolling(window=3).std()

# ── Page Header ───────────────────────────────────────────────────────────────
st.title("Market Trends")
st.caption("Perilaku harga historis, sinyal momentum, dan risiko pasar")

# ── Helper: section label ─────────────────────────────────────────────────────
def section_label(text):
    st.subheader(text)

# ── Helper: interpretation card ───────────────────────────────────────────────
def insight_card(bullets: list, accent: str = "#002b5c"):
    for bullet in bullets:
        st.markdown(f"- {bullet}")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — ICP & WTI Price History
# ═══════════════════════════════════════════════════════════════════════════════
section_label("Price History")

fig_history = go.Figure()
fig_history.add_trace(go.Scatter(
    x=df['date'], y=df['icp_price'],
    name="ICP Price",
    line=dict(color='#002b5c', width=2.5),
    mode='lines',
    hovertemplate="ICP: $%{y:.2f}<extra></extra>",
))
fig_history.add_trace(go.Scatter(
    x=df['date'], y=df['wti_price'],
    name="WTI Price",
    line=dict(color='#b38b59', width=2, dash='dot'),
    mode='lines',
    hovertemplate="WTI: $%{y:.2f}<extra></extra>",
))
fig_history.update_layout(
    template="plotly_white",
    hovermode="x unified",
    margin=dict(l=10, r=10, t=10, b=10),
    height=320,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    xaxis=dict(showgrid=False, title=None),
    yaxis=dict(title="USD / BBL", showgrid=True, gridcolor="#f0f0f0"),
)
st.plotly_chart(fig_history, use_container_width=True)

insight_card([
    "ICP (navy) melacak harga domestik | WTI (emas putus-putus) adalah benchmark global | Keduanya bergerak bersama",
    "Celah lebar antara garis menandakan divergensi harga | Pantau untuk eksposur kontrak",
    "Penurunan tajam (2020, mid-2022) mencerminkan guncangan makro | Pemulihan menunjukkan ketahanan pasar",
])

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Short-Term Price Direction  |  Market Risk Temperature
# ═══════════════════════════════════════════════════════════════════════════════
section_label("Momentum & Risk")

col1, col2 = st.columns([1, 1], gap="medium")

with col1:
    st.caption("**Price Direction**")
    st.caption("Tren 3 bulan yang diratakan | Menyaring noise untuk menunjukkan arah mendasar")
    
    fig_ma = go.Figure()
    fig_ma.add_trace(go.Scatter(
        x=df['date'], y=df['icp_price'],
        name="ICP Price",
        line=dict(color='#dce0e8', width=1.5),
        mode='lines',
        hovertemplate="ICP: $%{y:.2f}<extra></extra>",
    ))
    fig_ma.add_trace(go.Scatter(
        x=df['date'], y=df['ma3'],
        name="3-Month Trend",
        line=dict(color='#002b5c', width=2.5),
        mode='lines',
        hovertemplate="Trend: $%{y:.2f}<extra></extra>",
    ))
    fig_ma.update_layout(
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=10, r=10, t=10, b=10),
        height=260,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(showgrid=False, title=None),
        yaxis=dict(title="USD / BBL", showgrid=True, gridcolor="#f0f0f0"),
    )
    st.plotly_chart(fig_ma, use_container_width=True)
    insight_card([
        "When the trend line rises, prices are in an upward cycle — favourable for sellers.",
        "A flattening or falling trend line signals a cooling market — review contract terms.",
        "The grey line (raw price) vs. navy line (trend) shows how much short-term noise exists.",
    ], accent="#002b5c")

with col2:
    st.caption("**Market Risk**")
    st.caption("Variabilitas harga 3 bulan | Lebih tinggi = lebih banyak ketidakpastian")
    
    fig_vol = go.Figure()
    fig_vol.add_trace(go.Scatter(
        x=df['date'], y=df['vol'],
        name="Risk Level",
        fill='tozeroy',
        line=dict(color='#b38b59', width=2),
        fillcolor='rgba(179,139,89,0.15)',
        hovertemplate="Risk: %{y:.2f}<extra></extra>",
    ))
    fig_vol.update_layout(
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=10, r=10, t=10, b=10),
        height=260,
        showlegend=False,
        xaxis=dict(showgrid=False, title=None),
        yaxis=dict(title="Variability (USD)", showgrid=True, gridcolor="#f0f0f0"),
    )
    st.plotly_chart(fig_vol, use_container_width=True)
    insight_card([
        "Tall spikes = high uncertainty — pricing decisions carry more risk during these periods.",
        "Low, flat areas = stable market — good conditions for longer-term contract commitments.",
        "Sustained elevation (e.g. 2022) reflects structural market stress, not just short-term noise.",
    ], accent="#b38b59")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Market Cycle Signals
# ═══════════════════════════════════════════════════════════════════════════════
section_label("Market Cycle Signals")

# Compute signals for the structured panel
if not df.empty and len(df) >= 6:
    recent_icp  = df['icp_price'].iloc[-1]
    prev_icp    = df['icp_price'].iloc[-2]
    ma3_latest  = df['ma3'].iloc[-1]
    vol_latest  = df['vol'].iloc[-1]
    vol_avg     = df['vol'].mean()
    mom_pct     = (recent_icp - prev_icp) / prev_icp * 100

    # Direction
    if recent_icp > ma3_latest * 1.02:
        direction, dir_color, dir_bg = "Bullish", "#0a7c42", "#e8f7ef"
    elif recent_icp < ma3_latest * 0.98:
        direction, dir_color, dir_bg = "Bearish", "#c0392b", "#fdecea"
    else:
        direction, dir_color, dir_bg = "Neutral", "#7a8290", "#f0f2f5"

    # Volatility
    if vol_latest < vol_avg * 0.8:
        risk_label, risk_color, risk_bg = "Low — Stable", "#0a7c42", "#e8f7ef"
    elif vol_latest > vol_avg * 1.2:
        risk_label, risk_color, risk_bg = "Elevated — Caution", "#c0392b", "#fdecea"
    else:
        risk_label, risk_color, risk_bg = "Moderate — Normal", "#b38b59", "#fdf6ec"

    # Momentum label
    mom_label = f"{mom_pct:+.1f}% vs. prior period"
    mom_color = "#0a7c42" if mom_pct > 0 else "#c0392b"

    # Stability
    stability = "Consolidating" if vol_latest < vol_avg * 0.8 else "Expanding" if vol_latest > vol_avg * 1.2 else "Ranging"

    col1, col2, col3, col4 = st.columns([1, 1, 1, 1], gap="small")

    with col1:
        st.metric(
            label="Price Direction",
            value=direction,
            help=f"ICP is {'above' if direction == 'Bullish' else 'below' if direction == 'Bearish' else 'in line with'} its 3-month trend"
        )

    with col2:
        st.metric(
            label="Market Risk",
            value=risk_label,
            help=f"Current variability is ${vol_latest:.2f} vs. avg ${vol_avg:.2f}"
        )

    with col3:
        st.metric(
            label="Momentum",
            value=mom_label,
            help=f"Latest ICP: ${recent_icp:.2f} | Prior: ${prev_icp:.2f}"
        )

    with col4:
        st.metric(
            label="Cycle Phase",
            value=stability,
            help="Based on 3-month price variability relative to historical average"
        )

    # Operational takeaway strip
    st.info(f"**Operational read:** {InsightService.get_market_trend_insight(df)}")

else:
    st.warning("Insufficient historical data to compute market cycle signals.")

render_footer()
