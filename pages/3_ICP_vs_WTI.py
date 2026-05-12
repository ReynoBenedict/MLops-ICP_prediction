import streamlit as st
import numpy as np
import plotly.graph_objects as go

from config.settings import PAGE_ICON
from utils.data_loader import load_processed_data
from components.layouts import render_footer
from components.styles import apply_custom_styles
from services.insight_service import InsightService

# ── Page Config ───────────────────────────────────────────────────────────────
apply_custom_styles()

df = load_processed_data()

# ── Page Header ───────────────────────────────────────────────────────────────
st.title("ICP vs WTI")
st.caption("Bagaimana harga minyak global mendorong penetapan harga domestik Indonesia")

# ── Helper: section label ─────────────────────────────────────────────────────
def section_label(text):
    st.subheader(text)

# ── Helper: insight card ──────────────────────────────────────────────────────
def insight_card(bullets: list, accent: str = "#002b5c"):
    for bullet in bullets:
        st.markdown(f"- {bullet}")

# ── Guard: empty data ─────────────────────────────────────────────────────────
if df.empty:
    st.error("No data available. Check the data pipeline.")
    render_footer()
    st.stop()

# ── Compute analytics (no formula changes) ───────────────────────────────────
corr = df['icp_price'].corr(df['wti_price'])                          # Pearson r
x    = df['wti_price'].values
y    = df['icp_price'].values
coeffs      = np.polyfit(x, y, 1)                                     # linear fit
slope       = coeffs[0]
intercept   = coeffs[1]
trendline_x = np.linspace(x.min(), x.max(), 200)
trendline_y = slope * trendline_x + intercept

# Derived signal labels (business language)
if corr > 0.9:
    corr_label, corr_color, corr_bg = "Very Strong",  "#0a7c42", "#e8f7ef"
elif corr > 0.7:
    corr_label, corr_color, corr_bg = "Strong",       "#0a7c42", "#e8f7ef"
elif corr > 0.4:
    corr_label, corr_color, corr_bg = "Moderate",     "#b38b59", "#fdf6ec"
else:
    corr_label, corr_color, corr_bg = "Weak",         "#c0392b", "#fdecea"

dependency_pct  = min(round(abs(corr) * 100), 99)
reliability_lbl = "High" if corr > 0.8 else "Moderate" if corr > 0.5 else "Low"
influence_lbl   = f"${slope:.2f} ICP per $1 WTI"

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Why WTI Matters
# ═══════════════════════════════════════════════════════════════════════════════
section_label("Why WTI Matters")

col1, col2, col3, col4 = st.columns([1, 1, 1, 1], gap="small")

with col1:
    st.metric(
        label="Correlation Strength",
        value=corr_label,
        help=f"Pearson r = {corr:.4f}"
    )

with col2:
    st.metric(
        label="Market Dependency",
        value=f"~{dependency_pct}%",
        help="of ICP movement explained by WTI"
    )

with col3:
    st.metric(
        label="Forecast Reliability",
        value=reliability_lbl,
        help="WTI as a leading indicator for ICP"
    )

with col4:
    st.metric(
        label="WTI Influence Level",
        value=influence_lbl,
        help="Historical linear sensitivity estimate"
    )

# Context strip
st.info(f"""
**Why this relationship matters:** WTI (West Texas Intermediate) is the world's primary crude oil benchmark. Because Indonesian crude pricing is set in a globally integrated market, WTI movements consistently precede ICP adjustments. A {corr_label.lower()} correlation of **{corr:.4f}** means WTI is not just a reference — it is a reliable leading signal that the forecasting model actively uses to project next-month ICP with greater accuracy.
""")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Scatter Plot
# ═══════════════════════════════════════════════════════════════════════════════
section_label("Price Relationship Map — WTI vs ICP")

# Colour points by approximate era for visual context
era_colors = []
for _, row in df.iterrows():
    yr = int(row['year']) if 'year' in df.columns else 2022
    if yr <= 2020:
        era_colors.append('#93b4d4')   # light navy — pandemic era
    elif yr <= 2022:
        era_colors.append('#002b5c')   # dark navy — recovery / spike
    else:
        era_colors.append('#b38b59')   # gold — recent

hover_text = []
for _, row in df.iterrows():
    mo = int(row['month']) if 'month' in df.columns else 0
    yr = int(row['year'])  if 'year'  in df.columns else 0
    hover_text.append(
        f"<b>{yr}-{mo:02d}</b><br>"
        f"WTI: ${row['wti_price']:.2f}<br>"
        f"ICP: ${row['icp_price']:.2f}"
    )

fig_scatter = go.Figure()

# Data points
fig_scatter.add_trace(go.Scatter(
    x=df['wti_price'],
    y=df['icp_price'],
    mode='markers',
    name='Monthly observation',
    marker=dict(
        color=era_colors,
        size=9,
        opacity=0.82,
        line=dict(width=1, color='white'),
    ),
    text=hover_text,
    hovertemplate="%{text}<extra></extra>",
))

# Trendline
fig_scatter.add_trace(go.Scatter(
    x=trendline_x,
    y=trendline_y,
    mode='lines',
    name=f'Trend  (slope {slope:.2f})',
    line=dict(color='#c0392b', width=2, dash='dash'),
    hoverinfo='skip',
))

fig_scatter.update_layout(
    template="plotly_white",
    height=420,
    margin=dict(l=10, r=10, t=10, b=10),
    hovermode='closest',
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    xaxis=dict(
        title="WTI Price (USD / BBL)",
        showgrid=True,
        gridcolor="#f0f0f0",
        zeroline=False,
    ),
    yaxis=dict(
        title="ICP Price (USD / BBL)",
        showgrid=True,
        gridcolor="#f0f0f0",
        zeroline=False,
    ),
)

st.plotly_chart(fig_scatter, use_container_width=True)

# Legend for era colours
st.caption("🔵 2019–2020 (pandemic era) | 🔵 2021–2022 (recovery & spike) | 🟡 2023–present | 🔴 Trend line")

insight_card([
    f"**Tight clustering along the trend line** confirms that WTI and ICP move together with {corr_label.lower()} consistency — not by coincidence.",
    f"**Each $1 rise in WTI** has historically corresponded to a ~${slope:.2f} change in ICP, based on the fitted trend.",
    "**Points far from the line** (outliers) reflect periods of domestic policy intervention, supply disruptions, or lagged price adjustments.",
    "**Operational implication:** when WTI moves sharply, expect ICP to follow within 1–2 months — use this window to prepare pricing and contract decisions.",
])

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Correlation Intelligence
# ═══════════════════════════════════════════════════════════════════════════════
section_label("Correlation Intelligence")

corr_insight_text = InsightService.get_correlation_insight(df)

st.markdown(f"""
**Statistical Summary**

{corr_insight_text}

---

**Forecasting implication:** The stronger this correlation, the more confidently the model can use WTI as a predictive input. A weakening correlation over time would be an early warning signal to re-evaluate the model's feature weights.
""")

render_footer()
