import numpy as np
import plotly.graph_objects as go
import streamlit as st

from components.layouts import render_footer
from components.styles import apply_custom_styles
from config.settings import PAGE_ICON
from services.insight_service import InsightService
from utils.data_loader import load_processed_data

# Page Configuration
st.set_page_config(
    page_title="Market Comparison | ICP Intelligence",
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded"
)

apply_custom_styles()

df = load_processed_data()

# ── Guard ─────────────────────────────────────────────────────────────────────
if df.empty:
    st.error("No data available. Check the data pipeline.")
    render_footer()
    st.stop()

# ── Core Analytics ────────────────────────────────────────────────────────────
corr = df["icp_price"].corr(df["wti_price"])

x = df["wti_price"].values
y = df["icp_price"].values

coeffs = np.polyfit(x, y, 1)

slope = coeffs[0]
intercept = coeffs[1]

trendline_x = np.linspace(x.min(), x.max(), 200)
trendline_y = slope * trendline_x + intercept

# ── Labels ────────────────────────────────────────────────────────────────────
if corr >= 0.9:
    corr_label = "Very Strong"
    corr_color = "#0a7c42"

elif corr >= 0.7:
    corr_label = "Strong"
    corr_color = "#0a7c42"

elif corr >= 0.5:
    corr_label = "Moderate"
    corr_color = "#b38b59"

else:
    corr_label = "Weak"
    corr_color = "#c0392b"

dependency_pct = min(round(abs(corr) * 100), 99)

reliability_lbl = "High" if corr >= 0.8 else "Moderate"

influence_lbl = f"{slope:.2f} ICP / 1 WTI"

corr_insight = InsightService.get_correlation_insight(df)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════════════════════
st.title("ICP vs WTI")

st.caption("Hubungan antara benchmark minyak global dan harga minyak mentah Indonesia.")

# ═══════════════════════════════════════════════════════════════════════════════
# HERO SECTION
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("### WTI Relationship Overview")

col1, col2, col3, col4 = st.columns(4, gap="medium")

with col1:
    st.metric(
        label="Correlation",
        value=corr_label,
        help=f"Pearson correlation: {corr:.4f}",
    )

with col2:
    st.metric(
        label="Market Dependency",
        value=f"~{dependency_pct}%",
        help="Estimated ICP dependency on WTI movement",
    )

with col3:
    st.metric(
        label="Forecast Reliability",
        value=reliability_lbl,
        help="WTI reliability as predictive indicator",
    )

with col4:
    st.metric(
        label="WTI Influence",
        value=influence_lbl,
        help="Historical sensitivity estimate",
    )

st.markdown("")

st.success(
    f"""
WTI masih menjadi driver utama pergerakan ICP.
Korelasi historis sebesar {corr:.2f} menunjukkan bahwa perubahan harga minyak global
masih sangat memengaruhi harga minyak domestik Indonesia.
"""
)

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN VISUAL
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("### Price Relationship Map")

st.caption("Semakin dekat titik observasi terhadap garis tren, semakin konsisten hubungan ICP dan WTI.")

# ── Era Colors ────────────────────────────────────────────────────────────────
era_colors = []

for _, row in df.iterrows():
    yr = int(row["year"]) if "year" in df.columns else 2022

    if yr <= 2020:
        era_colors.append("#9db8d3")

    elif yr <= 2022:
        era_colors.append("#002b5c")

    else:
        era_colors.append("#c7a96b")

# ── Hover Text ────────────────────────────────────────────────────────────────
hover_text = []

for _, row in df.iterrows():
    mo = int(row["month"]) if "month" in df.columns else 0
    yr = int(row["year"]) if "year" in df.columns else 0

    hover_text.append(f"<b>{yr}-{mo:02d}</b><br>WTI: ${row['wti_price']:.2f}<br>ICP: ${row['icp_price']:.2f}")

# ── Scatter Plot ──────────────────────────────────────────────────────────────
fig_scatter = go.Figure()

fig_scatter.add_trace(
    go.Scatter(
        x=df["wti_price"],
        y=df["icp_price"],
        mode="markers",
        name="Monthly Observation",
        marker=dict(
            color=era_colors,
            size=9,
            opacity=0.85,
            line=dict(
                width=1,
                color="white",
            ),
        ),
        text=hover_text,
        hovertemplate="%{text}<extra></extra>",
    )
)

# ── Trend Line ────────────────────────────────────────────────────────────────
fig_scatter.add_trace(
    go.Scatter(
        x=trendline_x,
        y=trendline_y,
        mode="lines",
        name=f"Trend Line ({slope:.2f})",
        line=dict(
            color="#c0392b",
            width=2.5,
            dash="dash",
        ),
        hoverinfo="skip",
    )
)

fig_scatter.update_layout(
    template="plotly_white",
    height=520,
    margin=dict(l=10, r=10, t=20, b=10),
    hovermode="closest",
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1,
    ),
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

st.plotly_chart(
    fig_scatter,
    width="stretch",
)

# ── Legend ────────────────────────────────────────────────────────────────────
legend_col1, legend_col2, legend_col3, legend_col4 = st.columns(4)

with legend_col1:
    st.caption("🔵 2019–2020")

with legend_col2:
    st.caption("🔷 2021–2022")

with legend_col3:
    st.caption("🟡 2023–Present")

with legend_col4:
    st.caption("🔴 Trend Line")

st.markdown("")

# ═══════════════════════════════════════════════════════════════════════════════
# INSIGHT SECTION
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("### Correlation Intelligence")

left_col, right_col = st.columns([1.2, 1], gap="large")

with left_col:
    st.markdown("#### Statistical Summary")

    st.write(corr_insight)

    st.markdown("")

    st.info(
        f"""
Market dependency terhadap WTI berada di sekitar {dependency_pct}%
dengan hubungan historis yang sangat konsisten.
Hal ini membuat WTI tetap menjadi indikator utama dalam proses forecasting ICP.
"""
    )

with right_col:
    st.markdown("#### Operational Interpretation")

    st.warning(
        """
Perubahan WTI biasanya lebih dulu terjadi sebelum penyesuaian ICP domestik.
Kondisi ini dapat digunakan sebagai early signal untuk pricing strategy,
kontrak energi, dan monitoring risiko pasar.
"""
    )

    st.markdown("")

    st.metric(
        label="Current Correlation Signal",
        value=f"{corr:.2f}",
        help="Pearson correlation between ICP and WTI",
    )
    st.caption("WTI and ICP remain tightly aligned.")

st.divider()

render_footer()

