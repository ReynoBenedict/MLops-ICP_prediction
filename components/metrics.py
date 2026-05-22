import streamlit as st


def render_hero_prediction(pred_val: float, **kwargs):
    st.metric(
        label="Forecast Price",
        value=f"${pred_val:.2f}",
        delta_color="off",
        help="Estimated ICP price (USD / Barrel)",
    )


def render_kpi_card(
    label: str,
    value: str,
    detail: str = "",
    delta: str | None = None,
    accent: str = "blue",
    **kwargs
):
    """Institutional KPI card rendered via safe HTML injection."""

    parts = []
    parts.append(f'<div class="analytics-card accent-{accent}">')
    parts.append(f'<div class="metric-label">{label}</div>')
    parts.append(f'<div class="metric-value">{value}</div>')

    if delta:
        color = "#10b981" if "+" in delta else "#ef4444"
        parts.append(
            f'<div style="font-size:0.85rem;color:{color};'
            f'font-weight:600;margin-top:0.4rem;">'
            f'{delta} vs Prev</div>'
        )

    if detail:
        parts.append(f'<div class="metric-subtitle">{detail}</div>')

    parts.append('</div>')

    st.markdown("".join(parts), unsafe_allow_html=True)


def render_analytics_card(
    label: str,
    value: str,
    subtitle: str = "",
    accent: str = "blue",
    **kwargs
):
    """Institutional analytics card rendered via safe HTML injection."""

    parts = []
    parts.append(f'<div class="analytics-card accent-{accent}">')
    parts.append(f'<div class="metric-label">{label}</div>')
    parts.append(f'<div class="metric-value">{value}</div>')

    if subtitle:
        parts.append(f'<div class="metric-subtitle">{subtitle}</div>')

    parts.append('</div>')

    st.markdown("".join(parts), unsafe_allow_html=True)


def render_narrative_card(
    title: str,
    content: str,
    accent: str = "blue",
):
    """Large narrative card for multi-paragraph analytical commentary."""

    paragraphs = content.split("\n\n")
    body_parts = []
    for p in paragraphs:
        text = p.strip()
        if text:
            body_parts.append(f'<p class="narrative-paragraph">{text}</p>')

    body_html = "".join(body_parts)

    html = (
        f'<div class="narrative-card accent-{accent}">'
        f'<div class="narrative-title">{title}</div>'
        f'<div class="narrative-body">{body_html}</div>'
        f'</div>'
    )

    st.markdown(html, unsafe_allow_html=True)


def render_confidence_card(
    value: str,
    label: str = "Confidence Range",
    **kwargs
):
    """Institutional range display."""

    display_value = value
    if " to " in value:
        low, high = value.split(" to ")
        display_value = f'{low}<span class="confidence-sep">to</span>{high}'
    elif " - " in value:
        low, high = value.split(" - ")
        display_value = f'{low}<span class="confidence-sep">-</span>{high}'

    html = (
        f'<div class="analytics-card accent-amber">'
        f'<div class="metric-label">{label}</div>'
        f'<div class="confidence-range">{display_value}</div>'
        f'<div class="metric-subtitle">Statistical model precision (RMSE)</div>'
        f'</div>'
    )

    st.markdown(html, unsafe_allow_html=True)