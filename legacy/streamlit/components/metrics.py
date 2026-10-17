from html import escape

import streamlit as st


def _render(html: str):
    st.markdown(html, unsafe_allow_html=True)


def render_hero_prediction(pred_val: float, **kwargs):
    render_kpi_card("Forecast Price", f"${pred_val:.2f}", accent="blue")


def _delta_html(delta):
    if not delta:
        return ""
    color = "#16a34a" if str(delta).startswith("+") else "#ba1a1a"
    return f'<div class="metric-delta" style="color:{color};">{escape(str(delta))} vs Prev</div>'


def render_kpi_card(label, value, detail="", delta=None, accent="blue", **kwargs):
    html = (
        f'<div class="analytics-card accent-{escape(accent)}">'
        f'<div class="metric-label">{escape(str(label))}</div>'
        f'<div class="metric-value">{escape(str(value))}</div>'
        f'{_delta_html(delta)}'
        f'<div class="metric-subtitle">{escape(str(detail))}</div>'
        f'</div>'
    )
    _render(html)


def render_analytics_card(label, value, subtitle="", accent="blue", **kwargs):
    html = (
        f'<div class="analytics-card accent-{escape(accent)}">'
        f'<div class="metric-label">{escape(str(label))}</div>'
        f'<div class="analytics-main">{escape(str(value))}</div>'
        f'<div class="metric-subtitle">{escape(str(subtitle))}</div>'
        f'</div>'
    )
    _render(html)


def render_narrative_card(title, content, accent="blue"):
    paragraphs = [
        f'<p class="narrative-paragraph">{escape(p.strip())}</p>'
        for p in str(content).split("\n\n")
        if p.strip()
    ]

    html = (
        f'<div class="narrative-card accent-{escape(accent)}">'
        f'<div class="narrative-title">{escape(str(title))}</div>'
        f'<div class="narrative-body">{"".join(paragraphs)}</div>'
        f'</div>'
    )
    _render(html)


def render_confidence_card(value, label="Confidence Range", **kwargs):
    html = (
        '<div class="analytics-card accent-blue">'
        f'<div class="metric-label">{escape(str(label))}</div>'
        f'<div class="confidence-range">{escape(str(value))}</div>'
        '<div class="metric-subtitle">Rentang prediksi historis model</div>'
        '</div>'
    )
    _render(html)
