import logging

import streamlit as st

from components.layouts import render_footer
from components.metrics import (
    render_analytics_card,
    render_kpi_card,
    render_narrative_card,
)
from components.styles import apply_custom_styles
from config.settings import PAGE_ICON
from services.prediction_service import get_prediction_service
from utils.data_loader import load_processed_data

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Price Simulator | ICP Intelligence",
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)

logger = logging.getLogger("predict_price")

apply_custom_styles()

# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────
with st.spinner("Loading market baseline data..."):
    df = load_processed_data()

if not df.empty:
    _latest = df.iloc[-1]
    defaults = {
        "lag_1": float(_latest.get("icp_price", 70.0)),
        "wti":   float(_latest.get("wti_price", 70.0)),
        "rm3":   float(_latest.get("rolling_mean_3", 70.0)),
    }
else:
    defaults = {"lag_1": 70.0, "wti": 70.0, "rm3": 70.0}

service = get_prediction_service()


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _extract_impact(raw_value) -> float:
    """
    Safely extract a numeric impact from whatever structure the service returns.

    Handles:
      • float / int            → returned directly
      • dict with 'impact' key → dict['impact']
      • dict without 'impact'  → first numeric value found, else 0.0
      • anything else          → 0.0
    """
    if isinstance(raw_value, (int, float)):
        return float(raw_value)
    if isinstance(raw_value, dict):
        if "impact" in raw_value:
            try:
                return float(raw_value["impact"])
            except (TypeError, ValueError):
                return 0.0
        # Fallback: take the first numeric value in the dict
        for v in raw_value.values():
            if isinstance(v, (int, float)):
                return float(v)
    return 0.0


def _parse_sensitivity(raw: dict) -> list[tuple[str, float]]:
    """
    Convert raw sensitivity response (any schema) into a sorted list of
    (feature_name, impact_float) tuples, descending by abs(impact).
    Never raises.
    """
    if not raw or not isinstance(raw, dict):
        return []
    parsed = []
    for feat, val in raw.items():
        impact = _extract_impact(val)
        parsed.append((feat, impact))
    parsed.sort(key=lambda x: abs(x[1]), reverse=True)
    return parsed


def _sensitivity_narrative(features: list[tuple[str, float]]) -> str:
    """Build a human-readable narrative from a parsed sensitivity list."""
    n = len(features)
    if n == 0:
        return "No active feature sensitivities detected for this payload combination."
    if n == 1:
        f0, v0 = features[0]
        return (
            f"The simulated ICP pricing is primarily governed by "
            f"**{f0}** (impact per +10 USD: {v0:+.4f} USD/BBL)."
        )
    if n == 2:
        f0, v0 = features[0]
        f1, v1 = features[1]
        return (
            f"The simulated ICP pricing is most sensitive to **{f0}** "
            f"(impact: {v0:+.4f}) and **{f1}** (impact: {v1:+.4f})."
        )
    # 3+ features
    f0, v0 = features[0]
    f1, v1 = features[1]
    f2, v2 = features[2]
    return (
        "The simulated ICP pricing is most sensitive to changes in "
        f"**{f0}** (impact per +10 USD: {v0:+.4f} USD/BBL), followed by "
        f"**{f1}** ({v1:+.4f}) and **{f2}** ({v2:+.4f}). "
        "This distribution confirms that international benchmarks and historical "
        "price lag are the primary directional drivers."
    )


# ─────────────────────────────────────────────────────────────────────────────
# PAGE HEADER  (hero section — uses CSS injected by apply_custom_styles)
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(
    """
<div class="hero-section">
    <div class="hero-label">SCENARIO SIMULATOR</div>
    <div class="hero-title">ICP Price Simulator</div>
    <div class="hero-description">
        Interactive scenario simulator to evaluate how shifts in global benchmarks (WTI)
        and domestic crude price momentum affect next-period ICP settlement projections.
    </div>
</div>
""",
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────────────────────────────────────
# SCENARIO OVERVIEW
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("### Scenario Simulator Overview")
st.markdown(
    "Configure hypothetical market conditions to simulate their impact on the "
    "Indonesian Crude Price (ICP). This tool allows risk analysts to evaluate "
    "various stress-test scenarios, including oil price shocks and domestic pricing trends."
)
st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# SCENARIO CONFIGURATION PANEL
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("### Scenario Configuration Panel")

with st.form("scenario_form"):
    left_col, right_col = st.columns([1, 1], gap="large")

    with left_col:
        st.markdown("#### Domestic ICP Conditions")
        st.caption("Adjust historical settlement prices and rolling short-term trends.")

        lag1 = st.slider(
            "ICP Month -1 (USD/BBL)",
            40.0, 120.0, defaults["lag_1"],
            help="The latest actual or simulated ICP settlement price.",
        )
        lag3 = st.slider(
            "ICP Month -3 (USD/BBL)",
            40.0, 120.0, defaults["lag_1"],
            help="Historical ICP price from three periods ago.",
        )
        rm3 = st.slider(
            "3-Month Rolling ICP Mean (USD/BBL)",
            40.0, 120.0, defaults["rm3"],
            help="Short-term moving average representing price momentum.",
        )

    with right_col:
        st.markdown("#### Global Market Conditions (WTI)")
        st.caption("Adjust the global benchmark price and trend indicators.")

        wti = st.slider(
            "Target WTI Price (USD/BBL)",
            40.0, 120.0, defaults["wti"],
            help="Target benchmark price for the simulated period.",
        )
        wti1 = st.slider(
            "WTI Month -1 (USD/BBL)",
            40.0, 120.0, defaults["wti"],
            help="Benchmark WTI price from the previous period.",
        )
        wtirm3 = st.slider(
            "3-Month Rolling WTI Mean (USD/BBL)",
            40.0, 120.0, defaults["wti"],
            help="WTI short-term trend proxy representing international direction.",
        )

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    submitted = st.form_submit_button(
        "▶  Run Simulation",
        use_container_width=True,
    )

# ─────────────────────────────────────────────────────────────────────────────
# SIMULATION ENGINE + RESULTS
# ─────────────────────────────────────────────────────────────────────────────
if submitted:
    payload = {
        "lag_1":               lag1,
        "lag_3":               lag3,
        "lag_6":               lag1,     # mirror of lag_1, matching original logic
        "rolling_mean_3":      rm3,
        "wti_price":           wti,
        "wti_lag_1":           wti1,
        "wti_rolling_mean_3":  wtirm3,
    }

    # ── STEP 1: Prediction ────────────────────────────────────────────────────
    pred_val: float | None = None
    model_meta: dict = {}

    try:
        with st.spinner("Executing simulation engine..."):
            pred_val = service.predict(payload)
    except Exception as exc:
        logger.error(f"Prediction failed: {exc}")
        st.error(f"Simulation engine error: {exc}")
        st.info("Please verify MLflow Registry connectivity and try again.")
        render_footer()
        st.stop()

    try:
        with st.spinner("Retrieving model intelligence..."):
            model_meta = service.get_model_metadata()
    except Exception as exc:
        logger.warning(f"Model metadata unavailable: {exc}")
        model_meta = {"error": str(exc)}

    # ── STEP 2: Derived Metrics ───────────────────────────────────────────────
    baseline = defaults["lag_1"]

    if pred_val is None or baseline <= 0:
        st.error(
            "Model did not return a valid prediction. "
            "Please check MLflow status in the Dashboard."
        )
        render_footer()
        st.stop()

    diff       = pred_val - baseline
    pct_change = (diff / baseline) * 100
    direction  = "Bullish" if diff > 0.5 else "Bearish" if diff < -0.5 else "Neutral"

    direction_display = {
        "Bullish": "↑ Bullish",
        "Bearish": "↓ Bearish",
        "Neutral": "→ Neutral",
    }
    dir_label  = direction_display.get(direction, direction)
    dir_accent = "emerald" if direction == "Bullish" else "amber" if direction == "Bearish" else "slate"

    # ── STEP 3: Prediction Result KPI Dashboard ───────────────────────────────
    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
    st.markdown("### Prediction Result Dashboard")
    st.caption(
        "Core simulation output — the four signals below are the primary "
        "decision-making indicators for this scenario."
    )

    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

    with kpi_col1:
        render_kpi_card(
            "Predicted ICP",
            f"${pred_val:.2f}",
            delta=f"{pct_change:+.2f}%",
            detail="Simulated next-period ICP settlement",
            accent="blue",
        )
    with kpi_col2:
        render_analytics_card(
            "Current ICP",
            f"${baseline:.2f}",
            subtitle="Baseline ICP (lag_1 reference)",
            accent="slate",
        )
    with kpi_col3:
        render_kpi_card(
            "Expected Change",
            f"${diff:+.2f}",
            delta=f"{pct_change:+.2f}%",
            detail="Absolute shift from baseline",
            accent=dir_accent,
        )
    with kpi_col4:
        render_analytics_card(
            "Market Signal",
            dir_label,
            subtitle="Simulated directional bias",
            accent=dir_accent,
        )

    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

    # ── STEP 4: Executive Interpretation ─────────────────────────────────────
    st.markdown("### Executive Interpretation")

    int_col1, int_col2 = st.columns([1.15, 0.85], gap="large")

    with int_col1:
        narrative_body = (
            f"The simulated parameters project next-period ICP settlement pricing at "
            f"${pred_val:.2f}/bbl, marking a deviation of {pct_change:+.2f}% from the "
            f"current baseline settlement of ${baseline:.2f}/bbl.\n\n"
            f"Under this scenario, global benchmark movements (WTI at ${wti:.2f}/bbl) "
            f"and short-term domestic price momentum exert primary control over the price "
            f"direction. The model's directional signal is {direction} — "
            + (
                "indicating upward pressure from current price levels."
                if direction == "Bullish" else
                "indicating downward pressure from current price levels."
                if direction == "Bearish" else
                "indicating no significant directional bias from current price levels."
            )
        )
        render_narrative_card(
            "Scenario Intelligence Synthesis",
            narrative_body,
            accent="blue",
        )

    with int_col2:
        dynamics_body = (
            f"Simulated WTI Crude Price: ${wti:.2f}/bbl\n\n"
            f"Simulated Price Drift: ${diff:+.2f} USD\n\n"
            f"Market Directional Bias: {direction}\n\n"
            f"WTI Momentum Input (MA-3): ${wtirm3:.2f}/bbl"
        )
        render_narrative_card(
            "Market Dynamics Summary",
            dynamics_body,
            accent=dir_accent,
        )

    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

    # ── STEP 5: Model Intelligence ────────────────────────────────────────────
    st.markdown("### Model Intelligence")

    intel_left, intel_right = st.columns([1.15, 0.85], gap="large")

    with intel_left:
        try:
            raw_sensitivity = service.get_feature_sensitivity(payload)
        except Exception as exc:
            logger.warning(f"Sensitivity call failed: {exc}")
            raw_sensitivity = {}

        parsed_features = _parse_sensitivity(raw_sensitivity)
        narrative_sens  = _sensitivity_narrative(parsed_features)

        render_narrative_card(
            "Market Sensitivity Analysis",
            narrative_sens,
            accent="blue",
        )

    with intel_right:
        if model_meta and "error" not in model_meta:
            flavor_list = model_meta.get("flavor", [])
            flavor      = flavor_list[0] if flavor_list else "Unknown"
            version     = model_meta.get("version", "N/A")
            stage       = model_meta.get("stage", "N/A")
            features    = model_meta.get("features", [])
            feat_count  = len(features) if isinstance(features, list) else "N/A"
            metadata_body = (
                f"Registry Version: {version}\n\n"
                f"Deployment Stage: {stage}\n\n"
                f"Inference Engine: {flavor}\n\n"
                f"Feature Count: {feat_count}\n\n"
                f"Registry: MLflow Active"
            )
        else:
            error_msg = model_meta.get("error", "Metadata unavailable") if model_meta else "Metadata unavailable"
            metadata_body = (
                f"Model metadata could not be retrieved.\n\n"
                f"Reason: {error_msg}\n\n"
                "Prediction pipeline remains functional."
            )

        render_narrative_card(
            "Model Registry Metadata",
            metadata_body,
            accent="slate",
        )

    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

    # ── STEP 6: Operational Summary ───────────────────────────────────────────
    st.markdown("### Operational Summary")

    sum_col1, sum_col2, sum_col3 = st.columns(3)
    with sum_col1:
        render_analytics_card(
            "Directional Shift",
            dir_label,
            subtitle="Simulated market signal",
            accent=dir_accent,
        )
    with sum_col2:
        render_kpi_card(
            "Delta Estimate",
            f"{pct_change:+.2f}%",
            detail="Change vs. baseline ICP",
            accent="blue",
        )
    with sum_col3:
        render_analytics_card(
            "WTI Reference",
            f"${wti:.2f}",
            subtitle="Global benchmark scenario",
            accent="amber",
        )

# ─────────────────────────────────────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────────────────────────────────────
render_footer()
