import streamlit as st

from utils.data_loader import load_processed_data
from services.prediction_service import get_prediction_service
from components.layouts import render_footer
from components.styles import apply_custom_styles
from services.insight_service import InsightService

# ── Page Config ───────────────────────────────────────────────────────────────
apply_custom_styles()

# ── Data Loading ──────────────────────────────────────────────────────────────
df = load_processed_data()

if not df.empty:
    latest = df.iloc[-1]

    defaults = {
        "lag_1": float(latest.get("icp_price", 70.0)),
        "wti": float(latest.get("wti_price", 70.0)),
        "rm3": float(latest.get("rolling_mean_3", 70.0)),
    }

else:
    defaults = {
        "lag_1": 70.0,
        "wti": 70.0,
        "rm3": 70.0,
    }

service = get_prediction_service()

# ── Page Header ───────────────────────────────────────────────────────────────
st.title("Predict Price")
st.caption(
    "Simulasi berbagai kondisi pasar untuk melihat potensi perubahan harga ICP."
)

st.success(
    """
Model simulasi memungkinkan pengguna menguji dampak perubahan harga WTI, 
momentum historis ICP, dan kondisi pasar global terhadap estimasi harga ICP periode berikutnya.
"""
)

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Scenario Configuration
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("Scenario Configuration")

st.info(
    """
Sesuaikan parameter pasar di bawah untuk mensimulasikan berbagai kondisi energi. 
Perubahan nilai input akan memengaruhi hasil estimasi harga ICP secara langsung.
"""
)

with st.form("scenario_form"):

    left_col, right_col = st.columns([1, 1], gap="large")

    # ── ICP INPUTS ────────────────────────────────────────────────────────────
    with left_col:

        st.markdown("### Kondisi ICP Domestik")

        lag1 = st.slider(
            "ICP Bulan Sebelumnya (USD/BBL)",
            40.0,
            120.0,
            defaults["lag_1"],
            help="Harga ICP periode terakhir.",
        )

        lag3 = st.slider(
            "ICP 3 Bulan Sebelumnya (USD/BBL)",
            40.0,
            120.0,
            defaults["lag_1"],
            help="Digunakan untuk membaca pola historis menengah.",
        )

        rm3 = st.slider(
            "Rata-rata ICP 3 Bulan (USD/BBL)",
            40.0,
            120.0,
            defaults["rm3"],
            help="Merepresentasikan tren rata-rata ICP jangka pendek.",
        )

    # ── WTI INPUTS ────────────────────────────────────────────────────────────
    with right_col:

        st.markdown("### Kondisi Pasar Global (WTI)")

        wti = st.slider(
            "Target Harga WTI (USD/BBL)",
            40.0,
            120.0,
            defaults["wti"],
            help="Simulasikan kenaikan atau penurunan benchmark minyak global.",
        )

        wti1 = st.slider(
            "WTI Bulan Sebelumnya (USD/BBL)",
            40.0,
            120.0,
            defaults["wti"],
            help="Digunakan untuk membaca momentum harga minyak global.",
        )

        wtirm3 = st.slider(
            "Rata-rata WTI 3 Bulan (USD/BBL)",
            40.0,
            120.0,
            defaults["wti"],
            help="Menunjukkan kecenderungan tren WTI jangka pendek.",
        )

    st.markdown("---")

    submitted = st.form_submit_button(
        "Jalankan Simulasi Harga ICP",
        use_container_width=True,
    )

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Prediction Results
# ═══════════════════════════════════════════════════════════════════════════════
if submitted:

    payload = {
        "lag_1": lag1,
        "lag_3": lag3,
        "lag_6": lag1,
        "rolling_mean_3": rm3,
        "wti_price": wti,
        "wti_lag_1": wti1,
        "wti_rolling_mean_3": wtirm3,
    }

    try:

        pred_val = service.predict(payload)

        model_meta = service.get_model_metadata()

        baseline = defaults["lag_1"]

        diff = pred_val - baseline

        pct_change = (
            (diff / baseline) * 100
            if baseline != 0
            else 0
        )

        direction = (
            "Bullish"
            if diff > 0
            else "Bearish"
            if diff < 0
            else "Stabil"
        )

        # ───────────────────────────────────────────────────────────────────────
        st.divider()

        st.subheader("Simulation Result")

        top_col1, top_col2, top_col3, top_col4 = st.columns(4)

        with top_col1:
            st.metric(
                label="Predicted ICP",
                value=f"${pred_val:.2f}",
                delta=f"{pct_change:+.2f}%",
            )

        with top_col2:
            st.metric(
                label="Current ICP",
                value=f"${baseline:.2f}",
            )

        with top_col3:
            st.metric(
                label="Market Direction",
                value=direction,
            )

        with top_col4:
            st.metric(
                label="WTI Scenario",
                value=f"${wti:.2f}",
            )

        # ═══════════════════════════════════════════════════════════════════════
        # SECTION 3 — Executive Interpretation
        # ═══════════════════════════════════════════════════════════════════════
        st.divider()

        st.subheader("Scenario Interpretation")

        interp_left, interp_right = st.columns([2, 1], gap="large")

        with interp_left:

            st.markdown(
                f"""
Simulasi menunjukkan estimasi harga ICP berada di sekitar **${pred_val:.2f}** 
dengan perubahan sekitar **{pct_change:+.2f}%** dibanding kondisi saat ini.

Kondisi ini mengindikasikan bahwa perubahan harga WTI global masih memiliki 
pengaruh signifikan terhadap arah harga ICP domestik. Ketika benchmark minyak global 
mengalami kenaikan, model cenderung membaca adanya potensi penguatan harga ICP 
pada periode berikutnya.
"""
            )

            st.markdown("##### Faktor yang paling memengaruhi simulasi")

            st.markdown(
                """
- Pergerakan harga WTI global  
- Momentum historis ICP  
- Tren rata-rata harga beberapa periode terakhir  
- Stabilitas kondisi pasar energi internasional  
"""
            )

        with interp_right:

            st.info(
                """
Gunakan simulasi ini sebagai alat eksplorasi skenario pasar, 
bukan sebagai keputusan final investasi atau pricing strategy.
"""
            )

            st.success(
                f"""
Sinyal model saat ini menunjukkan kondisi pasar:
### {direction}
"""
            )

        # ═══════════════════════════════════════════════════════════════════════
        # SECTION 4 — Model Intelligence
        # ═══════════════════════════════════════════════════════════════════════
        st.divider()

        st.subheader("Model Intelligence")

        intel_left, intel_right = st.columns([1, 1], gap="large")

        with intel_left:

            st.markdown("##### Market Sensitivity")

            sensitivity = service.get_feature_sensitivity(payload)

            st.write(
                InsightService.get_simulator_insight(
                    sensitivity,
                    pred_val,
                    baseline,
                )
            )

            st.markdown("##### Forecast Reliability")

            st.write(
                """
Hasil simulasi tetap dipengaruhi kondisi pasar aktual, volatilitas energi global, 
dan perubahan geopolitik yang tidak selalu dapat diprediksi model sepenuhnya.
"""
            )

        with intel_right:

            st.markdown("##### Model Information")

            flavor = model_meta.get("flavor", ["ML"])[0]
            version = model_meta.get("version", "N/A")

            st.success(
                f"""
Model inference aktif dan tervalidasi.

Version:
{version}

Engine:
{flavor}
"""
            )

            st.warning(
                """
Perubahan ekstrem pada harga minyak global dapat menghasilkan 
deviasi prediksi yang lebih besar dibanding kondisi pasar normal.
"""
            )

        # ═══════════════════════════════════════════════════════════════════════
        # SECTION 5 — Operational Summary
        # ═══════════════════════════════════════════════════════════════════════
        st.divider()

        sum_col1, sum_col2, sum_col3 = st.columns(3)

        with sum_col1:

            st.success(
                f"""
### {direction}

Arah pasar hasil simulasi.
"""
            )

        with sum_col2:

            st.info(
                f"""
### {pct_change:+.2f}%

Estimasi perubahan terhadap ICP saat ini.
"""
            )

        with sum_col3:

            st.warning(
                f"""
### ${wti:.2f}

Benchmark WTI pada simulasi aktif.
"""
            )

    except Exception as e:

        st.error(f"Simulasi gagal dijalankan: {str(e)}")

# ── Footer ────────────────────────────────────────────────────────────────────
render_footer()