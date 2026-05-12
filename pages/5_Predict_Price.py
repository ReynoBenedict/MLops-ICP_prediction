import streamlit as st

from config.settings import PAGE_ICON
from utils.data_loader import load_processed_data
from services.prediction_service import get_prediction_service
from components.layouts import render_footer
from components.styles import apply_custom_styles
from services.insight_service import InsightService

apply_custom_styles()

df = load_processed_data()
if not df.empty:
    latest = df.iloc[-1]
    defaults = {
        "lag_1": float(latest.get('icp_price', 70.0)),
        "wti": float(latest.get('wti_price', 70.0)),
        "rm3": float(latest.get('rolling_mean_3', 70.0))
    }
else:
    defaults = {"lag_1": 70.0, "wti": 70.0, "rm3": 70.0}

# PAGE CONTENT: SCENARIO SIMULATOR
st.title("Scenario Simulator")
st.caption("Uji ketahanan prediksi terhadap berbagai skenario pasar")

st.info("💡 **Gunakan panel di bawah** untuk menyesuaikan variabel pasar. Model akan menghitung dampak instan terhadap harga ICP.")

with st.form("scenario_form"):
    col_icp, col_wti = st.columns([1, 1], gap="small")
    
    with col_icp:
        st.markdown("### 📉 Kondisi Domestik (ICP)")
        lag1 = st.slider("ICP Bulan Sebelumnya ($)", 40.0, 120.0, defaults['lag_1'], help="Benchmark harga ICP terakhir.")
        lag3 = st.slider("ICP 3 Bulan Lalu ($)", 40.0, 120.0, defaults['lag_1'])
        rm3  = st.slider("ICP Rolling Mean 3 bln ($)", 40.0, 120.0, defaults['rm3'])
        
    with col_wti:
        st.markdown("### 🌎 Kondisi Global (WTI)")
        wti  = st.slider("Target Harga WTI ($)", 40.0, 120.0, defaults['wti'], help="Simulasikan kenaikan/penurunan harga WTI.")
        wti1 = st.slider("WTI Bulan Sebelumnya ($)", 40.0, 120.0, defaults['wti'])
        wtirm3 = st.slider("WTI Rolling Mean 3 bln ($)", 40.0, 120.0, defaults['wti'])
    
    st.markdown("---")
    submitted = st.form_submit_button("🚀 Jalankan Analisis Skenario", use_container_width=True)

if submitted:
    payload = {
        "lag_1": lag1, "lag_3": lag3, "lag_6": lag1, "rolling_mean_3": rm3,
        "wti_price": wti, "wti_lag_1": wti1, "wti_rolling_mean_3": wtirm3
    }
    service = get_prediction_service()
    model_meta = service.get_model_metadata()
    try:
        pred_val = service.predict(payload)
        
        st.markdown("---")
        st.markdown("### Hasil Analisis Dampak")
        
        res_col1, res_col2 = st.columns([1, 2], gap="medium")
        with res_col1:
            st.metric(label="PROYEKSI", value=f"${pred_val:.2f}", help="USD / BBL")
            
        with res_col2:
            diff = pred_val - defaults['lag_1']
            st.metric("Delta vs Baseline", f"{diff:+.2f} USD", delta_color="normal")
            sensitivity = service.get_feature_sensitivity(payload)
            st.info(InsightService.get_simulator_insight(sensitivity, pred_val, defaults['lag_1']))
            st.success(f"✅ Validated ({model_meta.get('flavor', ['ML'])[0]} v{model_meta.get('version', 'N/A')})")
            
    except Exception as e:
        st.error(f"Simulasi gagal: {str(e)}")

render_footer()
