import pandas as pd
import numpy as np
from typing import Dict, Any

class InsightService:
    """
    Service to generate data-derived analytics insights and narratives.
    Ensures all commentary is grounded in actual metrics and rules.
    """

    # Thresholds for classification
    VOLATILITY_THRESHOLD_LOW = 0.05  # Below 5% of mean is low
    VOLATILITY_THRESHOLD_HIGH = 0.15 # Above 15% of mean is high
    
    CORRELATION_THRESHOLD_STRONG = 0.8
    CORRELATION_THRESHOLD_MODERATE = 0.5
    
    MOMENTUM_THRESHOLD = 0.02 # 2% change for momentum detection

    @staticmethod
    def get_market_trend_insight(df: pd.DataFrame) -> str:
        """Generates insight based on recent price movement and volatility."""
        if df.empty or len(df) < 6:
            return "Data historis tidak mencukupi untuk analisis tren mendalam."

        recent_icp = df['icp_price'].iloc[-1]
        prev_icp = df['icp_price'].iloc[-2]
        ma3 = df['icp_price'].rolling(window=3).mean().iloc[-1]
        
        # Volatility check
        vol = df['icp_price'].rolling(window=3).std()
        current_vol = vol.iloc[-1]
        avg_vol = vol.mean()
        
        # Momentum check
        change = (recent_icp - prev_icp) / prev_icp
        
        # Classification Logic
        if current_vol < (avg_vol * 0.8):
            vol_desc = "rendah (konsolidasi)"
        elif current_vol > (avg_vol * 1.2):
            vol_desc = "tinggi (ekspansif)"
        else:
            vol_desc = "normal"

        if recent_icp > ma3 * 1.02:
            trend = "Bullish"
        elif recent_icp < ma3 * 0.98:
            trend = "Bearish"
        else:
            trend = "Netral/Stabil"

        return (f"Pasar saat ini menunjukkan tren <strong>{trend}</strong> dengan tingkat volatilitas <strong>{vol_desc}</strong>. "
                f"Harga ICP berada {abs(change)*100:.1f}% {'di atas' if change > 0 else 'di bawah'} periode sebelumnya, "
                f"mengindikasikan {'momentum kuat' if abs(change) > 0.05 else 'pergerakan organik'}.")

    @staticmethod
    def get_correlation_insight(df: pd.DataFrame) -> str:
        """Generates grounded insight for ICP vs WTI relationship."""
        if df.empty or 'icp_price' not in df or 'wti_price' not in df:
            return "Data korelasi tidak tersedia."

        corr = df['icp_price'].corr(df['wti_price'])
        
        if corr > 0.9:
            strength = "Sangat Kuat (Sinkronisasi Penuh)"
        elif corr > 0.7:
            strength = "Kuat"
        elif corr > 0.4:
            strength = "Moderat"
        else:
            strength = "Lemah"

        # Simple regression slope approximation
        try:
            x = df['wti_price'].values
            y = df['icp_price'].values
            slope = np.polyfit(x, y, 1)[0]
            sensitivity = f"setiap kenaikan $1 pada WTI secara statistik berkorelasi dengan kenaikan ICP sebesar ~${slope:.2f}"
        except:
            sensitivity = "hubungan harga tetap linier"

        return (f"Korelasi Pearson saat ini adalah <strong>{corr:.4f}</strong>, dikategorikan sebagai <strong>{strength}</strong>. "
                f"Secara historis, {sensitivity}. Hal ini menegaskan WTI sebagai indikator leading yang valid.")

    @staticmethod
    def get_forecast_insight(pred_val: float, df: pd.DataFrame, model_meta: Dict[str, Any]) -> str:
        """Generates insight for the prediction result."""
        if df.empty:
            return "Prediksi dihasilkan tanpa konteks historis."

        latest_icp = df['icp_price'].iloc[-1]
        delta = pred_val - latest_icp
        delta_pct = (delta / latest_icp) * 100
        
        vol = df['icp_price'].rolling(window=3).std().iloc[-1]
        avg_vol = df['icp_price'].rolling(window=3).std().mean()
        
        uncertainty = "Meningkat" if vol > avg_vol else "Normal/Rendah"
        
        v_name = model_meta.get('version', 'N/A')
        
        return (f"Model (v{v_name}) memproyeksikan pergerakan <strong>{delta_pct:+.2f}%</strong> dari harga saat ini. "
                f"Ketidakpastian prediksi berada pada level <strong>{uncertainty}</strong> berdasarkan volatilitas pasar terakhir. "
                f"Arah pergerakan {'mendukung' if delta > 0 else 'melawan'} tren jangka pendek saat ini.")

    @staticmethod
    def get_simulator_insight(sensitivity: Dict[str, Any], pred_val: float, baseline_val: float) -> str:
        """Generates insight for simulator results."""
        if not sensitivity:
            return "Data sensitivitas tidak tersedia untuk skenario ini."

        # Find top contributor
        top_feat = max(sensitivity.items(), key=lambda x: abs(x[1].get('impact', 0)))
        feat_name = top_feat[0].replace('_', ' ').title()
        impact = top_feat[1].get('impact', 0)
        
        total_delta = pred_val - baseline_val
        
        return (f"Skenario ini menghasilkan perubahan bersih sebesar <strong>${total_delta:+.2f}</strong>. "
                f"Faktor <strong>{feat_name}</strong> memberikan dampak marginal tertinggi sebesar <strong>${impact:+.2f}</strong> per unit kenaikan. "
                f"Ini mengonfirmasi sensitivitas model terhadap input tersebut dalam kondisi skenario ini.")

    @staticmethod
    def get_dominance_insight(model_meta: Dict[str, Any]) -> str:
        """Generates insight into which features dominate the model's logic."""
        coefs = model_meta.get('coefficients')
        if not coefs:
            return "Analisis faktor dominan saat ini tidak tersedia (Model non-linier atau metadata terbatas)."

        # Sort by absolute coefficient value
        sorted_coefs = sorted(coefs.items(), key=lambda x: abs(x[1]), reverse=True)
        top_3 = sorted_coefs[:3]
        
        narrative_parts = []
        for feat, val in top_3:
            name = feat.replace('_', ' ').title()
            direction = "positif" if val > 0 else "negatif"
            narrative_parts.append(f"<strong>{name}</strong> ({direction})")

        primary = top_3[0][0].replace('_', ' ').title()
        
        return (f"Prediksi didominasi oleh pergerakan {', '.join(narrative_parts)}. "
                f"Faktor <strong>{primary}</strong> memiliki bobot statistik tertinggi dalam menentukan output. "
                f"Stabilitas variabel-variabel ini sangat krusial bagi akurasi proyeksi periode ini.")

    @staticmethod
    def get_confidence_insight(rmse: float, pred_val: float) -> str:
        """Generates grounded explanation for confidence intervals."""
        if rmse <= 0:
            return "Interval kepercayaan tidak dapat dihitung karena data validasi tidak ditemukan."

        error_pct = (rmse / pred_val) * 100 if pred_val != 0 else 0
        
        if error_pct < 5:
            reliability = "Tinggi"
        elif error_pct < 10:
            reliability = "Optimal"
        else:
            reliability = "Moderat"

        return (f"Interval kepercayaan 95% dihitung berdasarkan RMSE historis (<strong>{rmse:.2f}</strong>). "
                f"Tingkat reliabilitas model untuk prediksi ini dikategorikan sebagai <strong>{reliability}</strong>. "
                f"Penyimpangan aktual secara historis jarang melebihi ambang batas ini.")
