from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd


class InsightService:
    """
    Market intelligence engine for ICP executive dashboard
    and forecasting analytics.
    """

    # ===========================================================
    # DASHBOARD NARRATIVES (Indonesian)
    # ===========================================================

    @staticmethod
    def generate_intelligence_summary(
        df: pd.DataFrame,
        pred_val: float | None,
        corr_val: float,
    ) -> str:
        """Main narrative for Market Intelligence Summary card."""
        if df.empty or len(df) < 6:
            return "Data pasar belum mencukupi untuk analisis komprehensif."

        recent_icp = df["icp_price"].iloc[-1]
        prev_icp = df["icp_price"].iloc[-2]
        latest_wti = df["wti_price"].iloc[-1]
        ma3 = df["icp_price"].rolling(3).mean().iloc[-1]
        std_recent = df["icp_price"].tail(6).std()
        avg_price = df["icp_price"].mean()

        # trend direction
        if recent_icp > prev_icp * 1.005:
            trend_phrase = "masih mengalami penguatan yang cukup stabil"
        elif recent_icp < prev_icp * 0.995:
            trend_phrase = "mengalami tekanan pelemahan pada siklus terakhir"
        else:
            trend_phrase = "bergerak relatif datar tanpa perubahan signifikan"

        # momentum
        if recent_icp > ma3 * 1.01:
            momentum = (
                "Momentum bullish masih terlihat cukup kuat, "
                "didukung oleh posisi harga di atas rata-rata pergerakan jangka pendek"
            )
        elif recent_icp < ma3 * 0.99:
            momentum = (
                "Momentum bullish mulai melambat dalam beberapa settlement terakhir, "
                "dengan harga bergerak di bawah rata-rata jangka pendek"
            )
        else:
            momentum = (
                "Momentum harga cenderung netral, "
                "tanpa sinyal kuat ke arah bullish maupun bearish"
            )

        # volatility
        if std_recent > 5:
            vol_desc = "volatilitas yang cukup tinggi"
            vol_impl = "pelaku pasar perlu mempertimbangkan risiko fluktuasi yang lebih besar"
        elif std_recent > 2:
            vol_desc = "volatilitas pada level moderat"
            vol_impl = "harga masih dalam rentang yang wajar"
        else:
            vol_desc = "volatilitas yang relatif rendah"
            vol_impl = "harga cenderung stabil tanpa gejolak signifikan"

        pos = "di atas" if recent_icp > avg_price else "di bawah"

        para1 = (
            f"ICP {trend_phrase} sejalan dengan dinamika benchmark "
            f"WTI yang berada pada level ${latest_wti:.2f}/bbl. Korelasi antar "
            f"kedua komoditas tetap tinggi pada level {corr_val:.2f}, menandakan "
            f"sensitivitas ICP terhadap harga minyak global masih sangat dominan."
        )

        para2 = (
            f"{momentum}. Posisi harga saat ini berada "
            f"{pos} area rata-rata historis (${avg_price:.2f}), "
            f"dengan {vol_desc} yang mengindikasikan {vol_impl}."
        )

        return f"{para1}\n\n{para2}"

    @staticmethod
    def generate_correlation_narrative(corr_val: float) -> str:
        """Explain WHAT correlation means in business terms."""
        if corr_val > 0.95:
            return (
                f"Nilai korelasi {corr_val:.2f} berarti ICP "
                f"hampir sepenuhnya searah dengan benchmark WTI global. Setiap "
                f"kenaikan atau penurunan signifikan pada WTI akan tercermin "
                f"langsung pada dinamika harga ICP."
            )
        elif corr_val > 0.8:
            return (
                f"Korelasi pada level {corr_val:.2f} mengonfirmasi hubungan "
                f"yang kuat antara ICP dan WTI, meskipun terdapat faktor lokal "
                f"yang sesekali menyebabkan deviasi dari pola global."
            )
        else:
            return (
                f"Korelasi {corr_val:.2f} mengonfirmasi adanya divergensi yang "
                f"cukup signifikan antara ICP dan WTI. Faktor domestik "
                f"kemungkinan memberikan pengaruh lebih besar terhadap "
                f"penetapan harga."
            )

    @staticmethod
    def generate_condition_narrative(
        df: pd.DataFrame, rmse: float
    ) -> Tuple[str, str]:
        """Returns (label, narrative) for current market condition."""
        if df.empty or len(df) < 6:
            return "Stable", "Data belum mencukupi untuk penilaian kondisi pasar."

        std_recent = df["icp_price"].tail(6).std()
        recent_icp = df["icp_price"].iloc[-1]
        ma3 = df["icp_price"].rolling(3).mean().iloc[-1]

        if std_recent > 5 or rmse > 4.5:
            label = "Volatile"
            narrative = (
                f"Pasar sedang mengalami volatilitas tinggi dengan deviasi "
                f"{std_recent:.2f} dalam 6 periode terakhir. Kondisi ini "
                f"menandakan ketidakpastian harga yang lebih besar dan "
                f"potensi swing tajam dalam jangka pendek."
            )
        elif std_recent < 2 and abs(recent_icp - ma3) / ma3 < 0.01:
            label = "Consolidation"
            narrative = (
                f"Pasar sedang memasuki fase konsolidasi dengan rentang "
                f"harga yang sangat terbatas. Volatilitas rendah pada level "
                f"{std_recent:.2f} menandakan pelaku pasar menunggu katalis "
                f"baru sebelum mengambil posisi lebih agresif."
            )
        else:
            label = "Stable"
            narrative = (
                f"Kondisi pasar relatif stabil dengan volatilitas pada level "
                f"moderat ({std_recent:.2f}). Harga bergerak dalam koridor "
                f"yang wajar tanpa tekanan signifikan baik dari sisi "
                f"demand maupun supply global."
            )

        return label, narrative

    @staticmethod
    def generate_positioning_narrative(
        df: pd.DataFrame,
        pred_val: float | None,
        trend_label: str,
        corr_val: float,
    ) -> str:
        """Large analyst commentary for Market Positioning."""
        if df.empty or len(df) < 6 or pred_val is None:
            return "Data belum mencukupi untuk analisis positioning pasar."

        recent_icp = df["icp_price"].iloc[-1]
        latest_wti = df["wti_price"].iloc[-1]
        delta_pct = ((pred_val / recent_icp) - 1) * 100
        ma3 = df["icp_price"].rolling(3).mean().iloc[-1]
        ma6 = df["icp_price"].rolling(6).mean().iloc[-1]

        if trend_label == "Bullish":
            p1 = (
                f"Secara keseluruhan, pasar ICP berada dalam posisi bullish "
                f"dengan proyeksi harga mengarah ke ${pred_val:.2f}/bbl, "
                f"naik sekitar {abs(delta_pct):.1f}% dari level settlement "
                f"terakhir. Penguatan ini sejalan dengan tren positif yang "
                f"terjadi pada pasar minyak mentah global."
            )
        elif trend_label == "Bearish":
            p1 = (
                f"Posisi pasar cenderung bearish dengan proyeksi harga "
                f"menuju ${pred_val:.2f}/bbl, turun sekitar "
                f"{abs(delta_pct):.1f}% dari level settlement terakhir. "
                f"Tekanan turun ini sejalan dengan koreksi "
                f"yang terjadi pada benchmark global."
            )
        else:
            p1 = (
                f"Pasar ICP berada dalam fase netral dengan proyeksi harga "
                f"di kisaran ${pred_val:.2f}/bbl, relatif stabil dibandingkan "
                f"level settlement terakhir. Belum ada katalis kuat yang "
                f"mendorong arah tertentu secara signifikan."
            )

        if recent_icp > ma3 and ma3 > ma6:
            structure = (
                "Dari sisi teknikal, rata-rata jangka pendek masih berada "
                "di atas jangka menengah, menandakan tren naik masih terjaga."
            )
        elif recent_icp < ma3 and ma3 < ma6:
            structure = (
                "Secara teknikal, rata-rata jangka pendek sudah berada "
                "di bawah jangka menengah, sinyal bahwa tekanan jual "
                "masih mendominasi."
            )
        else:
            structure = (
                "Secara teknikal, pola harga masih campuran tanpa konfirmasi "
                "tren yang jelas. Pasar masih mencari arah yang lebih definitif."
            )

        p3 = (
            f"Dengan korelasi ICP-WTI pada level {corr_val:.2f}, harga minyak mentah "
            f"global tetap menjadi faktor penentu utama. Level WTI di ${latest_wti:.2f}/bbl "
            f"memberikan referensi penting bagi penetapan ICP periode berikutnya. "
            f"Pelaku pasar perlu memperhatikan dinamika supply-demand global serta "
            f"sentimen geopolitik yang dapat mempengaruhi arah harga jangka pendek."
        )

        return f"{p1}\n\n{structure}\n\n{p3}"

    @staticmethod
    def generate_bias_subtitle(
        pred_val: float | None, latest_icp: float
    ) -> str:
        """Short Indonesian subtitle for Market Bias KPI card."""
        if pred_val is None or latest_icp <= 0:
            return "Estimasi arah pasar tidak tersedia saat ini."

        delta_pct = ((pred_val / latest_icp) - 1) * 100

        if delta_pct > 1:
            return (
                "Sentimen pasar condong bullish, didukung oleh penguatan "
                "benchmark dan pola momentum terkini."
            )
        elif delta_pct < -1:
            return (
                "Sentimen pasar mengonfirmasi tekanan bearish jangka "
                "pendek sejalan dengan koreksi harga global."
            )
        else:
            return (
                "Pasar dalam kondisi netral tanpa bias arah yang "
                "signifikan dalam jangka pendek."
            )

    # ===========================================================
    # FORECASTING PAGE NARRATIVES
    # ===========================================================

    @staticmethod
    def generate_forecast_interpretation(
        df: pd.DataFrame,
        pred_val: float | None,
        rmse: float,
    ) -> str:
        """Forecast interpretation narrative (Indonesian)."""
        if df.empty or pred_val is None:
            return "Interpretasi forecast tidak tersedia karena data prediksi belum tersedia."

        recent_icp = df["icp_price"].iloc[-1]
        delta = pred_val - recent_icp
        delta_pct = (delta / recent_icp) * 100
        conf_low = max(0, pred_val - rmse)
        conf_high = pred_val + rmse

        if delta_pct > 1:
            direction_text = (
                f"Model memproyeksikan kenaikan harga ICP sebesar "
                f"{abs(delta_pct):.1f}% ke level ${pred_val:.2f}/bbl."
            )
        elif delta_pct < -1:
            direction_text = (
                f"Model memproyeksikan penurunan harga ICP sebesar "
                f"{abs(delta_pct):.1f}% ke level ${pred_val:.2f}/bbl."
            )
        else:
            direction_text = (
                f"Model memproyeksikan harga ICP relatif stabil "
                f"di kisaran ${pred_val:.2f}/bbl."
            )

        confidence_text = (
            f"Berdasarkan akurasi historis model (RMSE: {rmse:.2f}), "
            f"harga diperkirakan berada dalam rentang "
            f"${conf_low:.2f} hingga ${conf_high:.2f}. "
            f"Rentang ini mencerminkan tingkat ketidakpastian "
            f"yang wajar untuk proyeksi satu periode ke depan."
        )

        ma3 = df["icp_price"].rolling(3).mean().iloc[-1]
        if pred_val > ma3:
            trend_context = (
                "Proyeksi ini berada di atas rata-rata 3 periode terakhir, "
                "mengonfirmasi kelanjutan tren positif jangka pendek."
            )
        elif pred_val < ma3:
            trend_context = (
                "Proyeksi ini berada di bawah rata-rata 3 periode terakhir, "
                "mengonfirmasi adanya tekanan koreksi pada harga."
            )
        else:
            trend_context = (
                "Proyeksi ini sejalan dengan rata-rata 3 periode terakhir, "
                "mengonfirmasi tidak adanya perubahan tren yang signifikan."
            )

        return f"{direction_text}\n\n{confidence_text}\n\n{trend_context}"

    @staticmethod
    def generate_directional_outlook(
        df: pd.DataFrame,
        pred_val: float | None,
        trend_label: str,
    ) -> str:
        """Forward-looking directional analysis (Indonesian)."""
        if df.empty or pred_val is None:
            return "Outlook tidak tersedia."

        _recent_icp = df["icp_price"].iloc[-1]
        latest_wti = df["wti_price"].iloc[-1]
        _ma3 = df["icp_price"].rolling(3).mean().iloc[-1]
        _ma6 = df["icp_price"].rolling(6).mean().iloc[-1]
        std_recent = df["icp_price"].tail(6).std()

        if trend_label == "Bullish":
            outlook = (
                f"Outlook jangka pendek cenderung positif. Harga ICP "
                f"berpotensi melanjutkan penguatan ke area ${pred_val:.2f}/bbl "
                f"apabila benchmark WTI tetap bertahan di atas ${latest_wti * 0.98:.2f}."
            )
        elif trend_label == "Bearish":
            outlook = (
                f"Outlook jangka pendek cenderung negatif. Harga ICP "
                f"berpotensi terkoreksi ke area ${pred_val:.2f}/bbl, terutama "
                f"apabila WTI gagal bertahan di atas level ${latest_wti * 0.95:.2f}."
            )
        else:
            outlook = (
                f"Outlook jangka pendek cenderung netral. Harga ICP "
                f"diperkirakan bergerak di sekitar level ${pred_val:.2f}/bbl "
                f"dengan rentang terbatas."
            )

        risk = ""
        if std_recent > 4:
            risk = (
                "Volatilitas yang tinggi saat ini menjadi faktor risiko utama "
                "yang perlu diperhatikan dalam pengambilan keputusan."
            )
        else:
            risk = (
                "Volatilitas yang terkendali memberikan tingkat kepercayaan "
                "yang cukup baik terhadap proyeksi ini."
            )

        return f"{outlook}\n\n{risk}"

    @staticmethod
    def generate_scenario_cards(
        pred_val: float, rmse: float
    ) -> dict:
        """Returns scenario data for optimistic/base/pessimistic cards."""
        return {
            "optimistic": {
                "price": pred_val + rmse,
                "label": "Optimistic",
                "desc": (
                    "Skenario terbaik jika WTI mengalami penguatan lebih lanjut "
                    "dan sentimen pasar global membaik."
                ),
            },
            "base": {
                "price": pred_val,
                "label": "Base Case",
                "desc": (
                    "Skenario paling mungkin berdasarkan pola historis "
                    "dan kondisi pasar saat ini."
                ),
            },
            "pessimistic": {
                "price": max(0, pred_val - rmse),
                "label": "Pessimistic",
                "desc": (
                    "Skenario terburuk jika WTI terkoreksi tajam "
                    "atau terjadi gangguan supply global."
                ),
            },
        }

    # ===========================================================
    # EXISTING METHODS (preserved for other pages)
    # ===========================================================

    @staticmethod
    def get_market_trend_insight(df: pd.DataFrame) -> str:
        """Larger institutional narrative block for Market Dynamics."""

        if df.empty or len(df) < 6:
            return "Sample size insufficient for structural market validation."

        recent_icp = df["icp_price"].iloc[-1]
        _prev_icp = df["icp_price"].iloc[-2]
        ma3 = df["icp_price"].rolling(3).mean().iloc[-1]
        corr = df["icp_price"].corr(df["wti_price"])

        if recent_icp > ma3 * 1.01:
            momentum = "showing strengthening bullish momentum after the latest settlement cycle"
        elif recent_icp < ma3 * 0.99:
            momentum = "indicating weakening bullish momentum following recent price retracement"
        else:
            momentum = "moving within a neutral consolidation range"

        avg_price = df["icp_price"].mean()
        positioning = "above" if recent_icp > avg_price else "below"

        return (
            f"ICP prices continue to show elevated directional sensitivity toward WTI,  "
            f"with correlation remaining historically strong at {corr:.2f}. "
            f"\n\nRecent market is {momentum}, although overall price positioning "
            f"still remains {positioning} historical average support levels. "
            f"\n\nVolatility conditions are currently stable, suggesting lower short-term shock risk "
            f"despite ongoing directional pressure in global crude benchmarks."
        )

    @staticmethod
    def get_executive_summary(df: pd.DataFrame, pred_val: float) -> str:
        """Concise market narrative centerpiece above the chart."""
        if df.empty or pred_val is None:
            return "Market data integration in progress."

        recent_icp = df["icp_price"].iloc[-1]
        delta = pred_val - recent_icp

        if delta > 1.5:
            trend_desc = "recovery momentum after recent downside consolidation"
        elif delta < -1.5:
            trend_desc = "downward pressure following global benchmark shifts"
        else:
            trend_desc = "stable lateral action within current price channels"

        target_note = (
            f"possible re-entry into the ${pred_val:.0f} range"
            if abs(pred_val - 100) < 10
            else f"stabilization near ${pred_val:.1f}"
        )

        return (
            f"ICP trend: {trend_desc}, with latest forecast pointing toward {target_note}."
        )

    @staticmethod
    def get_correlation_insight(df: pd.DataFrame) -> str:
        """Simplified relationship insight."""
        if df.empty:
            return "Cross-commodity data unavailable."

        return (
            "ICP continues to move closely with WTI benchmark behavior, "
            "confirming strong cross-commodity alignment in the current market."
        )

    @staticmethod
    def get_dominance_insight(model_meta: Dict[str, Any]) -> str:
        """Simplified driver explanation in business language."""
        return (
            "WTI crude remains the dominant external factor affecting ICP trajectory. "
            "Secondary influence is derived from historical settlement momentum and recent "
            "short-term price continuation patterns."
        )

    @staticmethod
    def get_market_condition_label(rmse: float, df: pd.DataFrame) -> tuple[str, str]:
        """Executive-friendly market condition states."""
        if rmse > 4.5:
            return "Volatile", "Elevated price variance suggests cautious positioning."

        recent_icp = df["icp_price"].iloc[-1]
        ma3 = df["icp_price"].rolling(3).mean().iloc[-1]

        if abs(recent_icp - ma3) / ma3 > 0.03:
            return "High Momentum", "Strong directional shift overriding baseline stability."
        elif abs(recent_icp - ma3) / ma3 < 0.005:
            return "Consolidation", "Market tightening within a narrow range."
        else:
            return "Stable", "Price action aligned with historical volatility norms."

    @staticmethod
    def get_forecast_bias_narrative(pred_val: float, latest_icp: float) -> str:
        """Institutional tone for Market Bias card."""
        delta_pct = ((pred_val / latest_icp) - 1) * 100

        if delta_pct > 1:
            return "Upside potential following recent benchmark strength."
        elif delta_pct < -1:
            return "Short-term bearish pressure following recent price retracement."
        else:
            return "Neutral consolidation with minimal directional bias."
