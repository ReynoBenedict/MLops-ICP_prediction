import pandas as pd
import numpy as np
from typing import Dict, Any


class InsightService:
    """
    Generate concise business-oriented insights
    for forecasting dashboard pages.
    """

    @staticmethod
    def get_market_trend_insight(df: pd.DataFrame) -> str:
        """Executive market pulse for Dashboard page."""

        if df.empty or len(df) < 6:
            return (
                "Data historis belum cukup untuk membaca kondisi pasar."
            )

        recent_icp = df["icp_price"].iloc[-1]
        prev_icp = df["icp_price"].iloc[-2]

        ma3 = df["icp_price"].rolling(3).mean().iloc[-1]

        change_pct = ((recent_icp - prev_icp) / prev_icp) * 100

        volatility = (
            df["icp_price"].rolling(3).std().iloc[-1]
        )

        avg_volatility = (
            df["icp_price"].rolling(3).std().mean()
        )

        # Trend
        if recent_icp > ma3 * 1.02:
            trend = "naik"
        elif recent_icp < ma3 * 0.98:
            trend = "melemah"
        else:
            trend = "relatif stabil"

        # Volatility
        if volatility > avg_volatility * 1.2:
            risk = "Volatilitas pasar masih cukup tinggi."
        elif volatility < avg_volatility * 0.8:
            risk = "Pergerakan pasar relatif stabil."
        else:
            risk = "Pasar bergerak dalam volatilitas normal."

        return (
            f"ICP saat ini berada dalam tren {trend} "
            f"dengan perubahan {change_pct:+.1f}% "
            f"dibanding periode sebelumnya. "
            f"{risk}"
        )

    @staticmethod
    def get_correlation_insight(df: pd.DataFrame) -> str:
        """Relationship insight for ICP vs WTI page."""

        if df.empty:
            return (
                "Hubungan ICP dan WTI belum dapat dianalisis."
            )

        corr = df["icp_price"].corr(df["wti_price"])

        if corr >= 0.9:
            strength = "sangat kuat"
        elif corr >= 0.7:
            strength = "kuat"
        elif corr >= 0.5:
            strength = "cukup kuat"
        else:
            strength = "lemah"

        try:
            slope = np.polyfit(
                df["wti_price"],
                df["icp_price"],
                1,
            )[0]

            impact_text = (
                f"Secara historis, kenaikan WTI sebesar "
                f"1 USD diikuti perubahan ICP sekitar "
                f"{slope:.2f} USD."
            )

        except Exception:
            impact_text = (
                "Pergerakan historis ICP dan WTI "
                "masih menunjukkan pola yang konsisten."
            )

        return (
            f"Korelasi ICP dan WTI tergolong {strength} "
            f"dengan nilai Pearson {corr:.2f}. "
            f"{impact_text}"
        )

    @staticmethod
    def get_forecast_insight(
        pred_val: float,
        df: pd.DataFrame,
        model_meta: Dict[str, Any],
    ) -> str:
        """Forecast interpretation for Forecasting page."""

        if df.empty:
            return (
                "Forecast berhasil dibuat tanpa konteks historis."
            )

        latest_icp = df["icp_price"].iloc[-1]

        delta = pred_val - latest_icp
        delta_pct = (delta / latest_icp) * 100

        if delta_pct > 2:
            direction = "kenaikan"
        elif delta_pct < -2:
            direction = "penurunan"
        else:
            direction = "pergerakan stabil"

        return (
            f"Model memproyeksikan {direction} ICP "
            f"sebesar {delta_pct:+.2f}% "
            f"dibanding harga saat ini. "
            f"Prediksi masih mengikuti pola pergerakan "
            f"jangka pendek pasar minyak global."
        )

    @staticmethod
    def get_simulator_insight(
        sensitivity: Dict[str, Any],
        pred_val: float,
        baseline_val: float,
    ) -> str:
        """Interactive scenario insight for Predict Price page."""

        if not sensitivity:
            return (
                "Analisis sensitivitas belum tersedia."
            )

        top_feat = max(
            sensitivity.items(),
            key=lambda x: abs(
                x[1].get("impact", 0)
            ),
        )

        feature = (
            top_feat[0]
            .replace("_", " ")
            .replace("wti", "WTI")
            .upper()
        )

        impact = top_feat[1].get("impact", 0)

        delta = pred_val - baseline_val

        direction = (
            "meningkat"
            if delta > 0
            else "menurun"
        )

        return (
            f"Skenario ini membuat proyeksi ICP "
            f"{direction} sebesar {abs(delta):.2f} USD. "
            f"Faktor paling dominan berasal dari "
            f"{feature} dengan pengaruh sekitar "
            f"{impact:+.2f} USD."
        )

    @staticmethod
    def get_dominance_insight(
        model_meta: Dict[str, Any],
    ) -> str:
        """Model driver explanation for Forecasting page."""

        coefs = model_meta.get("coefficients")

        if not coefs:
            return (
                "Faktor dominan model belum tersedia."
            )

        sorted_coefs = sorted(
            coefs.items(),
            key=lambda x: abs(x[1]),
            reverse=True,
        )

        top_features = sorted_coefs[:3]

        clean_names = []

        for feat, _ in top_features:
            clean = (
                feat.replace("_", " ")
                .replace("wti", "WTI")
                .title()
            )
            clean_names.append(clean)

        primary_driver = clean_names[0]

        return (
            f"Prediksi ICP saat ini paling dipengaruhi oleh "
            f"{primary_driver}. "
            f"Beberapa driver utama lainnya meliputi "
            f"{', '.join(clean_names[1:])}."
        )

    @staticmethod
    def get_confidence_insight(
        rmse: float,
        pred_val: float,
    ) -> str:
        """Confidence explanation for Forecasting page."""

        if rmse <= 0:
            return (
                "Tingkat confidence belum dapat dihitung."
            )

        error_pct = (
            (rmse / pred_val) * 100
            if pred_val != 0
            else 0
        )

        if error_pct < 5:
            confidence = "tinggi"
        elif error_pct < 10:
            confidence = "baik"
        else:
            confidence = "moderat"

        return (
            f"Model menunjukkan tingkat confidence "
            f"{confidence} dengan estimasi error "
            f"historis sekitar ±{rmse:.2f} USD."
        )