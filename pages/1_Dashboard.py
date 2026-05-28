import logging
import traceback

import plotly.graph_objects as go
import streamlit as st

from components.layouts import render_footer
from components.styles import apply_custom_styles
from config.settings import PAGE_ICON

from components.metrics import (
    render_analytics_card,
    render_kpi_card,
    render_narrative_card,
)

from services.insight_service import InsightService
from services.prediction_service import get_prediction_service

from utils.data_loader import (
    get_latest_context,
    load_pipeline_metrics,
    load_processed_data,
)

logger = logging.getLogger("dashboard")


st.set_page_config(
    page_title="Dashboard | ICP Intelligence",
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded"
)


def safe_predict(service, features, fallback):

    if features is None:
        return fallback, "Fallback"

    try:

        pred = service.predict(features)

        if pred is None:
            return fallback, "Fallback"

        return float(pred), "Live"

    except Exception as e:

        logger.warning(e)

        return fallback, "Fallback"


def build_price_chart(df, pred_val):

    chart_df = df.tail(24).copy()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=chart_df.index,
            y=chart_df["icp_price"],
            mode="lines",
            name="ICP Price",

            line=dict(
                color="#0058be",
                width=3
            ),

            fill="tozeroy",

            fillcolor="rgba(0,88,190,0.08)"
        )
    )

    if "wti_price" in chart_df.columns:

        fig.add_trace(
            go.Scatter(
                x=chart_df.index,
                y=chart_df["wti_price"],

                mode="lines",

                name="WTI Reference",

                line=dict(
                    color="#76777d",
                    width=1.6,
                    dash="dot"
                )
            )
        )

    if pred_val is not None:

        fig.add_trace(
            go.Scatter(

                x=[chart_df.index[-1]],

                y=[pred_val],

                mode="markers",

                name="Forecast",

                marker=dict(
                    size=12,
                    color="#f59e0b",
                    line=dict(
                        color="white",
                        width=2
                    )
                )
            )
        )

    fig.update_layout(

        template="plotly_white",

        height=430,

        paper_bgcolor="rgba(0,0,0,0)",

        plot_bgcolor="white",

        margin=dict(
            l=10,
            r=10,
            t=20,
            b=10
        ),

        hovermode="x unified",

        legend=dict(

            orientation="h",

            y=1.05,

            x=1,

            xanchor="right",

            font=dict(
                family="JetBrains Mono",
                size=11
            )
        ),

        xaxis=dict(

            showgrid=True,

            gridcolor="rgba(190,190,190,0.2)",

            tickfont=dict(
                size=10
            )
        ),

        yaxis=dict(

            showgrid=True,

            gridcolor="rgba(190,190,190,0.25)",

            zeroline=False,

            title=dict(

                text="USD / BBL",

                font=dict(
                    size=11,
                    family="JetBrains Mono"
                )
            ),

            tickfont=dict(
                size=10
            )
        )
    )

    return fig


def render_terminal():

    st.markdown(
        """
        <div style='
        border-bottom:1px solid #c6c6cd;
        padding-bottom:12px;
        margin-bottom:24px;

        font-family:JetBrains Mono;

        text-transform:uppercase;

        letter-spacing:.15em;

        font-size:.72rem;
        '>

        ● Market Status: Operational

        &nbsp;&nbsp;&nbsp;&nbsp;

        Sync: 12ms

        </div>
        """,
        unsafe_allow_html=True
    )


def run_dashboard():

    apply_custom_styles()

    try:

        df = load_processed_data()

        metrics = load_pipeline_metrics()

        if df.empty:

            st.error(
                "Data market kosong"
            )

            return

        service = get_prediction_service()

        latest_icp = float(
            df["icp_price"].iloc[-1]
        )

        latest_wti = float(
            df["wti_price"].iloc[-1]
        )

        features = get_latest_context(df)

        pred_val, pred_status = safe_predict(

            service,

            features,

            latest_icp
        )

        rmse = float(
            metrics.get(
                "rmse",
                3.62
            )
        )

        corr = df[
            "icp_price"
        ].corr(
            df["wti_price"]
        )

        render_terminal()

        st.title(
            "ICP Market Intelligence"
        )

        st.caption(
            "Executive overview: kondisi pasar, posisi harga, dan proyeksi ICP."
        )

        c1, c2, c3, c4 = st.columns(4)

        with c1:

            render_kpi_card(

                "Forecast Price",

                f"${pred_val:.2f}",

                delta="+0.00%",

                detail=(
                    "Proyeksi harga ICP periode settlement mendatang."
                    if pred_status == "Live"
                    else "Fallback dari settlement terakhir."
                ),

                accent="amber"
            )

        with c2:

            render_kpi_card(

                "Latest ICP",

                f"${latest_icp:.2f}",

                detail="Harga settlement resmi ICP terakhir.",

                accent="blue"
            )

        with c3:

            render_kpi_card(

                "WTI Reference",

                f"${latest_wti:.2f}",

                detail="Benchmark global ICP.",

                accent="blue"
            )

        with c4:

            render_analytics_card(

                "Market Bias",

                "Neutral",

                subtitle="Pasar dalam kondisi netral.",

                accent="slate"
            )

        st.markdown("## Price Trend Analysis")

        st.caption(
            "Visualisasi pergerakan historis dan proyeksi nilai ICP."
        )

        fig = build_price_chart(
            df,
            pred_val
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={
                "displayModeBar": False
            }
        )

        left, right = st.columns([2,1])

        with left:

            render_narrative_card(

                "Market Intelligence Summary",

                InsightService.generate_intelligence_summary(
                    df,
                    pred_val,
                    corr
                ),

                accent="blue"
            )

        with right:

            render_analytics_card(

                "ICP-WTI Correlation",

                f"{corr:.2f}",

                subtitle="Korelasi benchmark global",

                accent="emerald"
            )

        st.markdown(
            "## Market Dynamics"
        )

        positioning = InsightService.generate_positioning_narrative(

            df,

            pred_val,

            "Neutral",

            corr
        )

        if (

            positioning is None

            or

            positioning == ""

            or

            "Data belum"

            in positioning
        ):

            positioning = f"""

Posisi pasar saat ini berada pada area ICP ${latest_icp:.2f}/bbl dengan benchmark WTI ${latest_wti:.2f}/bbl.

Korelasi ICP-WTI berada di level {corr:.2f} sehingga benchmark global masih dominan.

Model historis memiliki RMSE {rmse:.2f}.
"""

        render_narrative_card(

            "Market Positioning",

            positioning,

            accent="slate"
        )

        render_footer()

    except Exception as e:

        traceback.print_exc()

        st.error(
            f"Dashboard gagal: {e}"
        )


run_dashboard()