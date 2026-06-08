import plotly.graph_objects as go
import streamlit as st

PLOT_TEMPLATE = dict(
    template="plotly_dark",
    hovermode="x unified",
    margin=dict(l=10, r=10, t=20, b=10),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1,
        bgcolor="rgba(0,0,0,0)",
    ),
    xaxis=dict(
        showgrid=False,
        zeroline=False,
    ),
    yaxis=dict(
        showgrid=True,
        gridcolor="rgba(255,255,255,0.06)",
        zeroline=False,
    ),
)


def render_timeseries_analysis(df):

    if df.empty:
        st.warning("No market data available for visualization.")
        return

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["icp_price"],
            name="ICP Price",
            mode="lines",
            line=dict(
                color="#3b82f6",
                width=3,
            ),
            hovertemplate="ICP: $%{y:.2f}<extra></extra>",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["wti_price"],
            name="WTI Benchmark",
            mode="lines",
            line=dict(
                color="#f59e0b",
                width=2.2,
                dash="dot",
            ),
            hovertemplate="WTI: $%{y:.2f}<extra></extra>",
        )
    )

    fig.update_layout(
        **PLOT_TEMPLATE,
        height=420,
        yaxis_title="USD / BBL",
    )

    st.plotly_chart(
        fig,
        width="stretch",
        config={
            "displayModeBar": False,
            "responsive": True,
        },
    )


def render_correlation_scatter(df):

    if df.empty:
        st.warning("No correlation data available.")
        return

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df["wti_price"],
            y=df["icp_price"],
            mode="markers",
            name="Observation",
            marker=dict(
                color="#3b82f6",
                size=9,
                opacity=0.7,
                line=dict(
                    width=1,
                    color="rgba(255,255,255,0.18)",
                ),
            ),
            hovertemplate=(
                "WTI: $%{x:.2f}<br>"
                "ICP: $%{y:.2f}<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        **PLOT_TEMPLATE,
        height=420,
        xaxis_title="WTI Price (USD / BBL)",
        yaxis_title="ICP Price (USD / BBL)",
    )

    st.plotly_chart(
        fig,
        width="stretch",
        config={
            "displayModeBar": False,
            "responsive": True,
        },
    )
