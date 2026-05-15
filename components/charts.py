import plotly.graph_objects as go
import streamlit as st


def render_timeseries_analysis(df):
    """Interactive Plotly timeseries for ICP and WTI prices."""
    if df.empty:
        st.warning("No data available for visualization.")
        return

    fig = go.Figure()

    # ICP Price Trace
    fig.add_trace(
        go.Scatter(
            x=df["date"], y=df["icp_price"], name="ICP Price", line=dict(color="#002b5c", width=3), mode="lines+markers"
        )
    )

    # WTI Price Trace
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["wti_price"],
            name="WTI Price",
            line=dict(color="#b38b59", width=2, dash="dot"),
            mode="lines",
        )
    )

    fig.update_layout(
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=20, r=20, t=20, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(showgrid=False),
        yaxis=dict(title="Price (USD/BBL)", showgrid=True, gridcolor="#f0f0f0"),
    )

    st.plotly_chart(fig, width="stretch")


def render_correlation_scatter(df):
    """Scatter plot showing correlation between ICP and WTI."""
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["wti_price"],
            y=df["icp_price"],
            mode="markers",
            marker=dict(color="#002b5c", size=8, opacity=0.6, line=dict(width=1, color="white")),
        )
    )
    fig.update_layout(
        xaxis_title="WTI Price ($)",
        yaxis_title="ICP Price ($)",
        template="plotly_white",
        margin=dict(l=20, r=20, t=20, b=20),
    )
    st.plotly_chart(fig, width="stretch")
