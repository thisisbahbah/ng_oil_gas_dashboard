"""
app.py
──────
Nigeria Oil & Gas Production Analytics Dashboard
Streamlit entry point.

Usage:
    streamlit run dashboard/app.py
"""

import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# ── Page config ───────────────────────────────────────────
st.set_page_config(
    page_title="Nigeria Oil & Gas Dashboard",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Colour palette ────────────────────────────────────────
COLORS = {
    "primary":   "#1D9E75",
    "secondary": "#185FA5",
    "amber":     "#BA7517",
    "coral":     "#993C1D",
    "light_bg":  "#F8FAF9",
    "grid":      "#E5E7EB",
}

# ── DB connection ─────────────────────────────────────────
@st.cache_resource
def get_engine():
    # Streamlit Cloud: reads from st.secrets
    # Local: reads from .env
    try:
        host = st.secrets.get("DB_HOST", os.getenv("DB_HOST", "localhost"))
        port = st.secrets.get("DB_PORT", os.getenv("DB_PORT", "5432"))
        name = st.secrets.get("DB_NAME", os.getenv("DB_NAME", "ng_oil_gas"))
        user = st.secrets.get("DB_USER", os.getenv("DB_USER", "postgres"))
        pwd  = st.secrets.get("DB_PASSWORD", os.getenv("DB_PASSWORD", ""))
    except Exception:
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        name = os.getenv("DB_NAME", "ng_oil_gas")
        user = os.getenv("DB_USER", "postgres")
        pwd  = os.getenv("DB_PASSWORD", "")

    conn_str = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}"
    return create_engine(conn_str)


@st.cache_data(ttl=3600)
def load_view(view_name: str) -> pd.DataFrame:
    """Load a view from the database."""
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(f"SELECT * FROM {view_name}"), conn)
    return df


def plot_config():
    """Standard plotly layout settings."""
    return dict(
        font_family="Inter, sans-serif",
        font_color="#374151",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=30, b=0),
        xaxis=dict(gridcolor=COLORS["grid"], showgrid=True),
        yaxis=dict(gridcolor=COLORS["grid"], showgrid=True),
    )


# ── Sidebar ───────────────────────────────────────────────
with st.sidebar:
    st.image("https://flagcdn.com/w80/ng.png", width=40)
    st.title("Nigeria Oil & Gas")
    st.caption("Production Analytics Dashboard")
    st.divider()

    st.subheader("Filters")
    year_range = st.slider(
        "Year range",
        min_value=2010,
        max_value=2025,
        value=(2015, 2025),
    )
    show_raw = st.checkbox("Show raw data tables", value=False)

    st.divider()
    st.caption("Data sources: EIA · OPEC · NUPRC")
    st.caption("Built by Oluwafemi")
    st.caption("[GitHub](https://github.com/yourusername/ng_oil_gas_dashboard) · "
               "[Medium](https://medium.com/@yourusername)")


# ── Main page ─────────────────────────────────────────────
st.title("🛢️ Nigeria Crude Oil Production Dashboard")
st.caption("Monthly production trends, field-level breakdown, OPEC compliance, and Brent price correlation.")

# ── Try DB load; fall back to placeholder if DB unavailable ─
try:
    prod_price = load_view("v_monthly_production_price")
    opec_data  = load_view("v_opec_compliance")
    top_fields = load_view("v_top_fields_recent")
    shutin_monthly = load_view("v_national_shutin_monthly")
    price_corr = load_view("v_price_production_rolling")
    db_available = True
except Exception as e:
    st.warning(
        f"⚠️ Database not connected. Showing sample data.  \n"
        f"Run the ingestion scripts and set up PostgreSQL to see live data.  \n"
        f"Error: `{e}`"
    )
    db_available = False
    # Generate placeholder data so the UI still renders
    months = pd.date_range("2015-01-01", "2024-12-01", freq="MS")
    np.random.seed(42)
    prod_price = pd.DataFrame({
        "production_month":      months,
        "production_kbd":        1400 + 200 * np.sin(np.arange(len(months)) / 12) + np.random.randn(len(months)) * 80,
        "brent_price_usd":       65 + 30 * np.sin(np.arange(len(months)) / 18 + 1) + np.random.randn(len(months)) * 8,
        "yoy_change_pct":        np.random.randn(len(months)) * 10,
    })
    opec_data = pd.DataFrame({
        "quota_month":    months[-24:],
        "quota_kbd":      [1742.0] * 24,
        "actual_kbd":     1400 + np.random.randn(24) * 100,
        "compliance_pct": np.random.randn(24) * 8 - 15,
    })
    top_fields = pd.DataFrame({
        "field_name":         ["Bonny", "Forcados", "Qua Iboe", "Escravos", "Bonga", "Agbami", "Erha"],
        "operator":           ["SPDC", "SPDC", "ExxonMobil", "Chevron", "SPDC", "Chevron", "ExxonMobil"],
        "crude_grade":        ["Bonny Light", "Forcados", "Qua Iboe", "Escravos", "Bonga", "Agbami", "Erha"],
        "avg_production_kbd": [162.5, 135.3, 180.2, 118.4, 92.1, 75.3, 65.8],
        "avg_shut_in_kbd":    [87.5, 34.7, 19.8, 41.6, 27.9, 5.3, 2.2],
        "avg_nameplate_kbd":  [250.0, 170.0, 200.0, 160.0, 120.0, 80.6, 68.0],
        "avg_shut_in_pct":    [35.0, 20.4, 9.9, 26.0, 23.3, 6.6, 3.2],
    })
    shutin_monthly = pd.DataFrame({
        "production_month":   months,
        "total_production_kbd": prod_price["production_kbd"].values,
        "total_nameplate_kbd":  prod_price["production_kbd"].values + 300,
        "total_shutin_kbd":     300 + np.random.randn(len(months)) * 50,
        "national_shutin_pct":  18 + np.random.randn(len(months)) * 4,
    })
    price_corr = prod_price.copy()
    price_corr["rolling_12m_corr"] = 0.45 + np.random.randn(len(months)) * 0.15


# ── Filter by year range ──────────────────────────────────
def filter_years(df, col):
    df[col] = pd.to_datetime(df[col])
    return df[(df[col].dt.year >= year_range[0]) & (df[col].dt.year <= year_range[1])]

prod_price     = filter_years(prod_price, "production_month")
shutin_monthly = filter_years(shutin_monthly, "production_month")
price_corr     = filter_years(price_corr, "production_month")


# ── KPI cards ─────────────────────────────────────────────
latest = prod_price.dropna(subset=["production_kbd"]).iloc[-1]
latest_opec = opec_data.dropna(subset=["actual_kbd"]).iloc[-1] if len(opec_data) else None
latest_shutin = shutin_monthly.dropna(subset=["national_shutin_pct"]).iloc[-1]

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        "Current Production",
        f"{latest['production_kbd']:,.0f} kbd",
        delta=f"{latest.get('yoy_change_pct', 0):+.1f}% YoY",
    )
with col2:
    opec_delta = f"{latest_opec['compliance_pct']:+.1f}% vs quota" if latest_opec is not None else "N/A"
    opec_actual = f"{latest_opec['actual_kbd']:,.0f} kbd" if latest_opec is not None else "N/A"
    st.metric("OPEC Actual (latest)", opec_actual, delta=opec_delta)
with col3:
    brent = latest.get("brent_price_usd")
    st.metric("Brent Price", f"${brent:.2f}/bbl" if brent else "N/A")
with col4:
    st.metric(
        "National Shut-in",
        f"{latest_shutin['total_shutin_kbd']:,.0f} kbd",
        delta=f"{latest_shutin['national_shutin_pct']:.1f}% of capacity offline",
        delta_color="inverse",
    )

st.divider()


# ── Chart 1: Production trend ─────────────────────────────
st.subheader("Production Trend")
col_left, col_right = st.columns([3, 1])

with col_left:
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(
            x=prod_price["production_month"],
            y=prod_price["production_kbd"],
            name="Production (kbd)",
            line=dict(color=COLORS["primary"], width=2),
            fill="tozeroy",
            fillcolor="rgba(29,158,117,0.08)",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=prod_price["production_month"],
            y=prod_price["brent_price_usd"],
            name="Brent Price (USD)",
            line=dict(color=COLORS["amber"], width=1.5, dash="dot"),
            opacity=0.8,
        ),
        secondary_y=True,
    )
    fig.update_layout(
        **plot_config(),
        height=300,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        hovermode="x unified",
    )
    fig.update_yaxes(title_text="Production (kbd)", secondary_y=False)
    fig.update_yaxes(title_text="Brent USD/bbl", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.caption("**Key context**")
    st.caption("• Nigeria's OPEC quota: ~1,742 kbd (2023)")
    st.caption("• Peak production: ~2,500 kbd (2010–2011)")
    st.caption("• Primary constraints: pipeline vandalism, maintenance shutdowns")
    st.caption("• Bonny Light is Nigeria's benchmark crude (low sulphur, high API)")


# ── Chart 2: OPEC compliance ──────────────────────────────
st.subheader("OPEC Quota vs Actual Production")
opec_filtered = filter_years(opec_data.copy(), "quota_month") if len(opec_data) else opec_data

fig2 = go.Figure()
fig2.add_trace(go.Bar(
    x=opec_filtered["quota_month"],
    y=opec_filtered["quota_kbd"],
    name="OPEC Quota",
    marker_color=COLORS["grid"],
    opacity=0.7,
))
fig2.add_trace(go.Scatter(
    x=opec_filtered["quota_month"],
    y=opec_filtered["actual_kbd"],
    name="Actual Production",
    line=dict(color=COLORS["secondary"], width=2),
    mode="lines+markers",
    marker=dict(size=4),
))
fig2.update_layout(**plot_config(), height=280, hovermode="x unified")
fig2.update_yaxes(title_text="Thousand barrels/day")
st.plotly_chart(fig2, use_container_width=True)


# ── Chart 3: Top fields ───────────────────────────────────
st.subheader("Top Fields — Average Production (Last 12 Months)")
col3a, col3b = st.columns([2, 1])

with col3a:
    top10 = top_fields.head(10).sort_values("avg_production_kbd")
    fig3 = go.Figure()
    fig3.add_trace(go.Bar(
        y=top10["field_name"],
        x=top10["avg_production_kbd"],
        name="Production",
        orientation="h",
        marker_color=COLORS["primary"],
    ))
    fig3.add_trace(go.Bar(
        y=top10["field_name"],
        x=top10["avg_shut_in_kbd"],
        name="Shut-in",
        orientation="h",
        marker_color=COLORS["coral"],
        opacity=0.75,
    ))
    fig3.update_layout(
        **plot_config(),
        height=320,
        barmode="stack",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        xaxis_title="Thousand barrels/day",
    )
    st.plotly_chart(fig3, use_container_width=True)

with col3b:
    st.caption("**Shut-in % by field**")
    for _, row in top_fields.head(7).iterrows():
        pct = row.get("avg_shut_in_pct", 0) or 0
        color = "#E24B4A" if pct > 25 else "#BA7517" if pct > 10 else "#1D9E75"
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;margin-bottom:4px;font-size:13px'>"
            f"<span>{row['field_name']}</span>"
            f"<span style='color:{color};font-weight:500'>{pct:.1f}%</span></div>",
            unsafe_allow_html=True,
        )


# ── Chart 4: Shut-in over time ────────────────────────────
st.subheader("National Shut-in Volume Over Time")
fig4 = make_subplots(specs=[[{"secondary_y": True}]])
fig4.add_trace(
    go.Scatter(
        x=shutin_monthly["production_month"],
        y=shutin_monthly["total_shutin_kbd"],
        fill="tozeroy",
        name="Shut-in volume (kbd)",
        line=dict(color=COLORS["coral"], width=2),
        fillcolor="rgba(153,60,29,0.1)",
    ),
    secondary_y=False,
)
fig4.add_trace(
    go.Scatter(
        x=shutin_monthly["production_month"],
        y=shutin_monthly["national_shutin_pct"],
        name="Shut-in % of capacity",
        line=dict(color=COLORS["amber"], width=1.5, dash="dash"),
    ),
    secondary_y=True,
)
fig4.update_layout(**plot_config(), height=260, hovermode="x unified")
fig4.update_yaxes(title_text="Shut-in (kbd)", secondary_y=False)
fig4.update_yaxes(title_text="% of nameplate", secondary_y=True)
st.plotly_chart(fig4, use_container_width=True)


# ── Chart 5: Price-production rolling correlation ─────────
st.subheader("Rolling 12-Month Brent Price / Production Correlation")
corr_clean = price_corr.dropna(subset=["rolling_12m_corr"])

fig5 = go.Figure()
fig5.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5)
fig5.add_hline(y=0.5, line_dash="dot", line_color=COLORS["primary"], opacity=0.3,
               annotation_text="Strong positive", annotation_position="right")
fig5.add_hline(y=-0.5, line_dash="dot", line_color=COLORS["coral"], opacity=0.3,
               annotation_text="Strong negative", annotation_position="right")
fig5.add_trace(go.Scatter(
    x=corr_clean["production_month"],
    y=corr_clean["rolling_12m_corr"],
    fill="tozeroy",
    line=dict(color=COLORS["secondary"], width=2),
    fillcolor="rgba(24,95,165,0.08)",
    name="12-month rolling r",
))
fig5.update_layout(
    **plot_config(),
    height=240,
    yaxis=dict(range=[-1, 1], gridcolor=COLORS["grid"]),
    yaxis_title="Pearson r",
)
st.plotly_chart(fig5, use_container_width=True)
st.caption(
    "Interpretation: A positive correlation means higher Brent prices coincide with "
    "higher Nigerian production. Negative periods often reflect geopolitical/operational "
    "constraints overriding price incentives."
)


# ── Raw data tables (optional) ────────────────────────────
if show_raw:
    st.divider()
    st.subheader("Raw Data")
    tab1, tab2, tab3 = st.tabs(["Production & Price", "Top Fields", "OPEC Compliance"])
    with tab1:
        st.dataframe(prod_price, use_container_width=True, height=300)
    with tab2:
        st.dataframe(top_fields, use_container_width=True, height=300)
    with tab3:
        st.dataframe(opec_data, use_container_width=True, height=300)


# ── Footer ────────────────────────────────────────────────
st.divider()
st.caption(
    "Data: EIA Open Data API · OPEC Monthly Oil Market Report · NUPRC Production Reports  |  "
    "Built by Oluwafemi · [GitHub](https://github.com/yourusername/ng_oil_gas_dashboard)"
)