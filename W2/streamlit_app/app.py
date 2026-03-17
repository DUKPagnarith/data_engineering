"""
Streamlit Dashboard — Weather Data from PostgreSQL

Displays daily weather data fetched by the Airflow ETL pipeline.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

# ── Page config ──
st.set_page_config(
    page_title="🌤️ Phnom Penh Weather Dashboard",
    page_icon="🌤️",
    layout="wide",
)

# ── Database connection ──
DB_URL = "postgresql+psycopg2://stock_user:stockpassword@postgres:5432/stock_db"


@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_data():
    """Load weather data from PostgreSQL."""
    engine = create_engine(DB_URL)
    try:
        df = pd.read_sql("SELECT * FROM weather_daily ORDER BY date", con=engine)
        df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception as e:
        st.error(f"Could not load data: {e}")
        return pd.DataFrame()


# ── Load data ──
df = load_data()

# ── Header ──
st.title("🌤️ Phnom Penh Weather Dashboard")
st.markdown("Daily weather data powered by **Open-Meteo API** and **Airflow ETL pipeline**.")

if df.empty:
    st.warning("⚠️ No weather data found. Please run the Airflow DAG (`etl_weather_daily`) first.")
    st.stop()

# ── KPI Cards ──
st.markdown("---")
col1, col2, col3, col4 = st.columns(4)

latest = df.iloc[-1]
with col1:
    st.metric("📅 Latest Date", latest["date"].strftime("%Y-%m-%d"))
with col2:
    st.metric("🌡️ Avg Temp", f"{latest['temp_avg_c']:.1f} °C")
with col3:
    st.metric("🌧️ Precipitation", f"{latest['precipitation_mm']:.1f} mm")
with col4:
    st.metric("💨 Max Wind", f"{latest['windspeed_max_kmh']:.1f} km/h")

st.markdown("---")

# ── Temperature Chart ──
st.subheader("🌡️ Temperature Trends")

fig_temp = go.Figure()
fig_temp.add_trace(go.Scatter(
    x=df["date"], y=df["temp_max_c"],
    name="Max Temp (°C)",
    line=dict(color="#FF6B6B", width=2),
    fill=None,
))
fig_temp.add_trace(go.Scatter(
    x=df["date"], y=df["temp_min_c"],
    name="Min Temp (°C)",
    line=dict(color="#4ECDC4", width=2),
    fill="tonexty",
    fillcolor="rgba(78, 205, 196, 0.15)",
))
fig_temp.add_trace(go.Scatter(
    x=df["date"], y=df["temp_avg_c"],
    name="Avg Temp (°C)",
    line=dict(color="#FFE66D", width=2, dash="dot"),
))
fig_temp.update_layout(
    xaxis_title="Date",
    yaxis_title="Temperature (°C)",
    template="plotly_dark",
    height=400,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)
st.plotly_chart(fig_temp, use_container_width=True)

# ── Precipitation & Wind Charts ──
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("🌧️ Daily Precipitation")
    fig_precip = px.bar(
        df, x="date", y="precipitation_mm",
        color="precipitation_mm",
        color_continuous_scale="Blues",
        labels={"precipitation_mm": "Precipitation (mm)", "date": "Date"},
    )
    fig_precip.update_layout(template="plotly_dark", height=350, showlegend=False)
    st.plotly_chart(fig_precip, use_container_width=True)

with col_right:
    st.subheader("💨 Max Wind Speed")
    fig_wind = px.line(
        df, x="date", y="windspeed_max_kmh",
        labels={"windspeed_max_kmh": "Wind Speed (km/h)", "date": "Date"},
    )
    fig_wind.update_traces(line=dict(color="#A8E6CF", width=2))
    fig_wind.update_layout(template="plotly_dark", height=350)
    st.plotly_chart(fig_wind, use_container_width=True)

# ── Temperature Range Chart ──
st.subheader("📊 Daily Temperature Range")
fig_range = px.bar(
    df, x="date", y="temp_range_c",
    color="temp_range_c",
    color_continuous_scale="Oranges",
    labels={"temp_range_c": "Temp Range (°C)", "date": "Date"},
)
fig_range.update_layout(template="plotly_dark", height=300, showlegend=False)
st.plotly_chart(fig_range, use_container_width=True)

# ── Data Table ──
st.subheader("📋 Raw Data")
st.dataframe(
    df.sort_values("date", ascending=False).style.format({
        "temp_max_c": "{:.1f}",
        "temp_min_c": "{:.1f}",
        "temp_avg_c": "{:.1f}",
        "temp_range_c": "{:.1f}",
        "precipitation_mm": "{:.1f}",
        "windspeed_max_kmh": "{:.1f}",
    }),
    use_container_width=True,
    height=400,
)

# ── Footer ──
st.markdown("---")
st.caption("Data source: [Open-Meteo API](https://open-meteo.com/) | ETL: Apache Airflow | Dashboard: Streamlit")
