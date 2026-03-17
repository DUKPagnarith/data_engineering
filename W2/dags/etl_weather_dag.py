"""
Airflow DAG: Daily Weather Data ETL → PostgreSQL

Fetches weather data from the Open-Meteo API (free, no key required)
for Phnom Penh, Cambodia, and loads it into PostgreSQL.

Schedule: Daily
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ── DAG default args ──
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

# ── Configuration ──
LATITUDE = 11.56
LONGITUDE = 104.92
TIMEZONE = "Asia/Phnom_Penh"
API_URL = (
    f"https://api.open-meteo.com/v1/forecast"
    f"?latitude={LATITUDE}&longitude={LONGITUDE}"
    f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"
    f"&past_days=30&timezone={TIMEZONE}"
)
DB_URL = "postgresql+psycopg2://stock_user:stockpassword@postgres:5432/stock_db"


# ═══════════════════════════════════════════════════════
# Task functions
# ═══════════════════════════════════════════════════════
def extract(**context):
    """Fetch weather data from Open-Meteo API."""
    import requests

    print(f"[Extract] Fetching weather data from Open-Meteo …")
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()
    print(f"[Extract] Got data for {len(data['daily']['time'])} days.")

    context["ti"].xcom_push(key="raw_weather", value=data)


def transform(**context):
    """Parse raw JSON into a clean DataFrame."""
    import pandas as pd

    raw = context["ti"].xcom_pull(task_ids="extract", key="raw_weather")
    daily = raw["daily"]

    print("[Transform] Building DataFrame …")
    df = pd.DataFrame({
        "date": daily["time"],
        "temp_max_c": daily["temperature_2m_max"],
        "temp_min_c": daily["temperature_2m_min"],
        "precipitation_mm": daily["precipitation_sum"],
        "windspeed_max_kmh": daily["windspeed_10m_max"],
    })

    # Computed columns
    df["temp_range_c"] = df["temp_max_c"] - df["temp_min_c"]
    df["temp_avg_c"] = (df["temp_max_c"] + df["temp_min_c"]) / 2

    # Drop rows with all-null weather values
    df = df.dropna(subset=["temp_max_c", "temp_min_c"])

    print(f"[Transform] {len(df)} rows after cleaning.")
    context["ti"].xcom_push(key="transformed_weather", value=df.to_json())


def load(**context):
    """Load transformed weather data into PostgreSQL."""
    import pandas as pd
    from sqlalchemy import create_engine

    transformed_json = context["ti"].xcom_pull(
        task_ids="transform", key="transformed_weather"
    )
    df = pd.read_json(transformed_json)

    print("[Load] Writing to PostgreSQL table 'weather_daily' …")
    engine = create_engine(DB_URL)
    df.to_sql("weather_daily", con=engine, if_exists="replace", index=False)
    print(f"[Load] Done ✓ — {len(df)} rows written to 'weather_daily'.")


# ═══════════════════════════════════════════════════════
# DAG definition
# ═══════════════════════════════════════════════════════
with DAG(
    dag_id="etl_weather_daily",
    default_args=default_args,
    description="ETL pipeline: Daily weather data (Open-Meteo) → PostgreSQL",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "weather", "open-meteo"],
) as dag:

    # Install Python packages inside the Airflow container
    install_deps = BashOperator(
        task_id="install_dependencies",
        bash_command="pip install requests pandas sqlalchemy psycopg2-binary",
    )

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    install_deps >> extract_task >> transform_task >> load_task
