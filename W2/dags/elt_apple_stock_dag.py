"""
Task 2 — Airflow DAG: Apple Stock Log Returns → PostgreSQL

Same ELT logic as task1_simple_elt.py, but orchestrated as an Airflow DAG
with three tasks: extract → transform → load.

Note: The Airflow containers install yfinance at runtime via BashOperator.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ── DAG default args ──
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ── Configuration ──
TICKER = "AAPL"
# Inside Docker network, postgres hostname is the service name "postgres" on port 5432
DB_URL = "postgresql+psycopg2://stock_user:stockpassword@postgres:5432/stock_db"


# ═══════════════════════════════════════════════════════
# Task functions
# ═══════════════════════════════════════════════════════
def extract(**context):
    """Download 1 year of AAPL stock data and push to XCom."""
    import yfinance as yf

    print(f"[Extract] Downloading {TICKER} data …")
    ticker = yf.Ticker(TICKER)
    raw = ticker.history(period="1y")
    raw = raw.reset_index()
    raw["Date"] = raw["Date"].astype(str)
    print(f"[Extract] Got {len(raw)} rows.")

    # Push as JSON so XCom can serialize it
    context["ti"].xcom_push(key="raw_data", value=raw.to_json())


def transform(**context):
    """Calculate log returns and add signal column."""
    import numpy as np
    import pandas as pd

    raw_json = context["ti"].xcom_pull(task_ids="extract", key="raw_data")
    raw = pd.read_json(raw_json)

    print("[Transform] Calculating log returns …")
    df = raw[["Date", "Close"]].copy()
    df["log_return"] = np.log(df["Close"] / df["Close"].shift(1))
    df["signal"] = df["log_return"].apply(
        lambda x: "positive" if x >= 0 else "negative"
    )
    df = df.dropna()
    print(f"[Transform] {len(df)} rows after transformation.")

    context["ti"].xcom_push(key="transformed_data", value=df.to_json())


def load(**context):
    """Load transformed data into PostgreSQL."""
    import pandas as pd
    from sqlalchemy import create_engine

    transformed_json = context["ti"].xcom_pull(
        task_ids="transform", key="transformed_data"
    )
    df = pd.read_json(transformed_json)

    print("[Load] Writing to PostgreSQL …")
    engine = create_engine(DB_URL)
    df.to_sql("aapl_log_returns_airflow", con=engine, if_exists="replace", index=False)
    print(f"[Load] Done ✓  — {len(df)} rows written.")


# ═══════════════════════════════════════════════════════
# DAG definition
# ═══════════════════════════════════════════════════════
with DAG(
    dag_id="elt_apple_stock_log_returns",
    default_args=default_args,
    description="ELT pipeline: Apple stock log returns → PostgreSQL",
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["elt", "stock", "apple"],
) as dag:

    # Install Python packages inside the Airflow container
    install_deps = BashOperator(
        task_id="install_dependencies",
        bash_command="pip install yfinance numpy pandas sqlalchemy psycopg2-binary",
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
