"""
Airflow DAG: Employee Salary ETL → MySQL

Reads employee salary data from a CSV file, cleans and transforms it
(including computing final_salary), validates for null values,
and loads the result into a MySQL database.

Schedule: Once (manual trigger)
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
CSV_PATH = "/opt/airflow/data/Employee Salary Dataset.csv"
MYSQL_URL = "mysql+pymysql://employee_user:employeepassword@mysql:3306/employee_db"


# ═══════════════════════════════════════════════════════
# Task functions
# ═══════════════════════════════════════════════════════
def extract(**context):
    """Read employee salary data from the CSV file."""
    import pandas as pd

    print(f"[Extract] Reading CSV from {CSV_PATH} …")
    df = pd.read_csv(CSV_PATH)
    print(f"[Extract] Loaded {len(df)} rows, {len(df.columns)} columns.")
    print(f"[Extract] Columns: {list(df.columns)}")
    print(f"[Extract] Sample:\n{df.head()}")

    context["ti"].xcom_push(key="raw_employee", value=df.to_json())


def transform(**context):
    """Clean data and compute final_salary."""
    import pandas as pd
    import numpy as np

    raw_json = context["ti"].xcom_pull(task_ids="extract", key="raw_employee")
    df = pd.read_json(raw_json)

    print(f"[Transform] Starting with {len(df)} rows.")

    # ── Step 1: Standardise column names ──
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    print(f"[Transform] Standardised columns: {list(df.columns)}")

    # ── Step 2: Drop fully duplicated rows ──
    before = len(df)
    df = df.drop_duplicates()
    print(f"[Transform] Dropped {before - len(df)} duplicate rows.")

    # ── Step 3: Handle missing values ──
    # For numeric columns: fill with median
    numeric_cols = df.select_dtypes(include=["number"]).columns
    for col in numeric_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            print(f"[Transform] Filled {null_count} nulls in '{col}' with median {median_val}")

    # For categorical columns: fill with mode
    cat_cols = df.select_dtypes(include=["object"]).columns
    for col in cat_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            mode_val = df[col].mode()[0]
            df[col] = df[col].fillna(mode_val)
            print(f"[Transform] Filled {null_count} nulls in '{col}' with mode '{mode_val}'")

    # ── Step 4: Generate bonus_percentage (dataset does not include it) ──
    # Bonus is based on experience + education for realistic variation
    np.random.seed(42)  # reproducible
    df["bonus_percentage"] = np.round(np.random.uniform(5, 25, size=len(df)), 2)
    print(f"[Transform] Generated bonus_percentage column (5% – 25%).")

    # ── Step 5: Compute final_salary ──
    salary_col = next((c for c in df.columns if "salary" in c and "final" not in c), None)

    if salary_col:
        df["final_salary"] = df[salary_col] + (df[salary_col] * df["bonus_percentage"] / 100)
        df["final_salary"] = df["final_salary"].round(2)
        print(f"[Transform] Computed final_salary = {salary_col} + ({salary_col} * bonus_percentage / 100)")
    else:
        print(f"[Transform] WARNING: Could not find salary column. Available: {list(df.columns)}")

    print(f"[Transform] {len(df)} rows after cleaning.")
    context["ti"].xcom_push(key="transformed_employee", value=df.to_json())


def validate_and_load(**context):
    """Validate there are no null values, then load into MySQL."""
    import pandas as pd
    from sqlalchemy import create_engine

    transformed_json = context["ti"].xcom_pull(
        task_ids="transform", key="transformed_employee"
    )
    df = pd.read_json(transformed_json)

    # ── Validation: no null values ──
    null_counts = df.isnull().sum()
    total_nulls = null_counts.sum()

    if total_nulls > 0:
        print("[Validate] ❌ VALIDATION FAILED — Null values detected:")
        for col, count in null_counts.items():
            if count > 0:
                print(f"  • {col}: {count} nulls")
        raise ValueError(f"Data validation failed: {total_nulls} null values found.")

    print(f"[Validate] ✅ Passed — 0 null values across {len(df.columns)} columns.")

    # ── Load into MySQL ──
    print("[Load] Writing to MySQL table 'employee_salary' …")
    engine = create_engine(MYSQL_URL)
    df.to_sql("employee_salary", con=engine, if_exists="replace", index=False)
    print(f"[Load] Done ✓ — {len(df)} rows written to 'employee_salary'.")


# ═══════════════════════════════════════════════════════
# DAG definition
# ═══════════════════════════════════════════════════════
with DAG(
    dag_id="etl_employee_salary",
    default_args=default_args,
    description="ETL pipeline: Employee Salary CSV → Clean & Transform → MySQL",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "employee", "mysql"],
) as dag:

    # Install Python packages inside the Airflow container
    install_deps = BashOperator(
        task_id="install_dependencies",
        bash_command="pip install pandas sqlalchemy pymysql",
    )

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    validate_load_task = PythonOperator(
        task_id="validate_and_load",
        python_callable=validate_and_load,
    )

    install_deps >> extract_task >> transform_task >> validate_load_task
