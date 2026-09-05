from datetime import datetime, timedelta
import pandas as pd
import requests
import urllib3
from sqlalchemy import create_engine
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ---------------------------------------------------
# LAB-ONLY SSL WORKAROUND
# ---------------------------------------------------
# The corporate/self-signed certificate chain caused
# requests.get() SSL verification to fail inside Docker.
# Do NOT use verify=False as a production solution.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------
# DEFAULT SETTINGS
# ---------------------------------------------------
AIRTRAVEL_FALLBACK_CSV = """Month,1958,1959,1960
JAN,340,360,417
FEB,318,342,391
MAR,362,406,419
APR,348,396,461
MAY,363,420,472
JUN,435,472,535
JUL,491,548,622
AUG,505,559,606
SEP,404,463,508
OCT,359,407,461
NOV,310,362,390
DEC,337,405,432
"""

default_args = {
    "owner": "lab3a",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# ---------------------------------------------------
# TASK 1 - EXTRACT
# ---------------------------------------------------
def extract(**context):
    url = (
        "https://people.sc.fsu.edu/"
        "~jburkardt/data/csv/airtravel.csv"
    )
    print(f"Downloading CSV from: {url}")
    try:
        response = requests.get(
            url,
            timeout=30,
            verify=False,
        )
        response.raise_for_status()
        csv_text = response.text
    except requests.RequestException as exc:
        print(f"Download failed: {exc}")
        print("Using embedded fallback airtravel CSV for repeatable lab execution.")
        csv_text = AIRTRAVEL_FALLBACK_CSV

    tmp_path = "/tmp/airtravel_raw.csv"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(csv_text)
    print(
        f"Prepared {len(csv_text)} bytes "
        f"-> {tmp_path}"
    )
    print("CSV preview:")
    print(csv_text[:300])
    return tmp_path

# ---------------------------------------------------
# TASK 2 - VALIDATE
# ---------------------------------------------------
def validate(**context):
    ti = context["ti"]
    tmp_path = ti.xcom_pull(
        task_ids="extract_csv"
    )
    print(f"Reading: {tmp_path}")
    df = pd.read_csv(
        tmp_path,
        skipinitialspace=True,
    )
    # Normalize CSV headers
    df.columns = (
        df.columns
        .str.strip()
        .str.strip('"')
        .str.strip()
    )
    print("Detected columns:")
    print(df.columns.tolist())
    print("Data preview:")
    print(df.head())
    required_cols = {
        "Month",
        "1958",
        "1959",
        "1960",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing expected columns: {missing}. "
            f"Found columns: {df.columns.tolist()}"
        )
    if len(df) < 5:
        raise ValueError(
            f"Too few rows: {len(df)}"
        )
    null_counts = df.isnull().sum()
    if null_counts.any():
        raise ValueError(
            "Nulls detected:\n"
            f"{null_counts[null_counts > 0]}"
        )
    print(
        f"Validation successful. "
        f"Rows: {len(df)}"
    )
    return tmp_path

# ---------------------------------------------------
# TASK 3 - TRANSFORM
# ---------------------------------------------------
def transform(**context):
    ti = context["ti"]
    tmp_path = ti.xcom_pull(
        task_ids="validate_data"
    )
    print(f"Transforming: {tmp_path}")
    df = pd.read_csv(
        tmp_path,
        skipinitialspace=True,
    )
    df.columns = (
        df.columns
        .str.strip()
        .str.strip('"')
        .str.strip()
    )
    df = df.rename(
        columns={
            "Month": "month",
            "1958": "y1958",
            "1959": "y1959",
            "1960": "y1960",
        }
    )
    df["month"] = (
        df["month"]
        .astype(str)
        .str.strip()
        .str.strip('"')
    )
    numeric_columns = [
        "y1958",
        "y1959",
        "y1960",
    ]
    for col in numeric_columns:
        df[col] = pd.to_numeric(
            df[col],
            errors="raise",
        )
    df["total_passengers"] = (
        df["y1958"]
        + df["y1959"]
        + df["y1960"]
    )
    df["yoy_growth_pct"] = (
        (
            (df["y1960"] - df["y1958"])
            / df["y1958"]
            * 100
        )
        .round(2)
    )
    out_path = "/tmp/airtravel_summary.csv"
    df.to_csv(
        out_path,
        index=False,
    )
    print("Transformed data:")
    print(df)
    print(
        f"Saved transformed file -> {out_path}"
    )
    return out_path

# ---------------------------------------------------
# TASK 4 - LOAD
# ---------------------------------------------------
def load(**context):
    ti = context["ti"]
    out_path = ti.xcom_pull(
        task_ids="transform_data"
    )
    print(f"Loading: {out_path}")
    df = pd.read_csv(out_path)
    print(f"Rows to load: {len(df)}")
    engine = create_engine(
        "postgresql+psycopg2://"
        "airflow:airflow@postgres:5432/airflow"
    )
    df.to_sql(
        name="airtravel_summary",
        con=engine,
        if_exists="replace",
        index=False,
    )
    print(
        f"Successfully loaded {len(df)} rows "
        "into PostgreSQL table "
        "'airtravel_summary'."
    )
    engine.dispose()

# ---------------------------------------------------
# DAG
# ---------------------------------------------------
with DAG(
    dag_id="etl_csv_to_postgres",
    default_args=default_args,
    description=(
        "CSV -> validate -> "
        "transform -> PostgreSQL"
    ),
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lab3a", "etl"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_csv",
        python_callable=extract,
    )
    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate,
    )
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )
    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load,
    )
    (
        extract_task
        >> validate_task
        >> transform_task
        >> load_task
    )
