from datetime import datetime, timedelta
import os
import pandas as pd
from sqlalchemy import create_engine
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ---------------------------------------------------
# FILE PATHS
# ---------------------------------------------------
SOURCE_FILE = "/opt/airflow/data/online_retail_II(Year 2010-2011).csv"
RAW_COPY = "/tmp/online_retail_raw.csv"
TRANSFORMED_FILE = "/tmp/retail_summary.csv"

# ---------------------------------------------------
# DEFAULT SETTINGS
# ---------------------------------------------------
default_args = {
    "owner": "retail_project",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# ---------------------------------------------------
# TASK 1 - EXTRACT RETAIL DATA
# ---------------------------------------------------
def extract_retail(**context):
    print(f"Reading retail CSV from: {SOURCE_FILE}")

    # Check that the source file exists
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(
            f"Retail CSV not found: {SOURCE_FILE}"
        )

    # Read the retail CSV
    df = pd.read_csv(
        SOURCE_FILE,
        encoding="utf-8-sig"
    )

    # Standardise the original Online Retail column names
    df.columns = df.columns.str.strip()

    df = df.rename(columns={
        "Invoice": "InvoiceNo",
        "Price": "UnitPrice",
        "Customer ID": "CustomerID"
    })

    print(f"Rows extracted: {len(df)}")
    print("Detected columns:")
    print(df.columns.tolist())
    print("First 5 rows:")
    print(df.head())

    # Save a temporary copy for the next Airflow task
    df.to_csv(
        RAW_COPY,
        index=False
    )

    print(f"Raw data saved to: {RAW_COPY}")

    return RAW_COPY

    # ---------------------------------------------------
# TASK 2 - VALIDATE RETAIL DATA
# ---------------------------------------------------
def validate_retail(**context):
    ti = context["ti"]

    # Get the file path returned by the Extract task
    raw_path = ti.xcom_pull(
        task_ids="extract_retail"
    )

    print(f"Validating retail data: {raw_path}")

    df = pd.read_csv(
        raw_path,
        encoding="ISO-8859-1"
    )

    # Remove accidental spaces from column names
    df.columns = df.columns.str.strip()

    print("Detected columns:")
    print(df.columns.tolist())

    # Columns expected in the Online Retail dataset
    required_cols = {
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    }

    missing = required_cols - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing expected columns: {missing}"
        )

    if len(df) == 0:
        raise ValueError("The retail dataset is empty.")

    print(f"Validation successful. Rows: {len(df)}")
    print("Missing values:")
    print(df.isnull().sum())

    return raw_path

    # ---------------------------------------------------
# AIRFLOW STEP 3 - TRANSFORM RETAIL DATA
# ---------------------------------------------------
def transform_retail(**context):
    ti = context["ti"]

    raw_path = ti.xcom_pull(
        task_ids="validate_retail"
    )

    print(f"Transforming retail data: {raw_path}")

    df = pd.read_csv(
        raw_path,
        encoding="ISO-8859-1"
    )

    df.columns = df.columns.str.strip()

    # Convert Quantity and UnitPrice to numeric values
    df["Quantity"] = pd.to_numeric(
        df["Quantity"],
        errors="coerce"
    )

    df["UnitPrice"] = pd.to_numeric(
        df["UnitPrice"],
        errors="coerce"
    )

    # Convert InvoiceDate to a date/time value
    df["InvoiceDate"] = pd.to_datetime(
        df["InvoiceDate"],
        errors="coerce"
    )

    rows_before = len(df)

    # Remove invalid or cancelled sales
    df = df[
        (df["Quantity"] > 0)
        & (df["UnitPrice"] > 0)
        & (~df["InvoiceNo"].astype(str).str.startswith("C"))
    ].copy()

    # Calculate revenue for each transaction line
    df["Revenue"] = (
        df["Quantity"] * df["UnitPrice"]
    ).round(2)

    rows_after = len(df)

    print(f"Rows before cleaning: {rows_before}")
    print(f"Rows after cleaning: {rows_after}")
    print(f"Rows removed: {rows_before - rows_after}")

    print("Transformed data preview:")
    print(df.head())

    df.to_csv(
        TRANSFORMED_FILE,
        index=False
    )

    print(
        f"Transformed data saved to: "
        f"{TRANSFORMED_FILE}"
    )

    return TRANSFORMED_FILE

    # ---------------------------------------------------
# AIRFLOW STEP 4 - LOAD RETAIL DATA TO POSTGRESQL
# ---------------------------------------------------
def load_retail(**context):
    ti = context["ti"]

    transformed_path = ti.xcom_pull(
        task_ids="transform_retail"
    )

    print(f"Loading transformed data: {transformed_path}")

    df = pd.read_csv(transformed_path)

    print(f"Rows to load: {len(df)}")

    # Connect to PostgreSQL
    engine = create_engine(
        "postgresql+psycopg2://"
        "airflow:airflow@postgres:5432/airflow"
    )

    # Load the cleaned retail data into PostgreSQL
    df.to_sql(
        name="retail_transactions",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=5000,
        method="multi"
    )

    print(
        f"Successfully loaded {len(df)} rows "
        "into PostgreSQL table 'retail_transactions'."
    )

    engine.dispose()

    # ---------------------------------------------------
# AIRFLOW DAG
# ---------------------------------------------------
with DAG(
    dag_id="retail_etl_to_postgres",
    default_args=default_args,
    description=(
        "Online Retail CSV -> validate -> "
        "transform -> PostgreSQL"
    ),
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "etl", "task3"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_retail",
        python_callable=extract_retail,
    )

    validate_task = PythonOperator(
        task_id="validate_retail",
        python_callable=validate_retail,
    )

    transform_task = PythonOperator(
        task_id="transform_retail",
        python_callable=transform_retail,
    )

    load_task = PythonOperator(
        task_id="load_retail_to_postgres",
        python_callable=load_retail,
    )

    (
        extract_task
        >> validate_task
        >> transform_task
        >> load_task
    )