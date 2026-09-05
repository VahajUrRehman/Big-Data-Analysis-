# Diagnostic Fragment A

## 🚀 Getting started

> **Prerequisites:** Docker Desktop, Python 3.10+, Java 11 (for local PySpark), Jupyter

<details>
<summary><b>1. SQL & NoSQL (Task 1)</b></summary>

```bash
cd "Task 1"
jupyter notebook Task1_sql_analysis.ipynb
```
Requires a running PostgreSQL instance — update connection settings in the notebook's config cell.
</details>

<details>
<summary><b>2. PySpark (Task 2)</b></summary>

```bash
cd "Task 2"
jupyter notebook task2_pyspark.ipynb
```
Requires `JAVA_HOME`, the PostgreSQL JDBC driver, and (on Windows) `HADOOP_HOME`/`winutils` set up before the notebook starts its `SparkSession`.
</details>

<details>
<summary><b>3. Docker · Airflow · Kafka (Task 3)</b></summary>

```bash
# Airflow (batch ETL)
cd "Task 3/Task3/airflow 2/airflow"
docker compose up -d
# → Airflow UI: http://localhost:8080

# Kafka + Kafdrop (streaming)
cd "../../kafka_retail"
docker compose up -d
# → Kafdrop UI: http://localhost:9000

python retail_producer.py    # publish sample events
python retail_consumer.py    # consume, flag high-value, count in windows
```
</details>
