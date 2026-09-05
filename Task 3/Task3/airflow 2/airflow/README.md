# Airflow 3.3.1 Docker ETL Lab

ETL pipeline using Apache Airflow 3.3.1 + Docker Compose. Extracts a CSV, validates it, transforms it, and loads results into PostgreSQL.

## Prerequisites

- **Docker Desktop** installed and running
- **macOS or Windows** terminal

## Project Structure

```
airflow/
├── docker-compose.yml      # All services (API server, scheduler, postgres)
├── dags/
│   └── etl_pipeline.py     # ETL DAG: extract → validate → transform → load
├── logs/                   # Airflow task logs (auto-populated)
├── config/                 # Airflow config overrides
├── plugins/                # Airflow plugins folder
└── README.md
```

---

## First-Time Setup (run once)

### 1. Navigate to the project folder

**macOS:**
```bash
cd /Users/mohamedrasik.rasik/Documents/bignxt/airflow
```

**Windows (PowerShell):**
```powershell
cd C:\path\to\airflow
```

---

### 2. Start the database only

```bash
docker compose up -d postgres
```

Wait 10 seconds for it to be healthy:
```bash
docker compose ps
```

---

### 3. Initialize the Airflow database

> **Note:** Airflow 3.x uses `db migrate`, not `db init`.

```bash
docker compose run --rm airflow-api-server airflow db migrate
```

Expected last line: `Database migration done!`

---

### 4. Create the admin login user

> **Note:** The `airflow users create` CLI is broken in Airflow 3.3.1. Use this Python workaround instead.

```bash
docker compose exec -T airflow-api-server python3 << 'EOF'
from werkzeug.security import generate_password_hash
import psycopg2

conn = psycopg2.connect(host='postgres', port=5432, database='airflow',
                        user='airflow', password='airflow')
cur = conn.cursor()
cur.execute("""
INSERT INTO ab_user (id, first_name, last_name, username, password, active, email, created_on, changed_on)
VALUES (nextval('ab_user_id_seq'), 'Air', 'Flow', 'airflow', %s, true,
        'airflow@example.com', NOW(), NOW())
ON CONFLICT (username) DO NOTHING
""", (generate_password_hash('airflow', method='pbkdf2:sha256'),))
conn.commit()
cur.close()
conn.close()
print("User 'airflow' created. Password: airflow")
EOF
```

---

### 5. Start all services

```bash
docker compose up -d
```

Wait ~30 seconds, then check status:

```bash
docker compose ps
```

Both `airflow-api-server` and `airflow-scheduler` should show **Up** or **healthy**.

---

### 6. Get the auto-generated `admin` password (optional)

Airflow also creates a built-in `admin` user on first start. To find its password:

**macOS:**
```bash
docker compose logs airflow-api-server | grep "Password for user"
```

**Windows:**
```powershell
docker compose logs airflow-api-server | findstr "Password for user"
```

Or just use the account created in Step 4: `airflow` / `airflow`.

---

### 7. Unpause the DAG

DAGs start **paused by default** in Airflow. Run this once to activate it:

```bash
docker compose exec -T postgres psql -U airflow -d airflow \
  -c "UPDATE dag SET is_paused = false WHERE dag_id = 'etl_csv_to_postgres';"
```

---

## Running the ETL Pipeline

### Option 1: Web UI (recommended)

1. Open **http://localhost:8080**
2. Log in with `airflow` / `airflow` (or `admin` + password from Step 6)
3. Find the DAG **`etl_csv_to_postgres`**
4. Click the **▶ Trigger** button
5. Watch all 4 tasks turn green

### Option 2: CLI trigger

```bash
docker compose exec airflow-scheduler airflow dags trigger etl_csv_to_postgres
```

---

## DAG Pipeline

```
extract_csv       Download CSV from FSU server → /tmp/airtravel_raw.csv
      ↓
validate_data     Check schema, row count, no nulls
      ↓
transform_data    Rename columns, calculate metrics → /tmp/airtravel_summary.csv
      ↓
load_to_postgres  Write 12 rows into PostgreSQL table airtravel_summary
```

Metrics calculated in transform:
- `total_passengers` = sum of 1958 + 1959 + 1960 passengers
- `yoy_growth_pct` = % growth from 1958 to 1960

---

## Verify Results in PostgreSQL

```bash
docker compose exec postgres psql -U airflow -d airflow
```

Inside the SQL prompt:

```sql
-- View the data
SELECT * FROM airtravel_summary;

-- Check row count (expected: 12)
SELECT COUNT(*) FROM airtravel_summary;

-- Exit
\q
```

---

## Useful Commands

| Action | Command |
|--------|---------|
| View all service logs | `docker compose logs -f` |
| View scheduler logs only | `docker compose logs -f airflow-scheduler` |
| Check service health | `docker compose ps` |
| List DAGs | `docker compose exec airflow-scheduler airflow dags list` |
| Check for DAG errors | `docker compose exec airflow-scheduler airflow dags list-import-errors` |
| Re-scan DAG files | `docker compose exec airflow-scheduler airflow dags reserialize` |
| Stop all services | `docker compose down` |
| Stop and delete all data | `docker compose down -v` |

---

## Troubleshooting

### DAG not showing in UI
Run the reserialize command and refresh the browser:
```bash
docker compose exec airflow-scheduler airflow dags reserialize
```

Then unpause it:
```bash
docker compose exec -T postgres psql -U airflow -d airflow \
  -c "UPDATE dag SET is_paused = false WHERE dag_id = 'etl_csv_to_postgres';"
```

### Tasks fail immediately with "state mismatch"
Both the API server and scheduler must share the same JWT secret. This is already configured in `docker-compose.yml`. If it recurs after a reset:
```bash
docker compose down && docker compose up -d
```

### Login not working
Use the auto-generated `admin` password (Step 6), or recreate the `airflow` user (Step 4).

### Check PostgreSQL directly
```bash
docker compose exec postgres psql -U airflow -d airflow
```

---

## Known Airflow 3.3.1 Breaking Changes

These are API changes from Airflow 2.x — not bugs in the DAG code:

| Old (Airflow 2.x) | New (Airflow 3.x) |
|---|---|
| `airflow webserver` | `airflow api-server` |
| `airflow db init` | `airflow db migrate` |
| `airflow users create --role Admin` | Broken in 3.3.1 — use Python workaround (Step 4) |
| `from airflow import DAG` | `from airflow.sdk import DAG` |
| `from airflow.operators.python import PythonOperator` | `from airflow.providers.standard.operators.python import PythonOperator` |
| `schedule_interval=` | `schedule=` |

---

## Architecture

| Service | Purpose |
|---------|---------|
| `airflow-api-server` | Web UI + REST API on port 8080 |
| `airflow-scheduler` | Parses DAGs and runs tasks via LocalExecutor |
| `postgres` | Airflow metadata DB + ETL results table |

> **LocalExecutor** is used (not Celery). Tasks run as subprocesses inside the scheduler container — no separate worker needed.
