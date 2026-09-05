# Reset and Start from Scratch

Follow these steps in order. Each command must finish before running the next.

---

## Step 1 — Open your terminal and navigate to the project


**Windows (PowerShell):**
```powershell
cd C:\path\to\airflow
```

---

## Step 2 — Stop and delete everything

This removes all containers, networks, and the PostgreSQL data volume (all Airflow data is erased):

```bash
docker compose down -v --remove-orphans
```

Expected output: containers removed, volumes removed, network removed.

---

## Step 3 — Start only the database

```bash
docker compose up -d postgres
```

Wait for it to be healthy:
```bash
docker compose ps
```

The `postgres` service should show `(healthy)` before continuing.

---

## Step 4 — Initialize the Airflow database schema

```bash
docker compose run --rm airflow-api-server airflow db migrate
```

Expected last line: `Database migration done!`

---

## Step 5 — Create the admin user

> The standard `airflow users create` CLI is broken in Airflow 3.3.1. Use this workaround:

```bash
docker compose run --rm --entrypoint python3 airflow-api-server << 'EOF'
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
print("Done. Login: airflow / airflow")
EOF
```

Expected output: `Done. Login: airflow / airflow`

---

## Step 6 — Start all services

```bash
docker compose up -d
```

Wait ~30 seconds then verify:
```bash
docker compose ps
```

`airflow-api-server` should show `(healthy)` and `airflow-scheduler` should show `Up`.

---

## Step 7 — Unpause and trigger the DAG

```bash
docker compose exec airflow-scheduler airflow dags reserialize

docker compose exec -T postgres psql -U airflow -d airflow \
  -c "UPDATE dag SET is_paused = false WHERE dag_id = 'etl_csv_to_postgres';"

docker compose exec airflow-scheduler airflow dags trigger etl_csv_to_postgres
```

---

## Step 8 — Open the UI and watch it run

Open: **http://localhost:8080**

Login:
- Username: `airflow`
- Password: `airflow`

Find `etl_csv_to_postgres` — all 4 tasks should turn **green** within ~30 seconds.

---

## Step 9 — Verify the data in PostgreSQL

```bash
docker compose exec postgres psql -U airflow -d airflow \
  -c "SELECT * FROM airtravel_summary;"
```

Expected: **12 rows** of monthly air travel data.

---

## Summary of Commands (copy-paste block)

```bash
cd /Users/mohamedrasik.rasik/Documents/bignxt/airflow

# 1. Wipe everything
docker compose down -v --remove-orphans

# 2. Start database
docker compose up -d postgres && sleep 12

# 3. Initialize schema
docker compose run --rm airflow-api-server airflow db migrate

# 4. Create admin user
docker compose run --rm --entrypoint python3 airflow-api-server << 'EOF'
from werkzeug.security import generate_password_hash
import psycopg2
conn = psycopg2.connect(host='postgres', port=5432, database='airflow', user='airflow', password='airflow')
cur = conn.cursor()
cur.execute("""INSERT INTO ab_user (id, first_name, last_name, username, password, active, email, created_on, changed_on)
VALUES (nextval('ab_user_id_seq'), 'Air', 'Flow', 'airflow', %s, true, 'airflow@example.com', NOW(), NOW())
ON CONFLICT (username) DO NOTHING""", (generate_password_hash('airflow', method='pbkdf2:sha256'),))
conn.commit(); cur.close(); conn.close()
print("Done. Login: airflow / airflow")
EOF

# 5. Start all services
docker compose up -d && sleep 30

# 6. Unpause and trigger DAG
docker compose exec airflow-scheduler airflow dags reserialize
docker compose exec -T postgres psql -U airflow -d airflow -c "UPDATE dag SET is_paused = false WHERE dag_id = 'etl_csv_to_postgres';"
docker compose exec airflow-scheduler airflow dags trigger etl_csv_to_postgres

# 7. Verify results
docker compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM airtravel_summary;"
```

Expected final output: `count = 12`
