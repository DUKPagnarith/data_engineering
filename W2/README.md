# W2 — ELT Pipeline Walkthrough

## Project Structure

```
W2/
├── docker-compose.yml              # PostgreSQL (x2), pgAdmin, Airflow
├── requirements.txt                # Python dependencies
├── task1_simple_elt.py             # Task 1: standalone Python ELT script
├── task1_simple_elt.ipynb          # Task 1: notebook version
├── dags/
│   └── elt_apple_stock_dag.py     # Task 2: Airflow DAG
└── logs/                           # Airflow logs (auto-created)
```

---

## Docker Services

| Service | Container | Port | Credentials |
|---------|-----------|------|-------------|
| **PostgreSQL (Stock Data)** | `w2_postgres` | `localhost:5433` | User: `stock_user` / Pass: `stockpassword` / DB: `stock_db` |
| **PostgreSQL (Airflow)** | `w2_postgres_airflow` | internal only | User: `airflow_user` / Pass: `airflowpassword` / DB: `airflow_db` |
| **pgAdmin** | `w2_pgadmin` | `localhost:5051` | Email: `admin@admin.com` / Pass: `admin` |
| **Airflow Webserver** | `w2_airflow_webserver` | `localhost:8081` | User: `airflow` / Pass: `airflow` |
| **Airflow Scheduler** | `w2_airflow_scheduler` | internal only | — |

---

## Getting Started

### 1. Start all services

```bash
cd W2
docker compose up -d
```

### 2. Verify containers are running

```bash
docker compose ps
```

You should see 5 containers (postgres, postgres-airflow, pgadmin, airflow-webserver, airflow-scheduler).

---

## Task 1 — Simple Python ELT

### What it does

1. **Extract**: Downloads 1 year of AAPL stock data via `yfinance`
2. **Transform**: Calculates `log_return = ln(Close_t / Close_{t-1})`, adds a `signal` column (`positive` / `negative`)
3. **Load**: Writes to `aapl_log_returns` table in PostgreSQL

### How to run

**Option A — Python script:**
```bash
python task1_simple_elt.py
```

**Option B — Jupyter notebook:**
Open `task1_simple_elt.ipynb` and run all cells.

---

## Task 2 — Airflow DAG

### What it does

Same ELT as Task 1, but orchestrated by Airflow as 4 tasks:
```
install_dependencies → extract → transform → load
```

### How to use

1. Open **http://localhost:8081**
2. Login: `airflow` / `airflow`
3. Find the DAG: `elt_apple_stock_log_returns`
4. **Toggle it ON** (switch on the left)
5. Click the **▶️ Play button** → **Trigger DAG**
6. Click the DAG name to watch progress — each task turns green ✅ when done
7. Click any task → **Log** tab to see output

> **Note**: The DAG file (`dags/elt_apple_stock_dag.py`) is NOT meant to be run with `python` directly. It runs inside the Airflow Docker container.

---

## How to use pgAdmin

1. Open **http://localhost:5051**
2. Login: `admin@admin.com` / `admin`
3. **Register a new server** (first time only):
   - Right-click **Servers** → **Register** → **Server**
   - **General tab**: Name = `Stock DB`
   - **Connection tab**:
     - Host: `postgres` ← (Docker service name, NOT localhost)
     - Port: `5432`
     - Username: `stock_user`
     - Password: `stockpassword`
   - Click **Save**
4. Browse: **Servers → Stock DB → Databases → stock_db → Schemas → public → Tables**
5. Right-click a table → **View/Edit Data → All Rows**

### Run SQL queries

Click the ⚡ **Query Tool** icon, then:
```sql
SELECT * FROM aapl_log_returns LIMIT 10;
```

---

## How to use PostgreSQL via CLI

```bash
docker exec -it w2_postgres psql -U stock_user -d stock_db
```

Useful commands:
```sql
\dt                                    -- list tables
SELECT * FROM aapl_log_returns LIMIT 10;  -- view data
SELECT COUNT(*) FROM aapl_log_returns;    -- count rows
\q                                     -- exit
```

---

## Stopping / Restarting

```bash
# Stop all services
cd W2
docker compose down

# Start again
docker compose up -d

# Full reset (delete all data)
docker compose down -v
docker compose up -d
```
