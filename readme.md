# GRU Connect Analytics

> **End-to-End Data Engineering Portfolio** — passenger connection risk analysis at Guarulhos International Airport (GRU/SBGR) using official ANAC data.

![CI](https://github.com/Pacodeoliv/gru-connect-analytics/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.10%2B-blue?style=flat-square)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?style=flat-square)
![dbt](https://img.shields.io/badge/dbt--spark-1.8-green?style=flat-square)
![Airflow](https://img.shields.io/badge/Airflow-2.9-red?style=flat-square)
![Cosmos](https://img.shields.io/badge/Astronomer--Cosmos-1.7-purple?style=flat-square)
![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.5-teal?style=flat-square)

---

## Business Context

The success of an airport hub depends on the **Minimum Connect Time (MCT)** — the shortest time for a passenger to deplane, transit, and board a connecting flight. This project analyses real ANAC flight data (VRA) to automatically classify every connection pair at GRU:

| Status | Connection Window | Risk |
|--------|:-----------------:|------|
| **Critical Risk** | < 60 min | High miss-connection probability |
| **Medium Risk** | 60 – 90 min | Feasible but under pressure |
| **Safe** | >= 90 min | Comfortable connection |

---

## Architecture

```mermaid
graph TD
    subgraph Source
        ANAC["ANAC VRA - Monthly CSV"]
    end

    subgraph Airflow["Orchestration — Apache Airflow 2.9"]
        DAG_B["dag_bronze_ingestion (monthly)"]
        DAG_S["dag_silver_transform (ExternalTaskSensor)"]
        DAG_G["dag_gold_dbt_cosmos (Astronomer Cosmos)"]
        DAG_B -->|ExternalTaskSensor| DAG_S
        DAG_S -->|ExternalTaskSensor| DAG_G
    end

    subgraph Lakehouse["Data Lakehouse — Medallion Architecture"]
        BRONZE["Bronze — Iceberg raw"]
        SILVER["Silver — Iceberg typed + cleaned"]
        GOLD["Gold — Star Schema dbt"]
        BRONZE -->|"PySpark silver_transformation.py"| SILVER
        SILVER -->|"dbt-spark via Cosmos"| GOLD
    end

    subgraph Gold_Tables["Gold Layer — Star Schema"]
        FATO["fato_conexoes"]
        D1["dim_aeroportos"]
        D2["dim_empresas"]
        D3["dim_calendario"]
        GOLD --> FATO
        GOLD --> D1
        GOLD --> D2
        GOLD --> D3
    end

    ANAC -->|"requests + PySpark ingestion_vra.py"| BRONZE
```

### Why Astronomer Cosmos?

Cosmos converts the dbt dependency graph directly into individual Airflow tasks. Each dbt model (`stg_anac_vra`, `dim_aeroportos`, `fato_conexoes`, etc.) has its own log, retry, and state in Airflow — enabling granular debugging and full observability.

```
# Without Cosmos: 1 BashOperator "dbt run"
[dbt_run_gold]

# With Cosmos: real lineage in Airflow
[stg_anac_vra] → [dim_aeroportos] → [fato_conexoes]
              → [dim_empresas]   ↗
              → [dim_calendario] ↗
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion & Processing | PySpark 3.5 |
| Storage Format | **Apache Iceberg 1.5** (ACID, Time Travel, Schema Evolution) |
| Analytical Modeling | dbt-spark 1.8 (incremental merge) |
| Orchestration | Apache Airflow 2.9 |
| dbt-Airflow Integration | **Astronomer Cosmos 1.7** |
| Data Quality | dbt tests (not_null, unique, accepted_values) |
| Containerization | Docker Compose (Airflow + Spark + Postgres) |
| CI/CD | GitHub Actions (lint + dbt validate + pytest) |
| Linting | Ruff |

---

## Project Structure

```
gru-connect-analytics/
├── dags/
│   ├── dag_bronze_ingestion.py   # Bronze: ANAC download + PySpark ingestion
│   ├── dag_silver_transform.py   # Silver: typing and cleaning via PySpark
│   └── dag_gold_dbt_cosmos.py    # Gold: dbt modeling via Cosmos
├── spark_jobs/
│   ├── ingestion_vra.py          # Download and Bronze ingestion logic
│   ├── silver_transformation.py  # Silver transformations
│   └── inspect_gold.py           # Gold inspection utility
├── dbt_gru/
│   └── models/
│       ├── staging/
│       │   ├── stg_anac_vra.sql  # Staging VRA
│       │   └── schema.yml        # Quality tests
│       └── marts/
│           ├── fato_conexoes.sql
│           ├── dim_aeroportos.sql
│           ├── dim_empresas.sql
│           ├── dim_calendario.sql
│           └── schema.yml        # Tests and documentation
├── scripts/
│   ├── docker-init.sh            # Bootstrap: DB migrate + admin user + Spark connection
│   └── backfill.sh               # Multi-month VRA ingestion utility
├── Dockerfile.airflow            # Custom Airflow image (Java + PySpark + dbt + Cosmos)
├── docker-compose.yml            # Full stack: Airflow + Spark + Postgres
├── .env.example                  # Required environment variables
└── pyproject.toml                # Python dependencies
```

---

## Quick Start

### Prerequisites

- Docker and Docker Compose

### 1. Clone and configure

```bash
git clone https://github.com/Pacodeoliv/gru-connect-analytics.git
cd gru-connect-analytics
cp .env.example .env
```

Edit `.env` if needed (defaults work out of the box):

```bash
AIRFLOW_UID=1000          # your Linux UID — run: echo $(id -u)
POSTGRES_PASSWORD=airflow
ANAC_ANO=2025             # reference year for ingestion
ANAC_MES=01               # reference month for ingestion
```

### 2. Start the stack

```bash
docker compose up -d
```

> **Note:** First startup takes ~5 minutes while Airflow initializes the database and scans providers. Subsequent restarts are much faster.

### 3. Access the UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) | admin / admin |
| **Spark Master UI** | [http://localhost:8082](http://localhost:8082) | — |

### 4. Run the pipeline

1. Open the Airflow UI at `http://localhost:8080`
2. Unpause `dag_bronze_ingestion` and trigger it manually
3. `dag_silver_transform` will start automatically once Bronze completes (ExternalTaskSensor)
4. `dag_gold_dbt_cosmos` will start automatically once Silver completes

### 5. Generate dbt documentation

```bash
docker compose exec airflow-webserver bash -c \
  "cd /opt/airflow/dbt_gru && dbt docs generate --profiles-dir . && dbt docs serve --port 8081"
```

Access at [http://localhost:8081](http://localhost:8081) — includes the full lineage graph.

### 6. Stop the stack

```bash
docker compose down          # stop and remove containers
docker compose down -v       # also remove volumes (reset all data)
```

---

## Services

| Service | Description | Ports |
|---------|-------------|-------|
| `postgres` | Airflow metadata database (PostgreSQL 15) | 5432 (internal) |
| `airflow-init` | One-shot: DB migration, admin user, Spark connection | — |
| `airflow-webserver` | Airflow UI | 8080 |
| `airflow-scheduler` | DAG scheduling and task execution | — |
| `spark-master` | Apache Spark 3.5 master node | 7077, 8082 |
| `spark-worker` | Spark worker (2 cores, 2 GB RAM) | — |

---

## Data Model (Gold — Star Schema)

```
dim_calendario ─┐
dim_aeroportos ─┤
dim_empresas   ─┼──► fato_conexoes
                │      - cd_icao_empresa
                │      - nr_voo_chegada
                │      - nr_voo_partida
                │      - janela_conexao_min
                └──────- desc_status_risco
```

---

## Author

**Paco de Oliveira Saavedra** — [GitHub](https://github.com/Pacodeoliv)

Advanced Data Engineering portfolio project, demonstrating an E2E pipeline with real data from the Brazilian civil aviation sector.
