# 🗽 NYC 311 Data Pipeline

> A production-grade ELT pipeline that ingests NYC 311 complaint data from the Socrata Open Data API and transforms it into an analytics-ready Snowflake warehouse, orchestrated end-to-end with Airflow.

[![dbt](https://img.shields.io/badge/dbt-1.10-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-data%20warehouse-29B5E8?logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Airflow](https://img.shields.io/badge/Airflow-3.0-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-containerized-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)

---

## 📖 Overview

This project ingests **every NYC 311 service request** — noise complaints, potholes, heat outages, rodent sightings, illegal parking, and 150+ other complaint types — from the city's public API into a fully-modeled Snowflake warehouse. It implements modern data engineering practices end-to-end: medallion architecture, dimensional modeling, incremental builds, SCD Type 2 history tracking, event-driven orchestration, and data contracts.

**One click, full pipeline.** Trigger the extraction DAG in Airflow and the entire chain runs unattended: API → S3 → Snowflake bronze → silver → gold star schema → reporting marts.

---

## 🏗️ Architecture

┌───────────▼────────────┐
│   Snowflake SILVER     │
│ ┌────────────────────┐ │
│ │ stg_nyc311__base   │ │  ◀── Flatten JSON
│ │ stg_nyc311__compl. │ │  ◀── Dedupe, normalize
│ │ int_complaints__   │ │  ◀── Business logic
│ │   enriched         │ │
│ └────────────────────┘ │
└───────────┬────────────┘
│
┌───────────▼────────────┐
│   Snowflake GOLD       │
│ ┌────────────────────┐ │
│ │ dim_borough        │ │
│ │ dim_complaint_type │ │  ◀── Star schema
│ │ fct_complaints     │ │      (incremental)
│ │ dashboard_daily_   │ │
│ │   summary          │ │  ◀── Reporting mart
│ └────────────────────┘ │
└────────────────────────┘
┌─────────────────────────────────────────────────┐
│              Airflow (Asset-Driven)             │
│                                                 │
│   nyc311_extraction_pipeline                    │
│           │                                     │
│           ▼                                     │
│   📦 bronze_complaints (Asset)                  |
│           │                                     │
│           ▼                                     │
│   nyc311_dbt_pipeline                           │
│     ├── PRE-FLIGHT (deps, source freshness)     │
│     ├── BUILD SILVER (staging, intermediate)    │
│     ├── BUILD GOLD (seeds, dims, facts, report) │
│     └── HISTORY (SCD Type 2 snapshots)          │
└─────────────────────────────────────────────────┘

---

## ✨ Features

### 📊 Data Modeling
- **Medallion architecture** — bronze (raw) → silver (cleaned) → gold (analytics-ready)
- **Star schema** in gold — `fct_complaints` joined to conformed dimensions (`dim_borough`, `dim_complaint_type`) via surrogate keys
- **Reporting marts** — pre-aggregated tables for fast BI consumption (`dashboard_daily_summary`)
- **Seeds** — hand-curated reference data for borough metadata and complaint type categorization
- **Unknown sentinel rows** — fact foreign keys are never NULL; missing values route to dimension sentinels (Kimball pattern)

### ⚡ Advanced dbt Patterns
- **Incremental fact table** — `fct_complaints` only processes new rows since last run via MERGE on `complaint_id`
- **SCD Type 2 snapshots** — historical tracking of complaint state transitions (status, closure, agency reassignment) from the bronze source
- **Data contracts** — every mart model declares typed column constraints; build fails on schema drift
- **Custom schema macro** — overrides dbt's default schema naming to land models in BRONZE/SILVER/GOLD directly

### 🧪 Testing & Data Quality
- **40+ data tests** — uniqueness, not-null, referential integrity, accepted values, custom expressions
- **Cross-column invariants** — model-level tests asserting relationships between columns (e.g., `closed_at >= created_at`)
- **Source freshness checks** — defensive monitoring of bronze data staleness
- **Generic + singular tests** — both YAML-declared and SQL-file-defined

### 🪂 Orchestration
- **Event-driven chaining** — extraction DAG produces an Airflow Asset; dbt DAG consumes it (no manual triggers, no polling)
- **TaskGroups** — pipeline organized into visual phases (pre-flight, silver, gold, history)
- **Granular dbt selectors** — each task scopes to a layer for independent re-runs without rebuilding everything
- **Retries with backoff** — transient failures (network, Snowflake throttling) recover automatically
- **Execution timeouts** — prevent runaway queries from blocking the pipeline

### 📐 Production-Grade Engineering
- **Containerized** — entire pipeline runs in Docker via `docker-compose`
- **Secrets management** — credentials sourced from `.env`, injected into containers as environment variables, never committed
- **Version-controlled reference data** — seed CSVs in git, editable via PRs
- **Infrastructure as code** — `docker-compose.yml`, `Dockerfile`, and DAG files fully describe the runtime environment

---

## 🧱 Tech Stack

| Layer | Tool |
|---|---|
| **Extraction** | Python (`requests`, `boto3`) |
| **Storage** | AWS S3 (raw JSON landing) |
| **Warehouse** | Snowflake (BRONZE / SILVER / GOLD schemas) |
| **Ingestion** | Snowflake Snowpipe (auto-ingest from S3) |
| **Transformation** | dbt-core 1.10 + dbt-snowflake |
| **Orchestration** | Apache Airflow 3.0 (TaskFlow API + Assets) |
| **Containerization** | Docker + docker-compose |
| **Source data** | NYC 311 Socrata Open Data API |

---

## 📁 Project Structure
personal_pipeline/
├── airflow/                          # Airflow orchestration
│   ├── dags/
│   │   ├── assets.py                # Shared Asset definitions
│   │   ├── nyc311_extraction_dag.py  # Pulls from API → S3
│   │   └── nyc311_dbt_dag.py         # Builds all dbt models
│   ├── dbt_profiles/
│   │   └── profiles.yml              # dbt config for the container (env-var driven)
│   ├── config/
│   ├── logs/
│   └── plugins/
│
├── ingestion/
│   └── nyc311_extractor.py           # Python Socrata API extractor
│
├── nyc311_dbt/                       # dbt project
│   ├── models/
│   │   ├── staging/                  # Flatten + clean source
│   │   ├── intermediate/             # Business logic
│   │   └── marts/
│   │       ├── core/                 # dim, fct_ (contracts enforced)
│   │       └── reporting/            # dashboard_daily_summary
│   ├── seeds/                        # Reference CSVs (borough, complaint type)
│   ├── snapshots/                    # SCD Type 2 history
│   ├── macros/                       # Reusable Jinja (schema naming, etc.)
│   ├── tests/                        # Custom data tests
│   ├── analyses/                     # Ad-hoc analytical queries
│   ├── dbt_project.yml
│   └── packages.yml
│
├── docker-compose.yml                # Orchestrates Airflow + Postgres + Redis
├── Dockerfile                        # Extends Airflow image with dbt
├── .env                              # Secrets (gitignored)
├── .gitignore
└── README.md
