# GoodRx ED Subscription Analysis

Analysis of GoodRx ED (erectile dysfunction) subscriptions, organized in three phases:

1. **Data transformation / ETL** — ingest source data and build a Delta medallion (bronze → silver → gold).
2. **EDA** — exploratory analysis on the cleaned/feature tables.
3. **Modeling** — churn / subscribe prediction model (MLflow-tracked).

## Architecture at a glance

```
Redshift (source of truth, read-only)
   │   exported via DataGrip → CSV
   ▼
Databricks Volume (raw file landing zone)
   /Volumes/general_scratch_catalog/general_scratch/checkpoints/jiny/ed_subs_raw_uploads/
   │   spark.read.csv() via load_bronze.py
   ▼
general_scratch_catalog.general_scratch   (single schema — all medallion tables here)
   ├── ed_bronze_<table>   raw Delta tables, 1:1 with source CSVs (ED-filtered at export)
   ├── ed_silver_<table>   cleaned, typed, deduped, conformed keys
   └── ed_gold_<table>     model-ready feature tables + churn label
```

> **No live Redshift → Databricks connection.** The Spark Redshift connector is not viable
> (network `SocketTimeoutException`, and the two systems hold different data). The required
> schema (`goodrx_dbt`) lives only in Redshift. The sanctioned path is: export from Redshift
> via DataGrip → CSV → upload to the Databricks Volume → load into bronze Delta tables.

## Source tables (goodrx_dbt schema in Redshift)

> **Data pull date: June 30, 2026** — all tables were exported from Redshift on this date.

| Table | Description |
| --- | --- |
| `subscriptions` | Core subscription record, one row per subscription |
| `subscription_terms` | One row per billing term (renewal period) |
| `subscription_plan_terms` | Drug / plan details within each term |
| `subscription_charges` | Individual payment charge events |
| `subscription_invoices` | One row per invoice (billing cycle) |
| `subscriptions_churn` | Pre-aggregated churn view with forecasted churn date |

## Tools & roles

| Tool | Role |
| --- | --- |
| Redshift | Source of truth for data (read-only) |
| DataGrip | Explore Redshift, write/validate extraction SQL, export CSVs |
| Databricks | Compute + storage (Delta medallion, MLflow, jobs) |
| PyCharm | Local Python development |
| GitHub | Version control + PRs; synced into Databricks via Repos / Git folders |
| Cursor agent | Writes code directly in the local repo |

## Repository layout

```
.
├── conf/                       # Configuration (catalog, schema, prefixes, paths)
├── sql/redshift/exports/
│   └── goodrx_dbt/             # Extraction SQL per table (run in DataGrip against Redshift)
├── data/exports/               # Local CSV exports from DataGrip (gitignored)
├── notebooks/                  # Databricks notebooks, ordered by pipeline stage
│   ├── 00_ingestion/           # load_bronze.py: CSV → ed_bronze_* Delta tables
│   ├── 01_bronze/              # Bronze inspection / validation
│   ├── 02_silver/              # bronze → ed_silver_* (clean, type, dedupe)
│   ├── 03_gold/                # silver → ed_gold_* (features + churn label)
│   ├── 04_eda/                 # Phase 2: exploratory analysis
│   └── 05_modeling/            # Phase 3: churn / subscribe model (MLflow)
├── src/ed_subs/                # Reusable Python package (importable in notebooks & PyCharm)
│   ├── config.py               # Central config: catalog, schema, prefixes, volume path
│   ├── ingestion/              # Phase 1: CSV → bronze
│   ├── transform/              # bronze → silver → gold
│   ├── features/               # feature engineering + churn label
│   ├── eda/                    # Phase 2 helpers
│   ├── models/                 # Phase 3 churn/subscribe model
│   └── utils/                  # shared helpers (Spark session, IO, logging)
├── jobs/                       # Databricks Workflows / job definitions
├── tests/                      # Unit tests
└── docs/                       # Handoff notes, architecture, data dictionary
```

## Getting started (local / PyCharm)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # values are pre-filled; adjust if needed
```

## Workflow

1. Write/validate extraction SQL in `sql/redshift/exports/goodrx_dbt/`, run in DataGrip against Redshift.
2. Export results to CSV into `data/exports/` (gitignored).
3. Upload CSVs to the Databricks Volume at `/Volumes/general_scratch_catalog/general_scratch/checkpoints/jiny/ed_subs_raw_uploads/`.
4. Run `notebooks/00_ingestion/load_bronze.py` in Databricks → writes `ed_bronze_*` Delta tables.
5. Build `ed_silver_*` → `ed_gold_*` using `src/ed_subs/transform`.
6. EDA in `notebooks/04_eda`; modeling in `notebooks/05_modeling`.

See [`docs/handoff.md`](docs/handoff.md) for full project context and [`docs/data_dictionary.md`](docs/data_dictionary.md) for column-level documentation.
