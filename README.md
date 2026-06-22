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
Databricks scratch catalog (compute + storage)
   ├── <scratch>.ed_bronze.*   raw CSV uploads, 1:1 with source (ED-filtered at export)
   ├── <scratch>.ed_silver.*   cleaned, typed, deduped, conformed keys
   └── <scratch>.ed_gold.*     model-ready feature tables + churn label
```

> **No live Redshift → Databricks connection.** The Spark Redshift connector is not viable
> (network `SocketTimeoutException`, and the two systems hold different data). The required
> schemas (`goodrx`, `goodrx_raw`, `edr`, `claim`, …) live only in Redshift. The sanctioned path
> is: export from Redshift via DataGrip → CSV → upload to the Databricks scratch catalog → build
> from there. **Start small.**

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
├── conf/                  # Configuration (catalog/schema names, paths)
├── sql/redshift/exports/  # Extraction SQL per source schema/table (run in DataGrip)
├── data/exports/          # Local CSV exports from DataGrip (gitignored)
├── notebooks/             # Databricks notebooks, ordered by pipeline stage
│   ├── 00_ingestion/      # CSV → bronze
│   ├── 01_bronze/
│   ├── 02_silver/
│   ├── 03_gold/
│   ├── 04_eda/            # Phase 2
│   └── 05_modeling/       # Phase 3
├── src/ed_subs/           # Reusable Python package (importable in notebooks & PyCharm)
│   ├── ingestion/         # Phase 1: CSV upload → bronze
│   ├── transform/         # bronze → silver → gold
│   ├── features/          # feature engineering + churn label
│   ├── eda/               # Phase 2 helpers
│   ├── models/            # Phase 3 churn/subscribe model
│   └── utils/             # shared helpers (Spark session, IO, logging)
├── jobs/                  # Databricks Workflows / job definitions
├── tests/                 # Unit tests
└── docs/                  # Handoff notes, architecture, data dictionary
```

## Getting started (local / PyCharm)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then fill in your values
```

## Workflow

1. Write/validate extraction SQL in `sql/redshift/exports/`, run it in DataGrip against Redshift.
2. Export results to CSV (into `data/exports/`, which is gitignored).
3. Upload CSVs to the Databricks scratch catalog and load into `ed_bronze` (see `notebooks/00_ingestion`).
4. Build `ed_silver` → `ed_gold` using `src/ed_subs/transform`.
5. EDA in `notebooks/04_eda`; modeling in `notebooks/05_modeling`.

See [`docs/handoff.md`](docs/handoff.md) for full project context.
