# GoodRx ED Subscription Analysis — Project Handoff

## Project goal

Analyze GoodRx ED (erectile dysfunction) subscriptions. Three phases:

1. Data transformation / ETL
2. EDA
3. Churn / subscribe prediction model

Collaborating with at least one other person via GitHub.

## Tools & roles

- **Redshift** — source of truth for data (read-only).
- **DataGrip** — explore Redshift, write/validate extraction SQL, export CSVs.
- **Databricks** — compute + storage (Delta medallion, MLflow, jobs). Holds different data than Redshift.
- **PyCharm** — local Python dev.
- **GitHub** — version control + PRs; synced into Databricks via Repos / Git folders.
- **Cursor agent** — writes code directly in the local repo.

## Key decisions & findings

- **No live Redshift → Databricks connection.** A `spark.read.format("redshift")` attempt failed
  with `SocketTimeoutException` (network timeout, not auth). Data engineer (Nora) confirmed
  Databricks and Redshift hold different data, and the needed schemas (`goodrx`, `goodrx_raw`,
  `edr`, `claim`, …) live only in Redshift. **Do not pursue the Spark Redshift connector.**
- **Sanctioned ingestion path:** export needed tables from Redshift via DataGrip → CSV → upload
  to the Databricks scratch catalog → build from there. The required tables are not among the
  Databricks copies. **Advice: start small.**
- **Medallion architecture in the scratch catalog** (cannot create a top-level catalog, so use
  schemas inside the scratch catalog you can write to):
  - `<scratch>.ed_bronze.*` — raw CSV uploads, 1:1 with source tables (ED-filtered at export).
  - `<scratch>.ed_silver.*` — cleaned, typed, deduped, conformed keys.
  - `<scratch>.ed_gold.*` — model-ready feature tables + churn label.

## Open items (to be provided later)

- Model structure (Phase 3).
- Churn / key definitions.
- Exact tables needed per source schema.
