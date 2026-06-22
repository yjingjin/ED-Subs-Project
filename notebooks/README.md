# notebooks/

Databricks notebooks, ordered by pipeline stage. These are synced into Databricks via
GitHub Repos / Git folders. Keep heavy/reusable logic in `src/ed_subs/` and import it here so
the same code is testable locally in PyCharm.

| Folder | Stage | Phase |
| --- | --- | --- |
| `00_ingestion/` | CSV uploads → `ed_bronze` | 1 |
| `01_bronze/` | bronze inspection / validation | 1 |
| `02_silver/` | bronze → `ed_silver` (clean, type, dedupe) | 1 |
| `03_gold/` | silver → `ed_gold` (features + churn label) | 1 |
| `04_eda/` | exploratory analysis | 2 |
| `05_modeling/` | churn / subscribe model (MLflow) | 3 |
