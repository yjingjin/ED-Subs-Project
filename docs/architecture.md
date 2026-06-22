# Architecture

## Data flow

```
┌─────────────┐   DataGrip      ┌──────────────┐   upload    ┌────────────────────────────┐
│  Redshift   │  extraction SQL │  CSV exports │  to scratch │   Databricks scratch catalog │
│ (read-only) ├────────────────►│ data/exports ├────────────►│                              │
│  goodrx     │   ED-filtered   │ (gitignored) │             │  ed_bronze ─► ed_silver ─►   │
│  goodrx_raw │                 └──────────────┘             │  ed_gold ─► model (MLflow)   │
│  edr, claim │                                              └────────────────────────────┘
└─────────────┘
```

## Medallion layers

| Layer | Schema | Contents | Code |
| --- | --- | --- | --- |
| Bronze | `ed_bronze` | Raw CSV uploads, 1:1 with source (ED-filtered at export), as strings + ingest metadata | `src/ed_subs/ingestion` |
| Silver | `ed_silver` | Cleaned, typed, deduped, conformed keys | `src/ed_subs/transform/silver.py` |
| Gold | `ed_gold` | Model-ready feature tables + churn label | `src/ed_subs/transform/gold.py`, `src/ed_subs/features` |

## Why no direct connector

`spark.read.format("redshift")` timed out (`SocketTimeoutException`) and the two systems hold
different data. The required schemas exist only in Redshift, so the CSV export path is the
sanctioned bridge. See `docs/handoff.md`.

## Local ↔ Databricks parity

Reusable logic lives in `src/ed_subs/` (importable in both PyCharm and Databricks notebooks).
`utils/spark.get_spark()` returns the active Databricks session or builds a local one, so the
same transforms run and can be unit-tested in either environment.
