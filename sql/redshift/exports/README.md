# Redshift extraction SQL

These queries are run in **DataGrip against Redshift** (read-only source of truth) to extract
the tables needed for the ED analysis. Results are exported to CSV and uploaded to the
Databricks scratch catalog, then loaded into `ed_bronze`.

## Conventions

- One `.sql` file per source table, grouped by schema folder (`goodrx_dbt/`, `goodrx_raw/`, `edr/`,
  `claim/`, …).
- **ED-filter at export.** Each query should already restrict to ED-relevant rows so bronze is
  ED-scoped and uploads stay small.
- Keep queries idempotent and readable; document any non-obvious filter/join.
- **Start small** — export a sample or the minimal columns first, validate, then expand.

## Naming

```
sql/redshift/exports/<schema>/<table_pull_date>.sql   ->   CSV   ->   <scratch>.ed_bronze.<table>
```
